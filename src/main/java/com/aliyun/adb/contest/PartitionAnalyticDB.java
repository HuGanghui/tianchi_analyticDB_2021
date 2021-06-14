package com.aliyun.adb.contest;

import com.aliyun.adb.contest.common.Constant;
import com.aliyun.adb.contest.data.DataLog;
import com.aliyun.adb.contest.partition.HighEightPartitioner;
import com.aliyun.adb.contest.partition.HighTenPartitioner;
import com.aliyun.adb.contest.partition.Partitionable;
import com.aliyun.adb.contest.spi.AnalyticDB;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.*;

import static com.aliyun.adb.contest.common.Utils.long2bytes;
import static com.aliyun.adb.contest.common.Utils.longToBytes;
import static com.aliyun.adb.contest.common.Utils.printTimeAndMemory;

public class PartitionAnalyticDB implements AnalyticDB {

    private Map<String, DataLog[]> dataLogMap = new HashMap<>();
    private Map<String, int[]> dataLogSizePrefixSumMap = new HashMap<>();
    private volatile Partitionable partitionable;

    // partition num
    private final int partitionNum = 1 << 8;

    // 每个文件可保存的最大行数
    private final int TOTAL_LINE = (int) (3 * Math.pow(10, 8));
//    private final int TOTAL_LINE = (int) (10000);
    /**
     *
     * The implementation must contain a public no-argument constructor.
     *
     */
    public PartitionAnalyticDB() {
        partitionable = new HighEightPartitioner();
    }

    @Override
    public void load(String tpchDataFileDir, String workspaceDir) throws Exception {
        Date date = new Date();
        System.out.println("date:" + date);
        long startTime = System.currentTimeMillis();

        File dir = new File(tpchDataFileDir);

        for (File dataFile : dir.listFiles()) {
            saveToDisk(workspaceDir, dataFile);
        }
        printTimeAndMemory("load", "load ended", startTime, System.currentTimeMillis());
    }

    private void saveToDisk(String workspaceDir, File dataFile) throws Exception {
        long startTime = System.currentTimeMillis();

        BufferedReader reader = new BufferedReader(new FileReader(dataFile), Constant.Buffer_CAP);
        String[] columns = reader.readLine().split(",");
        reader.close();
        final int columnLength = columns.length;
        String[] tableColumns = new String[columnLength];
        String table = dataFile.getName();
        for (int i = 0; i < columnLength; i++) {
            tableColumns[i] = tableColumnKey(table, columns[i]);
            DataLog[] dataLogs = new DataLog[partitionNum];
            int[] dataLogSizePrefixSum = new int[partitionNum];
            dataLogMap.put(tableColumns[i], dataLogs);
            for (int j = 0; j < partitionNum; j++) {
                dataLogs[j] = new DataLog();
                dataLogs[j].init(workspaceDir, tableColumns[i], j);
            }
            dataLogSizePrefixSumMap.put(tableColumns[i], dataLogSizePrefixSum);
        }
        RandomAccessFile raf = new RandomAccessFile(dataFile, "r");
        FileChannel readFileChannel = raf.getChannel();
        ByteBuffer byteBuffer = ByteBuffer.allocate(Constant.Buffer_CAP);

        long startWriteTime = System.currentTimeMillis();

        int index;
        long l;
        byte cur;
        final byte ten = 10;
        final byte ff = 44;
        while (readFileChannel.read(byteBuffer) != -1) {
            byteBuffer.flip();
            byte[] bufferBytes = byteBuffer.array();
            int n = byteBuffer.limit();
            int back = 0;
            if (bufferBytes[n-1] != ten) {
                while (bufferBytes[n-1] != ten) {
                    n--;
                    back++;
                }
                raf.seek(raf.getFilePointer() - back);
            }
            int byteStartIndex = 0;
            for (int i = 0; i < n; i++) {
                cur = bufferBytes[i];
                if (cur == ten || cur == ff) {
                    try {
                        l = convertToLong(bufferBytes, byteStartIndex, i);
                        byteStartIndex = i+1;
//                        byte partition = (byte) ((l >> 56) & 0xff);
//                        partition = partitionable.getPartition(long2bytes(l));
//                        index = (cur == ff ? 0 : 1);
//                        final DataLog dataLog = dataLogMap.get(tableColumns[index])[partition];
//                        dataLog.write(l);
                    } catch (NumberFormatException e) {
//                        String temp = new String(bufferBytes, byteStartIndex, i-byteStartIndex, StandardCharsets.US_ASCII);
                        byteStartIndex = i+1;
//                        System.out.println(temp);
                    }
                }
            }
            byteBuffer.clear();
        }
        printTimeAndMemory("saveToDisk2", "no write + no dataLog + no index + no partition",
                startWriteTime, System.currentTimeMillis());

        for (int i = 0; i < columnLength; i++) {
            String key = tableColumns[i];
            dataLogSizePrefixSumMap.get(key)[0] =
                    dataLogMap.get(tableColumns[i])[0].destroy();
            for (int j = 1; j < partitionNum; j++) {
                dataLogSizePrefixSumMap.get(key)[j] = dataLogSizePrefixSumMap.get(key)[j-1] +
                        dataLogMap.get(tableColumns[i])[j].destroy();
            }
        }
        readFileChannel.close();
        printTimeAndMemory("saveToDisk2", "saveToDisk ended", startTime, System.currentTimeMillis());
    }

    private long convertToLong(byte[] bytes, int startIndex, int endIndex) {
        // ASCII convert to long
        if (bytes[startIndex] < 48 || bytes[startIndex] > 57) {
            throw new NumberFormatException();
        }
        long result = 0;
        for (int i = startIndex; i < endIndex; i++) {
            result = result * 10 + (bytes[i] - 48);
        }
        return result;
    }

    @Override
    public String quantile(String table, String column, double percentile) throws Exception {
        long startTime = System.currentTimeMillis();

        String tableColumn = tableColumnKey(table, column);
        int[] prefixSum = dataLogSizePrefixSumMap.get(tableColumn);
        int targetPercentile = (int) (TOTAL_LINE * percentile);
        int dataLogIndex = findFirstLargerNumIndex(prefixSum, targetPercentile);
        DataLog dataLog = dataLogMap.get(tableColumn)[dataLogIndex];
        long[] values = dataLog.read();
        Arrays.sort(values);
        printTimeAndMemory("quantile", "quantile ended", startTime, System.currentTimeMillis());
        String result = null;
        if (dataLogIndex == 0) {
            result = String.valueOf(values[targetPercentile] - 1);
        } else {
            result = String.valueOf(values[(targetPercentile - prefixSum[dataLogIndex-1]) - 1]);
        }
        return result;
    }

    private int findFirstLargerNumIndex(int[] array, int num) {
        int n = array.length;
        int left = 0;
        int right = n - 1;
        int result = -1;
        while (left <= right) {
            int mid = left + (right - left) / 2;
            if (array[mid] >= num) {
                if (mid == 0 || array[mid-1] < num) {
                    result = mid;
                    break;
                } else {
                    right = mid - 1;
                }
            } else {
                left = mid + 1;
            }
        }
        return result;
    }

    private String tableColumnKey(String table, String column) {
        return (table + "_" + column).toLowerCase();
    }


    /**
     * Deprecated
     *
     * 传统的使用BufferReader来读取的方法
     * 1. 速度比不上FileChannel
     * 2. 无法方便的进一步优化从字符串到Long
     * @param workspaceDir
     * @param dataFile
     * @throws Exception
     */
//    private void saveToDisk(String workspaceDir, File dataFile) throws Exception {
//        long startTime = System.currentTimeMillis();
//
//        BufferedReader reader = new BufferedReader(new FileReader(dataFile), Constant.Buffer_CAP);
//        String table = dataFile.getName();
//        String[] columns = reader.readLine().split(",");
//        final int columnLength = columns.length;
//        String[] tableColumns = new String[columnLength];
//        for (int i = 0; i < columnLength; i++) {
//            tableColumns[i] = tableColumnKey(table, columns[i]);
//            DataLog[] dataLogs = new DataLog[partitionNum];
//            int[] dataLogSizePrefixSum = new int[partitionNum];
//            dataLogMap.put(tableColumns[i], dataLogs);
//            for (int j = 0; j < partitionNum; j++) {
//                dataLogs[j] = new DataLog();
//                dataLogs[j].init(workspaceDir, tableColumns[i], j);
//            }
//            dataLogSizePrefixSumMap.put(tableColumns[i], dataLogSizePrefixSum);
//        }
//
//        long startWriteTime = System.currentTimeMillis();
//        String rawRow;
//        Long l;
//        while ((rawRow = reader.readLine()) != null) {
//            String[] row = rawRow.split(",");
//            for (int i = 0; i < columnLength; i++) {
//                l = new Long(row[i]);
//                int partition = partitionable.getPartition(long2bytes(l));
//                final DataLog dataLog = dataLogMap.get(tableColumns[i])[partition];
//                dataLog.write(l);
//            }
//        }
//        printTimeAndMemory("saveToDisk", "write into partitionDataLog",
//                startWriteTime, System.currentTimeMillis());
//
//        for (int i = 0; i < columnLength; i++) {
//            String key = tableColumns[i];
//            dataLogSizePrefixSumMap.get(key)[0] =
//                    dataLogMap.get(tableColumns[i])[0].destroy();
//            for (int j = 1; j < partitionNum; j++) {
//                dataLogSizePrefixSumMap.get(key)[j] = dataLogSizePrefixSumMap.get(key)[j-1] +
//                        dataLogMap.get(tableColumns[i])[j].destroy();
//            }
//        }
//        reader.close();
//        printTimeAndMemory("saveToDisk", "saveToDisk ended", startTime, System.currentTimeMillis());
//    }
}
