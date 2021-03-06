package com.aliyun.adb.contest;

import com.aliyun.adb.contest.spi.AnalyticDB;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.LongBuffer;
import java.nio.channels.FileChannel;
import java.util.*;

import static com.aliyun.adb.contest.common.Utils.bytesToLong;
import static com.aliyun.adb.contest.common.Utils.printTimeAndMemory;

public class MergeSortAnalyticDB implements AnalyticDB {

    private Map<String, List<File>> fileMap = new HashMap<>();
    private Map<String, File> sortedFileMap = new HashMap<>();
    // 每个文件可保存的最大行数
    private final int MAX_FILE_CAP = (int) (0.75 * Math.pow(10, 8));
    private final int TOTAL_LINE = (int) (3 * Math.pow(10, 8));
//    private final int TOTAL_LINE = (int) (10000);
//    private final int MAX_FILE_CAP = (int) (2500);

    private final int Buffer_CAP = 4096 * 16;
    /**
     *
     * The implementation must contain a public no-argument constructor.
     *
     */
    public MergeSortAnalyticDB() {
    }

    @Override
    public void load(String tpchDataFileDir, String workspaceDir) throws Exception {
        Date date = new Date();
        System.out.println("date:" + date);
        long startTime = System.currentTimeMillis();

        File dir = new File(tpchDataFileDir);

        for (File dataFile : dir.listFiles()) {
//            // You can write data to workspaceDir
            saveToDisk(workspaceDir, dataFile);

        }

        for (String key : fileMap.keySet()) {
            sort(key, workspaceDir);
        }
        printTimeAndMemory("load", "load ended", startTime, System.currentTimeMillis());
    }

    @Override
    public String quantile(String table, String column, double percentile) throws Exception {
        long startTime = System.currentTimeMillis();
        String tableColumn = tableColumnKey(table, column);
        RandomAccessFile raf = new RandomAccessFile(sortedFileMap.get(tableColumn), "r");
        long offset = (long) (TOTAL_LINE * percentile - 1) * 8;
        raf.seek(offset);
        byte[] bbuf = new byte[8];
        raf.read(bbuf);
        raf.close();
        printTimeAndMemory("quantile", "quantile ended", startTime, System.currentTimeMillis());
        return String.valueOf(bytesToLong(bbuf));
    }

    private void saveToDisk(String workspaceDir, File dataFile) throws Exception {
        long startTime = System.currentTimeMillis();

        BufferedReader reader = new BufferedReader(new FileReader(dataFile), Buffer_CAP);
        String table = dataFile.getName();
        String[] columns = reader.readLine().split(",");
        final int columnLength = columns.length;
        int file_index = 0;
        for (String column : columns) {
            String tableColumn = tableColumnKey(table, column);
            List<File> tempList = new ArrayList<>();
            fileMap.put(tableColumn, tempList);
        }

        long[][] valuesToSort = new long[columnLength][MAX_FILE_CAP];
        int valuesToSortIndex = 0;
        String rawRow;
        FileChannel[] outChannels = new FileChannel[columnLength];
        for (int i = 0; i < columnLength; i++) {
            String tableColumn = tableColumnKey(table, columns[i]);
            File file = new File(workspaceDir, tableColumn + "_" + file_index);
            fileMap.get(tableColumn).add(file);
            outChannels[i] = new FileOutputStream(file).getChannel();
        }

        while ((rawRow = reader.readLine()) != null) {
            String[] row = rawRow.split(",");
            for (int i = 0; i < columnLength; i++) {
                Long l = new Long(row[i]);
                valuesToSort[i][valuesToSortIndex] = l;
            }
            valuesToSortIndex++;

            if (valuesToSortIndex % MAX_FILE_CAP == 0) {
                valuesToSortIndex = 0;
                file_index++;
                for (int i = 0; i < columnLength; i++) {
                    eachSaveToDisk(valuesToSort[i], outChannels[i], true, true);
                    Date date = new Date();
                    System.out.println("date:" + date);
                    String tableColumn = tableColumnKey(table, columns[i]);
                    File file = new File(workspaceDir, tableColumn + "_" + file_index);
                    fileMap.get(tableColumn).add(file);
                    outChannels[i] = new FileOutputStream(file).getChannel();
                }
            }
        }

        for (int i = 0; i < columnLength; i++) {
            if (valuesToSortIndex == 0) {
                continue;
            }
            long[] leftToSort = new long[valuesToSortIndex];
            System.arraycopy(valuesToSort[i], 0, leftToSort, 0, valuesToSortIndex);
            eachSaveToDisk(leftToSort, outChannels[i], true, true);
        }
        reader.close();
        printTimeAndMemory("saveToDisk", "saveTodisk ended", startTime, System.currentTimeMillis());
    }

    private void sort(String key, String workspaceDir) throws Exception {
        long startTime = System.currentTimeMillis();

        List<File> fileList = fileMap.get(key);
        int n = fileList.size();
        FileChannel[] fileChannels = new FileChannel[n];
        ByteBuffer[] byteBuffers = new ByteBuffer[n];
        LongBuffer[] longBuffers = new LongBuffer[n];
        PriorityQueue<Pair> pq = new PriorityQueue<Pair>((o1, o2) -> {
            if (o1.value <= o2.value) {
                return -1;
            } else {
                return 1;
            }
        });
        for (int i = 0; i < n; i++) {
            fileChannels[i] = new FileInputStream(fileList.get(i)).getChannel();
            byteBuffers[i] = ByteBuffer.allocate(Buffer_CAP);
            longBuffers[i] = getNumbers(fileChannels[i], byteBuffers[i]);
            if (longBuffers[i] != null && longBuffers[i].hasRemaining()) {
                pq.add(new Pair(i, longBuffers[i].get()));
            }
        }
        long[] buf = new long[MAX_FILE_CAP*2];
        int buf_index = 0;
//        List<Long> forTest = new ArrayList<>();
        File file = new File(workspaceDir, key + "_" + "sorted");
        sortedFileMap.put(key, file);
        FileChannel outChannel = new FileOutputStream(file).getChannel();
        while (!pq.isEmpty()) {
            Pair pair = pq.poll();
            buf[buf_index++] = pair.value;
//            forTest.add(pair.value);
            if (buf_index == buf.length) {
                eachSaveToDisk(buf, outChannel, false, false);
                buf_index = 0;
            }
            if (longBuffers[pair.insIndex].hasRemaining()) {
                pq.offer(new Pair(pair.insIndex, longBuffers[pair.insIndex].get()));
            } else {
                longBuffers[pair.insIndex] =
                        getNumbers(fileChannels[pair.insIndex], byteBuffers[pair.insIndex]);
                if (longBuffers[pair.insIndex] != null && longBuffers[pair.insIndex].hasRemaining()) {
                    pq.offer(new Pair(pair.insIndex, longBuffers[pair.insIndex].get()));
                }
            }
        }
        if (buf_index != 0) {
            long[] leftToSort = new long[buf_index];
            System.arraycopy(buf, 0, leftToSort, 0, buf_index);
            eachSaveToDisk(leftToSort, outChannel, true, false);
        } else {
            outChannel.close();
        }
        for (int i = 0; i < n; i++) {
            fileChannels[i].close();
        }
        buf = null;
        printTimeAndMemory("sort", "sort ended", startTime, System.currentTimeMillis());

    }

    private void eachSaveToDisk(long[] valuesToSort, FileChannel outChannel, boolean nowClose, boolean needSort) throws Exception {
        if (needSort) {
            long startSortTime = System.currentTimeMillis();
            Arrays.sort(valuesToSort);
            printTimeAndMemory("eachSaveToDisk-Arrays.sort", "Arrays.sort ended",
                    startSortTime, System.currentTimeMillis());
        }
        long startTime = System.currentTimeMillis();
        ByteBuffer bbuf = ByteBuffer.allocate(Buffer_CAP);
        for (long l : valuesToSort) {
            bbuf.putLong(l);
            if (bbuf.position() == Buffer_CAP) {
                bbuf.flip();
                outChannel.write(bbuf);
                bbuf.clear();
            }
        }
        if (bbuf.position() != 0) {
            bbuf.flip();
            outChannel.write(bbuf);
            bbuf.clear();
        }
        if (nowClose) {
            outChannel.close();
        }
        printTimeAndMemory("eachSaveToDisk", "eachSaveToDisk ended", startTime, System.currentTimeMillis());
    }

    private LongBuffer getNumbers(FileChannel fileChannel, ByteBuffer byteBuffer) throws Exception {
        LongBuffer longBuffer = null;
        if (fileChannel.read(byteBuffer) != -1) {
            byteBuffer.flip();
            longBuffer = byteBuffer.asLongBuffer();
            byteBuffer.clear();
        }
        return longBuffer;
    }

    private class Pair {
        public int insIndex;
        public Long value;
        public Pair(int insIndex, Long value) {
            this.insIndex = insIndex;
            this.value = value;
        }
    }

    private String tableColumnKey(String table, String column) {
        return (table + "_" + column).toLowerCase();
    }
}
