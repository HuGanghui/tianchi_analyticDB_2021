package com.aliyun.adb.contest;

import com.aliyun.adb.contest.spi.AnalyticDB;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;

import static com.aliyun.adb.contest.common.Utils.bytesToLong;
import static com.aliyun.adb.contest.common.Utils.longToBytes;

public class MergeSortAnalyticDB implements AnalyticDB {

    private Map<String, List<File>> fileMap = new HashMap<>();
    private Map<String, File> sortedFileMap = new HashMap<>();
    // 每个文件可保存的最大行数
    private final int MAX_FILE_CAP = (int) (0.375 * Math.pow(10, 8));
    private final int TOTAL_LINE = (int) (3 * Math.pow(10, 8));
//    private final int TOTAL_LINE = (int) (10000);
//    private final int MAX_FILE_CAP = (int) (2500);

    private final int Buffer_CAP = 4096;
    /**
     *
     * The implementation must contain a public no-argument constructor.
     *
     */
    public MergeSortAnalyticDB() {
    }

    @Override
    public void load(String tpchDataFileDir, String workspaceDir) throws Exception {
        File dir = new File(tpchDataFileDir);

        Date date = new Date();
        System.out.println("date:" + date);
        loadInMemroy();

        for (File dataFile : dir.listFiles()) {
            System.out.println("Start loading table " + dataFile.getName());
            System.out.println("date:" + date);
            loadInMemroy();

//            // You can write data to workspaceDir
            saveToDisk(workspaceDir, dataFile);


//            loadInMemroy(dataFile);
        }

        for (String key : fileMap.keySet()) {
            sort(key, workspaceDir);
        }
    }

    @Override
    public String quantile(String table, String column, double percentile) throws Exception {
        String tableColumn = tableColumnKey(table, column);
        RandomAccessFile raf = new RandomAccessFile(sortedFileMap.get(tableColumn), "r");
        long offset = (long) (TOTAL_LINE * percentile - 1) * 8;
        raf.seek(offset);
        byte[] bbuf = new byte[8];
        raf.read(bbuf);
        raf.close();
        return String.valueOf(bytesToLong(bbuf));
    }

    private void saveToDisk(String workspaceDir, File dataFile) throws Exception {
        BufferedReader reader = new BufferedReader(new FileReader(dataFile));
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
        FileOutputStream[] outArray = new FileOutputStream[columnLength];
        for (int i = 0; i < columnLength; i++) {
            String tableColumn = tableColumnKey(table, columns[i]);
            File file = new File(workspaceDir, tableColumn + "_" + file_index);
            fileMap.get(tableColumn).add(file);
            outArray[i] = new FileOutputStream(file);
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
                    saveToDisk(valuesToSort[i], outArray[i], true);
                    String tableColumn = tableColumnKey(table, columns[i]);
                    File file = new File(workspaceDir, tableColumn + "_" + file_index);
                    fileMap.get(tableColumn).add(file);
                    outArray[i] = new FileOutputStream(file);
                }
            }
        }

        for (int i = 0; i < columnLength; i++) {
            if (valuesToSortIndex == 0) {
                continue;
            }
            long[] leftToSort = new long[valuesToSortIndex];
            System.arraycopy(valuesToSort[i], 0, leftToSort, 0, valuesToSortIndex);
            saveToDisk(leftToSort, outArray[i], true);
        }
        reader.close();
    }

    private void sort(String key, String workspaceDir) throws Exception {
        List<File> fileList = fileMap.get(key);
        int n = fileList.size();
        FileInputStream[] fileIns = new FileInputStream[n];
        PriorityQueue<Pair> pq = new PriorityQueue<Pair>((o1, o2) -> {
            if (o1.value <= o2.value) {
                return -1;
            } else {
                return 1;
            }
        });
        for (int i = 0; i < n; i++) {
            fileIns[i] = new FileInputStream(fileList.get(i));
            Long temp = getSingleNumber(fileIns[i]);
            if (temp != null) {
                pq.add(new Pair(i, temp));
            }
        }
        long[] buf = new long[MAX_FILE_CAP];
        List<Long> forTest = new ArrayList<>();
        File file = new File(workspaceDir, key + "_" + "sorted");
        sortedFileMap.put(key, file);
        FileOutputStream out = new FileOutputStream(file);
        int buf_index = 0;
        while (!pq.isEmpty()) {
            Pair pair = pq.poll();
            buf[buf_index++] = pair.value;
            forTest.add(pair.value);
            if (buf_index == MAX_FILE_CAP) {
                saveToDisk(buf, out, false);
                buf_index = 0;
            }
            Long temp = getSingleNumber(fileIns[pair.insIndex]);
            if (temp != null) {
                pq.offer(new Pair(pair.insIndex, temp));
            }
        }
        if (buf_index != 0) {
            long[] leftToSort = new long[buf_index];
            System.arraycopy(buf, 0, leftToSort, 0, buf_index);
            saveToDisk(leftToSort, out, true);
        }
    }

    private void saveToDisk(long[] valuesToSort, FileOutputStream out, boolean nowClose) throws Exception {
        Arrays.sort(valuesToSort);
        byte[] bbuf = new byte[Buffer_CAP];
        int bbuf_position = 0;
        for (long l : valuesToSort) {
            byte[] temp = longToBytes(l);
            System.arraycopy(temp, 0, bbuf, bbuf_position, temp.length);
            bbuf_position += temp.length;
            if (bbuf_position == Buffer_CAP) {
                out.write(bbuf);
                bbuf_position = 0;
            }
        }
        if (bbuf_position != 0) {
            byte[] temp = new byte[bbuf_position];
            System.arraycopy(bbuf, 0, temp, 0, bbuf_position);
            out.write(temp);
        }
        if (nowClose) {
            out.flush();
            out.close();
        }
    }

    private Long getSingleNumber(FileInputStream ins) throws Exception {
        byte[] bbuf = new byte[8];
        int hasRead = 0;
        if ((hasRead = ins.read(bbuf)) > 0) {
            return bytesToLong(bbuf);
        } else {
            return null;
        }
    }

    private class Pair {
        public int insIndex;
        public Long value;
        public Pair(int insIndex, Long value) {
            this.insIndex = insIndex;
            this.value = value;
        }
    }

    private void loadInMemroy() {
        System.out.println("freeMemory: " + Runtime.getRuntime().freeMemory()/1024/1024 + " M");
        System.out.println("totalMemory: " + Runtime.getRuntime().totalMemory()/1024/1024 + " M");
        System.out.println("maxMemory: " + Runtime.getRuntime().maxMemory()/1024/1024 + " M");
    }

    private String tableColumnKey(String table, String column) {
        return (table + "_" + column).toLowerCase();
    }
}
