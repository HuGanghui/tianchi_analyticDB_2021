package com.aliyun.adb.contest;

import com.aliyun.adb.contest.spi.AnalyticDB;
import org.junit.Test;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class TestSimpleAnalyticDB {
    private final String SPLITE = "-";
    private String test_data_dir = "src/test/resources/com.aliyun.adb.contest/test_data/";
    private String test_result_path = "src/test/resources/com.aliyun.adb.contest/result/results";

    private String test_split_file_dir = "./tmp";

    @Test
    public void testsaveToDisk() throws Exception {
        MergeSortAnalyticDB db = new MergeSortAnalyticDB();
        db.load(test_data_dir, "./tmp");
    }

    @Test
    public void testSplitFileRight() throws Exception {
        File dir = new File(test_split_file_dir);
//        String test = "lineitem_l_orderkey";
        String test = "lineitem_l_partkey";
        int total = 0;
        for (File dataFile : dir.listFiles()) {
            if (dataFile.getName().startsWith(test)) {
                FileInputStream fileInputStream = new FileInputStream(dataFile);
                total += testNum(fileInputStream);
            }
        }
        assertEquals(10000, total);
    }

    private int testNum(FileInputStream ins) throws Exception {
        byte[] bbuf2 = new byte[8];
        int hasRead = 0;
        int num = 0;
        while ((hasRead = ins.read(bbuf2)) > 0) {
            num++;
        }
        return num;
    }

    private byte[] longToBytes(long x) {
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.putLong(x);
        return buffer.array();
    }

    private long bytesToLong(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.put(bytes);
        buffer.flip();//need flip
        return buffer.getLong();
    }

    private String tableColumnKey(String table, String column) {
        return (table + "." + column).toLowerCase();
    }

    @Test
    public void testSimpleAnalyticDB() {
        try {
            SimpleAnalyticDB analyticDB = new SimpleAnalyticDB();
            analyticDB.load(test_data_dir, "./");
            Map<String, String> resultMap = getResult(test_result_path);
            commonTest(resultMap, analyticDB);

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    @Test
    public void testTDigestAnalyticDB() {
        try {
            TDigestAnalyticDB tDigestAnalyticDB = new TDigestAnalyticDB();
            tDigestAnalyticDB.load(test_data_dir, "./");
            Map<String, String> resultMap = getResult(test_result_path);
            commonTest(resultMap, tDigestAnalyticDB);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testMergeSortAnalyticDB() {
        try {
            MergeSortAnalyticDB mergeSortAnalyticDB = new MergeSortAnalyticDB();
            mergeSortAnalyticDB.load(test_data_dir, "./tmp");
            Map<String, String> resultMap = getResult(test_result_path);
            commonTest(resultMap, mergeSortAnalyticDB);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testPartitionAnalyticDB() {
        try {
            PartitionAnalyticDB partitionAnalyticDB = new PartitionAnalyticDB();
            partitionAnalyticDB.load(test_data_dir, "./tmp");
            Map<String, String> resultMap = getResult(test_result_path);
            commonTest(resultMap, partitionAnalyticDB);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void commonTest(Map<String, String> resultMap, AnalyticDB db) throws Exception {
        for (String key : resultMap.keySet()) {
            String[] columns = key.split(SPLITE);
            String result = db.quantile(columns[0], columns[1], Double.parseDouble(columns[2]));
            assertEquals(resultMap.get(key), result);
            System.out.println("estimate: " + result + " and " + "real: " + resultMap.get(key));
        }
    }

    @Test
    public void testQuery() throws Exception {
        Map<String, String> resultMap = getResult(test_result_path);
        RandomAccessFile randomAccessFileOne = new RandomAccessFile("/Users/hgh/Documents/GitHub/tianchi_analyticDB_2021/tmp/lineitem_l_orderkey_sorted", "r");
        RandomAccessFile randomAccessFileTwo = new RandomAccessFile("/Users/hgh/Documents/GitHub/tianchi_analyticDB_2021/tmp/lineitem_l_partkey_sorted", "r");
        for (String key : resultMap.keySet()) {
            String[] columns = key.split(SPLITE);
            String tableColumn = columns[0] + "_" + columns[1];
            if (tableColumn.toLowerCase().startsWith("lineitem_l_orderkey")) {
                randomAccessFileOne.seek((int) (10000 * Double.parseDouble(columns[2]) - 1) * 8);
                byte[] bbuf = new byte[8];
                randomAccessFileOne.read(bbuf);
                assertEquals(Long.parseLong(resultMap.get(key)), bytesToLong(bbuf));
            }
            if (tableColumn.toLowerCase().startsWith("lineitem_l_partkey")) {
                randomAccessFileTwo.seek((int) (10000 * Double.parseDouble(columns[2]) - 1) * 8);
                byte[] bbuf = new byte[8];
                randomAccessFileTwo.read(bbuf);
                assertEquals(Long.parseLong(resultMap.get(key)), bytesToLong(bbuf));
            }
        }
    }

    private Map<String, String> getResult(String test_result_path) throws Exception{
        File file = new File(test_result_path);
        BufferedReader reader = new BufferedReader(new FileReader(file));
        Map<String, String> resultMap = new HashMap<>();
        String rawRow;
        while ((rawRow = reader.readLine()) != null) {
            String[] columns = rawRow.split(" ");
            StringBuilder stringBuilder = new StringBuilder();
            for (int i = 0; i < 3; i++) {
                stringBuilder.append(columns[i]);
                stringBuilder.append(SPLITE);
            }
            resultMap.put(stringBuilder.toString(), columns[3]);
        }
        return resultMap;
    }
}
