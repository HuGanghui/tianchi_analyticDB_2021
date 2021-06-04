package com.aliyun.adb.contest;

import com.aliyun.adb.contest.spi.AnalyticDB;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class TestSimpleAnalyticDB {
    private final String SPLITE = "-";
    private String test_data_dir = "src/test/resources/com.aliyun.adb.contest/test_data/";
    private String test_result_path = "src/test/resources/com.aliyun.adb.contest/result/results";

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

    private void commonTest(Map<String, String> resultMap, AnalyticDB db) throws Exception {
        for (String key : resultMap.keySet()) {
            String[] columns = key.split(SPLITE);
            String result = db.quantile(columns[0], columns[1], Double.parseDouble(columns[2]));
//            assertEquals(estimate[estimate.length - 1], resultMap.get(key));
            System.out.println("estimate: " + result + " and " + "real: " + resultMap.get(key));
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

        System.out.println();
        return resultMap;
    }
}
