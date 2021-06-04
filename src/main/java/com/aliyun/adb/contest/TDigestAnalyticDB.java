package com.aliyun.adb.contest;

import com.aliyun.adb.contest.spi.AnalyticDB;
import com.aliyun.adb.contest.t_digest.MergingDigest;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.HashMap;
import java.util.Map;

public class TDigestAnalyticDB implements AnalyticDB {

    private Map<String, MergingDigest> tdmap = new HashMap<>();
    /**
     *
     * The implementation must contain a public no-argument constructor.
     *
     */
    public TDigestAnalyticDB() {
    }

    @Override
    public void load(String tpchDataFileDir, String workspaceDir) throws Exception {
        File dir = new File(tpchDataFileDir);

        for (File dataFile : dir.listFiles()) {
            System.out.println("Start loading table " + dataFile.getName());

//            // You can write data to workspaceDir
//            File yourDataFile = new File(workspaceDir, dataFile.getName());
//            yourDataFile.createNewFile();

            loadInMemroy(dataFile);
        }
    }

    @Override
    public String quantile(String table, String column, double percentile) throws Exception {
        MergingDigest mergingDigest = tdmap.get(tableColumnKey(table, column));

        if (mergingDigest == null) {
            throw new IllegalArgumentException();
        }
        String ans = String.valueOf((long) mergingDigest.quantile(percentile));

//        System.out.println("Query:" + table + ", " + column + ", " + percentile + " Answer:" + ans);

        return ans;
    }

    private void loadInMemroy(File dataFile) throws Exception {
        BufferedReader reader = new BufferedReader(new FileReader(dataFile));
        String table = dataFile.getName();
        String[] columns = reader.readLine().split(",");

        for (String column : columns) {
            tdmap.put(tableColumnKey(table, column), new MergingDigest(10000));
        }

        String rawRow;
        while ((rawRow = reader.readLine()) != null) {
            String[] row = rawRow.split(",");

            for (int i = 0; i < columns.length; i++) {
                tdmap.get(tableColumnKey(table, columns[i])).add(Double.parseDouble(row[i]), 1);
            }
        }

        tdmap.forEach((tableColumn, values) -> {
            System.out.println("Finish loading column " + tableColumn);
        });

        System.out.println("freeMemory: " + Runtime.getRuntime().freeMemory()/1024/1024 + " M");
        System.out.println("totalMemory: " + Runtime.getRuntime().totalMemory()/1024/1024 + " M");
        System.out.println("maxMemory: " + Runtime.getRuntime().maxMemory()/1024/1024 + " M");
    }

    private String tableColumnKey(String table, String column) {
        return (table + "." + column).toLowerCase();
    }


}
