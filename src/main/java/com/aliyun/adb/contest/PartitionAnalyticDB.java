package com.aliyun.adb.contest;

import com.aliyun.adb.contest.spi.AnalyticDB;

public class PartitionAnalyticDB implements AnalyticDB {
    /**
     *
     * The implementation must contain a public no-argument constructor.
     *
     */
    public PartitionAnalyticDB() {

    }

    @Override
    public void load(String tpchDataFileDir, String workspaceDir) throws Exception {

    }

    @Override
    public String quantile(String table, String column, double percentile) throws Exception {
        return null;
    }
}
