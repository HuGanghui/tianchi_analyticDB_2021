package com.aliyun.adb.contest.partition;

/**
 * using high ten bit of the given key to determine which file it hits.
 */
public class HighTenPartitioner implements Partitionable {
    @Override
    public int getPartition(byte[] key) {
        return ((key[0] & 0xff));
    }
}
