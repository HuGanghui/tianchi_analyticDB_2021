package com.aliyun.adb.contest.partition;

/**
 * using high eight bit of the given key to determine which file it hits.
 */
public class HighEightPartitioner implements Partitionable {
    @Override
    public int getPartition(byte[] key) {
        return (key[0] & 0xff);
    }
}
