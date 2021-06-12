package com.aliyun.adb.contest.partition;

/**
 * determine how to hash the key
 */
public interface Partitionable {
    int getPartition(byte[] key);
}
