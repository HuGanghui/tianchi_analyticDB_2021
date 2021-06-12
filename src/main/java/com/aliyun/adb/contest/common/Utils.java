package com.aliyun.adb.contest.common;

import java.nio.ByteBuffer;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Utils {
//    public static final ExecutorService pool = Executors.newFixedThreadPool(8);

    public static byte[] longToBytes(long x) {
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.putLong(x);
        return buffer.array();
    }

    public static long bytesToLong(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.put(bytes);
        buffer.flip();//need flip
        return buffer.getLong();
    }

    public static void printTimeAndMemory(String funcName, String info, long startTime, long endTime) {
        System.out.println("=====================");
        System.out.println(funcName + ": " + info);
        double spendTime = (endTime - startTime) / 1000.0;
        System.out.println("Total Time: " + spendTime + " sec");
        loadInMemroy();
        System.out.println("=====================");
    }

    private static void loadInMemroy() {
        System.out.println("freeMemory: " + Runtime.getRuntime().freeMemory()/1024/1024 + " M");
        System.out.println("totalMemory: " + Runtime.getRuntime().totalMemory()/1024/1024 + " M");
        System.out.println("maxMemory: " + Runtime.getRuntime().maxMemory()/1024/1024 + " M");
    }
 }
