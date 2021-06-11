package com.aliyun.adb.contest.common;

import java.nio.ByteBuffer;
import java.util.Date;

public class Utils {
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

    public static void printTimeAndMemory(String funcName, String info) {
        System.out.println("=====================");
        System.out.println(funcName + ": " + info);
        Date date = new Date();
        System.out.println("date:" + date);
        loadInMemroy();
        System.out.println("=====================");
    }

    private static void loadInMemroy() {
        System.out.println("freeMemory: " + Runtime.getRuntime().freeMemory()/1024/1024 + " M");
        System.out.println("totalMemory: " + Runtime.getRuntime().totalMemory()/1024/1024 + " M");
        System.out.println("maxMemory: " + Runtime.getRuntime().maxMemory()/1024/1024 + " M");
    }
 }
