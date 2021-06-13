package com.aliyun.adb.contest;

import org.junit.Test;

import java.nio.ByteBuffer;

public class TestUtils {
    @Test
    public void testLong2Bytes() {
        long startTime = System.currentTimeMillis();
        long test = 6623985850280233638L;
        int time = (int) Math.pow(10, 9);
        for (int i = 0; i < time; i++) {
            long2bytes(test);
        }
        long endTime = System.currentTimeMillis();
        double spendTime = (endTime - startTime) / 1000.0;
        System.out.println("Total Time: " + spendTime + " sec");
    }

    private byte[] long2bytes(long values) {
        int offset = 64 - 8;
        byte byteOne = (byte) ((values >> offset) & 0xff);
        return new byte[]{byteOne};
    }

    private byte[] longToBytes(long x) {
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.putLong(x);
        return buffer.array();
    }

    /**
     * testString2Long
     *
     * time = (int) (6 * Math.pow(10, 8))
     *
     * newLong
     * Total Time: 32.301 sec
     *
     * longValueOf
     * Total Time: 46.782 sec
     *
     * parseLong
     * Total Time: 34.383 sec
     *
     */
    @Test
    public void testString2Long() {
        long startTime = System.currentTimeMillis();
        String test = "6623985850280233638";
        int time = (int) (6 * Math.pow(10, 8));
        for (int i = 0; i < time; i++) {
            parseLong(test);
//            longValueOf(test);
//            newLong(test);
        }
        long endTime = System.currentTimeMillis();
        double spendTime = (endTime - startTime) / 1000.0;
        System.out.println("Total Time: " + spendTime + " sec");
    }

    private void parseLong(String num) {
        long value = Long.parseLong(num);
    }

    private void longValueOf(String num) {
        long value = Long.valueOf(num);
    }

    private void newLong(String num) {
        long value = new Long(num);
    }
}
