package com.aliyun.adb.contest;

import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

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
     * reference: https://howtodoinjava.com/java/string/convert-string-to-long/
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
     * LongDecode
     * Total Time: 48.665 sec
     */
    @Test
    public void testString2Long() {
        long startTime = System.currentTimeMillis();
        String test = "6623985850280233638";
        int time = (int) (6 * Math.pow(10, 8));
        for (int i = 0; i < time; i++) {
//            parseLong(test);
//            longValueOf(test);
//            newLong(test);
            LongDecode(test);
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

    private void LongDecode(String num) {
        long value = Long.decode(num);
    }

    /**
     * testASCII2Long
     *
     * time = (int) (6 * Math.pow(10, 7))
     *
     * bytestoStringtolong
     * Total Time: 6.648 sec
     *
     * convertToLong
     * Total Time: 0.012 sec
     *
     * time = (int) (6 * Math.pow(10, 8))
     *
     * bytestoStringtolong
     * Total Time: 67.126 sec
     *
     * convertToLong
     * Total Time: 0.02 sec
     */
    @Test
    public void testASCII2Long() {
        long startTime = System.currentTimeMillis();
        String test = "6623985850280233638";
        byte[] bytes = test.getBytes();
        int time = (int) (6 * Math.pow(10, 8));
//        int time = 1;
        for (int i = 0; i < time; i++) {
//            bytestoStringtolong(bytes);
            convertToLong(bytes);
        }
        long endTime = System.currentTimeMillis();
        double spendTime = (endTime - startTime) / 1000.0;
        System.out.println("Total Time: " + spendTime + " sec");
    }

    private void bytestoStringtolong(byte[] bytes) {
        String temp = new String(bytes, StandardCharsets.US_ASCII);
        Long l = new Long(temp);
    }

    private void convertToLong(byte[] bytes) {
        int n = bytes.length;
        long result = 0;
        for (int i = 0; i < n; i++) {
            result = result * 10 + (bytes[i] - 48);
        }
    }
}
