package com.aliyun.adb.contest;

import org.junit.Test;

import java.nio.ByteBuffer;

public class TestUtils {
    @Test
    public void testlong2bytes() {
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

    private static byte[] long2bytes(long values) {
        int offset = 64 - 8;
        byte byteOne = (byte) ((values >> offset) & 0xff);
        return new byte[]{byteOne};
    }

    private byte[] longToBytes(long x) {
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.putLong(x);
        return buffer.array();
    }
}
