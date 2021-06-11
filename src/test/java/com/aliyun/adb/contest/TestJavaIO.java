package com.aliyun.adb.contest;

import org.junit.Test;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * 这个类主要用来对Java中不同对IO方式进行测试，以此了解不同IO的性能情况
 *
 * Read
 *
 * 通过对FileInputStream 以及FileChannel的ReadIO的测试，明确了
 * 两者都是属于缓冲IO（按照小林coding对文件IO的分类），也就是说都利用了用户空间的标准库缓存。
 * 同时也验证了当缓存区的大小是4KB或者其倍数时，性能达到最佳，因为如果小于4KB，会导致多次的系统
 * 调用read的产生，需要更频繁的在用户和内核态直接切换。测试结果如下：
 *
 * TestFileInputStreamIO 读入100M大小的数据
 * byteSize: 8 Total Time: 12.434 sec
 * byteSize: 512 Total Time: 0.209 sec
 * byteSize: 1024 Total Time: 0.14 sec
 * byteSize: 4096 Total Time: 0.037 sec
 * byteSize: 8192 Total Time: 0.026 sec
 *
 * TestFileChannelReadIO 读入100M大小的数据
 * byteSize: 8 Total Time: 13.27 sec
 * byteSize: 512 Total Time: 0.192 sec
 * byteSize: 1024 Total Time: 0.103 sec
 * byteSize: 4096 Total Time: 0.035 sec
 * byteSize: 8192 Total Time: 0.035 sec
 *
 * Write
 *
 * 由于通过Read部分已经明确了FileInputStream和FileChannel都是缓冲IO，没有本质区别，并且FileChannel
 * 提供了更丰富的API，因此Write部分只测试了FileChannel的WriteIO，如同预期，也是验证了当缓存区的大小是4KB或者其倍数时，
 * 性能达到最佳，因为如果小于4KB，会导致多次的系统调用write的产生，需要更频繁的在用户和内核态直接切换。测试结果如下：
 *
 * TestFileChannelWriteIO 写入100M大小的数据
 * byteSize: 8 Total Time: 47.593 sec
 * byteSize: 512 Total Time: 0.602 sec
 * byteSize: 1024 Total Time: 0.323 sec
 * byteSize: 4096 Total Time: 0.096 sec
 * byteSize: 8192 Total Time: 0.06 sec
 *
 */
public class TestJavaIO {
    // 100M文件
    private final String inputFileName =
            "/Users/hgh/Downloads/my_file/公开数据集/时间序列异常检测数据集/AIOps数据集/KPI异常检测决赛数据集/phase2_ground_truth.hdf";

    @Test
    public void testFileInputStreamIO() throws Exception {
        File source = new File(inputFileName);
        System.out.println("TestFileInputStreamIO");
        testFileInputStream(source, 8);
        testFileInputStream(source, 512);
        testFileInputStream(source, 1024);
        testFileInputStream(source, 4096);
        testFileInputStream(source, 4096 * 2);
    }


    private void testFileInputStream(File source, int byteSize) throws Exception {
        long startTime = System.currentTimeMillis();

        FileInputStream ins = new FileInputStream(source);

        byte[] bytes = new byte[byteSize];
        int len;
        while ((len = ins.read(bytes)) != -1) {

        }
        ins.close();

        long endTime = System.currentTimeMillis();
        double spendTime = (endTime - startTime) / 1000.0;
        System.out.println("byteSize: " + byteSize + " Total Time: " + spendTime + " sec");
    }

    @Test
    public void testFileChannelReadIO() throws Exception {
        File source = new File(inputFileName);
        System.out.println("TestFileChannelReadIO");
        testFileChannelRead(source, 8);
        testFileChannelRead(source, 512);
        testFileChannelRead(source, 1024);
        testFileChannelRead(source, 4096);
        testFileChannelRead(source, 4096 * 2);

    }

    private void testFileChannelRead(File source, int byteSize) throws Exception {
        long startTime = System.currentTimeMillis();

        FileInputStream ins = new FileInputStream(source);
        FileChannel fcin = ins.getChannel();

        ByteBuffer bbuff = ByteBuffer.allocate(byteSize);

        while (fcin.read(bbuff) != -1) {
            bbuff.flip();
            bbuff.clear();
        }
        fcin.close();

        long endTime = System.currentTimeMillis();
        double spendTime = (endTime - startTime) / 1000.0;
        System.out.println("byteSize: " + byteSize + " Total Time: " + spendTime + " sec");
    }

    @Test
    public void testFileChannelWriteIO() throws Exception {
        File target = new File("./tmp/writeTest.txt");
        int fileSize = (int) Math.pow(10, 8);
        System.out.println("TestFileChannelWriteIO");
        testFileChannelWrite(target, 8, fileSize);
        testFileChannelWrite(target, 512, fileSize);
        testFileChannelWrite(target, 1024, fileSize);
        testFileChannelWrite(target, 4096, fileSize);
        testFileChannelWrite(target, 4096 * 2, fileSize);

    }

    private void testFileChannelWrite(File target, int byteSize, int fileSize) throws Exception {
        long startTime = System.currentTimeMillis();

        FileOutputStream outs = new FileOutputStream(target);
        FileChannel fcout = outs.getChannel();

        byte[] data = new byte[byteSize];
        ByteBuffer bbuff = ByteBuffer.wrap(data);
        int time = fileSize / byteSize;
        int count = 0;
        while (fcout.write(bbuff) != -1) {
            bbuff.flip();
            bbuff.clear();
            count++;
            if (count >= time) {
                break;
            }
        }
        fcout.close();

        long endTime = System.currentTimeMillis();
        double spendTime = (endTime - startTime) / 1000.0;
        System.out.println("byteSize: " + byteSize + " Total Time: " + spendTime + " sec");
    }
}
