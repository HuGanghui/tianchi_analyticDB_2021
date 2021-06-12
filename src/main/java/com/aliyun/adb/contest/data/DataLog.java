package com.aliyun.adb.contest.data;

import com.aliyun.adb.contest.common.Constant;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class DataLog {
    private File file;
    private FileChannel fileChannel;
    private ByteBuffer byteBuffer;
    private ByteBuffer cacheByteBuffer;

    public void init(String workspaceDir, String fileName, int no) throws IOException  {
        this.file = new File(workspaceDir, fileName + "_" + no);
        this.fileChannel = new RandomAccessFile(this.file, "rw").getChannel();
        this.byteBuffer = ByteBuffer.allocate(Constant.Buffer_CAP);
        this.cacheByteBuffer = ByteBuffer.allocate(Constant.Long_Size * 10000);
    }

    public long[] read() throws Exception {
        FileChannel readRaf = new RandomAccessFile(this.file, "r").getChannel();
        ByteBuffer byteBuffer = ByteBuffer.allocate(Constant.Buffer_CAP);
        long[] longValues = new long[getFileLength(readRaf)];
        int index = 0;
        while (readRaf.read(byteBuffer) != -1) {
            byteBuffer.flip();
            while (byteBuffer.hasRemaining()) {
                longValues[index] = byteBuffer.getLong();
                index++;
            }
            byteBuffer.clear();
        }
        readRaf.close();
        return longValues;
    }

    public synchronized void write(long data) throws Exception {
        cacheByteBuffer.putLong(data);
        if (cacheByteBuffer.position() == cacheByteBuffer.capacity()) {
            cacheByteBuffer.flip();
            while (cacheByteBuffer.hasRemaining()) {
                byteBuffer.putLong(cacheByteBuffer.getLong());
                if (byteBuffer.position() == byteBuffer.capacity()) {
                    byteBuffer.flip();
                    fileChannel.write(byteBuffer);
                    byteBuffer.clear();
                }
            }
            if (byteBuffer.position() != 0) {
                byteBuffer.flip();
                fileChannel.write(byteBuffer);
                byteBuffer.clear();
            }
            cacheByteBuffer.clear();
        }
    }

    private int getFileLength() {
        return getFileLength(this.fileChannel);
    }

    public int getFileLength(FileChannel fileChannel) {
        try {
            return (int) (fileChannel.size() / Constant.Long_Size);
        } catch (IOException e) {
            e.printStackTrace();
            return 0;
        }
    }

    public synchronized int destroy() throws IOException {
        if (cacheByteBuffer.position() != 0) {
            cacheByteBuffer.flip();
            while (cacheByteBuffer.hasRemaining()) {
                byteBuffer.putLong(cacheByteBuffer.getLong());
                if (byteBuffer.position() == byteBuffer.capacity()) {
                    byteBuffer.flip();
                    fileChannel.write(byteBuffer);
                    byteBuffer.clear();
                }
            }
            if (byteBuffer.position() != 0) {
                byteBuffer.flip();
                fileChannel.write(byteBuffer);
                byteBuffer.clear();
            }
            cacheByteBuffer.clear();
        }
        int fileSize = getFileLength();
        this.byteBuffer = null;
        this.cacheByteBuffer = null;
        if (this.fileChannel != null) {
            this.fileChannel.close();
        }
        return fileSize;
    }
}
