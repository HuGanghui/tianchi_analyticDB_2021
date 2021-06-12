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

    public void init(String workspaceDir, String fileName, int no) throws IOException  {
        this.file = new File(workspaceDir, fileName + "_" + no);
        this.fileChannel = new RandomAccessFile(this.file, "rw").getChannel();
        this.byteBuffer = ByteBuffer.allocate(Constant.Buffer_CAP);
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

    public void write(long data) throws Exception {
        byteBuffer.putLong(data);
        if (byteBuffer.position() == byteBuffer.capacity()) {
            byteBuffer.flip();
            fileChannel.write(byteBuffer);
            byteBuffer.clear();
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

    public int destroy() throws IOException {
        if (byteBuffer.position() != 0) {
            byteBuffer.flip();
            fileChannel.write(byteBuffer);
            byteBuffer.clear();
        }
        int fileSize = getFileLength();
        this.byteBuffer = null;
        if (this.fileChannel != null) {
            this.fileChannel.close();
        }
        return fileSize;
    }
}
