package com.taosdata.jdbc.common;
import com.taosdata.jdbc.TSDBError;
import io.netty.buffer.*;
import io.netty.util.ReferenceCountUtil;

import java.sql.SQLException;

public class AutoExpandingBuffer {
    private CompositeByteBuf composite;
    private final PooledByteBufAllocator allocator;
    private final int bufferSize;
    private ByteBuf currentBuffer;
    private boolean stopWrite = false;

    public AutoExpandingBuffer(int initialBufferSize, int maxComponents) {
        this.allocator = PooledByteBufAllocator.DEFAULT;
        this.bufferSize = initialBufferSize;
        this.composite = allocator.compositeBuffer(maxComponents);
        this.currentBuffer = allocator.buffer(bufferSize);
    }

    // 写入数据，自动处理缓冲区扩展
    public void writeBytes(byte[] src) throws SQLException {
        int bytesWritten = 0;
        while (bytesWritten < src.length) {
            int writableBytes = currentBuffer.writableBytes();
            int bytesToWrite = Math.min(writableBytes, src.length - bytesWritten);

            currentBuffer.writeBytes(src, bytesWritten, bytesToWrite);

            bytesWritten += bytesToWrite;

            // 如果当前缓冲区已满，分配新缓冲区
            if (currentBuffer.writableBytes() == 0) {
                if (composite.numComponents() >= composite.maxNumComponents()) {
                    throw TSDBError.createSQLException(1, "Exceeded maximum components");
                }

                composite.addComponent(true, currentBuffer);
                currentBuffer = allocator.buffer(bufferSize);
            }
        }
    }

    public void writeInt(int src) throws SQLException {
        int writableBytes = currentBuffer.writableBytes();
        if (writableBytes > Integer.BYTES){
            currentBuffer.writeIntLE(src);
            return;
        }

        ByteBuf heapBuf = Unpooled.buffer(Integer.BYTES);
        heapBuf.writeIntLE(src);

        writeBytes(heapBuf.array());
        heapBuf.release();
    }

    public void writeShort(short src) throws SQLException {
        int writableBytes = currentBuffer.writableBytes();
        if (writableBytes > Short.BYTES){
            currentBuffer.writeShortLE(src);
            return;
        }

        ByteBuf heapBuf = Unpooled.buffer(Short.BYTES);
        heapBuf.writeShortLE(src);

        writeBytes(heapBuf.array());
        heapBuf.release();
    }

    public int serializeLong(long src, boolean isNull) throws SQLException {
        int writableBytes = currentBuffer.writableBytes();
        int totalLen = 26;

        if (writableBytes > totalLen){
            currentBuffer.writeIntLE(totalLen);
            currentBuffer.writeIntLE(5);
            currentBuffer.writeIntLE(1);
            currentBuffer.writeByte(isNull ? 1 : 0);
            currentBuffer.writeByte(0);
            currentBuffer.writeIntLE(8);
            currentBuffer.writeLongLE(isNull ? 0 : src);
            return totalLen;
        }

        ByteBuf heapBuf = Unpooled.buffer(totalLen);
        heapBuf.writeIntLE(totalLen);
        heapBuf.writeIntLE(5);
        heapBuf.writeIntLE(1);
        heapBuf.writeByte(isNull ? 1 : 0);
        heapBuf.writeByte(0);
        heapBuf.writeIntLE(8);
        heapBuf.writeLongLE(isNull ? 0 : src);

        writeBytes(heapBuf.array());
        heapBuf.release();
        return totalLen;
    }


    public int serializeTimeStamp(long src, boolean isNull) throws SQLException {
        int writableBytes = currentBuffer.writableBytes();
        int totalLen = 26;

        if (writableBytes > totalLen){
            currentBuffer.writeIntLE(totalLen);
            currentBuffer.writeIntLE(9);
            currentBuffer.writeIntLE(1);
            currentBuffer.writeByte(isNull ? 1 : 0);
            currentBuffer.writeByte(0);
            currentBuffer.writeIntLE(8);
            currentBuffer.writeLongLE(isNull ? 0 : src);
            return totalLen;
        }

        ByteBuf heapBuf = Unpooled.buffer(totalLen);
        heapBuf.writeIntLE(totalLen);
        heapBuf.writeIntLE(9);
        heapBuf.writeIntLE(1);
        heapBuf.writeByte(isNull ? 1 : 0);
        heapBuf.writeByte(0);
        heapBuf.writeIntLE(8);
        heapBuf.writeLongLE(isNull ? 0 : src);

        writeBytes(heapBuf.array());
        heapBuf.release();
        return totalLen;
    }

    public int serializeDouble(double src, boolean isNull) throws SQLException {
        int writableBytes = currentBuffer.writableBytes();
        int totalLen = 26;

        if (writableBytes > totalLen){
            currentBuffer.writeIntLE(totalLen);
            currentBuffer.writeIntLE(7);
            currentBuffer.writeIntLE(1);
            currentBuffer.writeByte(isNull ? 1 : 0);
            currentBuffer.writeByte(0);
            currentBuffer.writeIntLE(8);
            currentBuffer.writeDoubleLE(isNull ? 0 : src);
            return totalLen;
        }

        ByteBuf heapBuf = Unpooled.buffer(totalLen);
        heapBuf.writeIntLE(totalLen);
        heapBuf.writeIntLE(7);
        heapBuf.writeIntLE(1);
        heapBuf.writeByte(isNull ? 1 : 0);
        heapBuf.writeByte(0);
        heapBuf.writeIntLE(8);
        heapBuf.writeDoubleLE(isNull ? 0 : src);

        writeBytes(heapBuf.array());
        heapBuf.release();

        return totalLen;
    }

    public int serializeInt(int src, boolean isNull) throws SQLException {
        int writableBytes = currentBuffer.writableBytes();
        int totalLen = 22;
        if (writableBytes > totalLen){
            currentBuffer.writeIntLE(totalLen);
            currentBuffer.writeIntLE(4);
            currentBuffer.writeIntLE(1);
            currentBuffer.writeByte(isNull ? 1 : 0);
            currentBuffer.writeByte(0);
            currentBuffer.writeIntLE(4);
            currentBuffer.writeIntLE(isNull ? 0 : src);
            return totalLen;
        }

        ByteBuf heapBuf = Unpooled.buffer(totalLen);
        heapBuf.writeIntLE(totalLen);
        heapBuf.writeIntLE(4);
        heapBuf.writeIntLE(1);
        heapBuf.writeByte(isNull ? 1 : 0);
        heapBuf.writeByte(0);
        heapBuf.writeIntLE(4);
        heapBuf.writeIntLE(isNull ? 0 : src);

        writeBytes(heapBuf.array());
        heapBuf.release();

        return totalLen;
    }

    public int serializeFloat(float src, boolean isNull) throws SQLException {
        int writableBytes = currentBuffer.writableBytes();
        int totalLen = 22;
        if (writableBytes > totalLen){
            currentBuffer.writeIntLE(totalLen);
            currentBuffer.writeIntLE(6);
            currentBuffer.writeIntLE(1);
            currentBuffer.writeByte(isNull ? 1 : 0);
            currentBuffer.writeByte(0);
            currentBuffer.writeIntLE(4);
            currentBuffer.writeFloatLE(isNull ? 0 : src);
            return totalLen;
        }

        ByteBuf heapBuf = Unpooled.buffer(totalLen);
        heapBuf.writeIntLE(totalLen);
        heapBuf.writeIntLE(6);
        heapBuf.writeIntLE(1);
        heapBuf.writeByte(isNull ? 1 : 0);
        heapBuf.writeByte(0);
        heapBuf.writeIntLE(4);
        heapBuf.writeFloatLE(isNull ? 0 : src);

        writeBytes(heapBuf.array());
        heapBuf.release();

        return totalLen;
    }

    public int serializeBytes(byte[] src, boolean isNull, int type) throws SQLException {
        int writableBytes = currentBuffer.writableBytes();
        int totalLen = 22 + src.length;
        if (writableBytes > totalLen){
            currentBuffer.writeIntLE(totalLen);
            currentBuffer.writeIntLE(type);
            currentBuffer.writeIntLE(1);
            currentBuffer.writeByte(isNull ? 1 : 0);
            currentBuffer.writeByte(1);
            currentBuffer.writeIntLE(isNull ? 0 : src.length);
            currentBuffer.writeIntLE(isNull ? 0 : src.length);
            if (!isNull){
                currentBuffer.writeBytes(src);
            }
            return totalLen;
        }

        ByteBuf heapBuf = Unpooled.buffer(totalLen);
        heapBuf.writeIntLE(totalLen);
        heapBuf.writeIntLE(type);
        heapBuf.writeIntLE(1);
        heapBuf.writeByte(isNull ? 1 : 0);
        heapBuf.writeByte(1);
        heapBuf.writeIntLE(isNull ? 0 : src.length);
        heapBuf.writeIntLE(isNull ? 0 : src.length);
        if (!isNull){
            heapBuf.writeBytes(src);
        }

        writeBytes(heapBuf.array());
        heapBuf.release();

        return totalLen;
    }

    // 获取合并后的完整缓冲区
    public CompositeByteBuf getBuffer() {
        return composite;
    }

    // must call stopWrite before
    public void release() {
        if (composite != null && composite.refCnt() > 0) {
            ReferenceCountUtil.safeRelease(composite);
        }
        composite = null;
    }

    public void stopWrite(){
        if (!stopWrite) {
            composite.addComponent(true, currentBuffer); // 转移所有权
            stopWrite = true;
        }
    }
}