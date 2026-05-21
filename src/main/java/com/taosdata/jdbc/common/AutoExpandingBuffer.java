package com.taosdata.jdbc.common;
import com.taosdata.jdbc.TSDBError;
import com.taosdata.jdbc.TSDBErrorNumbers;
import com.taosdata.jdbc.utils.Utils;
import io.netty.buffer.*;

import java.nio.charset.StandardCharsets;
import java.sql.SQLException;

public class AutoExpandingBuffer {
    private CompositeByteBuf composite;
    private final PooledByteBufAllocator allocator;
    private final int bufferSize;
    private final int maxComponents;
    private ByteBuf currentBuffer;
    private boolean stopWrite = false;

    public AutoExpandingBuffer(int initialBufferSize, int maxComponents) {
        this.allocator = PooledByteBufAllocator.DEFAULT;
        this.bufferSize = initialBufferSize;
        this.maxComponents = maxComponents;
        this.composite = allocator.compositeBuffer(maxComponents);
        this.currentBuffer = allocator.buffer(bufferSize);
    }

     public void writeBytes(byte[] src) throws SQLException {
        writeBytes(src, 0, src.length);
    }

    public void writeBytes(byte[] src, int off, int len) throws SQLException {
        if (stopWrite){
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_INVALID_VARIABLE, "Cannot write to buffer after stopWrite has been called");
        }
        int bytesWritten = 0;
        while (bytesWritten < len) {
            int writableBytes = currentBuffer.writableBytes();
            int bytesToWrite = Math.min(writableBytes, len - bytesWritten);

            currentBuffer.writeBytes(src, off + bytesWritten, bytesToWrite);

            bytesWritten += bytesToWrite;

            // if current buffer is full, allocate a new buffer
            if (currentBuffer.writableBytes() == 0) {
                if (composite.numComponents() >= composite.maxNumComponents()) {
                    throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_INVALID_VARIABLE, "Data too long, exceeded maximum components");
                }
                composite.addComponent(true, currentBuffer);
                currentBuffer = allocator.buffer(bufferSize);
            }
        }
    }

    /** Append a single byte. Avoids any intermediate allocation. */
    public void writeByte(byte v) throws SQLException {
        if (stopWrite) {
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_INVALID_VARIABLE, "Cannot write to buffer after stopWrite has been called");
        }
        if (currentBuffer.writableBytes() > 0) {
            currentBuffer.writeByte(v);
        } else {
            if (composite.numComponents() >= composite.maxNumComponents()) {
                throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_INVALID_VARIABLE, "Data too long, exceeded maximum components");
            }
            composite.addComponent(true, currentBuffer);
            currentBuffer = allocator.buffer(bufferSize);
            currentBuffer.writeByte(v);
        }
    }

    /** Append a 4-byte little-endian int. Avoids Unpooled allocation on the slow path. */
    public void writeIntLE(int v) throws SQLException {
        if (currentBuffer.writableBytes() >= Integer.BYTES) {
            if (stopWrite) {
                throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_INVALID_VARIABLE, "Cannot write to buffer after stopWrite has been called");
            }
            currentBuffer.writeIntLE(v);
            return;
        }
        byte[] b = {(byte) v, (byte)(v >>> 8), (byte)(v >>> 16), (byte)(v >>> 24)};
        writeBytes(b, 0, 4);
    }

    /** Append an 8-byte little-endian long. Avoids Unpooled allocation on the slow path. */
    public void writeLongLE(long v) throws SQLException {
        if (currentBuffer.writableBytes() >= Long.BYTES) {
            if (stopWrite) {
                throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_INVALID_VARIABLE, "Cannot write to buffer after stopWrite has been called");
            }
            currentBuffer.writeLongLE(v);
            return;
        }
        byte[] b = {
            (byte) v,          (byte)(v >>> 8),  (byte)(v >>> 16), (byte)(v >>> 24),
            (byte)(v >>> 32),  (byte)(v >>> 40), (byte)(v >>> 48), (byte)(v >>> 56)
        };
        writeBytes(b, 0, 8);
    }

    /** Append a 4-byte little-endian float. Avoids Unpooled allocation on the slow path. */
    public void writeFloatLE(float v) throws SQLException {
        writeIntLE(Float.floatToRawIntBits(v));
    }

    /** Append an 8-byte little-endian double. Avoids Unpooled allocation on the slow path. */
    public void writeDoubleLE(double v) throws SQLException {
        writeLongLE(Double.doubleToRawLongBits(v));
    }
    public int writeString(String src) throws SQLException {
        if (stopWrite){
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_INVALID_VARIABLE, "Cannot write string after stopWrite has been called");
        }

        int totalLen = ByteBufUtil.utf8Bytes(src);
        int writableBytes = currentBuffer.writableBytes();


        if (writableBytes >= totalLen) {
            currentBuffer.writeCharSequence(src, StandardCharsets.UTF_8);
            return totalLen;
        } else {
            int bytesWritten = 0;
            byte[] srcBytes = src.getBytes(StandardCharsets.UTF_8);
            while (bytesWritten < totalLen) {
                writableBytes = currentBuffer.writableBytes();

                if (writableBytes >= totalLen - bytesWritten) {
                    currentBuffer.writeBytes(srcBytes, bytesWritten, totalLen - bytesWritten);
                    bytesWritten = totalLen;
                } else {
                    currentBuffer.writeBytes(srcBytes, bytesWritten, writableBytes);
                    bytesWritten += writableBytes;
                }
                if (currentBuffer.writableBytes() == 0) {
                    if (composite.numComponents() >= composite.maxNumComponents()) {
                        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_INVALID_VARIABLE, "Data too long, exceeded maximum components");
                    }
                    composite.addComponent(true, currentBuffer);
                    currentBuffer = allocator.buffer(bufferSize);
                }
            }
        }
        return totalLen;
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

    private void serializeLong(ByteBuf buffer, int totalLen, long src, boolean isNull, byte type) {
        buffer.writeIntLE(totalLen);
        buffer.writeIntLE(type);
        buffer.writeIntLE(1);
        buffer.writeByte(isNull ? 1 : 0);
        buffer.writeByte(0);
        buffer.writeIntLE(8);
        buffer.writeLongLE(isNull ? 0 : src);
    }

    public int serializeLong(long src, boolean isNull, byte type) throws SQLException {
        int writableBytes = currentBuffer.writableBytes();
        int totalLen = 26;

        if (writableBytes > totalLen){
            serializeLong(currentBuffer, totalLen, src, isNull, type);
            return totalLen;
        }

        ByteBuf heapBuf = Unpooled.buffer(totalLen);
        serializeLong(heapBuf, totalLen, src, isNull, type);

        writeBytes(heapBuf.array());
        heapBuf.release();
        return totalLen;
    }


    private void serializeTimeStamp(ByteBuf buffer, int totalLen, long src, boolean isNull) {
        buffer.writeIntLE(totalLen);
        buffer.writeIntLE(9);
        buffer.writeIntLE(1);
        buffer.writeByte(isNull ? 1 : 0);
        buffer.writeByte(0);
        buffer.writeIntLE(8);
        buffer.writeLongLE(isNull ? 0 : src);
    }

    public int serializeTimeStamp(long src, boolean isNull) throws SQLException {
        int writableBytes = currentBuffer.writableBytes();
        int totalLen = 26;

        if (writableBytes > totalLen){
            serializeTimeStamp(currentBuffer, totalLen, src, isNull);
            return totalLen;
        }

        ByteBuf heapBuf = Unpooled.buffer(totalLen);
        serializeTimeStamp(heapBuf, totalLen, src, isNull);

        writeBytes(heapBuf.array());
        heapBuf.release();
        return totalLen;
    }

    private void serializeDouble(ByteBuf buffer, int totalLen, double src, boolean isNull) {
        buffer.writeIntLE(totalLen);
        buffer.writeIntLE(7);
        buffer.writeIntLE(1);
        buffer.writeByte(isNull ? 1 : 0);
        buffer.writeByte(0);
        buffer.writeIntLE(8);
        buffer.writeDoubleLE(isNull ? 0 : src);
    }

    public int serializeDouble(double src, boolean isNull) throws SQLException {
        int writableBytes = currentBuffer.writableBytes();
        int totalLen = 26;

        if (writableBytes > totalLen){
            serializeDouble(currentBuffer, totalLen, src, isNull);
            return totalLen;
        }

        ByteBuf heapBuf = Unpooled.buffer(totalLen);
        serializeDouble(heapBuf, totalLen, src, isNull);

        writeBytes(heapBuf.array());
        heapBuf.release();

        return totalLen;
    }

    private void serializeBool(ByteBuf buffer, int totalLen, boolean src, boolean isNull) {
        buffer.writeIntLE(totalLen);
        buffer.writeIntLE(1);
        buffer.writeIntLE(1);
        buffer.writeByte(isNull ? 1 : 0);
        buffer.writeByte(0);
        buffer.writeIntLE(1);
        buffer.writeByte((!isNull && src) ? 1 : 0);
    }
    public int serializeBool(boolean src, boolean isNull) throws SQLException {
        int writableBytes = currentBuffer.writableBytes();
        int totalLen = 19;
        if (writableBytes > totalLen){
            serializeBool(currentBuffer, totalLen, src, isNull);
            return totalLen;
        }

        ByteBuf heapBuf = Unpooled.buffer(totalLen);
        serializeBool(heapBuf, totalLen, src, isNull);

        writeBytes(heapBuf.array());
        heapBuf.release();

        return totalLen;
    }

    private void serializeByte(ByteBuf buffer, int totalLen, byte src, boolean isNull, byte type) {
        buffer.writeIntLE(totalLen);
        buffer.writeIntLE(type);
        buffer.writeIntLE(1);
        buffer.writeByte(isNull ? 1 : 0);
        buffer.writeByte(0);
        buffer.writeIntLE(1);
        buffer.writeByte(isNull ? 0 : src);
    }
    public int serializeByte(byte src, boolean isNull, byte type) throws SQLException {
        int writableBytes = currentBuffer.writableBytes();
        int totalLen = 19;
        if (writableBytes > totalLen){
            serializeByte(currentBuffer, totalLen, src, isNull, type);
            return totalLen;
        }

        ByteBuf heapBuf = Unpooled.buffer(totalLen);
        serializeByte(heapBuf, totalLen, src, isNull, type);

        writeBytes(heapBuf.array());
        heapBuf.release();

        return totalLen;
    }

    private void serializeShort(ByteBuf buffer, int totalLen, short src, boolean isNull, byte type) {
        buffer.writeIntLE(totalLen);
        buffer.writeIntLE(type);
        buffer.writeIntLE(1);
        buffer.writeByte(isNull ? 1 : 0);
        buffer.writeByte(0);
        buffer.writeIntLE(2);
        buffer.writeShortLE(isNull ? 0 : src);
    }

    public int serializeShort(short src, boolean isNull, byte type) throws SQLException {
        int writableBytes = currentBuffer.writableBytes();
        int totalLen = 20;
        if (writableBytes > totalLen){
            serializeShort(currentBuffer, totalLen, src, isNull, type);
            return totalLen;
        }

        ByteBuf heapBuf = Unpooled.buffer(totalLen);
        serializeShort(heapBuf, totalLen, src, isNull, type);

        writeBytes(heapBuf.array());
        heapBuf.release();

        return totalLen;
    }
    private void serializeInt(ByteBuf buffer, int totalLen, int src, boolean isNull, byte type) {
        buffer.writeIntLE(totalLen);
        buffer.writeIntLE(type);
        buffer.writeIntLE(1);
        buffer.writeByte(isNull ? 1 : 0);
        buffer.writeByte(0);
        buffer.writeIntLE(4);
        buffer.writeIntLE(isNull ? 0 : src);
    }

    public int serializeInt(int src, boolean isNull, byte type) throws SQLException {
        int writableBytes = currentBuffer.writableBytes();
        int totalLen = 22;
        if (writableBytes > totalLen){
            serializeInt(currentBuffer, totalLen, src, isNull, type);
            return totalLen;
        }

        ByteBuf heapBuf = Unpooled.buffer(totalLen);
        serializeInt(heapBuf, totalLen, src, isNull, type);

        writeBytes(heapBuf.array());
        heapBuf.release();

        return totalLen;
    }

    private void serializeFloat(ByteBuf buffer, int totalLen, float src, boolean isNull) {
        buffer.writeIntLE(totalLen);
        buffer.writeIntLE(6);
        buffer.writeIntLE(1);
        buffer.writeByte(isNull ? 1 : 0);
        buffer.writeByte(0);
        buffer.writeIntLE(4);
        buffer.writeFloatLE(isNull ? 0 : src);
    }

    public int serializeFloat(float src, boolean isNull) throws SQLException {
        int writableBytes = currentBuffer.writableBytes();
        int totalLen = 22;
        if (writableBytes > totalLen){
            serializeFloat(currentBuffer, totalLen, src, isNull);
            return totalLen;
        }

        ByteBuf heapBuf = Unpooled.buffer(totalLen);
        serializeFloat(heapBuf, totalLen, src, isNull);

        writeBytes(heapBuf.array());
        heapBuf.release();

        return totalLen;
    }

    private void serializeBytes(ByteBuf buffer, int totalLen, byte[] src, boolean isNull, int type) {
        buffer.writeIntLE(totalLen);
        buffer.writeIntLE(type);
        buffer.writeIntLE(1);
        buffer.writeByte(isNull ? 1 : 0);
        buffer.writeByte(1);
        buffer.writeIntLE(isNull ? 0 : src.length);
        buffer.writeIntLE(isNull ? 0 : src.length);
        if (!isNull){
            buffer.writeBytes(src);
        }
    }

    public int serializeBytes(byte[] src, boolean isNull, int type) throws SQLException {
        int writableBytes = currentBuffer.writableBytes();
        int totalLen = 22 + src.length;
        if (writableBytes > totalLen){
            serializeBytes(currentBuffer, totalLen, src, isNull, type);
            return totalLen;
        }

        ByteBuf heapBuf = Unpooled.buffer(totalLen);
        serializeBytes(heapBuf, totalLen, src, isNull, type);

        writeBytes(heapBuf.array());
        heapBuf.release();

        return totalLen;
    }

    private void serializeString(ByteBuf buffer, int totalLen, int strlen, String src, boolean isNull, int type) {
        buffer.writeIntLE(totalLen);
        buffer.writeIntLE(type);
        buffer.writeIntLE(1);
        buffer.writeByte(isNull ? 1 : 0);
        buffer.writeByte(1);
        buffer.writeIntLE(isNull ? 0 : strlen);
        buffer.writeIntLE(isNull ? 0 : strlen);
        if (!isNull){
            ByteBufUtil.reserveAndWriteUtf8(buffer, src, strlen);
        }
    }

    public int serializeString(String src, boolean isNull, int type) throws SQLException {
        int writableBytes = currentBuffer.writableBytes();
        int requiredBytes = src != null ? ByteBufUtil.utf8Bytes(src) : 0;
        int totalLen = 22 + requiredBytes;
        if (writableBytes > totalLen){
            serializeString(currentBuffer, totalLen, requiredBytes, src, isNull, type);
            return totalLen;
        }

        ByteBuf heapBuf = Unpooled.buffer(totalLen);
        serializeString(heapBuf, totalLen, requiredBytes, src, isNull, type);

        writeBytes(heapBuf.array());
        heapBuf.release();

        return totalLen;
    }

    public CompositeByteBuf getBuffer() {
        return composite;
    }

    public int readableBytes() {
        if (composite == null) {
            return 0;
        }
        if (stopWrite) {
            return composite.readableBytes();
        }
        return composite.readableBytes() + currentBuffer.readableBytes();
    }

    public void truncateReadableBytes(int targetReadableBytes) {
        if (stopWrite) {
            throw new IllegalStateException("Cannot truncate after stopWrite has been called");
        }
        int readableBytes = readableBytes();
        if (targetReadableBytes < 0 || targetReadableBytes > readableBytes) {
            throw new IllegalArgumentException(
                    "targetReadableBytes out of range: " + targetReadableBytes + " (readableBytes=" + readableBytes + ")");
        }
        int bytesToTrim = readableBytes - targetReadableBytes;
        if (bytesToTrim == 0) {
            return;
        }

        int committedReadableBytes = composite.readableBytes();
        if (targetReadableBytes >= committedReadableBytes) {
            currentBuffer.writerIndex(targetReadableBytes - committedReadableBytes);
            return;
        }

        CompositeByteBuf truncatedComposite = allocator.compositeBuffer(composite.maxNumComponents());
        ByteBuf truncatedCurrent = null;
        boolean success = false;
        try {
            int remaining = targetReadableBytes;
            for (int i = 0; i < composite.numComponents() && remaining > 0; i++) {
                ByteBuf component = composite.component(i);
                int componentReadableBytes = component.readableBytes();
                if (remaining >= componentReadableBytes) {
                    truncatedComposite.addComponent(
                            true,
                            component.retainedSlice(component.readerIndex(), componentReadableBytes));
                    remaining -= componentReadableBytes;
                } else {
                    truncatedCurrent = allocator.buffer(Math.max(bufferSize, remaining));
                    truncatedCurrent.writeBytes(component, component.readerIndex(), remaining);
                    remaining = 0;
                }
            }
            if (truncatedCurrent == null) {
                truncatedCurrent = allocator.buffer(bufferSize);
            }

            Utils.releaseByteBuf(composite);
            Utils.releaseByteBuf(currentBuffer);
            composite = truncatedComposite;
            currentBuffer = truncatedCurrent;
            success = true;
        } finally {
            if (!success) {
                Utils.releaseByteBuf(truncatedComposite);
                if (truncatedCurrent != null && truncatedCurrent != currentBuffer) {
                    Utils.releaseByteBuf(truncatedCurrent);
                }
            }
        }
    }

    public ByteBuf retainedDuplicate() {
        stopWrite();
        return composite.retainedDuplicate();
    }

    // must call stopWrite before
    public void release() {
        stopWrite();
        if (composite != null && composite.refCnt() > 0) {
            Utils.releaseByteBuf(composite);
        }
        composite = null;
    }

    public void reset() {
        if (composite != null && composite.refCnt() > 0) {
            Utils.releaseByteBuf(composite);
        }
        if (!stopWrite && currentBuffer != null && currentBuffer.refCnt() > 0) {
            Utils.releaseByteBuf(currentBuffer);
        }
        composite = allocator.compositeBuffer(maxComponents);
        currentBuffer = allocator.buffer(bufferSize);
        stopWrite = false;
    }

    public void stopWrite(){
        if (!stopWrite) {
            composite.addComponent(true, currentBuffer);
            stopWrite = true;
        }
    }
}
