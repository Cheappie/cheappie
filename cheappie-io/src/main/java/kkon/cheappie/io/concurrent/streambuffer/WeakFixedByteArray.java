package kkon.cheappie.io.concurrent.streambuffer;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;

class WeakFixedByteArray {
    private final int maxSize;
    private byte[] internalBuffer;
    private int count;

    WeakFixedByteArray(int size, int throttleFactor) {
        this.internalBuffer = new byte[size];
        this.count = 0;
        this.maxSize = size * throttleFactor;
    }

    public byte[] elements() {
        return internalBuffer;
    }

    void writeTo(OutputStream outputStream) throws IOException {
        outputStream.write(internalBuffer, 0, count);
    }

    int size() {
        return count;
    }

    void reset() {
        count = 0;
    }

    boolean write(byte[] b, int readOffset, int len) {
        if (len <= freeSpace()) {
            directWrite(b, readOffset, len);
            return true;
        } else {
            int requiredCapacity = estimateRequiredCapacity(len);
            if (requiredCapacity < maxSize) {
                resize(requiredCapacity);
                directWrite(b, readOffset, len);
                return true;
            }

            if ((maxSize - count) >= len && internalBuffer.length < maxSize) {
                resize(maxSize);
                directWrite(b, readOffset, len);
                return true;
            }

            if (len > internalBuffer.length) {
                resize(requiredCapacity);
                directWrite(b, readOffset, len);
                return true;
            }
        }

        return false;
    }

    private int freeSpace() {
        return internalBuffer.length - count;
    }

    private int estimateRequiredCapacity(int incomingBytesCount) {
        int newBufferLength = internalBuffer.length;

        while (newBufferLength < count + incomingBytesCount) {
            newBufferLength *= 2;
        }

        return newBufferLength;
    }

    private void resize(int newBufferLength) {
        this.internalBuffer = Arrays.copyOf(internalBuffer, newBufferLength);
    }

    private void directWrite(byte[] b, int off, int len) {
        System.arraycopy(b, off, internalBuffer, count, len);
        count += len;
    }
}
