/*
 * Copyright (C) 2009 The Cheappie Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package kkon.cheappie.io.concurrent.streambuffer;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;

class ByteArray {
    private byte[] internalBuffer;
    private int count;

    ByteArray(int size) {
        this.internalBuffer = new byte[size];
        this.count = 0;
    }

    public byte[] elements() {
        return internalBuffer;
    }

    void writeTo(OutputStream outputStream) throws IOException {
        outputStream.write(internalBuffer, 0, count);
    }

    void softTrim(int len) {
        if (count > len) {
            int leftOversCount = count - len;

            System.arraycopy(internalBuffer, len, internalBuffer, 0, leftOversCount);
            count = leftOversCount;
        } else {
            reset();
        }
    }

    int size() {
        return count;
    }

    int freeSpace() {
        return internalBuffer.length - count;
    }

    void reset() {
        count = 0;
    }

    void write(int b) {
        ensureCapacity(1);

        internalBuffer[count] = (byte) b;
        count += 1;
    }

    void write(byte[] b) {
        write(b, 0, b.length);
    }

    void write(byte[] b, int off, int len) {
        ensureCapacity(len);

        System.arraycopy(b, off, internalBuffer, count, len);
        count += len;
    }

    private void ensureCapacity(int incomingBytesCount) {
        if (count + incomingBytesCount > internalBuffer.length) {
            int newBufferLength = internalBuffer.length;

            while (newBufferLength < count + incomingBytesCount) {
                newBufferLength *= 2;
            }

            this.internalBuffer = Arrays.copyOf(internalBuffer, newBufferLength);
        }
    }
}
