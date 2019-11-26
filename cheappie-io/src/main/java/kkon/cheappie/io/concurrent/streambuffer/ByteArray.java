/*
 * Copyright (C) 2019 Kamil Konior
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package kkon.cheappie.io.concurrent.streambuffer;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;


final class ByteArray {
    private byte[] arr;
    private int count;

    ByteArray(int size) {
        this.arr = new byte[size];
        this.count = 0;
    }

    byte[] elements() {
        return arr;
    }

    void writeTo(OutputStream outputStream) throws IOException {
        outputStream.write(arr, 0, count);
    }

    void resetAfterWriteTo(OutputStream outputStream) throws IOException {
        outputStream.write(arr, 0, count);
        reset();
    }

    void softTrim(int len) {
        if (count > len) {
            int leftOversCount = count - len;

            System.arraycopy(arr, len, arr, 0, leftOversCount);
            count = leftOversCount;
        } else {
            reset();
        }
    }

    int size() {
        return count;
    }

    void reset() {
        count = 0;
    }

    void write(int b) {
        ensureCapacity(1);

        arr[count++] = (byte) b;
    }

    void write(byte[] b) {
        write(b, 0, b.length);
    }

    void write(byte[] b, int off, int len) {
        ensureCapacity(len);

        System.arraycopy(b, off, arr, count, len);
        count += len;
    }

    private void ensureCapacity(int incomingBytesCount) {
        if (count + incomingBytesCount > arr.length) {
            int newBufferLength = arr.length;

            while (newBufferLength < count + incomingBytesCount) {
                newBufferLength *= 2;
            }

            this.arr = Arrays.copyOf(arr, newBufferLength);
        }
    }
}
