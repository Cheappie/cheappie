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

import org.apache.commons.lang3.ArrayUtils;
import org.junit.jupiter.api.Test;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;


class ByteArrayTest {
    static final int DEFAULT_SIZE = 3;

    @Test
    void shouldWriteSingleByte() {
        ByteArray byteArray = new ByteArray(DEFAULT_SIZE);

        byte _byte = 100;
        byteArray.write(_byte);
        assertEquals(_byte, byteArray.elements()[0]);
        assertEquals(1, byteArray.size());
    }

    @Test
    void shouldWriteArrayOfBytes() {
        byte[] bytes = new byte[] {1, 2, 3};

        ByteArray byteArray = new ByteArray(bytes.length);
        byteArray.write(bytes);
        assertArrayEquals(bytes, byteArray.elements());
    }

    @Test
    void shouldWriteArrayOfBytesUpToSpecifiedLength() {
        ByteArray byteArray = new ByteArray(DEFAULT_SIZE);

        byte[] bytes = new byte[] {1, 2};
        byteArray.write(bytes, 0, bytes.length);
        assertArrayEquals(bytes, Arrays.copyOfRange(byteArray.elements(), 0, bytes.length));
        assertEquals(2, byteArray.size());
    }

    @Test
    void shouldWriteFromSpecifiedReadOffset() {
        ByteArray byteArray = new ByteArray(DEFAULT_SIZE);

        byte[] expectedWrittenBytes = new byte[] {2, 3};
        int totalWrittenBytes = expectedWrittenBytes.length;

        byte[] sourceArray = ArrayUtils.insert(0, expectedWrittenBytes, (byte) 1);

        byteArray.write(sourceArray, 1, totalWrittenBytes);
        assertArrayEquals(expectedWrittenBytes, Arrays.copyOfRange(byteArray.elements(), 0, totalWrittenBytes));
        assertEquals(2, byteArray.size());
    }

    @Test
    void shouldResizeWhenNecessary() {
        ByteArray byteArray = new ByteArray(DEFAULT_SIZE);

        byte[] bytes = new byte[] {1, 2, 3, 4};
        assertTrue(bytes.length > DEFAULT_SIZE);
        byteArray.write(bytes);

        assertTrue(byteArray.size() > DEFAULT_SIZE);
        assertArrayEquals(bytes, Arrays.copyOfRange(byteArray.elements(), 0, bytes.length));
    }

    @Test
    void shouldResetSize() {
        ByteArray byteArray = new ByteArray(DEFAULT_SIZE);

        assertEquals(0, byteArray.size());

        byteArray.write(new byte[] {1}, 0, 1);
        assertEquals(1, byteArray.size());

        byteArray.reset();
        assertEquals(0, byteArray.size());
    }

    @Test
    void shouldPerformSoftTrimOfArrayContentUpToSpecifiedLength() {
        ByteArray byteArray = new ByteArray(DEFAULT_SIZE);

        int trimLen = 2;
        byte[] bytes = new byte[] {1, 2, 3};
        byteArray.write(bytes);
        byteArray.softTrim(trimLen);

        assertEquals(bytes.length - trimLen, byteArray.size());
        assertEquals(3, byteArray.elements()[0]);
    }

    @Test
    void shouldTransferBytes() throws IOException {
        ByteArray byteArray = new ByteArray(DEFAULT_SIZE);

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        byte[] bytes = new byte[] {1, 2, 3};
        byteArray.write(bytes, 0, bytes.length);
        byteArray.writeTo(outputStream);

        assertEquals(bytes.length, byteArray.size());
        assertArrayEquals(bytes, outputStream.toByteArray());
    }
}
