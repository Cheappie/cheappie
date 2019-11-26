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

import com.google.common.primitives.Chars;
import kkon.cheappie.io.concurrent.streambuffer.TestUtils.RethrowableConsumer;
import kkon.cheappie.io.concurrent.streambuffer.TestUtils.RethrowableFunction;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Random;
import java.util.function.BiConsumer;
import static kkon.cheappie.io.concurrent.streambuffer.TestUtils.readFully;
import static kkon.cheappie.io.concurrent.streambuffer.TestUtils.submitTaskToSeparateThread;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;


class CommittedGenericPipeTest {
    private Random random = new Random();

    @Test
    void shouldWriteSingleByte() throws Exception {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

        byte _byte = 1;
        write(outputStream, pipe -> pipe.write(_byte));

        assertArrayEquals(new byte[] {_byte}, outputStream.toByteArray());
    }

    private void write(ByteArrayOutputStream outputStream, RethrowableConsumer<GenericPipe> contentProvider)
                    throws Exception {
        ConcurrentOutputStreamBuffer streamBuffer = ConcurrentOutputStreamBuffer.builder(outputStream)
                        .withMinElementsWrittenUntilFlush(1024).build();

        ConcurrentOutputStreamBuffer.CompletionNotifier notifier = streamBuffer.declareExactProducersCount(1);
        try (CommittedGenericPipe pipe = streamBuffer.acquireWritableGenericPipe()) {
            contentProvider.accept(pipe);
            pipe.commit();
        }
        notifier.awaitCompletion();
    }

    @Test
    void shouldWriteByteArray() throws Exception {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

        byte[] bytes = new byte[] {1, 3, 2};
        write(outputStream, pipe -> pipe.write(bytes));

        assertArrayEquals(bytes, outputStream.toByteArray());
    }

    @Test
    void shouldWriteByteArrayFromRange() throws Exception {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

        byte[] bytes = new byte[] {1, 3, 2};
        write(outputStream, pipe -> pipe.write(bytes, 1, 2));

        assertArrayEquals(Arrays.copyOfRange(bytes, 1, 3), outputStream.toByteArray());
    }

    @Test
    void shouldWriteLong() throws Exception {
        long val = random.nextLong();
        assertValWrittenThroughGenericPipe(p -> p.writeBytesOfLong(val), DataInputStream::readLong, val);
    }

    <T> void assertValWrittenThroughGenericPipe(RethrowableConsumer<GenericPipe> pipe,
                    RethrowableFunction<DataInputStream, T> inMapper, T expectedVal) throws Exception {
        shouldWriteThroughGenericPipe(pipe, inMapper, expectedVal, Assertions::assertEquals);
    }

    <T> void shouldWriteThroughGenericPipe(RethrowableConsumer<GenericPipe> pipe,
                    RethrowableFunction<DataInputStream, T> inMapper, T expectedVal, BiConsumer<T, T> assertionMethod)
                    throws Exception {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

        write(outputStream, pipe);
        DataInputStream dataInputStream = new DataInputStream(new ByteArrayInputStream(outputStream.toByteArray()));
        assertionMethod.accept(expectedVal, inMapper.apply(dataInputStream));
    }

    @Test
    void shouldWriteInt() throws Exception {
        int val = random.nextInt();
        assertValWrittenThroughGenericPipe(p -> p.writeBytesOfInt(val), DataInputStream::readInt, val);
    }

    @Test
    void shouldWriteShort() throws Exception {
        short val = (short) random.nextInt();
        assertValWrittenThroughGenericPipe(p -> p.writeBytesOfShort(val), DataInputStream::readShort, val);
    }

    @Test
    void shouldWriteDouble() throws Exception {
        double val = random.nextDouble();
        assertValWrittenThroughGenericPipe(p -> p.writeBytesOfDouble(val), DataInputStream::readDouble, val);
    }

    @Test
    void shouldWriteFloat() throws Exception {
        float val = random.nextFloat();
        assertValWrittenThroughGenericPipe(p -> p.writeBytesOfFloat(val), DataInputStream::readFloat, val);
    }

    @Test
    void shouldWriteBoolean() throws Exception {
        boolean val = random.nextBoolean();
        assertValWrittenThroughGenericPipe(p -> p.writeBytesOfBoolean(val), DataInputStream::readBoolean, val);
    }

    @Test
    void shouldWriteChars() throws Exception {
        char[] primitiveCharArr = {1, 2};
        Character[] chars = new Character[] {1, 2};
        assertArrayWrittenThroughGenericPipe(p -> p.writeCharsUTF8(primitiveCharArr), this::dataInputStreamToChars,
                        chars);
    }

    private Character[] dataInputStreamToChars(DataInputStream dataInputStream) throws IOException {
        return Chars.asList(new String(readFully(dataInputStream)).toCharArray()).toArray(new Character[0]);
    }

    <T> void assertArrayWrittenThroughGenericPipe(RethrowableConsumer<GenericPipe> pipe,
                    RethrowableFunction<DataInputStream, T[]> inMapper, T[] expectedVal) throws Exception {
        shouldWriteThroughGenericPipe(pipe, inMapper, expectedVal, Assertions::assertArrayEquals);
    }

    @Test
    void shouldWriteCharsFromRange() throws Exception {
        char[] primitiveCharArr = {1, 2, 3};
        Character[] chars = new Character[] {2, 3};
        assertArrayWrittenThroughGenericPipe(p -> p.writeCharsUTF8(primitiveCharArr, 1, 2),
                        this::dataInputStreamToChars, chars);
    }

    @Test
    void shouldWriteStringUTF8() throws Exception {
        String str = "abcde";
        assertValWrittenThroughGenericPipe(p -> p.writeStringUTF8(str),
                        inputStream -> new String(readFully(inputStream), StandardCharsets.UTF_8), str);
    }

    @Test
    void shouldProperlyEncodeBothStringAndCharsInUTF8() throws Exception {
        for (char c = 0; c < 0xFFFF; c++) {
            char[] chars = {c};

            byte[] encodedStringUTF8ThroughGenericPipe =
                            writeUTF8ThroughGenericPipe(gp -> gp.writeStringUTF8(new String(chars)));
            byte[] encodedCharsUTF8ThroughGenericPipe = writeUTF8ThroughGenericPipe(gp -> gp.writeCharsUTF8(chars));
            byte[] encodedThroughJavaStringClass = new String(chars).getBytes(StandardCharsets.UTF_8);

            assertArrayEquals(encodedThroughJavaStringClass, encodedCharsUTF8ThroughGenericPipe);
            assertArrayEquals(encodedThroughJavaStringClass, encodedStringUTF8ThroughGenericPipe);
        }
    }

    private byte[] writeUTF8ThroughGenericPipe(RethrowableConsumer<GenericPipe> contentProvider) throws Exception {
        final ByteArrayOutputStream os = new ByteArrayOutputStream();
        final ConcurrentOutputStreamBuffer buffer = ConcurrentOutputStreamBuffer.builder(os).build();
        ConcurrentOutputStreamBuffer.CompletionNotifier notifier = buffer.declareExactProducersCount(1);

        final CommittedGenericPipe pipe = buffer.acquireWritableGenericPipe();

        contentProvider.accept(pipe);
        pipe.commit();
        pipe.close();

        notifier.awaitCompletion();
        return os.toByteArray();
    }

    @Test
    void shouldWriteStringUTF8FromRange() throws Exception {
        String str = "abcd";
        assertValWrittenThroughGenericPipe(p -> p.writeStringUTF8(str, 2, 2),
                        inputStream -> new String(readFully(inputStream), StandardCharsets.UTF_8), str.substring(2, 4));
    }

    @Test
    void secondaryCloseInvocationShouldHasNoEffectNeitherShouldThrow() {
        final ConcurrentOutputStreamBuffer buffer =
                        ConcurrentOutputStreamBuffer.builder(new ByteArrayOutputStream()).build();
        buffer.declareExactProducersCount(1);

        assertDoesNotThrow(() -> {
            submitTaskToSeparateThread(() -> {
                GenericPipe genericPipe = buffer.acquireWritableGenericPipe();
                genericPipe.close();

                // 2nd invocation
                genericPipe.close();
            }).get();
        });
    }
}
