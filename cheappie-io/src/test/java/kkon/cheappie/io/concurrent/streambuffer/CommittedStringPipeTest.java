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
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.function.BiConsumer;
import static kkon.cheappie.io.concurrent.streambuffer.TestUtils.nChars;
import static kkon.cheappie.io.concurrent.streambuffer.TestUtils.readFully;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Nested
class CommittedStringPipeTest {
    private Random random = new Random();

    @Test
    void shouldWriteCommited() throws IOException, ExecutionException, InterruptedException {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        ConcurrentOutputStreamBuffer buffer = ConcurrentOutputStreamBuffer.builder(outputStream)
                        .withMinElementsWrittenUntilFlush(128).build();
        final ConcurrentOutputStreamBuffer.CompletionNotifier notifier = buffer.declareExactProducersCount(1);

        int commitedCharsCount = 126;
        int writtenCharsCount = commitedCharsCount;
        try (CommittedStringPipe pipe = buffer.acquireWritableStringPipe()) {
            pipe.write(nChars(commitedCharsCount));
            pipe.commit();

            pipe.write("1");
            writtenCharsCount += 1;

            assertEquals(commitedCharsCount + 1, pipe.size());

            pipe.write("1");
            writtenCharsCount += 1;

            assertEquals(writtenCharsCount - commitedCharsCount, pipe.size());
        }
        notifier.awaitCompletion();

        assertTrue(outputStream.size() >= commitedCharsCount);
    }

    @Test
    void shouldWriteNewLine() throws Exception {
        assertValWrittenThroughStringPipe(StringPipe::newLine, this::readFullyBytesToString, "\n");
    }

    void write(ByteArrayOutputStream outputStream, RethrowableConsumer<StringPipe> contentProvider) throws Exception {
        ConcurrentOutputStreamBuffer streamBuffer = ConcurrentOutputStreamBuffer.builder(outputStream).build();

        ConcurrentOutputStreamBuffer.CompletionNotifier notifier = streamBuffer.declareExactProducersCount(1);
        try (CommittedStringPipe pipe = streamBuffer.acquireWritableStringPipe()) {
            contentProvider.accept(pipe);
            pipe.commit();
        }
        notifier.awaitCompletion();
    }

    <T> void assertValWrittenThroughStringPipe(RethrowableConsumer<StringPipe> pipe,
                    RethrowableFunction<DataInputStream, T> inMapper, T expectedVal) throws Exception {
        shouldWriteThroughStringPipe(pipe, inMapper, expectedVal, Assertions::assertEquals);
    }

    <T> void shouldWriteThroughStringPipe(RethrowableConsumer<StringPipe> pipe,
                    RethrowableFunction<DataInputStream, T> inMapper, T expectedVal, BiConsumer<T, T> assertionMethod)
                    throws Exception {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

        write(outputStream, pipe);
        DataInputStream dataInputStream = new DataInputStream(new ByteArrayInputStream(outputStream.toByteArray()));
        assertionMethod.accept(expectedVal, inMapper.apply(dataInputStream));
    }

    private String readFullyBytesToString(DataInputStream dataInputStream) throws IOException {
        return new String(readFully(dataInputStream));
    }

    @Test
    void shouldWriteInt() throws Exception {
        int val = random.nextInt();
        assertValWrittenThroughStringPipe(p -> p.write(val), d -> Integer.parseInt(readFullyBytesToString(d)), val);
    }

    @Test
    void shouldWriteLong() throws Exception {
        long val = random.nextLong();
        assertValWrittenThroughStringPipe(p -> p.write(val), d -> Long.parseLong(readFullyBytesToString(d)), val);
    }

    @Test
    void shouldWriteBoolean() throws Exception {
        boolean val = random.nextBoolean();
        assertValWrittenThroughStringPipe(p -> p.write(val), d -> Boolean.parseBoolean(readFullyBytesToString(d)), val);
    }

    @Test
    void shouldWriteDouble() throws Exception {
        double val = random.nextDouble();
        assertValWrittenThroughStringPipe(p -> p.write(val), d -> Double.parseDouble(readFullyBytesToString(d)), val);
    }

    @Test
    void shouldWriteFloat() throws Exception {
        float val = random.nextFloat();
        assertValWrittenThroughStringPipe(p -> p.write(val), d -> Float.parseFloat(readFullyBytesToString(d)), val);
    }


    @Test
    void shouldWriteChar() throws Exception {
        char val = (char) random.nextInt(0x80);
        assertValWrittenThroughStringPipe(p -> p.write(val), d -> readFullyBytesToString(d).toCharArray()[0], val);
    }

    @Test
    void shouldWriteChars() throws Exception {
        char[] val = new char[] {(char) random.nextInt(0x80), (char) random.nextInt(0x80)};
        assertArrayWrittenThroughStringPipe(p -> p.write(val),
                        d -> Chars.asList(readFullyBytesToString(d).toCharArray()).toArray(),
                        new Character[] {val[0], val[1]});
    }

    @Test
    void shouldWriteCharsFromRange() throws Exception {
        char[] val = new char[] {(char) random.nextInt(0x80), (char) random.nextInt(0x80), (char) random.nextInt(0x80)};

        assertArrayWrittenThroughStringPipe(p -> p.write(val, 1, 2),
                        d -> Chars.asList(readFullyBytesToString(d).toCharArray()).toArray(),
                        new Character[] {val[1], val[2]});
    }

    <T> void assertArrayWrittenThroughStringPipe(RethrowableConsumer<StringPipe> pipe,
                    RethrowableFunction<DataInputStream, T[]> inMapper, T[] expectedVal) throws Exception {
        shouldWriteThroughStringPipe(pipe, inMapper, expectedVal, Assertions::assertArrayEquals);
    }

    @Test
    void shouldWriteString() throws Exception {
        String str = String.valueOf(random.nextInt());
        assertValWrittenThroughStringPipe(p -> p.write(str), this::readFullyBytesToString, str);
    }

    @Test
    void shouldWriteCharSequence() throws Exception {
        CharSequence charSequence = String.valueOf(random.nextInt());
        assertValWrittenThroughStringPipe(p -> p.write(charSequence), this::readFullyBytesToString, charSequence);
    }

    @Test
    void shouldWriteStringBuffer() throws Exception {
        StringBuffer stringBuffer = new StringBuffer();
        stringBuffer.append(random.nextInt());
        assertValWrittenThroughStringPipe(p -> p.write(stringBuffer), this::readFullyBytesToString,
                        stringBuffer.toString());
    }

    @Test
    void shouldWriteCharSequenceFromRange() throws Exception {
        CharSequence charSequence = String.valueOf(random.nextInt() | 0x800);
        assertValWrittenThroughStringPipe(p -> p.write(charSequence, 1, 3), this::readFullyBytesToString,
                        charSequence.subSequence(1, 3));
    }
}
