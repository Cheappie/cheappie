/*
 * Copyright (C) 2019 The Cheappie Authors
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

import com.google.common.primitives.Bytes;
import com.google.common.primitives.Chars;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Phaser;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.spy;


class ConcurrentOutputStreamBufferTest {
    private final Random random = new Random();

    @Test
    void shouldDetachClosedPipes() throws InterruptedException, ExecutionException, TimeoutException {
        Mock mockBuilder = Mock.builder();
        mockBuilder.producerAssociatedBuffers = spy(new ConcurrentHashMap<>());
        mockBuilder.allProducersBuffers = spy(new CopyOnWriteArrayList<>());

        ConcurrentOutputStreamBuffer buffer = mockBuilder.build();
        buffer.declareExactProducersCount(2);

        CountDownLatch firstThreadLatch = new CountDownLatch(1);
        CountDownLatch secondThreadLatch = new CountDownLatch(1);
        CountDownLatch awaitProducersRegistered = new CountDownLatch(2);

        submitTaskToSeparateThread(() -> {
            try (ConcurrentOutputStreamBuffer.GenericPipe pipe = buffer.acquireWritableGenericPipe()) {
                awaitProducersRegistered.countDown();
                firstThreadLatch.await();
            }
        });

        submitTaskToSeparateThread(() -> {
            try (ConcurrentOutputStreamBuffer.GenericPipe pipe = buffer.acquireWritableGenericPipe()) {
                awaitProducersRegistered.countDown();
                secondThreadLatch.await();
            }
        });

        awaitWithTimeout(awaitProducersRegistered::await, 5);
        awaitAvailableBuffersChange(2, 4, mockBuilder);

        // deregister single pipe
        firstThreadLatch.countDown();
        awaitAvailableBuffersChange(1, 2, mockBuilder);

        // release 2nd pipe
        secondThreadLatch.countDown();
    }

    private void awaitAvailableBuffersChange(int producerAssociatedBuffers, int allProducersBuffers, Mock mockBuilder)
                    throws InterruptedException, ExecutionException, TimeoutException {
        awaitWithTimeout(() -> {
            boolean isDone;
            do {
                isDone = mockBuilder.producerAssociatedBuffers.size() == producerAssociatedBuffers
                                && mockBuilder.allProducersBuffers.size() == allProducersBuffers;
            } while (!isDone);
        }, 10);
    }

    @Test
    void shouldForwardCommitedBytes() throws ExecutionException, InterruptedException, TimeoutException {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        ConcurrentOutputStreamBuffer streamBuffer =
                        ConcurrentOutputStreamBuffer.builder(outputStream).withMinBytesWrittenUntilFlush(128).build();

        byte[] byteArrayWhichExceedPipeBuffer = nBytes(256);
        final ConcurrentOutputStreamBuffer.CompletionNotifier notifier = streamBuffer.declareExactProducersCount(1);

        Future<?> future = submitTaskToSeparateThread(() -> {
            try (ConcurrentOutputStreamBuffer.GenericPipe pipe = streamBuffer.acquireWritableGenericPipe()) {
                pipe.write(byteArrayWhichExceedPipeBuffer);
                pipe.commit();
            }
        });

        awaitWithTimeout(() -> {
            future.get();
            notifier.waitUntilDone();
        }, 5);

        assertArrayEquals(byteArrayWhichExceedPipeBuffer, outputStream.toByteArray());
    }

    private static byte[] nBytes(int n) {
        byte[] bytes = new byte[n];

        for (int i = 0; i < bytes.length; i++) {
            bytes[i] = (byte) i;
        }

        return bytes;
    }

    private static <T> Future<T> submitTaskToSeparateThread(Procedure procedure) {
        ExecutorService executorService = Executors.newSingleThreadExecutor();

        try {
            return executorService.submit(() -> {
                procedure.invoke();
                return null;
            });
        } finally {
            executorService.shutdown();
        }
    }

    private interface Procedure {
        void invoke() throws Exception;
    }

    private static void awaitWithTimeout(Procedure procedure, long secondsUntilTimeout)
                    throws ExecutionException, InterruptedException, TimeoutException {
        try {
            submitTaskToSeparateThread(procedure).get(secondsUntilTimeout, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            throw new TimeoutException("Test Failure, unfortunately timeout was reached before task was done");
        }
    }

    @Test
    void shouldNotForwardNotCommitedBytes() throws InterruptedException, ExecutionException, TimeoutException {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        ConcurrentOutputStreamBuffer streamBuffer =
                        ConcurrentOutputStreamBuffer.builder(outputStream).withMinBytesWrittenUntilFlush(128).build();

        byte[] bytesWhichExceedPipeBuffer = nBytes(256);
        final ConcurrentOutputStreamBuffer.CompletionNotifier notifier = streamBuffer.declareExactProducersCount(1);
        Future<?> future = submitTaskToSeparateThread(() -> {
            try (ConcurrentOutputStreamBuffer.GenericPipe pipe = streamBuffer.acquireWritableGenericPipe()) {
                pipe.write(bytesWhichExceedPipeBuffer);
            }
        });

        awaitWithTimeout(() -> {
            future.get();
            notifier.waitUntilDone();
        }, 5);

        assertEquals(0, outputStream.toByteArray().length);
    }

    @ParameterizedTest
    @ArgumentsSource(DefaultConcurrency.class)
    void shouldReleaseAllProducersEvenFromWaitingStateOnFailure(int concurrency)
                    throws InterruptedException, ExecutionException, TimeoutException {
        CountDownLatch hangBrokerThread = new CountDownLatch(1);
        CountDownLatch confirmCloseMethodInvocation = new CountDownLatch(1);

        // blocking outputStream, simulate slow consumer which cause failure at some point
        OutputStream outputStream = new OutputStream() {
            @Override
            public void write(int b) throws IOException {
                throw new UnsupportedOperationException();
            }

            @Override
            public void write(byte[] b) throws IOException {
                throw new UnsupportedOperationException();
            }

            @Override
            public void write(byte[] b, int off, int len) throws IOException {
                try {
                    hangBrokerThread.await();
                    throw new RuntimeException("Any kind of failure");
                } catch (InterruptedException ex) {
                    ExceptionUtils.rethrow(ex);
                }
            }

            @Override
            public void close() throws IOException {
                confirmCloseMethodInvocation.countDown();
            }
        };

        ConcurrentOutputStreamBuffer streamBuffer =
                        ConcurrentOutputStreamBuffer.builder(outputStream).withMinBytesWrittenUntilFlush(128).build();
        ConcurrentOutputStreamBuffer.CompletionNotifier notifier = streamBuffer.declareExactProducersCount(concurrency);
        List<Thread> producers = new ArrayList<>();

        // lets fill up buffers
        AtomicInteger releasedProducersCounter = new AtomicInteger();
        for (int k = 0; k < concurrency; k++) {
            Thread thread = new Thread(() -> {
                try (ConcurrentOutputStreamBuffer.GenericPipe pipe = streamBuffer.acquireWritableGenericPipe()) {
                    while (true) {
                        pipe.write(1);
                        pipe.commit();
                    }
                } catch (IOException ex) {
                    releasedProducersCounter.incrementAndGet();
                }
            });

            producers.add(thread);
            thread.start();
        }

        // put producers to sleep when consumer can't keep up, to prevent cpu waste
        awaitProducers(producers, Thread.State.WAITING);
        hangBrokerThread.countDown();

        // confirm producers are released on failure
        awaitProducers(producers, Thread.State.TERMINATED);
        assertEquals(concurrency, releasedProducersCounter.get());

        assertTrue(notifier.isTransferDone());
        awaitWithTimeout(confirmCloseMethodInvocation::await, 10);
    }

    private void awaitProducers(List<Thread> producers, Thread.State state)
                    throws InterruptedException, ExecutionException, TimeoutException {
        awaitWithTimeout(() -> {
            boolean areAllThreadsWaiting;
            do {
                areAllThreadsWaiting = producers.stream().allMatch(t -> t.getState() == state);
            } while (!areAllThreadsWaiting);
        }, 10);
    }

    static class DefaultConcurrency implements ArgumentsProvider {
        @Override
        public Stream<Arguments> provideArguments(ExtensionContext context) {
            return IntStream.rangeClosed(1, 32).boxed().map(Arguments::of);
        }
    }

    @Test
    void shouldThrowWhenThereIsMoreProducersThanSpecifiedInDeclaration() throws IOException {
        ConcurrentOutputStreamBuffer buffer = ConcurrentOutputStreamBuffer.builder(new ByteArrayOutputStream()).build();

        buffer.declareExactProducersCount(1);
        buffer.acquireWritableGenericPipe().close();

        // 2nd acquire
        assertThrows(ConcurrentOutputStreamBuffer.BufferException.class, buffer::acquireWritableGenericPipe);
    }

    @Test
    void shouldThrowWhenProducersCountWasNotDeclared() {
        final ConcurrentOutputStreamBuffer buffer =
                        ConcurrentOutputStreamBuffer.builder(new ByteArrayOutputStream()).build();
        assertThrows(ConcurrentOutputStreamBuffer.BufferException.class, buffer::acquireWritableGenericPipe);
    }

    private interface RethrowableConsumer<T> {
        void accept(T val) throws Exception;
    }

    private interface RethrowableFunction<T, R> {
        R apply(T val) throws Exception;
    }

    @Nested
    class GenericPipeTest {

        @ParameterizedTest
        @ArgumentsSource(DefaultConcurrency.class)
        void shouldPreserveCommitedWriteConsistency(int concurrency)
                        throws ExecutionException, InterruptedException, TimeoutException {

            byte[][] simplePhrase = phrase();
            int repetitions = 1_000_00;

            assertWriteConsistency(concurrency, repetitions, streamBuffer -> {
                try (ConcurrentOutputStreamBuffer.GenericPipe pipe = streamBuffer.acquireWritableGenericPipe()) {
                    for (int i = 0; i < repetitions; i++) {
                        for (byte[] bytes : simplePhrase) {
                            pipe.write(bytes);
                        }

                        pipe.commit();
                    }
                }
            });
        }

        private byte[][] phrase() {
            String[] arrayOfSingleChars = originalPhrase().split("");
            return Arrays.stream(arrayOfSingleChars).map(String::getBytes).toArray(byte[][]::new);
        }

        @Test
        void shouldWriteSingleByte() throws Exception {
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

            byte _byte = 1;
            write(outputStream, pipe -> pipe.write(_byte));

            assertArrayEquals(new byte[] {_byte}, outputStream.toByteArray());
        }

        private void write(ByteArrayOutputStream outputStream,
                        RethrowableConsumer<ConcurrentOutputStreamBuffer.GenericPipe> contentProvider)
                        throws Exception {
            ConcurrentOutputStreamBuffer streamBuffer = ConcurrentOutputStreamBuffer.builder(outputStream)
                            .withMinBytesWrittenUntilFlush(1024).build();

            ConcurrentOutputStreamBuffer.CompletionNotifier notifier = streamBuffer.declareExactProducersCount(1);
            try (ConcurrentOutputStreamBuffer.GenericPipe pipe = streamBuffer.acquireWritableGenericPipe()) {
                contentProvider.accept(pipe);
                pipe.commit();
            }
            notifier.waitUntilDone();
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

        <T> void assertValWrittenThroughGenericPipe(RethrowableConsumer<ConcurrentOutputStreamBuffer.GenericPipe> pipe,
                        RethrowableFunction<DataInputStream, T> inMapper, T expectedVal) throws Exception {
            shouldWriteThroughGenericPipe(pipe, inMapper, expectedVal, Assertions::assertEquals);
        }

        <T> void shouldWriteThroughGenericPipe(RethrowableConsumer<ConcurrentOutputStreamBuffer.GenericPipe> pipe,
                        RethrowableFunction<DataInputStream, T> inMapper, T expectedVal,
                        BiConsumer<T, T> assertionMethod) throws Exception {
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

        <T> void assertArrayWrittenThroughGenericPipe(
                        RethrowableConsumer<ConcurrentOutputStreamBuffer.GenericPipe> pipe,
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

        private byte[] writeUTF8ThroughGenericPipe(
                        RethrowableConsumer<ConcurrentOutputStreamBuffer.GenericPipe> contentProvider)
                        throws Exception {
            final ByteArrayOutputStream os = new ByteArrayOutputStream();
            final ConcurrentOutputStreamBuffer buffer = ConcurrentOutputStreamBuffer.builder(os).build();
            ConcurrentOutputStreamBuffer.CompletionNotifier notifier = buffer.declareExactProducersCount(1);

            final ConcurrentOutputStreamBuffer.GenericPipe genericPipe = buffer.acquireWritableGenericPipe();

            contentProvider.accept(genericPipe);
            genericPipe.commit();
            genericPipe.close();

            notifier.waitUntilDone();
            return os.toByteArray();
        }

        @Test
        void shouldWriteStringUTF8FromRange() throws Exception {
            String str = "abcd";
            assertValWrittenThroughGenericPipe(p -> p.writeStringUTF8(str, 2, 2),
                            inputStream -> new String(readFully(inputStream), StandardCharsets.UTF_8),
                            str.substring(2, 4));
        }

        @Test
        void secondaryCloseInvocationShouldHasNoEffectNeitherShouldThrow() {
            final ConcurrentOutputStreamBuffer buffer =
                            ConcurrentOutputStreamBuffer.builder(new ByteArrayOutputStream()).build();
            buffer.declareExactProducersCount(1);

            assertDoesNotThrow(() -> {
                submitTaskToSeparateThread(() -> {
                    ConcurrentOutputStreamBuffer.GenericPipe genericPipe = buffer.acquireWritableGenericPipe();
                    genericPipe.close();

                    // 2nd invocation
                    genericPipe.close();
                }).get();
            });
        }
    }

    private byte[] readFully(DataInputStream in) throws IOException {
        List<Byte> bytes = new ArrayList<>();
        while (in.available() > 0) {
            bytes.add(in.readByte());
        }
        return Bytes.toArray(bytes);
    }


    private void assertWriteConsistency(int concurrency, int repetitions,
                    RethrowableConsumer<ConcurrentOutputStreamBuffer> contentProvider)
                    throws InterruptedException, ExecutionException, TimeoutException {

        CountDownLatch confirmCloseMethodInvocation = new CountDownLatch(1);
        ByteArrayOutputStream outputStream = spy(new ByteArrayOutputStream() {
            @Override
            public void close() {
                confirmCloseMethodInvocation.countDown();
            }
        });
        ConcurrentOutputStreamBuffer streamBuffer = ConcurrentOutputStreamBuffer.builder(outputStream).build();

        // simulate equal chances for producers
        CountDownLatch awaitProducersLatch = new CountDownLatch(concurrency);
        CountDownLatch mainExecutionLatch = new CountDownLatch(1);
        final ConcurrentOutputStreamBuffer.CompletionNotifier notifier =
                        streamBuffer.declareExactProducersCount(concurrency);

        for (int k = 0; k < concurrency; k++) {
            submitTaskToSeparateThread(() -> {
                awaitProducersLatch.countDown();
                mainExecutionLatch.await();

                contentProvider.accept(streamBuffer);
            });
        }
        awaitProducersLatch.await();
        mainExecutionLatch.countDown();
        notifier.waitUntilDone();

        String result = new String(outputStream.toByteArray());
        String originalPhrase = originalPhrase();

        // assert consistency
        assertEquals(concurrency * repetitions * originalPhrase.length(), result.length());
        assertEquals(0, result.replace(originalPhrase, "").length());

        assertTrue(notifier.isTransferDone());
        awaitWithTimeout(confirmCloseMethodInvocation::await, 10);
    }

    private static String originalPhrase() {
        return "abcdefghijklmnopqrstuvwxyz";
    }

    @Nested
    public class StringPipeTest {
        @ParameterizedTest
        @ArgumentsSource(DefaultConcurrency.class)
        void shouldPreserveCommitedWriteConsistency(int concurrency)
                        throws InterruptedException, ExecutionException, TimeoutException {
            String[] simplePhrase = originalPhrase().split("");
            int repetitions = 1_000_00;

            assertWriteConsistency(concurrency, repetitions, streamBuffer -> {
                try (ConcurrentOutputStreamBuffer.StringPipe pipe = streamBuffer.acquireWritableStringPipe()) {
                    for (int i = 0; i < repetitions; i++) {
                        for (String singleLetter : simplePhrase) {
                            pipe.write(singleLetter);
                        }

                        pipe.commit();
                    }
                }
            });
        }

        @Test
        void shouldWriteNewLine() throws Exception {
            assertValWrittenThroughStringPipe(ConcurrentOutputStreamBuffer.StringPipe::newLine,
                            this::readFullyBytesToString, "\n");
        }

        void write(ByteArrayOutputStream outputStream,
                        RethrowableConsumer<ConcurrentOutputStreamBuffer.StringPipe> contentProvider) throws Exception {
            ConcurrentOutputStreamBuffer streamBuffer = ConcurrentOutputStreamBuffer.builder(outputStream).build();

            ConcurrentOutputStreamBuffer.CompletionNotifier notifier = streamBuffer.declareExactProducersCount(1);
            try (ConcurrentOutputStreamBuffer.StringPipe pipe = streamBuffer.acquireWritableStringPipe()) {
                contentProvider.accept(pipe);
                pipe.commit();
            }
            notifier.waitUntilDone();
        }

        <T> void assertValWrittenThroughStringPipe(RethrowableConsumer<ConcurrentOutputStreamBuffer.StringPipe> pipe,
                        RethrowableFunction<DataInputStream, T> inMapper, T expectedVal) throws Exception {
            shouldWriteThroughStringPipe(pipe, inMapper, expectedVal, Assertions::assertEquals);
        }

        <T> void shouldWriteThroughStringPipe(RethrowableConsumer<ConcurrentOutputStreamBuffer.StringPipe> pipe,
                        RethrowableFunction<DataInputStream, T> inMapper, T expectedVal,
                        BiConsumer<T, T> assertionMethod) throws Exception {
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
            assertValWrittenThroughStringPipe(p -> p.write(val), d -> Boolean.parseBoolean(readFullyBytesToString(d)),
                            val);
        }

        @Test
        void shouldWriteDouble() throws Exception {
            double val = random.nextDouble();
            assertValWrittenThroughStringPipe(p -> p.write(val), d -> Double.parseDouble(readFullyBytesToString(d)),
                            val);
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
            char[] val = new char[] {(char) random.nextInt(0x80), (char) random.nextInt(0x80),
                    (char) random.nextInt(0x80)};

            assertArrayWrittenThroughStringPipe(p -> p.write(val, 1, 2),
                            d -> Chars.asList(readFullyBytesToString(d).toCharArray()).toArray(),
                            new Character[] {val[1], val[2]});
        }

        <T> void assertArrayWrittenThroughStringPipe(RethrowableConsumer<ConcurrentOutputStreamBuffer.StringPipe> pipe,
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


    @Nested
    class WeakFixedByteArrayTest {
        static final int INITIAL_SIZE = 6;
        static final int DEFAULT_THROTTLE_FACTOR = 2;
        static final int CHUNK_SIZE = 3;

        @Test
        void shouldWriteBytes() {
            ConcurrentOutputStreamBuffer.WeakFixedByteArray byteArray =
                            new ConcurrentOutputStreamBuffer.WeakFixedByteArray(CHUNK_SIZE, INITIAL_SIZE,
                                            DEFAULT_THROTTLE_FACTOR);

            byte[] bytes = new byte[] {1, 2, 3};
            byteArray.write(bytes, 0, bytes.length);

            assertEquals(INITIAL_SIZE, byteArray.elements().length);
            assertArrayEquals(bytes, Arrays.copyOfRange(byteArray.elements(), 0, bytes.length));
        }

        @Test
        void shouldRejectWriteWhenNotEnoughFreeSpace() {
            ConcurrentOutputStreamBuffer.WeakFixedByteArray byteArray =
                            new ConcurrentOutputStreamBuffer.WeakFixedByteArray(CHUNK_SIZE, INITIAL_SIZE,
                                            DEFAULT_THROTTLE_FACTOR);

            byte[] fillArrayUpToMaxSize = new byte[] {1, 2, 3, 4, 5, 6};
            byteArray.write(fillArrayUpToMaxSize, 0, fillArrayUpToMaxSize.length);

            byte[] bytes = new byte[] {7};
            boolean hasWrote = byteArray.write(bytes, 0, bytes.length);
            assertFalse(hasWrote);
            assertEquals(INITIAL_SIZE, byteArray.elements().length);
        }

        @Test
        void shouldResizeUntilUpperBoundHasBeenReached() {
            int throttleFactor = 2;
            int maxSize = CHUNK_SIZE * throttleFactor;

            ConcurrentOutputStreamBuffer.WeakFixedByteArray byteArray =
                            new ConcurrentOutputStreamBuffer.WeakFixedByteArray(CHUNK_SIZE, INITIAL_SIZE,
                                            throttleFactor);

            byte[] fillArrayUpToMaxSize = new byte[] {1, 2, 3, 4, 5, 6};
            boolean hasFilledArray = byteArray.write(fillArrayUpToMaxSize, 0, fillArrayUpToMaxSize.length);
            assertTrue(hasFilledArray);
            assertEquals(maxSize, byteArray.elements().length);

            byte[] bytes = new byte[] {7};
            boolean hasWrote = byteArray.write(bytes, 0, bytes.length);
            assertFalse(hasWrote);
        }

        @Test
        void shouldNotResizeUntilNecessary() {
            int throttleFactor = 2;
            int maxSize = CHUNK_SIZE * throttleFactor;

            ConcurrentOutputStreamBuffer.WeakFixedByteArray byteArray =
                            new ConcurrentOutputStreamBuffer.WeakFixedByteArray(CHUNK_SIZE, INITIAL_SIZE,
                                            throttleFactor);

            byte[] fillInitialArraySize = new byte[] {1, 2, 3};
            boolean hasFilledChunk = byteArray.write(fillInitialArraySize, 0, fillInitialArraySize.length);
            assertTrue(hasFilledChunk);
            assertEquals(INITIAL_SIZE, byteArray.elements().length);

            byte[] bytes = new byte[] {4};
            boolean hasWrote = byteArray.write(bytes, 0, bytes.length);
            assertTrue(hasWrote);
            assertEquals(maxSize, byteArray.elements().length);
        }

        @Test
        void shouldResizeFurtherWhenSingleWriteExceedsMaxSize() {
            int throttleFactor = 2;
            int maxSize = CHUNK_SIZE * throttleFactor;

            ConcurrentOutputStreamBuffer.WeakFixedByteArray byteArray =
                            new ConcurrentOutputStreamBuffer.WeakFixedByteArray(CHUNK_SIZE, INITIAL_SIZE,
                                            throttleFactor);

            byte[] bytesToExceedMaxSize = new byte[] {1, 2, 3, 4, 5, 6, 7};
            boolean hasWrote = byteArray.write(bytesToExceedMaxSize, 0, bytesToExceedMaxSize.length);
            assertTrue(hasWrote);
            assertTrue(byteArray.size() > maxSize);
            assertArrayEquals(bytesToExceedMaxSize,
                            Arrays.copyOfRange(byteArray.elements(), 0, bytesToExceedMaxSize.length));
        }

        @Test
        void shouldProperlyCountFreeSpace() {
            ConcurrentOutputStreamBuffer.WeakFixedByteArray byteArray =
                            new ConcurrentOutputStreamBuffer.WeakFixedByteArray(CHUNK_SIZE, INITIAL_SIZE,
                                            DEFAULT_THROTTLE_FACTOR);

            assertEquals(INITIAL_SIZE, byteArray.freeSpace());

            byteArray.write(new byte[] {1}, 0, 1);
            assertEquals(INITIAL_SIZE - 1, byteArray.freeSpace());
        }

        @Test
        void shouldResetSize() {
            ConcurrentOutputStreamBuffer.WeakFixedByteArray byteArray =
                            new ConcurrentOutputStreamBuffer.WeakFixedByteArray(CHUNK_SIZE, INITIAL_SIZE,
                                            DEFAULT_THROTTLE_FACTOR);

            assertEquals(0, byteArray.size());

            byteArray.write(new byte[] {1}, 0, 1);
            assertEquals(1, byteArray.size());

            byteArray.reset();
            assertEquals(0, byteArray.size());
        }

        @Test
        void shouldTransferBytes() throws IOException {
            ConcurrentOutputStreamBuffer.WeakFixedByteArray byteArray =
                            new ConcurrentOutputStreamBuffer.WeakFixedByteArray(CHUNK_SIZE, INITIAL_SIZE,
                                            DEFAULT_THROTTLE_FACTOR);

            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            byte[] bytes = new byte[] {1, 2, 3};
            byteArray.write(bytes, 0, bytes.length);
            byteArray.writeTo(outputStream, false);

            assertEquals(bytes.length, byteArray.size());
            assertArrayEquals(bytes, outputStream.toByteArray());
        }

        @Test
        void shouldWriteEqualChunks() throws IOException {
            int chunkSize = 2;
            ConcurrentOutputStreamBuffer.WeakFixedByteArray byteArray =
                            new ConcurrentOutputStreamBuffer.WeakFixedByteArray(chunkSize, 3, 5);

            byte[][] writtenChunks = new byte[][] {new byte[] {1, 2}, new byte[] {3, 4}, new byte[] {5}};
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream() {
                int index = 0;

                @Override
                public synchronized void write(byte[] b, int off, int len) {
                    assertArrayEquals(writtenChunks[index++], Arrays.copyOfRange(b, off, off + len));
                }
            };

            byteArray.write(new byte[] {1, 2, 3, 4, 5}, 0, 5);
            byteArray.writeTo(outputStream, true);
        }
    }

    @Nested
    class ByteArrayTest {
        static final int DEFAULT_SIZE = 3;

        @Test
        void shouldWriteSingleByte() {
            ConcurrentOutputStreamBuffer.ByteArray byteArray = new ConcurrentOutputStreamBuffer.ByteArray(DEFAULT_SIZE);

            byte _byte = 100;
            byteArray.write(_byte);
            assertEquals(_byte, byteArray.elements()[0]);
            assertEquals(1, byteArray.size());
        }

        @Test
        void shouldWriteArrayOfBytes() {
            byte[] bytes = new byte[] {1, 2, 3};

            ConcurrentOutputStreamBuffer.ByteArray byteArray = new ConcurrentOutputStreamBuffer.ByteArray(bytes.length);
            byteArray.write(bytes);
            assertArrayEquals(bytes, byteArray.elements());
        }

        @Test
        void shouldWriteArrayOfBytesUpToSpecifiedLength() {
            ConcurrentOutputStreamBuffer.ByteArray byteArray = new ConcurrentOutputStreamBuffer.ByteArray(DEFAULT_SIZE);

            byte[] bytes = new byte[] {1, 2};
            byteArray.write(bytes, 0, bytes.length);
            assertArrayEquals(bytes, Arrays.copyOfRange(byteArray.elements(), 0, bytes.length));
            assertEquals(2, byteArray.size());
        }

        @Test
        void shouldWriteFromSpecifiedReadOffset() {
            ConcurrentOutputStreamBuffer.ByteArray byteArray = new ConcurrentOutputStreamBuffer.ByteArray(DEFAULT_SIZE);

            byte[] expectedWrittenBytes = new byte[] {2, 3};
            int totalWrittenBytes = expectedWrittenBytes.length;

            byte[] sourceArray = ArrayUtils.insert(0, expectedWrittenBytes, (byte) 1);

            byteArray.write(sourceArray, 1, totalWrittenBytes);
            assertArrayEquals(expectedWrittenBytes, Arrays.copyOfRange(byteArray.elements(), 0, totalWrittenBytes));
            assertEquals(2, byteArray.size());
        }

        @Test
        void shouldResizeWhenNecessary() {
            ConcurrentOutputStreamBuffer.ByteArray byteArray = new ConcurrentOutputStreamBuffer.ByteArray(DEFAULT_SIZE);

            byte[] bytes = new byte[] {1, 2, 3, 4};
            assertTrue(bytes.length > DEFAULT_SIZE);
            byteArray.write(bytes);

            assertTrue(byteArray.size() > DEFAULT_SIZE);
            assertArrayEquals(bytes, Arrays.copyOfRange(byteArray.elements(), 0, bytes.length));
        }

        @Test
        void shouldResetSize() {
            ConcurrentOutputStreamBuffer.ByteArray byteArray = new ConcurrentOutputStreamBuffer.ByteArray(DEFAULT_SIZE);

            assertEquals(0, byteArray.size());

            byteArray.write(new byte[] {1}, 0, 1);
            assertEquals(1, byteArray.size());

            byteArray.reset();
            assertEquals(0, byteArray.size());
        }

        @Test
        void shouldPerformSoftTrimOfArrayContentUpToSpecifiedLength() {
            ConcurrentOutputStreamBuffer.ByteArray byteArray = new ConcurrentOutputStreamBuffer.ByteArray(DEFAULT_SIZE);

            int trimLen = 2;
            byte[] bytes = new byte[] {1, 2, 3};
            byteArray.write(bytes);
            byteArray.softTrim(trimLen);

            assertEquals(bytes.length - trimLen, byteArray.size());
            assertEquals(3, byteArray.elements()[0]);
        }

        @Test
        void shouldTransferBytes() throws IOException {
            ConcurrentOutputStreamBuffer.ByteArray byteArray = new ConcurrentOutputStreamBuffer.ByteArray(DEFAULT_SIZE);

            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            byte[] bytes = new byte[] {1, 2, 3};
            byteArray.write(bytes, 0, bytes.length);
            byteArray.writeTo(outputStream);

            assertEquals(bytes.length, byteArray.size());
            assertArrayEquals(bytes, outputStream.toByteArray());
        }
    }

    @Nested
    class TimedRecurringReleaseResourceTaskTest {
        @Test
        void shouldShutdownExecutorIfBufferDoesNotHaveActivePipes()
                        throws InterruptedException, ExecutionException, TimeoutException {
            Mock mockBuilder = Mock.builder();
            ConcurrentOutputStreamBuffer buffer = mockBuilder.build();

            ConcurrentOutputStreamBuffer.TimedRecurringReleaseResourceTask releaseResourceTask =
                            new ConcurrentOutputStreamBuffer.TimedRecurringReleaseResourceTask(10, buffer);

            awaitWithTimeout(releaseResourceTask::run, 5);
            assertTrue(mockBuilder.executor.isShutdown());
        }

        @Test
        void shouldWakeUpWaitingWriters() throws InterruptedException, ExecutionException, TimeoutException {
            Mock mockBuilder = Mock.builder();

            ConcurrentOutputStreamBuffer.Buffer buffer1 = new ConcurrentOutputStreamBuffer.Buffer(1, 10);
            ConcurrentOutputStreamBuffer.Buffer buffer2 = new ConcurrentOutputStreamBuffer.Buffer(1, 10);

            AtomicInteger activePipes = new AtomicInteger();
            mockBuilder.allProducersBuffers =
                            new CopyOnWriteArrayList<>(new ConcurrentOutputStreamBuffer.Buffer[] {buffer1, buffer2});
            mockBuilder.activePipesCount = activePipes;

            ConcurrentOutputStreamBuffer buffer = mockBuilder.build();

            ConcurrentOutputStreamBuffer.TimedRecurringReleaseResourceTask timedRecurringReleaseResourcesTask =
                            new ConcurrentOutputStreamBuffer.TimedRecurringReleaseResourceTask(20, buffer);

            submitTaskToSeparateThread(() -> {
                buffer1.await();
                activePipes.decrementAndGet();
            });

            submitTaskToSeparateThread(() -> {
                buffer2.await();
                activePipes.decrementAndGet();
            });

            awaitWithTimeout(timedRecurringReleaseResourcesTask::run, 10);
        }
    }

    private static class Mock {
        OutputStream outputStream = new ByteArrayOutputStream();
        int minBytesWrittenUntilFlush = 128;
        int memThrottleFactor = 8;
        long millisUntilThrottleProducer = 100;
        boolean writeEqualChunks = false;

        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
        ConcurrentHashMap<Integer, List<ConcurrentOutputStreamBuffer.Buffer>> producerAssociatedBuffers =
                        new ConcurrentHashMap<>();
        CopyOnWriteArrayList<ConcurrentOutputStreamBuffer.Buffer> allProducersBuffers = new CopyOnWriteArrayList<>();
        CopyOnWriteArrayList<Integer> detachedPipes = new CopyOnWriteArrayList<>();

        AtomicBoolean isStreamClosed = new AtomicBoolean();
        AtomicInteger activePipesCount = new AtomicInteger();
        AtomicBoolean hasDirtyWrite = new AtomicBoolean();

        AtomicInteger availableSlotsForProducers = new AtomicInteger();
        AtomicBoolean areProducersAlreadyDeclared = new AtomicBoolean();
        Phaser awaitProducersWithTimeoutPhaser = new Phaser();

        static Mock builder() {
            return new Mock();
        }

        ConcurrentOutputStreamBuffer build() {
            return new ConcurrentOutputStreamBuffer(outputStream, minBytesWrittenUntilFlush, memThrottleFactor,
                            millisUntilThrottleProducer, writeEqualChunks, executor, producerAssociatedBuffers,
                            allProducersBuffers, detachedPipes, isStreamClosed, activePipesCount, hasDirtyWrite,
                            availableSlotsForProducers, areProducersAlreadyDeclared, awaitProducersWithTimeoutPhaser);
        }
    }
}
