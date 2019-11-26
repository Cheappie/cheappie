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

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Phaser;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import static kkon.cheappie.io.concurrent.streambuffer.TestUtils.awaitWithTimeout;
import static kkon.cheappie.io.concurrent.streambuffer.TestUtils.nBytes;
import static kkon.cheappie.io.concurrent.streambuffer.TestUtils.submitTaskToSeparateThread;
import static org.apache.commons.lang3.exception.ExceptionUtils.rethrow;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.spy;


class ConcurrentOutputStreamBufferTest {

    @Test
    void check() {
        // todo
    }

    @Test
    void shouldCloseResourcesEvenWhenOutputStreamWouldCauseFailure() {
        Mock mockBuilder = Mock.builder();
        mockBuilder.outputStream = new OutputStream() {
            @Override
            public void write(int b) {}

            @Override
            public void close() throws IOException {
                throw new IOException("any kind of failure");
            }
        };

        ConcurrentOutputStreamBuffer buffer = mockBuilder.build();
        ConcurrentOutputStreamBuffer.CompletionNotifier notifier = buffer.declareExactProducersCount(1);

        submitTaskToSeparateThread(() -> {
            try (TXGenericPipe pipe = buffer.acquireWritableGenericPipe()) {
                pipe.write(1);
            }
        });

        try {
            notifier.awaitCompletion();
        } catch (Exception ignored) {
        }

        assertTrue(mockBuilder.executor.isShutdown());
    }

    @Test
    void producerShouldFillOnlyAssociatedBuffers() throws InterruptedException, ExecutionException, TimeoutException {
        Mock mockBuilder = Mock.builder();

        // setup small buffers to quickly fill them up
        mockBuilder.allProducersBuffers = spy(new CopyOnWriteArrayList<>());
        mockBuilder.memThrottleFactor = 8;
        mockBuilder.minBytesWrittenUntilFlush = 128;

        // phaser with 1 party will make broker thread await n + 1 producers to prevent any action from broker
        Phaser phaser = new Phaser(1);
        mockBuilder.awaitProducersWithTimeoutPhaser = phaser;

        ConcurrentOutputStreamBuffer buffer = mockBuilder.build();
        buffer.declareExactProducersCount(2);

        AtomicInteger activeProducerId = new AtomicInteger();
        CountDownLatch producerIdLatch = new CountDownLatch(1);

        Future<?> activelyWritingProducer = submitTaskToSeparateThread(() -> {
            try (TXGenericPipe pipe = buffer.acquireWritableGenericPipe()) {
                activeProducerId.set(pipe.getProducerId());
                producerIdLatch.countDown();

                while (!Thread.currentThread().isInterrupted()) {
                    pipe.write(1);
                    pipe.commit();
                }
            }
        });

        Future<?> lazyProducer = submitTaskToSeparateThread(() -> {
            try (TXGenericPipe pipe = buffer.acquireWritableGenericPipe()) {
                while (!Thread.currentThread().isInterrupted()) {
                }
            }
        });

        awaitWithTimeout(producerIdLatch::await, 5);

        try {
            assertEachProducerWriteOnlyToAssociatedBuffer(mockBuilder, activeProducerId.get());
        } finally {
            activelyWritingProducer.cancel(true);
            lazyProducer.cancel(true);
            phaser.arriveAndDeregister();
        }
    }

    private void assertEachProducerWriteOnlyToAssociatedBuffer(Mock mockBuilder, int activeProducerId)
                    throws InterruptedException, ExecutionException, TimeoutException {

        List<ConcurrentOutputStreamBuffer.Buffer> activelyWrittenBuffers =
                        mockBuilder.producerAssociatedBuffers.get(activeProducerId);

        List<ConcurrentOutputStreamBuffer.Buffer> lazyBuffers = mockBuilder.producerAssociatedBuffers.entrySet()
                        .stream().filter(p -> p.getKey() != activeProducerId).map(Map.Entry::getValue)
                        .flatMap(List::stream).collect(Collectors.toList());

        awaitWithTimeout(() -> {
            boolean isDataCorrectlyDistributed;

            do {
                isDataCorrectlyDistributed = true;

                for (ConcurrentOutputStreamBuffer.Buffer buffer : activelyWrittenBuffers) {
                    buffer.rwLock();
                    try {
                        if (buffer.size() == 0) {
                            isDataCorrectlyDistributed = false;
                        }
                    } finally {
                        buffer.rwUnlock();
                    }
                }

                for (ConcurrentOutputStreamBuffer.Buffer buffer : lazyBuffers) {
                    buffer.rwLock();
                    try {
                        if (buffer.size() != 0) {
                            isDataCorrectlyDistributed = false;
                        }
                    } finally {
                        buffer.rwUnlock();
                    }
                }

            } while (!isDataCorrectlyDistributed);
        }, 5);
    }

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
            try (TXGenericPipe pipe = buffer.acquireWritableGenericPipe()) {
                awaitProducersRegistered.countDown();
                firstThreadLatch.await();
            }
        });

        submitTaskToSeparateThread(() -> {
            try (TXGenericPipe pipe = buffer.acquireWritableGenericPipe()) {
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
                        ConcurrentOutputStreamBuffer.builder(outputStream).inWrite(128).build();

        byte[] byteArrayWhichExceedPipeBuffer = nBytes(256);
        final ConcurrentOutputStreamBuffer.CompletionNotifier notifier = streamBuffer.declareExactProducersCount(1);

        Future<?> future = submitTaskToSeparateThread(() -> {
            try (TXGenericPipe pipe = streamBuffer.acquireWritableGenericPipe()) {
                pipe.write(byteArrayWhichExceedPipeBuffer);
                pipe.commit();
            }
        });

        awaitWithTimeout(() -> {
            future.get();
            notifier.awaitCompletion();
        }, 5);

        assertArrayEquals(byteArrayWhichExceedPipeBuffer, outputStream.toByteArray());
    }

    @Test
    void shouldNotForwardNotCommitedBytes() throws InterruptedException, ExecutionException, TimeoutException {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        ConcurrentOutputStreamBuffer streamBuffer =
                        ConcurrentOutputStreamBuffer.builder(outputStream).inWrite(128).build();

        byte[] bytesWhichExceedPipeBuffer = nBytes(256);
        final ConcurrentOutputStreamBuffer.CompletionNotifier notifier = streamBuffer.declareExactProducersCount(1);
        Future<?> future = submitTaskToSeparateThread(() -> {
            try (TXGenericPipe pipe = streamBuffer.acquireWritableGenericPipe()) {
                pipe.write(bytesWhichExceedPipeBuffer);
            }
        });

        awaitWithTimeout(() -> {
            future.get();
            notifier.awaitCompletion();
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
                    rethrow(ex);
                }
            }

            @Override
            public void close() throws IOException {
                confirmCloseMethodInvocation.countDown();
            }
        };

        ConcurrentOutputStreamBuffer streamBuffer = ConcurrentOutputStreamBuffer.builder(outputStream).inWrite(128)
                        .withMillisUntilThrottleProducer(100).build();
        ConcurrentOutputStreamBuffer.CompletionNotifier notifier = streamBuffer.declareExactProducersCount(concurrency);
        List<Thread> producers = new ArrayList<>();

        // lets fill up buffers
        AtomicInteger releasedProducersCounter = new AtomicInteger();
        for (int k = 0; k < concurrency; k++) {
            Thread thread = new Thread(() -> {
                try (TXGenericPipe pipe = streamBuffer.acquireWritableGenericPipe()) {
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

    private static void awaitProducers(List<Thread> producers, Thread.State state)
                    throws InterruptedException, ExecutionException, TimeoutException {
        awaitWithTimeout(() -> {
            boolean areAllThreadsWaiting;
            do {
                areAllThreadsWaiting = producers.stream().allMatch(t -> t.getState() == state);
            } while (!areAllThreadsWaiting);
        }, 10);
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

    @ParameterizedTest
    @ArgumentsSource(DefaultConcurrency.class)
    void shouldPreserveCommitedWriteConsistencyThroughGenericPipe(int concurrency)
                    throws ExecutionException, InterruptedException, TimeoutException {

        byte[][] simplePhrase = phraseByteByByte();
        int repetitions = 1_000_00;

        assertWriteConsistency(concurrency, repetitions, streamBuffer -> {
            try (TXGenericPipe pipe = streamBuffer.acquireWritableGenericPipe()) {
                for (int i = 0; i < repetitions; i++) {
                    for (byte[] bytes : simplePhrase) {
                        pipe.write(bytes);
                    }

                    pipe.commit();
                }
            }
        });
    }

    @ParameterizedTest
    @ArgumentsSource(DefaultConcurrency.class)
    void shouldPreserveCommitedWriteConsistencyThroughStringPipe(int concurrency)
                    throws InterruptedException, ExecutionException, TimeoutException {
        String[] simplePhrase = originalPhrase().split("");
        int repetitions = 1_000_00;

        assertWriteConsistency(concurrency, repetitions, streamBuffer -> {
            try (TXStringPipe pipe = streamBuffer.acquireWritableStringPipe()) {
                for (int i = 0; i < repetitions; i++) {
                    for (String singleLetter : simplePhrase) {
                        pipe.write(singleLetter);
                    }

                    pipe.commit();
                }
            }
        });
    }


    static void assertWriteConsistency(int concurrency, int repetitions,
                    TestUtils.RethrowableConsumer<ConcurrentOutputStreamBuffer> contentProvider)
                    throws InterruptedException, ExecutionException, TimeoutException {

        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        ConcurrentOutputStreamBuffer streamBuffer =
                        ConcurrentOutputStreamBuffer.builder(outputStream).inWrite(512).outWrite(512).build();

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
        notifier.awaitCompletion();

        String result = new String(outputStream.toByteArray());
        String originalPhrase = originalPhrase();

        // assert consistency
        assertEquals(concurrency * repetitions * originalPhrase.length(), result.length());
        assertEquals(0, result.replace(originalPhrase, "").length());

        assertTrue(notifier.isTransferDone());
    }

    static String originalPhrase() {
        return "abcdefghijklmnopqrstuvwxyz";
    }

    static byte[][] phraseByteByByte() {
        String[] arrayOfSingleChars = originalPhrase().split("");
        return Arrays.stream(arrayOfSingleChars).map(String::getBytes).toArray(byte[][]::new);
    }

    @Nested
    class BufferTest {

        @Test
        void tryLockShouldGetRejectedWhenLockIsAlreadyTaken()
                        throws InterruptedException, TimeoutException, ExecutionException {
            ConcurrentOutputStreamBuffer.Buffer buffer = new ConcurrentOutputStreamBuffer.Buffer(1, 8);
            CountDownLatch separateThreadHasTakenLockLatch = new CountDownLatch(1);
            CountDownLatch awaitAssertionsLatch = new CountDownLatch(1);
            CountDownLatch releaseLockLatch = new CountDownLatch(1);

            submitTaskToSeparateThread(() -> {
                buffer.rwLock();

                separateThreadHasTakenLockLatch.countDown();
                awaitAssertionsLatch.await();

                releaseLockLatch.countDown();
                buffer.rwUnlock();
            });

            awaitWithTimeout(separateThreadHasTakenLockLatch::await, 5);
            assertFalse(buffer.rwTryLock());

            awaitAssertionsLatch.countDown();

            awaitWithTimeout(releaseLockLatch::await, 5);
            assertTrue(buffer.rwTryLock());
            buffer.rwUnlock();
        }

        @Test
        void shouldPutThreadIntoWaitingState() throws InterruptedException, ExecutionException, TimeoutException {
            ConcurrentOutputStreamBuffer.Buffer buffer = new ConcurrentOutputStreamBuffer.Buffer(1, 8);

            Thread th = new Thread(buffer::await);
            th.start();

            awaitProducers(Collections.singletonList(th), Thread.State.WAITING);
        }

        @Test
        void shouldReleaseThreadFromWaitingState() throws InterruptedException, ExecutionException, TimeoutException {
            ConcurrentOutputStreamBuffer.Buffer buffer = new ConcurrentOutputStreamBuffer.Buffer(1, 8);

            Thread th = new Thread(() -> {
                buffer.await();
                while (!Thread.currentThread().isInterrupted()) {
                }
            });
            th.start();

            List<Thread> producers = Collections.singletonList(th);
            awaitProducers(producers, Thread.State.WAITING);

            // release from waiting state
            buffer.signal();

            awaitProducers(producers, Thread.State.RUNNABLE);
            th.interrupt();
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

            byteArray.clear();
            assertEquals(0, byteArray.size());
        }

        // @Test
        // void shouldTransferBytes() throws IOException {
        // ConcurrentOutputStreamBuffer.WeakFixedByteArray byteArray =
        // new ConcurrentOutputStreamBuffer.WeakFixedByteArray(CHUNK_SIZE, INITIAL_SIZE,
        // DEFAULT_THROTTLE_FACTOR);
        //
        // ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        // byte[] bytes = new byte[] {1, 2, 3};
        // byteArray.write(bytes, 0, bytes.length);
        // byteArray.writeTo(outputStream, false);
        //
        // assertEquals(bytes.length, byteArray.size());
        // assertArrayEquals(bytes, outputStream.toByteArray());
        // }
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
            return new ConcurrentOutputStreamBuffer(new SimplePassThroughGateway(outputStream),
                            minBytesWrittenUntilFlush, memThrottleFactor, millisUntilThrottleProducer, executor,
                            producerAssociatedBuffers, allProducersBuffers, detachedPipes, isStreamClosed,
                            activePipesCount, hasDirtyWrite, availableSlotsForProducers, areProducersAlreadyDeclared,
                            awaitProducersWithTimeoutPhaser);
        }
    }
}
