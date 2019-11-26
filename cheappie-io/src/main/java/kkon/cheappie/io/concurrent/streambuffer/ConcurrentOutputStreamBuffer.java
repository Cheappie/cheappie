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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Phaser;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Stream;
import static java.util.stream.Collectors.toCollection;
import static org.apache.commons.lang3.exception.ExceptionUtils.rethrow;


public final class ConcurrentOutputStreamBuffer {
    private static final int INITIAL_BUFFER_SIZE_MULTIPLIER = 8;

    private final OutputStream outputStream;
    private final int memThrottleFactor;
    private final long millisUntilThrottleProducer;
    private final boolean writeEqualChunks;
    private final int minElementsWrittenUntilFlush;

    private final ScheduledExecutorService executor;
    private final ConcurrentHashMap<Integer, List<Buffer>> producerAssociatedBuffers;
    private final CopyOnWriteArrayList<Buffer> allProducersBuffers;
    final CopyOnWriteArrayList<Integer> detachedPipes;

    private final AtomicBoolean isStreamClosed;
    private final AtomicBoolean hasDirtyWrite;
    final AtomicInteger activePipesCount;

    private final AtomicInteger availableSlotsForProducers;
    private final AtomicBoolean areProducersAlreadyDeclared;
    private final Phaser awaitProducersWithTimeoutPhaser;

    private ConcurrentOutputStreamBuffer(OutputStream outputStream, int minElementsWrittenUntilFlush,
                    int memThrottleFactor, long millisUntilThrottleProducer, boolean writeEqualChunks) {
        this.outputStream = outputStream;
        this.minElementsWrittenUntilFlush = minElementsWrittenUntilFlush;
        this.memThrottleFactor = memThrottleFactor;
        this.millisUntilThrottleProducer = millisUntilThrottleProducer;
        this.writeEqualChunks = writeEqualChunks;

        this.executor = Executors.newSingleThreadScheduledExecutor();
        this.producerAssociatedBuffers = new ConcurrentHashMap<>();
        this.allProducersBuffers = new CopyOnWriteArrayList<>();
        this.detachedPipes = new CopyOnWriteArrayList<>();

        this.isStreamClosed = new AtomicBoolean(false);
        this.hasDirtyWrite = new AtomicBoolean(false);
        this.activePipesCount = new AtomicInteger(0);

        this.availableSlotsForProducers = new AtomicInteger(0);
        this.areProducersAlreadyDeclared = new AtomicBoolean(false);
        this.awaitProducersWithTimeoutPhaser = new Phaser();
    }

    /**
     * This constructor is an attempt to allow mocks in tests without introducing external dependencies which can
     * overcome java encapsulation. Please do not use this constructor under any circumstances, there are no guarantees
     * that this constructor will remain the same!
     */
    @VisibleForTesting
    ConcurrentOutputStreamBuffer(OutputStream outputStream, int minElementsWrittenUntilFlush, int memThrottleFactor,
                    long millisUntilThrottleProducer, boolean writeEqualChunks, ScheduledExecutorService executor,
                    ConcurrentHashMap<Integer, List<Buffer>> producerAssociatedBuffers,
                    CopyOnWriteArrayList<Buffer> allProducersBuffers, CopyOnWriteArrayList<Integer> detachedPipes,
                    AtomicBoolean isStreamClosed, AtomicInteger activePipesCount, AtomicBoolean hasDirtyWrite,
                    AtomicInteger availableSlotsForProducers, AtomicBoolean areProducersAlreadyDeclared,
                    Phaser awaitProducersWithTimeoutPhaser) {
        this.outputStream = outputStream;
        this.minElementsWrittenUntilFlush = minElementsWrittenUntilFlush;
        this.memThrottleFactor = memThrottleFactor;
        this.millisUntilThrottleProducer = millisUntilThrottleProducer;
        this.writeEqualChunks = writeEqualChunks;
        this.executor = executor;
        this.producerAssociatedBuffers = producerAssociatedBuffers;
        this.allProducersBuffers = allProducersBuffers;
        this.detachedPipes = detachedPipes;
        this.isStreamClosed = isStreamClosed;
        this.activePipesCount = activePipesCount;
        this.hasDirtyWrite = hasDirtyWrite;
        this.availableSlotsForProducers = availableSlotsForProducers;
        this.areProducersAlreadyDeclared = areProducersAlreadyDeclared;
        this.awaitProducersWithTimeoutPhaser = awaitProducersWithTimeoutPhaser;
    }

    public CompletionNotifier declareExactProducersCount(int declaredProducersCount) {
        Preconditions.checkArgument(declaredProducersCount > 0, "Required: declaredProducersCount > 0");
        if (!areProducersAlreadyDeclared.compareAndSet(false, true)) {
            throw new IllegalArgumentException("Producers count was already declared.");
        }

        availableSlotsForProducers.set(declaredProducersCount);
        activePipesCount.set(declaredProducersCount);
        awaitProducersWithTimeoutPhaser.bulkRegister(declaredProducersCount);

        Future<Void> future = startTransfer();
        return new CompletionNotifier(future);
    }

    public CommittedStringPipe acquireWritableStringPipe() {
        return acquireWritableStringPipe(Charset.defaultCharset());
    }

    public CommittedStringPipe acquireWritableStringPipe(Charset charset) {
        int producerId = registerPipe();
        initBuffers(producerId);

        ConcurrentOutputStreamGateway OutputStreamGateway = new ConcurrentOutputStreamGateway(this, producerId);
        return new CommittedStringPipe(new CommittedBytePipe(OutputStreamGateway, minElementsWrittenUntilFlush),
                        charset);
    }

    private int registerPipe() {
        if (!areProducersAlreadyDeclared.get()) {
            throw new BufferException("Producers count must be declared before acquiring pipe.");
        }

        // eager registration
        int producerId = availableSlotsForProducers.decrementAndGet();
        if (producerId < 0) {
            availableSlotsForProducers.incrementAndGet();
            throw new BufferException("All declared producers have already arrived, if you wish to allow more threads "
                            + "writing to buffer then please increase producers count during declaration.");
        }

        awaitProducersWithTimeoutPhaser.arriveAndDeregister();
        return producerId;
    }

    private void initBuffers(Integer producerId) {
        ArrayList<Buffer> buffers = Stream.generate(() -> new Buffer(minElementsWrittenUntilFlush, memThrottleFactor))
                        .limit(2).collect(toCollection(ArrayList::new));

        producerAssociatedBuffers.put(producerId, buffers);
        allProducersBuffers.addAll(buffers);
    }

    public CommittedGenericPipe acquireWritableGenericPipe() {
        final int producerId = registerPipe();
        initBuffers(producerId);

        ConcurrentOutputStreamGateway OutputStreamGateway = new ConcurrentOutputStreamGateway(this, producerId);
        return new CommittedGenericPipe(new CommittedBytePipe(OutputStreamGateway, minElementsWrittenUntilFlush));
    }

    void write(byte[] b, int off, int len, Integer producerId) throws IOException {
        if (len > 0) {
            List<Buffer> associatedBuffers = producerAssociatedBuffers.get(producerId);
            long lastEpochMillis = System.currentTimeMillis();
            while (true) {
                if (isStreamClosed.get()) {
                    throw new IOException(
                                    "Write on closed stream is prohibited. More details may be acquired by awaiting broker completion.");
                }

                for (Buffer buffer : associatedBuffers) {
                    hasDirtyWrite.set(true);
                    if (buffer.rwTryLock()) {
                        try {
                            WeakFixedByteArray buf = buffer.byteArray;
                            if (buf.write(b, off, len)) {
                                return;
                            }
                        } finally {
                            buffer.rwUnlock();
                        }
                    }

                    if (System.currentTimeMillis() - lastEpochMillis >= millisUntilThrottleProducer) {
                        buffer.await();
                        lastEpochMillis = System.currentTimeMillis();
                        break;
                    }
                }
            }
        }
    }

    private Future<Void> startTransfer() {
        return executor.submit(() -> {
            try {
                awaitProducersWithTimeoutPhaser.awaitAdvanceInterruptibly(0, 20, TimeUnit.SECONDS);

                boolean taskRequireContinuation;
                do {
                    int producerBuffersCountBeforeTransfer = allProducersBuffers.size();
                    int emptyBuffersCount = transferBytesToOutpuStreamFrom(allProducersBuffers);
                    detachClosedPipes();

                    int producerBuffersCountAfterTransfer = allProducersBuffers.size();
                    taskRequireContinuation = emptyBuffersCount != producerBuffersCountAfterTransfer
                                    || producerBuffersCountBeforeTransfer != producerBuffersCountAfterTransfer
                                    || hasActivePipes() || hasDirtyWrite.getAndSet(false);
                } while (taskRequireContinuation);
            } catch (Exception e) {
                executor.submit(new TimedRecurringReleaseResourceTask(60, this));
                if (e instanceof TimeoutException) {
                    throw new BufferException("Broker Timeout: some producers are missing, they didn't acquire pipe.");
                }

                throw e;
            } finally {
                isStreamClosed.set(true);
            }

            close();
            return null;
        });
    }

    private int transferBytesToOutpuStreamFrom(List<Buffer> buffers) throws IOException {
        int emptyBuffersCount = 0;

        for (Buffer buffer : buffers) {
            if (buffer.rwTryLock()) {
                try {
                    WeakFixedByteArray buf = buffer.byteArray;
                    if (buf.size() > 0) {
                        buf.writeTo(outputStream, writeEqualChunks);
                        buf.reset();
                    } else {
                        emptyBuffersCount++;
                    }
                } finally {
                    buffer.rwUnlock();
                    buffer.signal();
                }
            }
        }

        return emptyBuffersCount;
    }

    private void detachClosedPipes() throws IOException {
        if (!detachedPipes.isEmpty()) {
            List<Buffer> detachedBuffers = new ArrayList<>();
            List<Integer> removedProducers = new ArrayList<>();

            for (Integer producerId : detachedPipes) {
                removedProducers.add(producerId);
                detachedBuffers.addAll(producerAssociatedBuffers.get(producerId));
                producerAssociatedBuffers.remove(producerId);
            }
            transferBytesToOutpuStreamFrom(detachedBuffers);

            detachedPipes.removeAll(removedProducers);
            allProducersBuffers.removeAll(detachedBuffers);
        }
    }

    private boolean hasActivePipes() {
        return activePipesCount.get() > 0;
    }

    private void close() throws IOException {
        executor.shutdown();
        outputStream.close();
    }

    public static Builder builder(OutputStream outputStream) {
        return new Builder(outputStream);
    }

    public static final class Builder {
        private final OutputStream outputStream;
        private int minElementsWrittenUntilFlush = 8192;
        private int memThrottleFactor = 64;
        private boolean writeEqualChunks = false;
        private long millisUntilThrottleProducer = 1000;

        private Builder(OutputStream outputStream) {
            Preconditions.checkNotNull(outputStream, "Required: outputStream not null");
            this.outputStream = outputStream;
        }

        public Builder withMinElementsWrittenUntilFlush(int minElementsWrittenUntilFlush) {
            Preconditions.checkArgument(minElementsWrittenUntilFlush >= 128,
                            "Required: minElementsWrittenUntilFlush >= 128");
            this.minElementsWrittenUntilFlush = minElementsWrittenUntilFlush;
            return this;
        }

        public Builder withMemoryThrottleFactor(int memThrottleFactor) {
            Preconditions.checkArgument(memThrottleFactor >= 8, "Required: memThrottleFactor >= 8");
            this.memThrottleFactor = memThrottleFactor;
            return this;
        }

        public Builder withMillisUntilThrottleProducer(long millis) {
            Preconditions.checkArgument(millis >= 1, "Required: millis >= 1");
            this.millisUntilThrottleProducer = millis;
            return this;
        }

        public Builder withEqualChunks(boolean writeEqualChunks) {
            this.writeEqualChunks = writeEqualChunks;
            return this;
        }

        public ConcurrentOutputStreamBuffer build() {
            return new ConcurrentOutputStreamBuffer(outputStream, minElementsWrittenUntilFlush, memThrottleFactor,
                            millisUntilThrottleProducer, writeEqualChunks);
        }
    }

    @VisibleForTesting
    static final class Buffer {
        private final ReentrantLock lock;
        final WeakFixedByteArray byteArray;
        final Object condition;

        Buffer(int chunkSize, int memThrottleFactor) {
            this.lock = new ReentrantLock();
            this.condition = new Object();

            int initialSize = chunkSize * INITIAL_BUFFER_SIZE_MULTIPLIER;
            this.byteArray = new WeakFixedByteArray(chunkSize, initialSize, memThrottleFactor);
        }

        void rwLock() {
            this.lock.lock();
        }

        boolean rwTryLock() {
            return this.lock.tryLock();
        }

        void rwUnlock() {
            this.lock.unlock();
        }

        void await() {
            synchronized (condition) {
                try {
                    condition.wait();
                } catch (InterruptedException e) {
                    rethrow(e);
                }
            }
        }

        void signal() {
            synchronized (condition) {
                condition.notifyAll();
            }
        }
    }

    @VisibleForTesting
    static final class WeakFixedByteArray {
        private final int maxSize;
        private final int chunkSize;
        private byte[] arr;
        private int count;

        WeakFixedByteArray(int chunkSize, int initialSize, int memThrottleFactor) {
            this.maxSize = chunkSize * memThrottleFactor;
            Preconditions.checkArgument(maxSize >= initialSize, "memThrottleFactor is too low.");

            this.chunkSize = chunkSize;
            this.arr = new byte[initialSize];
            this.count = 0;
        }

        byte[] elements() {
            return arr;
        }

        void writeTo(OutputStream outputStream, boolean equalChunks) throws IOException {
            if (equalChunks && count > chunkSize) {
                int skipLastChunkDuringIteration = count - chunkSize;
                int readOffset = 0;

                while (readOffset < skipLastChunkDuringIteration) {
                    outputStream.write(arr, readOffset, chunkSize);
                    readOffset += chunkSize;
                }

                int leftOvers = count - readOffset;
                if (leftOvers != 0) {
                    outputStream.write(arr, readOffset, leftOvers);
                }

            } else {
                outputStream.write(arr, 0, count);
            }
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

                if ((maxSize - count) >= len && arr.length < maxSize) {
                    resize(maxSize);
                    directWrite(b, readOffset, len);
                    return true;
                }

                if (len > arr.length) {
                    resize(requiredCapacity);
                    directWrite(b, readOffset, len);
                    return true;
                }
            }

            return false;
        }

        int freeSpace() {
            return arr.length - count;
        }

        private int estimateRequiredCapacity(int incomingBytesCount) {
            int newBufferLength = arr.length;

            while (newBufferLength < count + incomingBytesCount) {
                newBufferLength *= 2;
            }

            return newBufferLength;
        }

        private void resize(int newBufferLength) {
            this.arr = Arrays.copyOf(arr, newBufferLength);
        }

        private void directWrite(byte[] b, int off, int len) {
            System.arraycopy(b, off, arr, count, len);
            count += len;
        }
    }

    public static final class CompletionNotifier {
        private final Future<?> task;

        CompletionNotifier(Future<?> task) {
            this.task = task;
        }

        public boolean isTransferDone() {
            return task.isDone();
        }

        public void awaitCompletion() throws ExecutionException, InterruptedException {
            task.get();
        }
    }

    public static final class BufferException extends RuntimeException {
        private BufferException(String message) {
            super(message);
        }
    }

    @VisibleForTesting
    static final class TimedRecurringReleaseResourceTask implements Runnable {
        private final LocalDateTime deadline;
        private final ConcurrentOutputStreamBuffer concurrentOutputStreamBuffer;

        TimedRecurringReleaseResourceTask(int secondsUntilTimeout,
                        ConcurrentOutputStreamBuffer concurrentOutputStreamBuffer) {
            Preconditions.checkArgument(secondsUntilTimeout > 0);
            this.deadline = LocalDateTime.now().plusSeconds(secondsUntilTimeout);
            this.concurrentOutputStreamBuffer = concurrentOutputStreamBuffer;
        }

        @Override
        public void run() {
            if (LocalDateTime.now().isBefore(deadline) && concurrentOutputStreamBuffer.hasActivePipes()) {
                concurrentOutputStreamBuffer.allProducersBuffers.forEach(Buffer::signal);
                concurrentOutputStreamBuffer.executor.schedule(this, 100, TimeUnit.MILLISECONDS);
            } else {
                try {
                    concurrentOutputStreamBuffer.close();
                } catch (Exception ignored) {
                }
            }
        }
    }
}
