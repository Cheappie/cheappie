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
import it.unimi.dsi.fastutil.ints.IntArrayList;
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

    private final BufferOutGateway bufferOutGateway;
    private final int memThrottleFactor;
    private final long millisUntilThrottleProducer;
    private final int minPipeTXSize;

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

    private ConcurrentOutputStreamBuffer(BufferOutGateway bufferOutGateway, int minPipeTXSize, int memThrottleFactor,
                    long millisUntilThrottleProducer) {
        this.bufferOutGateway = bufferOutGateway;
        this.minPipeTXSize = minPipeTXSize;
        this.memThrottleFactor = memThrottleFactor;
        this.millisUntilThrottleProducer = millisUntilThrottleProducer;

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
    ConcurrentOutputStreamBuffer(BufferOutGateway bufferOutGateway, int minPipeTXSize, int memThrottleFactor,
                    long millisUntilThrottleProducer, ScheduledExecutorService executor,
                    ConcurrentHashMap<Integer, List<Buffer>> producerAssociatedBuffers,
                    CopyOnWriteArrayList<Buffer> allProducersBuffers, CopyOnWriteArrayList<Integer> detachedPipes,
                    AtomicBoolean isStreamClosed, AtomicInteger activePipesCount, AtomicBoolean hasDirtyWrite,
                    AtomicInteger availableSlotsForProducers, AtomicBoolean areProducersAlreadyDeclared,
                    Phaser awaitProducersWithTimeoutPhaser) {
        this.bufferOutGateway = bufferOutGateway;
        this.minPipeTXSize = minPipeTXSize;
        this.memThrottleFactor = memThrottleFactor;
        this.millisUntilThrottleProducer = millisUntilThrottleProducer;
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

    public TXStringPipe acquireWritableStringPipe() {
        return acquireWritableStringPipe(Charset.defaultCharset());
    }

    public TXStringPipe acquireWritableStringPipe(Charset charset) {
        int producerId = registerPipe();
        initBuffers(producerId);

        BufferInGateway OutputStreamGateway = new BufferInGateway(this, producerId);
        return new TXStringPipe(new TXBytePipe(OutputStreamGateway, minPipeTXSize), charset);
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
        ArrayList<Buffer> buffers = Stream.generate(() -> new Buffer(minPipeTXSize, memThrottleFactor)).limit(2)
                        .collect(toCollection(ArrayList::new));

        producerAssociatedBuffers.put(producerId, buffers);
        allProducersBuffers.addAll(buffers);
    }

    public TXGenericPipe acquireWritableGenericPipe() {
        final int producerId = registerPipe();
        initBuffers(producerId);

        BufferInGateway OutputStreamGateway = new BufferInGateway(this, producerId);
        return new TXGenericPipe(new TXBytePipe(OutputStreamGateway, minPipeTXSize));
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
                            if (buffer.write(b, off, len)) {
                                buffer.commitBytes(len - off);
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
                    if (buffer.size() > 0) {
                        bufferOutGateway.eatBytes(buffer);
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
        bufferOutGateway.close();
    }

    public static Builder builder(OutputStream outputStream) {
        return new Builder(outputStream);
    }

    public static final class Builder {
        private final OutputStream outputStream;
        private int minPipeTXSize = 8192;
        private int memThrottleFactor = 64;
        private long millisUntilThrottleProducer = 100;
        private int desiredOutputChunkSize = 0;

        private Builder(OutputStream outputStream) {
            Preconditions.checkNotNull(outputStream, "Required: outputStream not null");
            this.outputStream = outputStream;
        }

        public Builder inWrite(int minPipeTXSize) {
            Preconditions.checkArgument(minPipeTXSize >= 128, "Required: minPipeTXSize >= 128");
            Preconditions.checkArgument(desiredOutputChunkSize == 0 || desiredOutputChunkSize >= minPipeTXSize,
                            "Required: outWrite >= inWrite");

            this.minPipeTXSize = minPipeTXSize;
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

        public Builder outWrite(int desiredOutputChunkSize) {
            Preconditions.checkArgument(desiredOutputChunkSize >= 128, "Required: desiredOutputChunkSize >= 128");
            Preconditions.checkArgument(desiredOutputChunkSize >= minPipeTXSize, "Required: outWrite >= inWrite");

            this.desiredOutputChunkSize = desiredOutputChunkSize;
            return this;
        }

        public ConcurrentOutputStreamBuffer build() {
            if (desiredOutputChunkSize != 0) {
                return new ConcurrentOutputStreamBuffer(
                                new ResizableOutputStreamGateway(outputStream, desiredOutputChunkSize), minPipeTXSize,
                                memThrottleFactor, millisUntilThrottleProducer);
            } else {
                return new ConcurrentOutputStreamBuffer(new SimplePassThroughGateway(outputStream), minPipeTXSize,
                                memThrottleFactor, millisUntilThrottleProducer);
            }
        }
    }

    @VisibleForTesting
    static final class Buffer {
        private final ReentrantLock lock;
        private final Object condition;
        private final WeakFixedByteArray byteArray;
        final Repository repository;

        Buffer(int chunkSize, int memThrottleFactor) {
            this.lock = new ReentrantLock();
            this.condition = new Object();
            this.repository = new Repository(memThrottleFactor);

            int initialSize = chunkSize * INITIAL_BUFFER_SIZE_MULTIPLIER;
            this.byteArray = new WeakFixedByteArray(chunkSize, initialSize, memThrottleFactor);
        }

        boolean write(byte[] b, int off, int len) {
            return byteArray.write(b, off, len);
        }

        void writeTo(OutputStream outputStream, int off, int len) throws IOException {
            byteArray.writeTo(outputStream, off, len);
        }

        void writeTo(ByteArray b, int off, int len) throws IOException {
            byteArray.writeTo(b, off, len);
        }

        void commitBytes(int len) {
            repository.commit(len);
        }

        void clear() {
            byteArray.clear();
        }

        int size() {
            return byteArray.size();
        }

        void size(int size) {
            byteArray.size(size);
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

    static class Repository {
        private final IntArrayList commits;
        private int totalCommittedBytes;

        Repository(int initialSize) {
            this.commits = new IntArrayList(initialSize);
            this.totalCommittedBytes = 0;
        }

        void commit(int len) {
            totalCommittedBytes += len;
            commits.add(len);
        }

        void reset() {
            commits.clear();
            totalCommittedBytes = 0;
        }

        int consume(int len) {
            if (totalCommittedBytes <= 0) {
                return 0;
            }

            int size = commits.size();
            if (size == 1 || len >= totalCommittedBytes) {
                return consumeAll();
            }

            int consumedBytes = 0;
            for (int i = 0; i < len; i++) {
                int reverseIdx = size - i - 1;
                consumedBytes += commits.getInt(reverseIdx);

                if (consumedBytes >= len) {
                    commits.size(size - (i + 1));
                    totalCommittedBytes -= consumedBytes;
                    break;
                }
            }

            return consumedBytes;
        }

        int consumeAll() {
            int total = totalCommittedBytes;
            reset();

            return total;
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

        void writeTo(OutputStream outputStream) throws IOException {
            writeTo(outputStream, 0, arr.length);
        }

        void writeTo(OutputStream outputStream, int off, int len) throws IOException {
            outputStream.write(arr, off, len);
        }

        void writeTo(ByteArray b, int off, int len) throws IOException {
            b.write(arr, off, len);
        }

        int size() {
            return count;
        }

        void size(int newSize) {
            count = Math.min(count, newSize);
        }

        void clear() {
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
