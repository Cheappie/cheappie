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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.io.Closeable;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
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
    private final int minBytesWrittenUntilFlush;
    private final int memThrottleFactor;
    private final long millisUntilThrottleProducer;
    private final boolean writeEqualChunks;

    private final ScheduledExecutorService executor;
    private final ConcurrentHashMap<Integer, List<Buffer>> producerAssociatedBuffers;
    private final CopyOnWriteArrayList<Buffer> allProducersBuffers;
    private final CopyOnWriteArrayList<Integer> detachedPipes;

    private final AtomicBoolean isStreamClosed;
    private final AtomicInteger activePipesCount;
    private final AtomicBoolean hasDirtyWrite;

    private final AtomicInteger availableSlotsForProducers;
    private final AtomicBoolean areProducersAlreadyDeclared;
    private final Phaser awaitProducersWithTimeoutPhaser;

    private ConcurrentOutputStreamBuffer(OutputStream outputStream, int minBytesWrittenUntilFlush,
                    int memThrottleFactor, long millisUntilThrottleProducer, boolean writeEqualChunks) {
        this.outputStream = outputStream;
        this.minBytesWrittenUntilFlush = minBytesWrittenUntilFlush;
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
    ConcurrentOutputStreamBuffer(OutputStream outputStream, int minBytesWrittenUntilFlush, int memThrottleFactor,
                    long millisUntilThrottleProducer, boolean writeEqualChunks, ScheduledExecutorService executor,
                    ConcurrentHashMap<Integer, List<Buffer>> producerAssociatedBuffers,
                    CopyOnWriteArrayList<Buffer> allProducersBuffers, CopyOnWriteArrayList<Integer> detachedPipes,
                    AtomicBoolean isStreamClosed, AtomicInteger activePipesCount, AtomicBoolean hasDirtyWrite,
                    AtomicInteger availableSlotsForProducers, AtomicBoolean areProducersAlreadyDeclared,
                    Phaser awaitProducersWithTimeoutPhaser) {
        this.outputStream = outputStream;
        this.minBytesWrittenUntilFlush = minBytesWrittenUntilFlush;
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

    public StringPipe acquireWritableStringPipe() {
        return acquireWritableStringPipe(Charset.defaultCharset());
    }

    public StringPipe acquireWritableStringPipe(Charset charset) {
        int producerId = registerPipe();
        initBuffers(producerId);

        return new StringPipe(this, producerId, charset);
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
        ArrayList<Buffer> buffers = Stream.generate(() -> new Buffer(minBytesWrittenUntilFlush, memThrottleFactor))
                        .limit(2).collect(toCollection(ArrayList::new));

        producerAssociatedBuffers.put(producerId, buffers);
        allProducersBuffers.addAll(buffers);
    }

    public GenericPipe acquireWritableGenericPipe() {
        final int producerId = registerPipe();
        initBuffers(producerId);

        return new GenericPipe(this, producerId);
    }

    private void write(byte[] b, int off, int len, Integer producerId) throws IOException {
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
                    if (buffer.lock.tryLock()) {
                        try {
                            WeakFixedByteArray buf = buffer.byteArray;
                            if (buf.write(b, off, len)) {
                                return;
                            }
                        } finally {
                            buffer.lock.unlock();
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
                awaitProducersWithTimeoutPhaser.awaitAdvanceInterruptibly(0, 10, TimeUnit.SECONDS);

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
                outputStream.close();
            }

            executor.shutdown();
            return null;
        });
    }

    private int transferBytesToOutpuStreamFrom(List<Buffer> buffers) throws IOException {
        int emptyBuffersCount = 0;

        for (Buffer buffer : buffers) {
            if (buffer.lock.tryLock()) {
                try {
                    WeakFixedByteArray buf = buffer.byteArray;
                    if (buf.size() > 0) {
                        buf.writeTo(outputStream, writeEqualChunks);
                        buf.reset();
                    } else {
                        emptyBuffersCount++;
                    }
                } finally {
                    buffer.lock.unlock();
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

    public static Builder builder(OutputStream outputStream) {
        return new Builder(outputStream);
    }

    public static final class Builder {
        private final OutputStream outputStream;
        private int minBytesWrittenUntilFlush = 8192;
        private int memThrottleFactor = 64;
        private boolean writeEqualChunks = false;
        private long millisUntilThrottleProducer = 100;

        private Builder(OutputStream outputStream) {
            Preconditions.checkNotNull(outputStream, "Required: outputStream not null");
            this.outputStream = outputStream;
        }

        public Builder withMinBytesWrittenUntilFlush(int minBytesWrittenUntilFlush) {
            Preconditions.checkArgument(minBytesWrittenUntilFlush >= 128, "Required: minBytesWrittenUntilFlush >= 128");
            this.minBytesWrittenUntilFlush = minBytesWrittenUntilFlush;
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
            return new ConcurrentOutputStreamBuffer(outputStream, minBytesWrittenUntilFlush, memThrottleFactor,
                            millisUntilThrottleProducer, writeEqualChunks);
        }
    }

    @VisibleForTesting
    static final class Buffer {
        final ReentrantLock lock;
        final WeakFixedByteArray byteArray;
        final Object condition;

        Buffer(int chunkSize, int memThrottleFactor) {
            this.lock = new ReentrantLock();
            this.condition = new Object();

            int initialSize = chunkSize * INITIAL_BUFFER_SIZE_MULTIPLIER;
            this.byteArray = new WeakFixedByteArray(chunkSize, initialSize, memThrottleFactor);
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
    static final class BytePipe implements Closeable {
        private final Integer producerId;
        private final ByteArray locBuffer;
        private final ConcurrentOutputStreamBuffer concurrentOutputStreamBuffer;

        private int lastCommitIndex = 0;
        private boolean isPipeClosed = false;

        private BytePipe(ConcurrentOutputStreamBuffer concurrentOutputStreamBuffer, Integer producerId) {
            this.producerId = producerId;
            this.locBuffer = new ByteArray(concurrentOutputStreamBuffer.minBytesWrittenUntilFlush);
            this.concurrentOutputStreamBuffer = concurrentOutputStreamBuffer;
        }

        boolean isPipeClosed() {
            return isPipeClosed;
        }

        void commit() {
            lastCommitIndex = locBuffer.size();
        }

        void write(int b) throws IOException {
            ensureOpen();

            flush(1, false);
            locBuffer.write(b);
        }

        private void flush(int incomingBytesCount, boolean forceWriteCommited) throws IOException {
            try {
                if ((forceWriteCommited || locBuffer.size()
                                + incomingBytesCount >= concurrentOutputStreamBuffer.minBytesWrittenUntilFlush)
                                && lastCommitIndex > 0) {
                    concurrentOutputStreamBuffer.write(locBuffer.elements(), 0, lastCommitIndex, producerId);
                    locBuffer.softTrim(lastCommitIndex);
                    lastCommitIndex = 0;
                }
            } catch (Exception e) {
                closePipeAndCleanup();
                throw e;
            }
        }

        private void ensureOpen() throws IOException {
            if (isPipeClosed) {
                throw new IOException("Pipe has been closed.");
            }
        }

        void write(byte[] b) throws IOException {
            ensureOpen();

            flush(b.length, false);
            locBuffer.write(b);
        }

        void write(byte[] b, int readOffset, int len) throws IOException {
            ensureOpen();

            flush(len, false);
            locBuffer.write(b, readOffset, len);
        }

        @Override
        public void close() throws IOException {
            if (isPipeClosed) {
                return;
            }

            flush(locBuffer.size(), true);
            closePipeAndCleanup();
        }

        private void closePipeAndCleanup() {
            isPipeClosed = true;
            concurrentOutputStreamBuffer.activePipesCount.decrementAndGet();
            concurrentOutputStreamBuffer.detachedPipes.add(producerId);
        }
    }

    @VisibleForTesting
    static final class BytePipeToOutputStreamGateway extends OutputStream {
        private final BytePipe pipe;

        private BytePipeToOutputStreamGateway(BytePipe pipe) {
            this.pipe = pipe;
        }

        @Override
        public void write(int b) throws IOException {
            pipe.write(b);
        }

        @Override
        public void write(byte[] b) throws IOException {
            pipe.write(b);
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            pipe.write(b, off, len);
        }
    }

    public static final class GenericPipe implements Closeable {
        private final BytePipe pipe;
        private final DataOutputStream dataOutputStream;
        private final BytePipeToOutputStreamGateway bytePipeGateway;
        private final ByteArray utf8TemporaryBuffer;

        private GenericPipe(ConcurrentOutputStreamBuffer concurrentOutputStreamBuffer, Integer producerId) {
            this.pipe = new BytePipe(concurrentOutputStreamBuffer, producerId);

            this.bytePipeGateway = new BytePipeToOutputStreamGateway(pipe);
            this.dataOutputStream = new DataOutputStream(bytePipeGateway);
            this.utf8TemporaryBuffer = new ByteArray(concurrentOutputStreamBuffer.minBytesWrittenUntilFlush);
        }

        public void write(int b) throws IOException {
            pipe.write(b);
        }

        public void write(byte[] b) throws IOException {
            pipe.write(b);
        }

        public void write(byte[] b, int readOffset, int len) throws IOException {
            pipe.write(b, readOffset, len);
        }

        public void commit() {
            pipe.commit();
        }

        public void writeBytesOfBoolean(boolean v) throws IOException {
            dataOutputStream.writeBoolean(v);
        }

        public void writeBytesOfShort(int v) throws IOException {
            dataOutputStream.writeShort(v);
        }

        public void writeBytesOfInt(int v) throws IOException {
            dataOutputStream.writeInt(v);
        }

        public void writeBytesOfLong(long v) throws IOException {
            dataOutputStream.writeLong(v);
        }

        public void writeBytesOfFloat(float v) throws IOException {
            dataOutputStream.writeFloat(v);
        }

        public void writeBytesOfDouble(double v) throws IOException {
            dataOutputStream.writeDouble(v);
        }

        public void writeStringUTF8(String str) throws IOException {
            writeStringUTF8(str, 0, str.length());
        }

        public void writeStringUTF8(String str, int off, int len) throws IOException {
            for (int i = 0; i < len; i++) {
                char c = str.charAt(off + i);

                if (c < 0x80) {
                    utf8TemporaryBuffer.write(c);
                } else if (c < 0x800) {
                    utf8TemporaryBuffer.write(0xc0 | (c >> 6));
                    utf8TemporaryBuffer.write(0x80 | (c & 0x3f));
                } else if (!Character.isSurrogate(c)) {
                    utf8TemporaryBuffer.write(0xe0 | (c >> 12));
                    utf8TemporaryBuffer.write(0x80 | ((c >> 6) & 0x3f));
                    utf8TemporaryBuffer.write(0x80 | (c & 0x3f));
                } else {
                    utf8TemporaryBuffer.reset();
                    bytePipeGateway.write(str.getBytes(StandardCharsets.UTF_8));
                    return;
                }
            }
            utf8TemporaryBuffer.resetAfterWriteTo(bytePipeGateway);
        }

        public void writeCharsUTF8(char[] chars) throws IOException {
            writeCharsUTF8(chars, 0, chars.length);
        }

        public void writeCharsUTF8(char[] chars, int off, int len) throws IOException {
            for (int i = 0; i < len; i++) {
                char c = chars[off + i];

                if (c < 0x80) {
                    utf8TemporaryBuffer.write(c);
                } else if (c < 0x800) {
                    utf8TemporaryBuffer.write(0xc0 | (c >> 6));
                    utf8TemporaryBuffer.write(0x80 | (c & 0x3F));
                } else if (!Character.isSurrogate(c)) {
                    utf8TemporaryBuffer.write(0xe0 | (c >> 12));
                    utf8TemporaryBuffer.write(0x80 | ((c >> 6) & 0x3F));
                    utf8TemporaryBuffer.write(0x80 | (c & 0x3F));
                } else {
                    utf8TemporaryBuffer.reset();
                    bytePipeGateway.write(new String(chars).getBytes(StandardCharsets.UTF_8));
                    return;
                }
            }
            utf8TemporaryBuffer.resetAfterWriteTo(bytePipeGateway);
        }

        @Override
        public void close() throws IOException {
            if (pipe.isPipeClosed()) {
                return;
            }

            pipe.close();
        }
    }

    static final class StringPipe implements Closeable {
        private final String lineSeparator;
        private final OutputStreamWriter stringPipeOutputStream;
        private final StringBuilder buffer;
        private final BytePipe pipe;

        private final char[] transportBuffer;
        private final int chunkSize;
        private int lastCommitIndex;

        private StringPipe(ConcurrentOutputStreamBuffer concurrentOutputStreamBuffer, Integer producerId,
                        Charset charset) {
            int size = concurrentOutputStreamBuffer.minBytesWrittenUntilFlush;

            this.lineSeparator = System.lineSeparator();
            this.pipe = new BytePipe(concurrentOutputStreamBuffer, producerId);
            this.stringPipeOutputStream = new OutputStreamWriter(new BytePipeToOutputStreamGateway(pipe), charset);

            this.buffer = new StringBuilder(size);
            this.transportBuffer = new char[size];
            this.chunkSize = size;
            this.lastCommitIndex = 0;
        }

        private void writeTo(OutputStreamWriter outputStreamWriter, int readOffset, int len) throws IOException {
            int skipLastChunkDuringIteration = len - chunkSize;

            while (readOffset < skipLastChunkDuringIteration) {
                int nextEndIndex = readOffset + chunkSize;

                buffer.getChars(readOffset, nextEndIndex, transportBuffer, 0);
                outputStreamWriter.write(transportBuffer);

                readOffset += chunkSize;
            }

            int leftOvers = len - readOffset;
            if (leftOvers > 0) {
                buffer.getChars(readOffset, readOffset + leftOvers, transportBuffer, 0);
                outputStreamWriter.write(transportBuffer, 0, leftOvers);
            }
        }

        public void commit() {
            this.lastCommitIndex = buffer.length();
        }

        private void flushCommitedWhenBufferExceedsSingleChunkSize() throws IOException {
            if (buffer.length() >= chunkSize && lastCommitIndex > 0) {
                flushCommited();
                buffer.delete(0, lastCommitIndex);
                lastCommitIndex = 0;
            }
        }

        private void flushCommited() throws IOException {
            writeTo(stringPipeOutputStream, 0, lastCommitIndex);
            stringPipeOutputStream.flush();
            pipe.commit();
        }

        public void write(String str) throws IOException {
            buffer.append(str);
            flushCommitedWhenBufferExceedsSingleChunkSize();
        }

        public void write(StringBuffer sb) throws IOException {
            buffer.append(sb);
            flushCommitedWhenBufferExceedsSingleChunkSize();
        }

        public void write(CharSequence s) throws IOException {
            buffer.append(s);
            flushCommitedWhenBufferExceedsSingleChunkSize();
        }

        public void write(CharSequence s, int start, int end) throws IOException {
            buffer.append(s, start, end);
            flushCommitedWhenBufferExceedsSingleChunkSize();
        }

        public void write(char[] str) throws IOException {
            buffer.append(str);
            flushCommitedWhenBufferExceedsSingleChunkSize();
        }

        public void write(char[] str, int offset, int len) throws IOException {
            buffer.append(str, offset, len);
            flushCommitedWhenBufferExceedsSingleChunkSize();
        }

        public void write(boolean b) throws IOException {
            buffer.append(b);
            flushCommitedWhenBufferExceedsSingleChunkSize();
        }

        public void write(char c) throws IOException {
            buffer.append(c);
            flushCommitedWhenBufferExceedsSingleChunkSize();
        }

        public void write(int i) throws IOException {
            buffer.append(i);
            flushCommitedWhenBufferExceedsSingleChunkSize();
        }

        public void write(long lng) throws IOException {
            buffer.append(lng);
            flushCommitedWhenBufferExceedsSingleChunkSize();
        }

        public void write(float f) throws IOException {
            buffer.append(f);
            flushCommitedWhenBufferExceedsSingleChunkSize();
        }

        public void write(double d) throws IOException {
            buffer.append(d);
            flushCommitedWhenBufferExceedsSingleChunkSize();
        }

        public void newLine() throws IOException {
            buffer.append(lineSeparator);
            flushCommitedWhenBufferExceedsSingleChunkSize();
        }

        @Override
        public void close() throws IOException {
            if (pipe.isPipeClosed()) {
                return;
            }

            flushCommited();
            pipe.close();
        }
    }

    @VisibleForTesting
    static final class ByteArray {
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

        public void waitUntilDone() throws ExecutionException, InterruptedException {
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
                concurrentOutputStreamBuffer.executor.schedule(this, 1, TimeUnit.SECONDS);
            } else {
                concurrentOutputStreamBuffer.executor.shutdown();
            }
        }
    }
}
