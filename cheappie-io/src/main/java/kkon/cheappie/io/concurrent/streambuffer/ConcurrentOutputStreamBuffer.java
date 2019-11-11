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

import com.google.common.base.Preconditions;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toCollection;
import static org.apache.commons.lang3.exception.ExceptionUtils.rethrow;

public class ConcurrentOutputStreamBuffer implements Closeable {
    private final OutputStream outputStream;

    final int minBytesWrittenUntilFlush;
    private final int minAssumedConcurrency;
    private final int memThrottleFactor;
    private final int cpuThrottleFactor;

    private final ExecutorService executor;
    private final ConcurrentHashMap<Integer, List<Buffer>> writeBuffers;
    private final CopyOnWriteArrayList<Buffer> writeBuffersView;

    private final AtomicBoolean isStreamClosed;
    private final AtomicInteger uniquePipeIdGenerator;
    private final LongAdder dirtyWritesCount;
    final AtomicInteger activePipesCount;

    private final CompletionNotifier taskCompletionNotifier;

    private ConcurrentOutputStreamBuffer(OutputStream outputStream, int minAssumedConcurrency,
                                         int minBytesWrittenUntilFlush, int memThrottleFactor, int cpuThrottleFactor) {
        Preconditions.checkArgument(memThrottleFactor > minAssumedConcurrency,
                        "Required: throttleFactor > minAssumedConcurrency");

        this.outputStream = outputStream;
        this.minBytesWrittenUntilFlush = Math.max(128, minBytesWrittenUntilFlush);
        this.minAssumedConcurrency = Math.max(8, minAssumedConcurrency);
        this.memThrottleFactor = Math.max(64, memThrottleFactor);
        this.cpuThrottleFactor = Math.max(2, cpuThrottleFactor);

        this.executor = Executors.newSingleThreadExecutor();
        this.writeBuffers = new ConcurrentHashMap<>();
        this.writeBuffersView = new CopyOnWriteArrayList<>();

        this.isStreamClosed = new AtomicBoolean(false);
        this.uniquePipeIdGenerator = new AtomicInteger(0);
        this.dirtyWritesCount = new LongAdder();
        this.activePipesCount = new AtomicInteger(0);

        Future<Void> future = startTransfer();
        this.taskCompletionNotifier = new CompletionNotifier(future, isStreamClosed);
    }

    public static ConcurrentOutputStreamBuffer of(OutputStream outputStream, int minAssumedConcurrency,
                                                  int minBytesWrittenUntilFlush, int memThrottleFactor, int cpuThrottleFactor) {
        return new ConcurrentOutputStreamBuffer(outputStream, minAssumedConcurrency, minBytesWrittenUntilFlush,
                        memThrottleFactor, cpuThrottleFactor);
    }

    public Pipe acquireWritablePipe() throws IOException {
        // eager registration
        activePipesCount.incrementAndGet();

        if (isStreamClosed.get()) {
            activePipesCount.decrementAndGet();
            throw new IOException("Stream is closed.");
        }
        Integer pipeId = uniquePipeIdGenerator.incrementAndGet();
        ArrayList<Buffer> buffers = Stream.generate(
                        () -> new Buffer(minBytesWrittenUntilFlush * minAssumedConcurrency, memThrottleFactor)).limit(2)
                        .collect(toCollection(ArrayList::new));

        writeBuffers.put(pipeId, buffers);
        writeBuffersView.addAll(buffers);

        return Pipe.attachNewPipe(this, pipeId);
    }

    void write(byte[] b, int off, int len, Integer pipeId) {
        if (len > 0) {
            List<Buffer> buffersPerPipe = writeBuffers.get(pipeId);
            int mileage = 0;
            while (true) {
                mileage++;

                for (Buffer buffer : buffersPerPipe) {
                    if (mileage > cpuThrottleFactor) {
                        mileage = 0;
                        buffer.await();
                    }

                    dirtyWritesCount.increment();
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
                }
            }
        }
    }

    private Future<Void> startTransfer() {
        return executor.submit(() -> {
            boolean taskRequireContinuation;

            do {
                int writeBuffersCountBeforeTransfer = writeBuffersView.size();

                int emptyBuffersCount = 0;
                for (Buffer buffer : writeBuffersView) {
                    if (buffer.lock.tryLock()) {
                        try {
                            WeakFixedByteArray buf = buffer.byteArray;
                            if (buf.size() > 0) {
                                buf.writeTo(outputStream);
                                buf.reset();
                            } else {
                                emptyBuffersCount++;
                            }
                        } finally {
                            buffer.lock.unlock();
                        }
                    }

                    buffer.release();
                }

                int writeBuffersCountAfterTransfer = writeBuffersView.size();
                taskRequireContinuation = emptyBuffersCount != writeBuffersCountAfterTransfer
                                || writeBuffersCountBeforeTransfer != writeBuffersCountAfterTransfer || isStreamOpen()
                                || hasActivePipes() || hasDirtyWrites();
            } while (taskRequireContinuation);

            return null;
        });
    }

    private boolean isStreamOpen() {
        return !isStreamClosed.get();
    }

    private boolean hasActivePipes() {
        return activePipesCount.get() > 0;
    }

    private boolean hasDirtyWrites() {
        return dirtyWritesCount.sumThenReset() > 0;
    }

    @Override
    public void close() {
        if (isStreamClosed.compareAndSet(false, true)) {
            executor.shutdown();

            taskCompletionNotifier.runAsyncWhenDone(() -> {
                outputStream.close();
                return null;
            });
        }
    }

    public CompletionNotifier getTaskCompletionNotifier() {
        return taskCompletionNotifier;
    }

    private static class Buffer {
        final ReentrantLock lock;
        final WeakFixedByteArray byteArray;
        final Object condition;

        Buffer(int bufferSize, int memThrottleFactor) {
            this.lock = new ReentrantLock();
            this.byteArray = new WeakFixedByteArray(bufferSize, memThrottleFactor);
            this.condition = new Object();
        }

        void await() {
            try {
                synchronized (condition) {
                    condition.wait();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                rethrow(e);
            }
        }

        void release() {
            synchronized (condition) {
                condition.notifyAll();
            }
        }
    }
}
