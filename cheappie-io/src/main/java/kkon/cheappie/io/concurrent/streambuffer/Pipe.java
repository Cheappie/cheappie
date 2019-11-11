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

import java.io.Closeable;
import java.io.IOException;

class Pipe implements Closeable {
    private final Integer pipeId;
    private final ByteArray locBuffer;
    private final ConcurrentOutputStreamBuffer concurrentOutputStreamBuffer;

    private int lastCommitIndex;
    private boolean isPipeClosed = false;

    private Pipe(ConcurrentOutputStreamBuffer concurrentOutputStreamBuffer, Integer pipeId) {
        this.pipeId = pipeId;
        this.locBuffer = new ByteArray(concurrentOutputStreamBuffer.minBytesWrittenUntilFlush);
        this.concurrentOutputStreamBuffer = concurrentOutputStreamBuffer;
        this.lastCommitIndex = 0;
    }

    static Pipe attachNewPipe(ConcurrentOutputStreamBuffer concurrentOutputStreamBuffer, Integer pipeId) {
        return new Pipe(concurrentOutputStreamBuffer, pipeId);
    }

    public void write(int b) throws IOException {
        throwOnWriteToClosedPipe();

        flush(1, false);
        locBuffer.write(b);
    }

    private void throwOnWriteToClosedPipe() throws IOException {
        if (isPipeClosed) {
            throw new IOException("Writing to closed pipe is prohibited.");
        }
    }

    public void write(byte[] b) throws IOException {
        throwOnWriteToClosedPipe();

        flush(b.length, false);
        locBuffer.write(b);
    }

    public void write(byte[] b, int off, int len) throws IOException {
        throwOnWriteToClosedPipe();

        flush(len, false);
        locBuffer.write(b, off, len);
    }

    private void flush(int incomingBytesCount, boolean forceWriteCommited) {
        if ((forceWriteCommited || locBuffer.size()
                        + incomingBytesCount >= concurrentOutputStreamBuffer.minBytesWrittenUntilFlush)
                        && lastCommitIndex > 0) {
            concurrentOutputStreamBuffer.write(locBuffer.elements(), 0, lastCommitIndex, pipeId);
            locBuffer.softTrim(lastCommitIndex);
            lastCommitIndex = 0;
        }
    }

    public void commit() {
        lastCommitIndex = locBuffer.size();
    }

    public void close() throws IOException {
        flush(locBuffer.size(), true);
        isPipeClosed = true;
        concurrentOutputStreamBuffer.activePipesCount.decrementAndGet();
    }
}
