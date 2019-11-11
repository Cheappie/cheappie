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
