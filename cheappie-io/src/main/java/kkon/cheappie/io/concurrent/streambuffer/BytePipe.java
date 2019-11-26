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

import java.io.IOException;
import java.io.OutputStream;
import java.util.Optional;


class BytePipe extends OutputStream {
    private final OutputStream outputStream;
    protected final int minPipeTXSize;
    protected final ByteArray locBuffer;

    private boolean isPipeClosed = false;

    BytePipe(OutputStream outputStream, int minPipeTXSize) {
        this.outputStream = outputStream;
        this.minPipeTXSize = minPipeTXSize;
        this.locBuffer = new ByteArray(minPipeTXSize);
    }

    boolean isClosed() {
        return isPipeClosed;
    }

    public void write(int b) throws IOException {
        ensureOpen();

        flush(1, false);
        locBuffer.write(b);
    }

    protected void flush(int incomingBytesCount, boolean forceWrite) throws IOException {
        int actuallyWrittenSize = locBuffer.size();

        if (forceWrite || actuallyWrittenSize + incomingBytesCount >= minPipeTXSize) {
            unconditionalFlush(actuallyWrittenSize);
            locBuffer.reset();
        }
    }

    final void unconditionalFlush(int len) throws IOException {
        outputStream.write(locBuffer.elements(), 0, len);
    }

    private void ensureOpen() throws IOException {
        if (isPipeClosed) {
            throw new IOException("Pipe has been closed.");
        }
    }

    public void write(byte[] b) throws IOException {
        ensureOpen();

        flush(b.length, false);
        locBuffer.write(b);
    }

    public void write(byte[] b, int readOffset, int len) throws IOException {
        ensureOpen();

        flush(len, false);
        locBuffer.write(b, readOffset, len);
    }

    @Override
    public void close() throws IOException {
        if (isPipeClosed) {
            return;
        }
        isPipeClosed = true;

        Optional<Exception> exception =
                        ExceptionUtil.invokeUnconditionally(() -> flush(locBuffer.size(), true), outputStream::close);
        if (exception.isPresent()) {
            throw new IOException(exception.get());
        }
    }
}
