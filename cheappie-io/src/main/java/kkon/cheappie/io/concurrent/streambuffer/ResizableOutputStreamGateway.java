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


final class ResizableOutputStreamGateway extends BufferOutGateway {
    private final int desiredOutputChunkSize;
    private final ByteArray locBuffer;
    private int bytesRead;

    ResizableOutputStreamGateway(OutputStream outputStream, int desiredOutputChunkSize) {
        super(outputStream);
        this.desiredOutputChunkSize = desiredOutputChunkSize;
        this.locBuffer = new ByteArray(desiredOutputChunkSize * 2);
    }

    public void eatBytes(ConcurrentOutputStreamBuffer.Buffer buffer) throws IOException {
        int size;
        while ((size = buffer.size()) > 0) {
            if (bytesRead == 0 && size >= desiredOutputChunkSize) {
                int consumedBytes = buffer.repository.consume(desiredOutputChunkSize);

                int off = size - consumedBytes;
                buffer.writeTo(outputStream, off, consumedBytes);
                buffer.size(off);
            } else {
                int bytesCountNecessaryToAcquire = desiredOutputChunkSize - bytesRead;
                int consumedBytes = buffer.repository.consume(bytesCountNecessaryToAcquire);
                bytesRead += consumedBytes;

                int off = size - consumedBytes;
                buffer.writeTo(locBuffer, off, consumedBytes);
                buffer.size(off);

                if (bytesRead >= desiredOutputChunkSize) {
                    locBuffer.resetAfterWriteTo(outputStream);
                    bytesRead = 0;
                }
            }
        }
    }

    @Override
    public void close() throws IOException {
        Optional<Exception> exception = ExceptionUtil.invokeUnconditionally(this::flush, outputStream::close);
        if (exception.isPresent()) {
            throw new IOException(exception.get());
        }
    }

    private void flush() throws IOException {
        locBuffer.resetAfterWriteTo(outputStream);
    }
}
