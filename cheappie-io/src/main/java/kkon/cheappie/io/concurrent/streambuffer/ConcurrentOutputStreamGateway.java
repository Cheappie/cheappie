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

final class ConcurrentOutputStreamGateway extends OutputStream {
    private final ConcurrentOutputStreamBuffer concurrentOutputStreamBuffer;
    private final Integer producerId;

    ConcurrentOutputStreamGateway(ConcurrentOutputStreamBuffer concurrentOutputStreamBuffer,
                                  Integer producerId) {
        this.concurrentOutputStreamBuffer = concurrentOutputStreamBuffer;
        this.producerId = producerId;
    }

    Integer getProducerId() {
        return producerId;
    }

    @Override
    public void write(byte[] b) throws IOException {
        concurrentOutputStreamBuffer.write(b, 0, b.length, producerId);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        concurrentOutputStreamBuffer.write(b, off, len, producerId);
    }

    @Override
    public void close() throws IOException {
        concurrentOutputStreamBuffer.activePipesCount.decrementAndGet();
        concurrentOutputStreamBuffer.detachedPipes.add(producerId);
    }

    @Override
    public void write(int b) throws IOException {
        throw new UnsupportedOperationException();
    }
}