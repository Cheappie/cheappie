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
import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;

class StringPipe implements Closeable {
    private final OutputStreamWriter stringPipeOutputStream;
    private final StringBuilder buffer;
    private final BytePipe pipe;
    private final String lineSeparator;

    private final char[] transportBuffer;
    private final int chunkSize;

    StringPipe(BytePipe pipe, int chunkSize, Charset charset) {
        this.pipe = pipe;
        this.lineSeparator = System.lineSeparator();
        this.stringPipeOutputStream = new OutputStreamWriter(new BytePipeToOutputStreamGateway(pipe), charset);

        this.buffer = new StringBuilder(chunkSize);
        this.transportBuffer = new char[chunkSize];
        this.chunkSize = chunkSize;
    }

    @VisibleForTesting
    int size() {
        return buffer.length();
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

    protected void flush(int incomingCharsCount, boolean forceWrite) throws IOException {
        final int len = buffer.length();

        if (forceWrite || len >= chunkSize) {
            unconditionalFlush(len);
            softTrim(len);
        }
    }

    final void unconditionalFlush(int len) throws IOException {
        writeTo(stringPipeOutputStream, 0, len);
        stringPipeOutputStream.flush();
    }

    final void softTrim(int len) {
        buffer.delete(0, len);
    }

    public void write(String str) throws IOException {
        buffer.append(str);
        flush(0, false);
    }

    public void write(StringBuffer sb) throws IOException {
        buffer.append(sb);
        flush(0, false);
    }

    public void write(CharSequence s) throws IOException {
        buffer.append(s);
        flush(0, false);
    }

    public void write(CharSequence s, int start, int end) throws IOException {
        buffer.append(s, start, end);
        flush(0, false);
    }

    public void write(char[] str) throws IOException {
        buffer.append(str);
        flush(0, false);
    }

    public void write(char[] str, int offset, int len) throws IOException {
        buffer.append(str, offset, len);
        flush(0, false);
    }

    public void write(boolean b) throws IOException {
        buffer.append(b);
        flush(0, false);
    }

    public void write(char c) throws IOException {
        buffer.append(c);
        flush(0, false);
    }

    public void write(int i) throws IOException {
        buffer.append(i);
        flush(0, false);
    }

    public void write(long lng) throws IOException {
        buffer.append(lng);
        flush(0, false);
    }

    public void write(float f) throws IOException {
        buffer.append(f);
        flush(0, false);
    }

    public void write(double d) throws IOException {
        buffer.append(d);
        flush(0, false);
    }

    public void newLine() throws IOException {
        buffer.append(lineSeparator);
        flush(0, false);
    }

    @Override
    public void close() throws IOException {
        if (pipe.isClosed()) {
            return;
        }

        flush(0, true);
        pipe.close();
        stringPipeOutputStream.close();
    }
}
