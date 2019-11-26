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

import java.io.Closeable;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

class GenericPipe implements Closeable {
    private final BytePipe pipe;
    private final DataOutputStream dataOutputStream;
    private final BytePipeToOutputStreamGateway bytePipeGateway;
    private final ByteArray utf8TemporaryBuffer;

    GenericPipe(BytePipe pipe, int minElementsWrittenUntilFlush) {
        this.pipe = pipe;
        this.bytePipeGateway = new BytePipeToOutputStreamGateway(pipe);
        this.dataOutputStream = new DataOutputStream(bytePipeGateway);
        this.utf8TemporaryBuffer = new ByteArray(minElementsWrittenUntilFlush);
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
        if (pipe.isClosed()) {
            return;
        }

        pipe.close();
    }
}
