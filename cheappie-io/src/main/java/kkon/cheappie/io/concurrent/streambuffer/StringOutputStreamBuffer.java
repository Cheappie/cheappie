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

import com.google.common.base.Preconditions;
import java.io.OutputStream;
import java.nio.charset.Charset;


public final class StringOutputStreamBuffer extends StringPipe {

    private StringOutputStreamBuffer(BytePipe pipe, int size, Charset charset) {
        super(pipe, size, charset);
    }

    public static StringOutputStreamBuffer of(OutputStream outputStream) {
        return of(outputStream, 8192);
    }

    public static StringOutputStreamBuffer of(OutputStream outputStream, int chunkSize) {
        return of(outputStream, chunkSize, Charset.defaultCharset());
    }

    public static StringOutputStreamBuffer of(OutputStream outputStream, Charset charset) {
        return of(outputStream, 8192, charset);
    }

    public static StringOutputStreamBuffer of(OutputStream outputStream, int size, Charset charset) {
        Preconditions.checkNotNull(outputStream, "Required: outputStream not null");
        Preconditions.checkArgument(size >= 128, "Required: size >= 128");
        Preconditions.checkNotNull(charset, "Required: charset not null");

        return new StringOutputStreamBuffer(new BytePipe(outputStream, size), size, charset);
    }
}
