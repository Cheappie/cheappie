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


public final class GenericOutputStreamBuffer extends GenericPipe {
    private GenericOutputStreamBuffer(BytePipe pipe, int size) {
        super(pipe, size);
    }

    public static GenericOutputStreamBuffer of(OutputStream outputStream) {
        return of(outputStream, 8192);
    }

    public static GenericOutputStreamBuffer of(OutputStream outputStream, int size) {
        Preconditions.checkNotNull(outputStream, "Required: outputStream not null");
        Preconditions.checkArgument(size >= 128, "Required: chunkSize >= 128");

        return new GenericOutputStreamBuffer(new BytePipe(outputStream, size), size);
    }
}
