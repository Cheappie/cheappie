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
import java.nio.charset.Charset;

public final class CommittedStringPipe extends StringPipe {
    private final CommittedBytePipe pipe;
    private final int minElementsWrittenUntilFlush;
    private int lastCommitIndex;

    CommittedStringPipe(CommittedBytePipe pipe, Charset charset) {
        super(pipe, pipe.minElementsWrittenUntilFlush, charset);
        this.pipe = pipe;
        this.lastCommitIndex = 0;
        this.minElementsWrittenUntilFlush = pipe.minElementsWrittenUntilFlush;
    }

    @Override
    protected void flush(int incomingCharsCount, boolean forceWrite) throws IOException {
        int actuallyWrittenSize = size();

        if ((forceWrite || actuallyWrittenSize + incomingCharsCount >= minElementsWrittenUntilFlush)
                        && lastCommitIndex > 0) {
            unconditionalFlush(lastCommitIndex);
            pipe.commit();

            softTrim(lastCommitIndex);
            lastCommitIndex = 0;
        }
    }

    public void commit() {
        this.lastCommitIndex = size();
    }
}
