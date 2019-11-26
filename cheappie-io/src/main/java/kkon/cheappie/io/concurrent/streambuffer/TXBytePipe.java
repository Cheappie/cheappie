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

final class TXBytePipe extends BytePipe {
    private int lastCommitIndex = 0;
    private final Integer producerId;

    TXBytePipe(BufferInGateway outputStreamGateway, int minPipeTXSize) {
        super(outputStreamGateway, minPipeTXSize);
        this.producerId = outputStreamGateway.getProducerId();
    }

    Integer getProducerId() {
        return producerId;
    }

    @Override
    protected void flush(int incomingBytesCount, boolean forceWrite) throws IOException {
        int actuallyWrittenSize = locBuffer.size();
        if ((forceWrite || actuallyWrittenSize + incomingBytesCount >= minPipeTXSize)
                        && lastCommitIndex > 0) {
            unconditionalFlush(lastCommitIndex);
            locBuffer.softTrim(lastCommitIndex);
            lastCommitIndex = 0;
        }
    }

    public void commit() {
        this.lastCommitIndex = locBuffer.size();
    }
}
