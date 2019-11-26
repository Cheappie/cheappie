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

import com.google.common.primitives.Bytes;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class TestUtils {
    static byte[] readFully(DataInputStream in) throws IOException {
        List<Byte> bytes = new ArrayList<>();
        while (in.available() > 0) {
            bytes.add(in.readByte());
        }
        return Bytes.toArray(bytes);
    }

    static byte[] nBytes(int n) {
        byte[] bytes = new byte[n];

        for (int i = 0; i < bytes.length; i++) {
            bytes[i] = (byte) i;
        }

        return bytes;
    }

    static char[] nChars(int len) {
        char[] chars = new char[len];

        for (int i = 0; i < chars.length; i++) {
            chars[i] = (char) (i & 0x7F);
        }

        return chars;
    }

    static <T> Future<T> submitTaskToSeparateThread(Procedure procedure) {
        ExecutorService executorService = Executors.newSingleThreadExecutor();

        try {
            return executorService.submit(() -> {
                procedure.invoke();
                return null;
            });
        } finally {
            executorService.shutdown();
        }
    }

    static void awaitWithTimeout(Procedure procedure, long secondsUntilTimeout)
                    throws ExecutionException, InterruptedException, TimeoutException {
        try {
            submitTaskToSeparateThread(procedure).get(secondsUntilTimeout, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            throw new TimeoutException("Test Failure, unfortunately timeout was reached before task was done");
        }
    }

    interface RethrowableFunction<T, R> {
        R apply(T val) throws Exception;
    }

    interface RethrowableConsumer<T> {
        void accept(T val) throws Exception;
    }

    interface Procedure {
        void invoke() throws Exception;
    }
}
