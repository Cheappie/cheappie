/*
 * Copyright (C) 2009 The Cheappie Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package kkon.cheappie.io.concurrent.streambuffer;

import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

public final class CompletionNotifier {
    private final Future<Void> task;
    private final AtomicBoolean isStreamClosed;

    CompletionNotifier(Future<Void> task, AtomicBoolean isStreamClosed) {
        this.task = task;
        this.isStreamClosed = isStreamClosed;
    }

    public boolean isStreamClosed() {
        return isStreamClosed.get();
    }

    public boolean isTransferDone() {
        return task.isDone();
    }

    public void waitUntilDone() throws ExecutionException, InterruptedException {
        task.get();
    }

    void runAsyncWhenDone(Callable<Void> callable) {
        CompletableFuture.runAsync(() -> invokeIgnoringExceptions(new RecurringAsyncTask(task, callable)));
    }

    private static class RecurringAsyncTask implements Callable<Void> {
        private final Future<Void> task;
        private final Callable<Void> callable;

        RecurringAsyncTask(Future<Void> task, Callable<Void> callable) {
            this.task = task;
            this.callable = callable;
        }

        @Override
        public Void call() throws Exception {
            if (task.isDone()) {
                callable.call();
            } else {
                CompletableFuture.runAsync(() -> invokeIgnoringExceptions(this));
            }

            return null;
        }
    }

    private static void invokeIgnoringExceptions(Callable<?> callable) {
        try {
            callable.call();
        } catch (Exception ignored) {
        }
    }
}
