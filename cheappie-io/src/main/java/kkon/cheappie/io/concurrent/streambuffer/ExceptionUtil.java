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

import java.util.Optional;

final class ExceptionUtil {

    interface Procedure {
        void invoke() throws Exception;
    }

    static Optional<Exception> invokeUnconditionally(Procedure procedure) {
        try {
            procedure.invoke();
            return Optional.empty();
        } catch (Exception e) {
            return Optional.of(e);
        }
    }

    static Optional<Exception> invokeUnconditionally(Procedure... procedures) {

        Optional<Exception> firstThrownException = Optional.empty();
        for (Procedure procedure : procedures) {
            Optional<Exception> exception = invokeUnconditionally(procedure);

            if (!firstThrownException.isPresent() && exception.isPresent()) {
                firstThrownException = exception;
            }
        }

        return firstThrownException;
    }
}
