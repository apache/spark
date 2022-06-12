/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.memory;

import org.apache.spark.SparkThrowable;
import org.apache.spark.SparkThrowableHelper;
import org.apache.spark.annotation.Private;

/**
 * This exception is thrown when a task can not acquire memory from the Memory manager.
 * Instead of throwing {@link OutOfMemoryError}, which kills the executor,
 * we should use throw this exception, which just kills the current task.
 */
@Private
public final class SparkOutOfMemoryError extends OutOfMemoryError implements SparkThrowable {
    String errorClass;
    String[] messageParameters;

    public SparkOutOfMemoryError(String s) {
        super(s);
    }

    public SparkOutOfMemoryError(OutOfMemoryError e) {
        super(e.getMessage());
    }

    public SparkOutOfMemoryError(String errorClass, String[] messageParameters) {
        super(SparkThrowableHelper.getMessage(errorClass, null,
                messageParameters, ""));
        this.errorClass = errorClass;
        this.messageParameters = messageParameters;
    }

    @Override
    public String[] getMessageParameters() {
        return messageParameters;
    }

    @Override
    public String getErrorClass() {
        return errorClass;
    }
}
