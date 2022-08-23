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

package org.apache.spark;

import org.apache.spark.annotation.Evolving;

/**
 * Interface mixed into Throwables thrown from Spark.
 *
 * - For backwards compatibility, existing Throwable types can be thrown with an arbitrary error
 *   message with a null error class. See [[SparkException]].
 * - To promote standardization, Throwables should be thrown with an error class and message
 *   parameters to construct an error message with SparkThrowableHelper.getMessage(). New Throwable
 *   types should not accept arbitrary error messages. See [[SparkArithmeticException]].
 *
 * @since 3.2.0
 */
@Evolving
public interface SparkThrowable {
  // Succinct, human-readable, unique, and consistent representation of the error category
  // If null, error class is not set
  String getErrorClass();

  default String getErrorSubClass() {
    return null;
  }

  // Portable error identifier across SQL engines
  // If null, error class or SQLSTATE is not set
  default String getSqlState() {
    return SparkThrowableHelper.getSqlState(this.getErrorClass());
  }

  // True if this error is an internal error.
  default boolean isInternalError() {
    return SparkThrowableHelper.isInternalError(this.getErrorClass());
  }

  default String[] getMessageParameters() {
    return new String[]{};
  }

  // Returns a string array of all parameters that need to be passed to this error message.
  default String[] getParameterNames() {
    return SparkThrowableHelper.getParameterNames(this.getErrorClass(), this.getErrorSubClass());
  }

  default QueryContext[] getQueryContext() { return new QueryContext[0]; }
}
