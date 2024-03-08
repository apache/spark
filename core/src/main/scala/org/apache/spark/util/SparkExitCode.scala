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

package org.apache.spark.util

private[spark] object SparkExitCode {

  /** Successful termination. */
  val EXIT_SUCCESS = 0

  /** Failed termination. */
  val EXIT_FAILURE = 1

  /** Exception indicate invalid usage of some shell built-in command. */
  val ERROR_MISUSE_SHELL_BUILTIN = 2

  /** Exception appears when the computer cannot find the specified path. */
  val ERROR_PATH_NOT_FOUND = 3

  /** Exit due to executor failures exceeds the threshold. */
  val EXCEED_MAX_EXECUTOR_FAILURES = 11

  /** The default uncaught exception handler was reached. */
  val UNCAUGHT_EXCEPTION = 50

  /** The default uncaught exception handler was called and an exception was encountered while
      logging the exception. */
  val UNCAUGHT_EXCEPTION_TWICE = 51

  /** The default uncaught exception handler was reached, and the uncaught exception was an
      OutOfMemoryError. */
  val OOM = 52

  /** Exit because the driver is running over the given threshold. */
  val DRIVER_TIMEOUT = 124

  /** Exception indicate command not found. */
  val ERROR_COMMAND_NOT_FOUND = 127
}
