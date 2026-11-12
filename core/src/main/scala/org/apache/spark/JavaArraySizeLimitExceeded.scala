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

package org.apache.spark

import scala.jdk.CollectionConverters._

/**
 * Exception used when serializing a task, or the task binary shared by the tasks of a stage,
 * fails because the serialized bytes would exceed Java's maximum array size (about 2 GB).
 *
 * It carries the user-facing error condition `TASK_SERIALIZATION_TOO_LARGE`, so the scheduler
 * surfaces it to the user as is instead of wrapping it in a generic stage failure.
 *
 * This intentionally does not retain the original [[OutOfMemoryError]] as its cause, so this
 * deterministic size-limit failure is not mistaken for heap exhaustion, which more memory would
 * not fix. The JVM's message is kept in [[jvmMessage]] for logging.
 */
private[spark] class JavaArraySizeLimitExceeded private (
    errorClass: String,
    messageParameters: Map[String, String],
    val jvmMessage: String,
    stackTrace: Array[StackTraceElement])
  extends Exception(SparkThrowableHelper.getMessage(errorClass, messageParameters))
  with SparkThrowable {

  setStackTrace(stackTrace)

  override def getMessageParameters: java.util.Map[String, String] = messageParameters.asJava

  override def getCondition: String = errorClass
}

private[spark] object JavaArraySizeLimitExceeded {

  /** A single task, which carries the data of its partition, is too large to serialize. */
  def forTask(
      error: OutOfMemoryError,
      stageId: Int,
      partitionId: Int): JavaArraySizeLimitExceeded = {
    new JavaArraySizeLimitExceeded(
      "TASK_SERIALIZATION_TOO_LARGE.TASK",
      Map("stageId" -> stageId.toString, "partitionId" -> partitionId.toString),
      error.getMessage,
      error.getStackTrace)
  }

  /**
   * The task binary of a stage, which holds the RDD chain and the function shared by all of its
   * tasks, is too large to serialize.
   */
  def forTaskBinary(error: OutOfMemoryError, stageId: Int): JavaArraySizeLimitExceeded = {
    new JavaArraySizeLimitExceeded(
      "TASK_SERIALIZATION_TOO_LARGE.TASK_BINARY",
      Map("stageId" -> stageId.toString),
      error.getMessage,
      error.getStackTrace)
  }
}
