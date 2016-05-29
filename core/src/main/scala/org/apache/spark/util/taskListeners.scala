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

import java.util.EventListener

import org.apache.spark.TaskContext
import org.apache.spark.annotation.DeveloperApi

/**
 * :: DeveloperApi ::
 *
 * Listener providing a callback function to invoke when a task's execution completes.
 */
@DeveloperApi
trait TaskCompletionListener extends EventListener {
  def onTaskCompletion(context: TaskContext): Unit
}


/**
 * :: DeveloperApi ::
 *
 * Listener providing a callback function to invoke when a task's execution encounters an error.
 * Operations defined here must be idempotent, as `onTaskFailure` can be called multiple times.
 */
@DeveloperApi
trait TaskFailureListener extends EventListener {
  def onTaskFailure(context: TaskContext, error: Throwable): Unit
}


/**
 * Exception thrown when there is an exception in executing the callback in TaskCompletionListener.
 */
private[spark]
class TaskCompletionListenerException(
    errorMessages: Seq[String],
    val previousError: Option[Throwable] = None)
  extends RuntimeException {

  override def getMessage: String = {
    if (errorMessages.size == 1) {
      errorMessages.head
    } else {
      errorMessages.zipWithIndex.map { case (msg, i) => s"Exception $i: $msg" }.mkString("\n")
    } +
    previousError.map { e =>
      "\n\nPrevious exception in task: " + e.getMessage + "\n" +
        e.getStackTrace.mkString("\t", "\n\t", "")
    }.getOrElse("")
  }
}
