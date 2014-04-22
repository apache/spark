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

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.storage.BlockManagerId

/**
 * :: DeveloperApi ::
 * Various possible reasons why a task ended. The low-level TaskScheduler is supposed to retry
 * tasks several times for "ephemeral" failures, and only report back failures that require some
 * old stages to be resubmitted, such as shuffle map fetch failures.
 */
@DeveloperApi
sealed trait TaskEndReason

@DeveloperApi
case object Success extends TaskEndReason

@DeveloperApi
case object Resubmitted extends TaskEndReason // Task was finished earlier but we've now lost it

@DeveloperApi
case class FetchFailed(
    bmAddress: BlockManagerId,
    shuffleId: Int,
    mapId: Int,
    reduceId: Int)
  extends TaskEndReason

@DeveloperApi
case class ExceptionFailure(
    className: String,
    description: String,
    stackTrace: Array[StackTraceElement],
    metrics: Option[TaskMetrics])
  extends TaskEndReason

/**
 * :: DeveloperApi ::
 * The task finished successfully, but the result was lost from the executor's block manager before
 * it was fetched.
 */
@DeveloperApi
case object TaskResultLost extends TaskEndReason

@DeveloperApi
case object TaskKilled extends TaskEndReason

/**
 * :: DeveloperApi ::
 * The task failed because the executor that it was running on was lost. This may happen because
 * the task crashed the JVM.
 */
@DeveloperApi
case object ExecutorLostFailure extends TaskEndReason

/**
 * :: DeveloperApi ::
 * We don't know why the task ended -- for example, because of a ClassNotFound exception when
 * deserializing the task result.
 */
@DeveloperApi
case object UnknownReason extends TaskEndReason
