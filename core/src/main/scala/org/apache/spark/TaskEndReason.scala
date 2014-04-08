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

import org.apache.spark.annotations.DeveloperAPI
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.storage.BlockManagerId

/**
 * Various possible reasons why a task ended. The low-level TaskScheduler is supposed to retry
 * tasks several times for "ephemeral" failures, and only report back failures that require some
 * old stages to be resubmitted, such as shuffle map fetch failures.
 */
@DeveloperAPI
sealed trait TaskEndReason

/** <span class="developer badge">Developer API</span> */
case object Success extends TaskEndReason

/** <span class="developer badge">Developer API</span> */
case object Resubmitted extends TaskEndReason // Task was finished earlier but we've now lost it

/** <span class="developer badge">Developer API</span> */
case class FetchFailed(
    bmAddress: BlockManagerId,
    shuffleId: Int,
    mapId: Int,
    reduceId: Int)
  extends TaskEndReason

/** <span class="developer badge">Developer API</span> */
case class ExceptionFailure(
    className: String,
    description: String,
    stackTrace: Array[StackTraceElement],
    metrics: Option[TaskMetrics])
  extends TaskEndReason

/**
 * <span class="developer badge">Developer API</span>
 * The task finished successfully, but the result was lost from the executor's block manager before
 * it was fetched.
 */
case object TaskResultLost extends TaskEndReason

/** <span class="developer badge">Developer API</span> */
case object TaskKilled extends TaskEndReason

/**
 * <span class="developer badge">Developer API</span>
 * The task failed because the executor that it was running on was lost. This may happen because
 * the task crashed the JVM.
 */
case object ExecutorLostFailure extends TaskEndReason

/**
 * <span class="developer badge">Developer API</span>
 * We don't know why the task ended -- for example, because of a ClassNotFound exception when
 * deserializing the task result.
 */
case object UnknownReason extends TaskEndReason
