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
import org.apache.spark.util.Utils

/**
 * :: DeveloperApi ::
 * Various possible reasons why a task ended. The low-level TaskScheduler is supposed to retry
 * tasks several times for "ephemeral" failures, and only report back failures that require some
 * old stages to be resubmitted, such as shuffle map fetch failures.
 */
@DeveloperApi
sealed trait TaskEndReason

/**
 * :: DeveloperApi ::
 * Task succeeded.
 */
@DeveloperApi
case object Success extends TaskEndReason

/**
 * :: DeveloperApi ::
 * Various possible reasons why a task failed.
 */
@DeveloperApi
sealed trait TaskFailedReason extends TaskEndReason {
  /** Error message displayed in the web UI. */
  def toErrorString: String
}

/**
 * :: DeveloperApi ::
 * A [[org.apache.spark.scheduler.ShuffleMapTask]] that completed successfully earlier, but we
 * lost the executor before the stage completed. This means Spark needs to reschedule the task
 * to be re-executed on a different executor.
 */
@DeveloperApi
case object Resubmitted extends TaskFailedReason {
  override def toErrorString: String = "Resubmitted (resubmitted due to lost executor)"
}

/**
 * :: DeveloperApi ::
 * Task failed to fetch shuffle data from a remote node. Probably means we have lost the remote
 * executors the task is trying to fetch from, and thus need to rerun the previous stage.
 */
@DeveloperApi
case class FetchFailed(
    bmAddress: BlockManagerId,  // Note that bmAddress can be null
    shuffleId: Int,
    mapId: Int,
    reduceId: Int,
    message: String)
  extends TaskFailedReason {
  override def toErrorString: String = {
    val bmAddressString = if (bmAddress == null) "null" else bmAddress.toString
    s"FetchFailed($bmAddressString, shuffleId=$shuffleId, mapId=$mapId, reduceId=$reduceId, " +
      s"message=\n$message\n)"
  }
}

/**
 * :: DeveloperApi ::
 * Task failed due to a runtime exception. This is the most common failure case and also captures
 * user program exceptions.
 */
@DeveloperApi
case class ExceptionFailure(
    className: String,
    description: String,
    stackTrace: Array[StackTraceElement],
    metrics: Option[TaskMetrics])
  extends TaskFailedReason {
  override def toErrorString: String = Utils.exceptionString(className, description, stackTrace)
}

/**
 * :: DeveloperApi ::
 * The task finished successfully, but the result was lost from the executor's block manager before
 * it was fetched.
 */
@DeveloperApi
case object TaskResultLost extends TaskFailedReason {
  override def toErrorString: String = "TaskResultLost (result lost from block manager)"
}

/**
 * :: DeveloperApi ::
 * Task was killed intentionally and needs to be rescheduled.
 */
@DeveloperApi
case object TaskKilled extends TaskFailedReason {
  override def toErrorString: String = "TaskKilled (killed intentionally)"
}

/**
 * :: DeveloperApi ::
 * The task failed because the executor that it was running on was lost. This may happen because
 * the task crashed the JVM.
 */
@DeveloperApi
case class ExecutorLostFailure(execId: String) extends TaskFailedReason {
  override def toErrorString: String = s"ExecutorLostFailure (executor ${execId} lost)"
}

/**
 * :: DeveloperApi ::
 * We don't know why the task ended -- for example, because of a ClassNotFound exception when
 * deserializing the task result.
 */
@DeveloperApi
case object UnknownReason extends TaskFailedReason {
  override def toErrorString: String = "UnknownReason"
}
