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

import java.io.{ObjectInputStream, ObjectOutputStream}

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.AccumulableInfo
import org.apache.spark.storage.BlockManagerId
import org.apache.spark.util.{AccumulatorV2, Utils}

// ==============================================================================================
// NOTE: new task end reasons MUST be accompanied with serialization logic in util.JsonProtocol!
// ==============================================================================================

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

  /**
   * Whether this task failure should be counted towards the maximum number of times the task is
   * allowed to fail before the stage is aborted.  Set to false in cases where the task's failure
   * was unrelated to the task; for example, if the task failed because the executor it was running
   * on was killed.
   */
  def countTowardsTaskFailures: Boolean = true
}

/**
 * :: DeveloperApi ::
 * A `org.apache.spark.scheduler.ShuffleMapTask` that completed successfully earlier, but we
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

  /**
   * Fetch failures lead to a different failure handling path: (1) we don't abort the stage after
   * 4 task failures, instead we immediately go back to the stage which generated the map output,
   * and regenerate the missing data.  (2) we don't count fetch failures for blacklisting, since
   * presumably its not the fault of the executor where the task ran, but the executor which
   * stored the data. This is especially important because we we might rack up a bunch of
   * fetch-failures in rapid succession, on all nodes of the cluster, due to one bad node.
   */
  override def countTowardsTaskFailures: Boolean = false
}

/**
 * :: DeveloperApi ::
 * Task failed due to a runtime exception. This is the most common failure case and also captures
 * user program exceptions.
 *
 * `stackTrace` contains the stack trace of the exception itself. It still exists for backward
 * compatibility. It's better to use `this(e: Throwable, metrics: Option[TaskMetrics])` to
 * create `ExceptionFailure` as it will handle the backward compatibility properly.
 *
 * `fullStackTrace` is a better representation of the stack trace because it contains the whole
 * stack trace including the exception and its causes
 *
 * `exception` is the actual exception that caused the task to fail. It may be `None` in
 * the case that the exception is not in fact serializable. If a task fails more than
 * once (due to retries), `exception` is that one that caused the last failure.
 */
@DeveloperApi
case class ExceptionFailure(
    className: String,
    description: String,
    stackTrace: Array[StackTraceElement],
    fullStackTrace: String,
    private val exceptionWrapper: Option[ThrowableSerializationWrapper],
    accumUpdates: Seq[AccumulableInfo] = Seq.empty,
    private[spark] var accums: Seq[AccumulatorV2[_, _]] = Nil)
  extends TaskFailedReason {

  /**
   * `preserveCause` is used to keep the exception itself so it is available to the
   * driver. This may be set to `false` in the event that the exception is not in fact
   * serializable.
   */
  private[spark] def this(
      e: Throwable,
      accumUpdates: Seq[AccumulableInfo],
      preserveCause: Boolean) {
    this(e.getClass.getName, e.getMessage, e.getStackTrace, Utils.exceptionString(e),
      if (preserveCause) Some(new ThrowableSerializationWrapper(e)) else None, accumUpdates)
  }

  private[spark] def this(e: Throwable, accumUpdates: Seq[AccumulableInfo]) {
    this(e, accumUpdates, preserveCause = true)
  }

  private[spark] def withAccums(accums: Seq[AccumulatorV2[_, _]]): ExceptionFailure = {
    this.accums = accums
    this
  }

  def exception: Option[Throwable] = exceptionWrapper.flatMap(w => Option(w.exception))

  override def toErrorString: String =
    if (fullStackTrace == null) {
      // fullStackTrace is added in 1.2.0
      // If fullStackTrace is null, use the old error string for backward compatibility
      exceptionString(className, description, stackTrace)
    } else {
      fullStackTrace
    }

  /**
   * Return a nice string representation of the exception, including the stack trace.
   * Note: It does not include the exception's causes, and is only used for backward compatibility.
   */
  private def exceptionString(
      className: String,
      description: String,
      stackTrace: Array[StackTraceElement]): String = {
    val desc = if (description == null) "" else description
    val st = if (stackTrace == null) "" else stackTrace.map("        " + _).mkString("\n")
    s"$className: $desc\n$st"
  }
}

/**
 * A class for recovering from exceptions when deserializing a Throwable that was
 * thrown in user task code. If the Throwable cannot be deserialized it will be null,
 * but the stacktrace and message will be preserved correctly in SparkException.
 */
private[spark] class ThrowableSerializationWrapper(var exception: Throwable) extends
    Serializable with Logging {
  private def writeObject(out: ObjectOutputStream): Unit = {
    out.writeObject(exception)
  }
  private def readObject(in: ObjectInputStream): Unit = {
    try {
      exception = in.readObject().asInstanceOf[Throwable]
    } catch {
      case e : Exception => log.warn("Task exception could not be deserialized", e)
    }
  }
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
  override def countTowardsTaskFailures: Boolean = false
}

/**
 * :: DeveloperApi ::
 * Task requested the driver to commit, but was denied.
 */
@DeveloperApi
case class TaskCommitDenied(
    jobID: Int,
    partitionID: Int,
    attemptNumber: Int) extends TaskFailedReason {
  override def toErrorString: String = s"TaskCommitDenied (Driver denied task commit)" +
    s" for job: $jobID, partition: $partitionID, attemptNumber: $attemptNumber"
  /**
   * If a task failed because its attempt to commit was denied, do not count this failure
   * towards failing the stage. This is intended to prevent spurious stage failures in cases
   * where many speculative tasks are launched and denied to commit.
   */
  override def countTowardsTaskFailures: Boolean = false
}

/**
 * :: DeveloperApi ::
 * The task failed because the executor that it was running on was lost. This may happen because
 * the task crashed the JVM.
 */
@DeveloperApi
case class ExecutorLostFailure(
    execId: String,
    exitCausedByApp: Boolean = true,
    reason: Option[String]) extends TaskFailedReason {
  override def toErrorString: String = {
    val exitBehavior = if (exitCausedByApp) {
      "caused by one of the running tasks"
    } else {
      "unrelated to the running tasks"
    }
    s"ExecutorLostFailure (executor ${execId} exited ${exitBehavior})" +
      reason.map { r => s" Reason: $r" }.getOrElse("")
  }

  override def countTowardsTaskFailures: Boolean = exitCausedByApp
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
