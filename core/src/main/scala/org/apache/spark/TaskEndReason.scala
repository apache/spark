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

import java.io.{IOException, ObjectInputStream, ObjectOutputStream}

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
    metrics: Option[TaskMetrics],
    private val exceptionWrapper: Option[ThrowableSerializationWrapper])
  extends TaskFailedReason {

  /**
   * `preserveCause` is used to keep the exception itself so it is available to the
   * driver. This may be set to `false` in the event that the exception is not in fact
   * serializable.
   */
  private[spark] def this(e: Throwable, metrics: Option[TaskMetrics], preserveCause: Boolean) {
    this(e.getClass.getName, e.getMessage, e.getStackTrace, Utils.exceptionString(e), metrics,
      if (preserveCause) Some(new ThrowableSerializationWrapper(e)) else None)
  }

  private[spark] def this(e: Throwable, metrics: Option[TaskMetrics]) {
    this(e, metrics, preserveCause = true)
  }

  def exception: Option[Throwable] = exceptionWrapper.flatMap {
    (w: ThrowableSerializationWrapper) => Option(w.exception)
  }

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
}

/**
 * :: DeveloperApi ::
 * Task requested the driver to commit, but was denied.
 */
@DeveloperApi
case class TaskCommitDenied(jobID: Int, partitionID: Int, attemptID: Int) extends TaskFailedReason {
  override def toErrorString: String = s"TaskCommitDenied (Driver denied task commit)" +
    s" for job: $jobID, partition: $partitionID, attempt: $attemptID"
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
