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

package org.apache.spark.mapred

import java.io.IOException
import java.lang.reflect.Modifier

import org.apache.hadoop.mapred._
import org.apache.hadoop.mapreduce.{TaskAttemptContext => MapReduceTaskAttemptContext}
import org.apache.hadoop.mapreduce.{OutputCommitter => MapReduceOutputCommitter}

import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.executor.CommitDeniedException
import org.apache.spark.{Logging, SparkEnv, TaskContext}
import org.apache.spark.util.{Utils => SparkUtils}

private[spark]
trait SparkHadoopMapRedUtil {
  def newJobContext(conf: JobConf, jobId: JobID): JobContext = {
    val klass = firstAvailableClass("org.apache.hadoop.mapred.JobContextImpl",
      "org.apache.hadoop.mapred.JobContext")
    val ctor = klass.getDeclaredConstructor(classOf[JobConf],
      classOf[org.apache.hadoop.mapreduce.JobID])
    // In Hadoop 1.0.x, JobContext is an interface, and JobContextImpl is package private.
    // Make it accessible if it's not in order to access it.
    if (!Modifier.isPublic(ctor.getModifiers)) {
      ctor.setAccessible(true)
    }
    ctor.newInstance(conf, jobId).asInstanceOf[JobContext]
  }

  def newTaskAttemptContext(conf: JobConf, attemptId: TaskAttemptID): TaskAttemptContext = {
    val klass = firstAvailableClass("org.apache.hadoop.mapred.TaskAttemptContextImpl",
      "org.apache.hadoop.mapred.TaskAttemptContext")
    val ctor = klass.getDeclaredConstructor(classOf[JobConf], classOf[TaskAttemptID])
    // See above
    if (!Modifier.isPublic(ctor.getModifiers)) {
      ctor.setAccessible(true)
    }
    ctor.newInstance(conf, attemptId).asInstanceOf[TaskAttemptContext]
  }

  def newTaskAttemptID(
      jtIdentifier: String,
      jobId: Int,
      isMap: Boolean,
      taskId: Int,
      attemptId: Int): TaskAttemptID = {
    new TaskAttemptID(jtIdentifier, jobId, isMap, taskId, attemptId)
  }

  private def firstAvailableClass(first: String, second: String): Class[_] = {
    try {
      SparkUtils.classForName(first)
    } catch {
      case e: ClassNotFoundException =>
        SparkUtils.classForName(second)
    }
  }
}

object SparkHadoopMapRedUtil extends Logging {
  /**
   * Commits a task output.  Before committing the task output, we need to know whether some other
   * task attempt might be racing to commit the same output partition. Therefore, coordinate with
   * the driver in order to determine whether this attempt can commit (please see SPARK-4879 for
   * details).
   *
   * Output commit coordinator is only used when `spark.hadoop.outputCommitCoordination.enabled`
   * is set to true (which is the default).
   */
  def commitTask(
      committer: MapReduceOutputCommitter,
      mrTaskContext: MapReduceTaskAttemptContext,
      jobId: Int,
      splitId: Int): Unit = {

    val mrTaskAttemptID = SparkHadoopUtil.get.getTaskAttemptIDFromTaskAttemptContext(mrTaskContext)

    // Called after we have decided to commit
    def performCommit(): Unit = {
      try {
        committer.commitTask(mrTaskContext)
        logInfo(s"$mrTaskAttemptID: Committed")
      } catch {
        case cause: IOException =>
          logError(s"Error committing the output of task: $mrTaskAttemptID", cause)
          committer.abortTask(mrTaskContext)
          throw cause
      }
    }

    // First, check whether the task's output has already been committed by some other attempt
    if (committer.needsTaskCommit(mrTaskContext)) {
      val shouldCoordinateWithDriver: Boolean = {
        val sparkConf = SparkEnv.get.conf
        // We only need to coordinate with the driver if there are concurrent task attempts.
        // Note that this could happen even when speculation is not enabled (e.g. see SPARK-8029).
        // This (undocumented) setting is an escape-hatch in case the commit code introduces bugs.
        sparkConf.getBoolean("spark.hadoop.outputCommitCoordination.enabled", defaultValue = true)
      }

      if (shouldCoordinateWithDriver) {
        val outputCommitCoordinator = SparkEnv.get.outputCommitCoordinator
        val taskAttemptNumber = TaskContext.get().attemptNumber()
        val canCommit = outputCommitCoordinator.canCommit(jobId, splitId, taskAttemptNumber)

        if (canCommit) {
          performCommit()
        } else {
          val message =
            s"$mrTaskAttemptID: Not committed because the driver did not authorize commit"
          logInfo(message)
          // We need to abort the task so that the driver can reschedule new attempts, if necessary
          committer.abortTask(mrTaskContext)
          throw new CommitDeniedException(message, jobId, splitId, taskAttemptNumber)
        }
      } else {
        // Speculation is disabled or a user has chosen to manually bypass the commit coordination
        performCommit()
      }
    } else {
      // Some other attempt committed the output, so we do nothing and signal success
      logInfo(s"No need to commit output of task because needsTaskCommit=false: $mrTaskAttemptID")
    }
  }
}
