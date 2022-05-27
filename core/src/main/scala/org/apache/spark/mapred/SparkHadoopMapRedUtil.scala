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

import java.io.{FileNotFoundException, IOException}

import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.hadoop.mapreduce.{TaskAttemptContext => MapReduceTaskAttemptContext}
import org.apache.hadoop.mapreduce.{OutputCommitter => MapReduceOutputCommitter}
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter

import org.apache.spark.{SparkEnv, TaskContext}
import org.apache.spark.executor.CommitDeniedException
import org.apache.spark.internal.Logging
import org.apache.spark.util.Utils

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

    val mrTaskAttemptID = mrTaskContext.getTaskAttemptID

    // Called after we have decided to commit
    def performCommit(): Unit = {
      try {
        val (_, timeCost) = Utils.timeTakenMs(committer.commitTask(mrTaskContext))
        logInfo(s"$mrTaskAttemptID: Committed. Elapsed time: $timeCost ms.")
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
        val ctx = TaskContext.get()
        // We only to coordinate task outputs when the executor will be killed by other reasons.
        val shouldCoordinateWithTaskAttemptOutputs =
          SparkEnv.get.conf.getBoolean(
            "spark.hadoop.coordinateTaskCommitOutputs.enabled", defaultValue = false)
        val taskAttemptOutputs: Seq[Path] = if (shouldCoordinateWithTaskAttemptOutputs) {
          committer match {
            case f: FileOutputCommitter =>
              if (f.isCommitJobRepeatable(mrTaskContext)) {
                // If algorithmVersion is 2, Spark should get file under final output path.
                def getTaskAttemptCommittedPaths(
                  fs: FileSystem,
                  from: FileStatus,
                  to: Path): Seq[Path] = {
                  if (from.isFile) {
                    Seq(new Path(to, from.getPath.getName))
                  } else {
                    try {
                      fs.listStatus(from.getPath).flatMap { status =>
                        getTaskAttemptCommittedPaths(
                          fs, status, new Path(to, status.getPath.getName))
                      }
                    } catch {
                      case _: FileNotFoundException | _: IOException =>
                        Seq.empty
                    }
                  }
                }

                val taskAttemptPath = f.getTaskAttemptPath(mrTaskContext)
                val fs: FileSystem = taskAttemptPath.getFileSystem(mrTaskContext.getConfiguration)
                try {
                  val taskAttemptDirStatus = fs.getFileStatus(taskAttemptPath)
                  getTaskAttemptCommittedPaths(fs, taskAttemptDirStatus, f.getOutputPath)
                } catch {
                  case _: FileNotFoundException | _: IOException =>
                    Seq.empty
                }
              } else {
                // If algorithmVersion is 1, task commit final path is `committedTaskAttemptPath`.
                Seq(f.getCommittedTaskPath(mrTaskContext))
              }
            case _ => Seq.empty
          }
        } else {
          Seq.empty
        }
        val canCommit = outputCommitCoordinator.canCommit(ctx.stageId(), ctx.stageAttemptNumber(),
          splitId, ctx.attemptNumber(), taskAttemptOutputs)

        if (canCommit) {
          performCommit()
        } else {
          val message =
            s"$mrTaskAttemptID: Not committed because the driver did not authorize commit"
          logInfo(message)
          // We need to abort the task so that the driver can reschedule new attempts, if necessary
          committer.abortTask(mrTaskContext)
          throw new CommitDeniedException(message, ctx.stageId(), splitId, ctx.attemptNumber())
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
