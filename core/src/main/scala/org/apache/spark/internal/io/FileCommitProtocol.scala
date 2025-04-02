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

package org.apache.spark.internal.io

import org.apache.hadoop.fs._
import org.apache.hadoop.mapreduce._

import org.apache.spark.annotation.Unstable
import org.apache.spark.internal.Logging
import org.apache.spark.util.Utils


/**
 * An interface to define how a single Spark job commits its outputs. Three notes:
 *
 * 1. Implementations must be serializable, as the committer instance instantiated on the driver
 *    will be used for tasks on executors.
 * 2. Implementations should have a constructor with 2 or 3 arguments:
 *      (jobId: String, path: String) or
 *      (jobId: String, path: String, dynamicPartitionOverwrite: Boolean)
 * 3. A committer should not be reused across multiple Spark jobs.
 *
 * The proper call sequence is:
 *
 * 1. Driver calls setupJob.
 * 2. As part of each task's execution, executor calls setupTask and then commitTask
 *    (or abortTask if task failed).
 * 3. When all necessary tasks completed successfully, the driver calls commitJob. If the job
 *    failed to execute (e.g. too many failed tasks), the job should call abortJob.
 *
 * @note This class is exposed as an API considering the usage of many downstream custom
 * implementations, but will be subject to be changed and/or moved.
 */
@Unstable
abstract class FileCommitProtocol extends Logging {
  import FileCommitProtocol._

  /**
   * Setups up a job. Must be called on the driver before any other methods can be invoked.
   */
  def setupJob(jobContext: JobContext): Unit

  /**
   * Commits a job after the writes succeed. Must be called on the driver.
   */
  def commitJob(jobContext: JobContext, taskCommits: Seq[TaskCommitMessage]): Unit

  /**
   * Aborts a job after the writes fail. Must be called on the driver.
   *
   * Calling this function is a best-effort attempt, because it is possible that the driver
   * just crashes (or killed) before it can call abort.
   */
  def abortJob(jobContext: JobContext): Unit

  /**
   * Sets up a task within a job.
   * Must be called before any other task related methods can be invoked.
   */
  def setupTask(taskContext: TaskAttemptContext): Unit

  /**
   * Notifies the commit protocol to add a new file, and gets back the full path that should be
   * used. Must be called on the executors when running tasks.
   *
   * Note that the returned temp file may have an arbitrary path. The commit protocol only
   * promises that the file will be at the location specified by the arguments after job commit.
   *
   * A full file path consists of the following parts:
   *  1. the base path
   *  2. some sub-directory within the base path, used to specify partitioning
   *  3. file prefix, usually some unique job id with the task id
   *  4. bucket id
   *  5. source specific file extension, e.g. ".snappy.parquet"
   *
   * The "dir" parameter specifies 2, and "ext" parameter specifies both 4 and 5, and the rest
   * are left to the commit protocol implementation to decide.
   *
   * Important: it is the caller's responsibility to add uniquely identifying content to "ext"
   * if a task is going to write out multiple files to the same dir. The file commit protocol only
   * guarantees that files written by different tasks will not conflict.
   */
  @deprecated("use newTaskTempFile(..., spec: FileNameSpec) instead", "3.3.0")
  def newTaskTempFile(taskContext: TaskAttemptContext, dir: Option[String], ext: String): String

  /**
   * Notifies the commit protocol to add a new file, and gets back the full path that should be
   * used. Must be called on the executors when running tasks.
   *
   * Note that the returned temp file may have an arbitrary path. The commit protocol only
   * promises that the file will be at the location specified by the arguments after job commit.
   *
   * The "dir" parameter specifies the sub-directory within the base path, used to specify
   * partitioning. The "spec" parameter specifies the file name. The rest are left to the commit
   * protocol implementation to decide.
   *
   * Important: it is the caller's responsibility to add uniquely identifying content to "spec"
   * if a task is going to write out multiple files to the same dir. The file commit protocol only
   * guarantees that files written by different tasks will not conflict.
   *
   * @since 3.2.0
   */
  def newTaskTempFile(
      taskContext: TaskAttemptContext, dir: Option[String], spec: FileNameSpec): String = {
    if (spec.prefix.isEmpty) {
      newTaskTempFile(taskContext, dir, spec.suffix)
    } else {
      throw new UnsupportedOperationException(s"${getClass.getSimpleName}.newTaskTempFile does " +
        s"not support file name prefix: ${spec.prefix}")
    }
  }

  /**
   * Similar to newTaskTempFile(), but allows files to committed to an absolute output location.
   * Depending on the implementation, there may be weaker guarantees around adding files this way.
   *
   * Important: it is the caller's responsibility to add uniquely identifying content to "ext"
   * if a task is going to write out multiple files to the same dir. The file commit protocol only
   * guarantees that files written by different tasks will not conflict.
   */
  @deprecated("use newTaskTempFileAbsPath(..., spec: FileNameSpec) instead", "3.3.0")
  def newTaskTempFileAbsPath(
      taskContext: TaskAttemptContext, absoluteDir: String, ext: String): String

  /**
   * Similar to newTaskTempFile(), but allows files to committed to an absolute output location.
   * Depending on the implementation, there may be weaker guarantees around adding files this way.
   *
   * The "absoluteDir" parameter specifies the final absolute directory of file. The "spec"
   * parameter specifies the file name. The rest are left to the commit protocol implementation to
   * decide.
   *
   * Important: it is the caller's responsibility to add uniquely identifying content to "spec"
   * if a task is going to write out multiple files to the same dir. The file commit protocol only
   * guarantees that files written by different tasks will not conflict.
   *
   * @since 3.2.0
   */
  def newTaskTempFileAbsPath(
      taskContext: TaskAttemptContext, absoluteDir: String, spec: FileNameSpec): String = {
    if (spec.prefix.isEmpty) {
      newTaskTempFileAbsPath(taskContext, absoluteDir, spec.suffix)
    } else {
      throw new UnsupportedOperationException(
        s"${getClass.getSimpleName}.newTaskTempFileAbsPath does not support file name prefix: " +
          s"${spec.prefix}")
    }
  }

  /**
   * Commits a task after the writes succeed. Must be called on the executors when running tasks.
   */
  def commitTask(taskContext: TaskAttemptContext): TaskCommitMessage

  /**
   * Aborts a task after the writes have failed. Must be called on the executors when running tasks.
   *
   * Calling this function is a best-effort attempt, because it is possible that the executor
   * just crashes (or killed) before it can call abort.
   */
  def abortTask(taskContext: TaskAttemptContext): Unit

  /**
   * Specifies that a file should be deleted with the commit of this job. The default
   * implementation deletes the file immediately.
   */
  def deleteWithJob(fs: FileSystem, path: Path, recursive: Boolean): Boolean = {
    fs.delete(path, recursive)
  }

  /**
   * Called on the driver after a task commits. This can be used to access task commit messages
   * before the job has finished. These same task commit messages will be passed to commitJob()
   * if the entire job succeeds.
   */
  def onTaskCommit(taskCommit: TaskCommitMessage): Unit = {
    logDebug(s"onTaskCommit($taskCommit)")
  }
}


object FileCommitProtocol extends Logging {
  class TaskCommitMessage(val obj: Any) extends Serializable

  object EmptyTaskCommitMessage extends TaskCommitMessage(null)

  /**
   * Instantiates a FileCommitProtocol using the given className.
   */
  def instantiate(
      className: String,
      jobId: String,
      outputPath: String,
      dynamicPartitionOverwrite: Boolean = false): FileCommitProtocol = {

    logDebug(s"Creating committer $className; job $jobId; output=$outputPath;" +
      s" dynamic=$dynamicPartitionOverwrite")
    val clazz = Utils.classForName[FileCommitProtocol](className)
    // First try the constructor with arguments (jobId: String, outputPath: String,
    // dynamicPartitionOverwrite: Boolean).
    // If that doesn't exist, try the one with (jobId: string, outputPath: String).
    try {
      val ctor = clazz.getDeclaredConstructor(classOf[String], classOf[String], classOf[Boolean])
      logDebug("Using (String, String, Boolean) constructor")
      ctor.newInstance(jobId, outputPath, dynamicPartitionOverwrite.asInstanceOf[java.lang.Boolean])
    } catch {
      case _: NoSuchMethodException =>
        logDebug("Falling back to (String, String) constructor")
        require(!dynamicPartitionOverwrite,
          "Dynamic Partition Overwrite is enabled but" +
            s" the committer ${className} does not have the appropriate constructor")
        val ctor = clazz.getDeclaredConstructor(classOf[String], classOf[String])
        ctor.newInstance(jobId, outputPath)
    }
  }

  def getStagingDir(path: String, jobId: String): Path = {
    new Path(path, ".spark-staging-" + jobId)
  }
}

/**
 * The specification for Spark output file name.
 * This is used by [[FileCommitProtocol]] to create full path of file.
 *
 * @param prefix Prefix of file.
 * @param suffix Suffix of file.
 */
final case class FileNameSpec(prefix: String, suffix: String)
