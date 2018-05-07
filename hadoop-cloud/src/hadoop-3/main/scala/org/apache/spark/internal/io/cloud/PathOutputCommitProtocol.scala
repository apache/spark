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

package org.apache.spark.internal.io.cloud

import java.io.IOException

import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.{JobContext, TaskAttemptContext}
import org.apache.hadoop.mapreduce.lib.output.{FileOutputCommitter, PathOutputCommitter, PathOutputCommitterFactory}

import org.apache.spark.internal.io.{FileCommitProtocol, HadoopMapReduceCommitProtocol}
import org.apache.spark.internal.io.FileCommitProtocol.TaskCommitMessage

/**
 * Spark Commit protocol for Path Output Committers.
 * This committer will work with the `FileOutputCommitter` and subclasses.
 * All implementations *must* be serializable.
 *
 * Rather than ask the `FileOutputFormat` for a committer, it uses the
 * `org.apache.hadoop.mapreduce.lib.output.PathOutputCommitterFactory` factory
 * API to create the committer.
 * This is what [[org.apache.hadoop.mapreduce.lib.output.FileOutputFormat]] does,
 * but as [[HadoopMapReduceCommitProtocol]] still uses the original
 * `org.apache.hadoop.mapred.FileOutputFormat` binding
 * subclasses do not do this, overrides those subclasses to using the
 * factory mechanism now supported in the base class.
 *
 * In `setupCommitter` the factory is bonded to and the committer for
 * the destination path chosen.
 *
 * @constructor Instantiate. dynamic partition overwrite is not supported,
 *              so that committers for stores which do not support rename
 *              will not get confused.
 * @param jobId                     job
 * @param destination               destination
 * @param dynamicPartitionOverwrite does the caller want support for dynamic
 *                                  partition overwrite. If so, it will be
 *                                  refused.
 * @throws IOException when an unsupported dynamicPartitionOverwrite option is supplied.
 */
class PathOutputCommitProtocol(
    jobId: String,
    destination: String,
    dynamicPartitionOverwrite: Boolean = false)
  extends HadoopMapReduceCommitProtocol(jobId, destination, false) with Serializable {

  @transient var committer: PathOutputCommitter = _

  require(destination != null, "Null destination specified")

  val destPath: Path  = new Path(destination)

  logDebug(s"Instantiated committer with job ID=$jobId;" +
    s" destination=$destPath;" +
    s" dynamicPartitionOverwrite=$dynamicPartitionOverwrite")

  if (dynamicPartitionOverwrite) {
    // until there's explicit extensions to the PathOutputCommitProtocols
    // to support the spark mechanism, it's left to the individual committer
    // choice to handle partitioning.
    throw new IOException(PathOutputCommitProtocol.UNSUPPORTED)
  }

  def getDestination(): String = destination

  import PathOutputCommitProtocol._

  /**
   * Set up the committer.
   * This creates it by talking directly to the Hadoop factories, instead
   * of the V1 `mapred.FileOutputFormat` methods.
   * @param context task attempt
   * @return the committer to use. This will always be a subclass of
   *         [[PathOutputCommitter]].
   */
  override protected def setupCommitter(
    context: TaskAttemptContext): PathOutputCommitter = {

    logDebug(s"Setting up committer for path $destination")
    committer = PathOutputCommitterFactory.createCommitter(destPath, context)

    // Special feature to force out the FileOutputCommitter, so as to guarantee
    // that the binding is working properly.
    val rejectFileOutput = context.getConfiguration
      .getBoolean(REJECT_FILE_OUTPUT, REJECT_FILE_OUTPUT_DEFVAL)
    if (rejectFileOutput && committer.isInstanceOf[FileOutputCommitter]) {
      // the output format returned a file output format committer, which
      // is exactly what we do not want. So switch back to the factory.
      val factory = PathOutputCommitterFactory.getCommitterFactory(
        destPath,
        context.getConfiguration)
      logDebug(s"Using committer factory $factory")
      committer = factory.createOutputCommitter(destPath, context)
    }

    logDebug(s"Using committer ${committer.getClass}")
    logDebug(s"Committer details: $committer")
    if (committer.isInstanceOf[FileOutputCommitter]) {
      require(!rejectFileOutput,
        s"Committer created is the FileOutputCommitter $committer")

      if (committer.isCommitJobRepeatable(context)) {
        // If FileOutputCommitter says its job commit is repeatable, it means
        // it is using the v2 algorithm, which is not safe for task commit
        // failures. Warn
        logDebug(s"Committer $committer may not be tolerant of task commit failures")
      }
    }
    committer
  }

  /**
   * Create a temporary file for a task.
   *
   * @param taskContext task context
   * @param dir         optional subdirectory
   * @param ext         file extension
   * @return a path as a string
   */
  override def newTaskTempFile(
    taskContext: TaskAttemptContext,
    dir: Option[String],
    ext: String): String = {

    val workDir = committer.getWorkPath
    val parent = dir.map(d => new Path(workDir, d)).getOrElse(workDir)
    val file = new Path(parent, buildFilename(taskContext, ext))
    logDebug(s"Creating task file $file for dir $dir and ext $ext")
    file.toString
  }

  /**
   * Absolute files are still renamed into place with a warning.
   *
   * @param taskContext task
   * @param absoluteDir destination dir
   * @param ext         extension
   * @return an absolute path
   */
  override def newTaskTempFileAbsPath(
    taskContext: TaskAttemptContext,
    absoluteDir: String,
    ext: String): String = {

    val file = super.newTaskTempFileAbsPath(taskContext, absoluteDir, ext)
    logInfo(s"Creating temporary file $file for absolute path for dir $absoluteDir")
    file
  }

  /**
   * Build a filename which is unique across all task events.
   * It does not have to be consistent across multiple attempts of the same
   * task or job.
   *
   * @param taskContext task context
   * @param ext         extension
   * @return a name for a file which must be unique across all task attempts
   */
  protected def buildFilename(
    taskContext: TaskAttemptContext,
    ext: String): String = {

    // The file name looks like part-00000-2dd664f9-d2c4-4ffe-878f-c6c70c1fb0cb_00003-c000.parquet
    // Note that %05d does not truncate the split number, so if we have more than 100000 tasks,
    // the file name is fine and won't overflow.
    val split = taskContext.getTaskAttemptID.getTaskID.getId
    f"part-$split%05d-$jobId$ext"
  }

  override def setupJob(jobContext: JobContext): Unit = {
    logDebug("setup job")
    super.setupJob(jobContext)
  }

  override def commitJob(
    jobContext: JobContext,
    taskCommits: Seq[FileCommitProtocol.TaskCommitMessage]): Unit = {
    logDebug(s"commit job with ${taskCommits.length} task commit message(s)")
    super.commitJob(jobContext, taskCommits)
  }

  /**
   * Abort the job; log and ignore any IO exception thrown.
   * This is invariably invoked in an exception handler; raising
   * an exception here will lose the root cause of the failure.
   *
   * @param jobContext job context
   */
  override def abortJob(jobContext: JobContext): Unit = {
    try {
      super.abortJob(jobContext)
    } catch {
      case e: IOException =>
        logWarning("Abort job failed", e)
    }
  }

  override def setupTask(taskContext: TaskAttemptContext): Unit = {
    super.setupTask(taskContext)
  }

  override def commitTask(
    taskContext: TaskAttemptContext): FileCommitProtocol.TaskCommitMessage = {
    logDebug("Commit task")
    super.commitTask(taskContext)
  }

  /**
   * Abort the task; log and ignore any failure thrown.
   * This is invariably invoked in an exception handler; raising
   * an exception here will lose the root cause of the failure.
   * @param taskContext context
   */
  override def abortTask(taskContext: TaskAttemptContext): Unit = {
    logDebug("Abort task")
    try {
      super.abortTask(taskContext)
    } catch {
      case e: IOException =>
        logWarning("Abort task failed", e)
    }
  }

  override def onTaskCommit(msg: TaskCommitMessage): Unit = {
    logDebug(s"onTaskCommit($msg)")
    super.onTaskCommit(msg)
  }
}

object PathOutputCommitProtocol {

  /**
   * Hadoop configuration option.
   * Fail fast if the committer is using the path output protocol.
   * This option can be used to catch configuration issues early.
   *
   * It's mostly relevant when testing/diagnostics, as it can be used to
   * enforce that schema-specific options are triggering a switch
   * to a new committer.
   */
  val REJECT_FILE_OUTPUT = "pathoutputcommit.reject.fileoutput"

  /**
   * Default behavior: accept the file output.
   */
  val REJECT_FILE_OUTPUT_DEFVAL = false

  /** Error string for tests. */
  private[cloud] val UNSUPPORTED = "PathOutputCommitProtocol does not support" +
    " dynamicPartitionOverwrite"

}
