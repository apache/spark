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

import java.io.IOException
import java.util.{Date, UUID}

import scala.collection.mutable
import scala.util.Try

import org.apache.hadoop.conf.Configurable
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl

import org.apache.spark.SparkEnv
import org.apache.spark.TaskContext
import org.apache.spark.executor.CommitDeniedException
import org.apache.spark.internal.Logging

/**
 * An [[FileCommitProtocol]] implementation backed by an underlying Hadoop OutputCommitter
 * (from the newer mapreduce API, not the old mapred API).
 *
 * Unlike Hadoop's OutputCommitter, this implementation is serializable.
 *
 * @param jobId the job's or stage's id
 * @param path the job's output path, or null if committer acts as a noop
 * @param dynamicPartitionOverwrite If true, Spark will overwrite partition directories at runtime
 *                                  dynamically, i.e., we first write files under a staging
 *                                  directory with partition path, e.g.
 *                                  /path/to/staging/a=1/b=1/xxx.parquet. When committing the job,
 *                                  we first clean up the corresponding partition directories at
 *                                  destination path, e.g. /path/to/destination/a=1/b=1, and move
 *                                  files from staging directory to the corresponding partition
 *                                  directories under destination path.
 */
class HadoopMapReduceCommitProtocol(
    jobId: String,
    path: String,
    dynamicPartitionOverwrite: Boolean = false)
  extends FileCommitProtocol with Serializable with Logging {

  import FileCommitProtocol._

  /** OutputCommitter from Hadoop is not serializable so marking it transient. */
  @transient private var committer: OutputCommitter = _

  /**
   * Checks whether there are files to be committed to a valid output location.
   *
   * As committing and aborting a job occurs on driver, where `addedAbsPathFiles` is always null,
   * it is necessary to check whether a valid output path is specified.
   * [[HadoopMapReduceCommitProtocol#path]] need not be a valid [[org.apache.hadoop.fs.Path]] for
   * committers not writing to distributed file systems.
   */
  private val hasValidPath = Try { new Path(path) }.isSuccess

  @transient private var workPath: Path = null

  @transient private var outputPath: Path = null

  @transient private var committedPaths: mutable.Set[String] = null

  /**
   * The staging directory of this write job. Spark uses it to deal with files with absolute output
   * path, or writing data into partitioned directory with dynamicPartitionOverwrite=true.
   */
  private def stagingDir = new Path(path, ".spark-staging-" + jobId)

  protected def setupCommitter(context: TaskAttemptContext): OutputCommitter = {
    val format = context.getOutputFormatClass.getConstructor().newInstance()
    // If OutputFormat is Configurable, we should set conf to it.
    format match {
      case c: Configurable => c.setConf(context.getConfiguration)
      case _ => ()
    }
    format.getOutputCommitter(context)
  }

  override def newTaskTempFile(
      taskContext: TaskAttemptContext, dir: Option[String], ext: String): String = {
    val filename = getFilename(taskContext, ext)

    dir.map { d =>
      new Path(new Path(workPath, d), filename).toString
    }.getOrElse {
      new Path(workPath, filename).toString
    }
  }

  override def newTaskTempFileAbsPath(
                                       taskContext: TaskAttemptContext, absoluteDir: String,
                                       ext: String): String = {
    val filename = getFilename(taskContext, ext)
    outputPath = new Path(absoluteDir)

    // Include a UUID here to prevent file collisions for one task writing to different dirs.
    // In principle we could include hash(absoluteDir) instead but this is simpler.
    new Path(workPath, UUID.randomUUID().toString() + "-" + filename).toString
  }

  protected def getFilename(taskContext: TaskAttemptContext, ext: String): String = {
    // The file name looks like part-00000-2dd664f9-d2c4-4ffe-878f-c6c70c1fb0cb_00003-c000.parquet
    // Note that %05d does not truncate the split number, so if we have more than 100000 tasks,
    // the file name is fine and won't overflow.
    val split = taskContext.getTaskAttemptID.getTaskID.getId
    f"part-$split%05d-$jobId$ext"
  }

  override def setupJob(jobContext: JobContext): Unit = {
    // Setup IDs
    val jobId = SparkHadoopWriterUtils.createJobID(new Date, 0)
    val taskId = new TaskID(jobId, TaskType.MAP, 0)
    val taskAttemptId = new TaskAttemptID(taskId, 0)

    // Set up the configuration object
    jobContext.getConfiguration.set("mapreduce.job.id", jobId.toString)
    jobContext.getConfiguration.set("mapreduce.task.id", taskAttemptId.getTaskID.toString)
    jobContext.getConfiguration.set("mapreduce.task.attempt.id", taskAttemptId.toString)
    jobContext.getConfiguration.setBoolean("mapreduce.task.ismap", true)
    jobContext.getConfiguration.setInt("mapreduce.task.partition", 0)

    val taskAttemptContext = new TaskAttemptContextImpl(jobContext.getConfiguration, taskAttemptId)
    committer = setupCommitter(taskAttemptContext)
    committer.setupJob(jobContext)

    outputPath = committer match {
      case f: FileOutputCommitter =>
        new Path(Option(f.getOutputPath).map(_.toString).getOrElse(path))
      case _ => new Path(path)
    }
    committedPaths = mutable.Set[String]()
  }

  override def commitJob(jobContext: JobContext, taskCommits: Seq[TaskCommitMessage]): Unit = {
    committer.commitJob(jobContext)

    if (hasValidPath) {
      val outputPaths = taskCommits.map(_.obj.asInstanceOf[mutable.Set[String]]).flatten
      val dstPathsMap =
        outputPaths.map { path =>
          new Path(path) -> new Path(path.replace(stagingDir.toString, outputPath.toString))
        }.toMap
      val fs = stagingDir.getFileSystem(jobContext.getConfiguration)

      logDebug(s"Committing files staged for absolute locations $outputPaths")
      if (dynamicPartitionOverwrite) {
        val partitionPaths = dstPathsMap.values.map(_.getParent).toSet
        logDebug(s"Clean up absolute partition directories for overwriting: ${partitionPaths}")
        partitionPaths.foreach(fs.delete(_, true))
      }
      for ((src, dst) <- dstPathsMap) {
        recursiveRenameFile(fs, src, dst)
      }
      for (outputPath <- dstPathsMap.keySet) {
        fs.delete(outputPath, true)
      }
    }
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
      committer.abortJob(jobContext, JobStatus.State.FAILED)
    } catch {
      case e: IOException =>
        logWarning(s"Exception while aborting ${jobContext.getJobID}", e)
    }
    try {
      if (hasValidPath) {
        val fs = workPath.getFileSystem(jobContext.getConfiguration)
        fs.delete(workPath, true)
      }
    } catch {
      case e: IOException =>
        logWarning(s"Exception while aborting ${jobContext.getJobID}", e)
    }
  }

  override def setupTask(taskContext: TaskAttemptContext): Unit = {
    committer = setupCommitter(taskContext)
    committer.setupTask(taskContext)
    committedPaths = mutable.Set[String]()
    workPath = committer match {
      case f: FileOutputCommitter =>
        new Path(Option(f.getWorkPath).map(_.toString).getOrElse(path))
      case _ => new Path(path)
    }

    outputPath = committer match {
      case f: FileOutputCommitter =>
        new Path(Option(f.getOutputPath).map(_.toString).getOrElse(path))
      case _ => new Path(path)
    }
  }

  /**
   * Commits a task output.  Before committing the task output, we need to know whether some other
   * task attempt might be racing to commit the same output partition. Therefore, coordinate with
   * the driver in order to determine whether this attempt can commit (please see SPARK-4879 for
   * details).
   *
   * Output commit coordinator is only used when `spark.hadoop.outputCommitCoordination.enabled`
   * is set to true (which is the default).
   */
  override def commitTask(taskContext: TaskAttemptContext): TaskCommitMessage = {
    val attemptId = taskContext.getTaskAttemptID
    val splitId = attemptId.getTaskID.getId

    logTrace(s"Commit task ${attemptId}")

    // First, check whether the task's output has already been committed by some other attempt
    if (committer.needsTaskCommit(taskContext)) {
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
        val canCommit = outputCommitCoordinator.canCommit(ctx.stageId(), ctx.stageAttemptNumber(),
          splitId, ctx.attemptNumber())

        if (canCommit) {
          performCommit(taskContext)
        } else {
          val message =
            s"$attemptId: Not committed because the driver did not authorize commit"
          logInfo(message)
          // We need to abort the task so that the driver can reschedule new attempts, if necessary
          committer.abortTask(taskContext)
          throw new CommitDeniedException(message, ctx.stageId(), splitId, ctx.attemptNumber())
        }
      } else {
        // Speculation is disabled or a user has chosen to manually bypass the commit coordination
        performCommit(taskContext)
      }
    } else {
      // Some other attempt committed the output, so we do nothing and signal success
      logInfo(s"No need to commit output of task because needsTaskCommit=false: $attemptId")
    }

    new TaskCommitMessage(committedPaths)
  }

  /**
   * Abort the task; log and ignore any failure thrown.
   * This is invariably invoked in an exception handler; raising
   * an exception here will lose the root cause of the failure.
   *
   * @param taskContext context
   */
  override def abortTask(taskContext: TaskAttemptContext): Unit = {
    try {
      val fs = workPath.getFileSystem(taskContext.getConfiguration)
      committedPaths.map { pathString =>
        val path = new Path(pathString)
        if (fs.exists(path)) {
          fs.delete(path, true)
        }
      }
      committer.abortTask(taskContext)
    } catch {
      case e: IOException =>
        logWarning(s"Exception while aborting ${taskContext.getTaskAttemptID}", e)
    }
  }

  def performCommit(taskContext: TaskAttemptContext): Unit = {
    val attemptID = taskContext.getTaskAttemptID

    try {
      logDebug(s"Committing task attempt files from $workPath to $stagingDir")
      val fs = workPath.getFileSystem(taskContext.getConfiguration)
      recursiveRenameFile(fs, workPath, stagingDir)
      committer.commitTask(taskContext)
      logInfo(s"$attemptID: Committed")
    } catch {
      case cause: IOException =>
        logError(s"Error committing the output of task: $attemptID", cause)
        committer.abortTask(taskContext)
        throw cause
    }
  }

  /**
   *
   * @param fs
   * @param src
   * @param dst
   */
  def recursiveRenameFile(fs: FileSystem, src: Path, dst: Path): Unit = {
    src match {
      case _ if !fs.exists(src) =>
        throw new IOException(s"Path $src not exists!")

      case _ if fs.getFileStatus(src).isFile =>
        if (!fs.exists(dst.getParent)) {
          fs.mkdirs(dst.getParent)
        }
        fs.rename(src, dst)
        addCommitedPath(fs, dst)

      case _ if fs.getFileStatus(src).isDirectory =>
        fs.listStatus(src).map(_.getPath).foreach { file =>
          recursiveRenameFile(fs, file, new Path(dst, file.getName))
        }

      case _ =>
    }
  }

  def addCommitedPath(fs: FileSystem, path: Path): Unit = path match {
    case _ if fs.getFileStatus(path).isFile =>
      committedPaths += path.toString

    case _ if fs.getFileStatus(path).isDirectory =>
      fs.listStatus(path).map(_.getPath).foreach(addCommitedPath(fs, _))

    case _ =>
  }
}
