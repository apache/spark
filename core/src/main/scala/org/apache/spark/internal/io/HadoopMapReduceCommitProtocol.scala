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

import java.io.{File, IOException}
import java.util.{Date, UUID}

import scala.collection.mutable
import scala.util.Try

import org.apache.hadoop.conf.Configurable
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.lib.output.{FileOutputCommitter, FileOutputFormat}
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl

import org.apache.spark.SparkEnv
import org.apache.spark.internal.Logging
import org.apache.spark.mapred.SparkHadoopMapRedUtil

/**
 * An [[FileCommitProtocol]] implementation backed by an underlying Hadoop OutputCommitter
 * (from the newer mapreduce API, not the old mapred API).
 *
 * Unlike Hadoop's OutputCommitter, this implementation is serializable.
 *
 * @param jobId the job's or stage's id
 * @param path the job's output path, or null if committer acts as a noop
 * @param fileSourceWriteDesc a description for file source write operation
 */
class HadoopMapReduceCommitProtocol(
    jobId: String,
    path: String,
    fileSourceWriteDesc: Option[FileSourceWriteDesc])
  extends FileCommitProtocol with Serializable with Logging {

  import FileCommitProtocol._
  import HadoopMapReduceCommitProtocol._

  def this(jobId: String, path: String, dynamicPartitionOverwrite: Boolean = false) =
    this(jobId, path, Some(new FileSourceWriteDesc(dynamicPartitionOverwrite =
      dynamicPartitionOverwrite)))

  /**
   * If true, Spark will overwrite partition directories at runtime dynamically, i.e., we first
   * write files under a staging directory with partition path, e.g.
   * /path/to/staging/a=1/b=1/xxx.parquet.
   * When committing the job, we first clean up the corresponding partition directories at
   * destination path, e.g. /path/to/destination/a=1/b=1, and move files from staging directory to
   * the corresponding partition directories under destination path.
   */
  def dynamicPartitionOverwrite: Boolean =
    fileSourceWriteDesc.map(_.dynamicPartitionOverwrite).getOrElse(false)

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

  /**
   * Tracks files staged by this task for absolute output paths. These outputs are not managed by
   * the Hadoop OutputCommitter, so we must move these to their final locations on job commit.
   *
   * The mapping is from the temp output path to the final desired output path of the file.
   */
  @transient private var addedAbsPathFiles: mutable.Map[String, String] = null

  /**
   * Tracks partitions with default path that have new files written into them by this task,
   * e.g. a=1/b=2. Files under these partitions will be saved into staging directory and moved to
   * destination directory at the end, if `dynamicPartitionOverwrite` is true.
   */
  @transient private var partitionPaths: mutable.Set[String] = null

  /**
   * The staging directory of this write job. Spark uses it to deal with files with absolute output
   * path, or writing data into partitioned directory with dynamicPartitionOverwrite=true.
   */
  private def stagingDir = new Path(path, ".spark-staging-" + jobId)

  /**
   * For InsertIntoHadoopFsRelation operation, we support concurrent write to different partitions
   * in a same table.
   */
  def supportConcurrent: Boolean =
    fileSourceWriteDesc.map(_.isInsertIntoHadoopFsRelation).getOrElse(false)

  /**
   * Get escaped static partition key and value pairs, the default is empty.
   */
  private def escapedStaticPartitionKVs =
    fileSourceWriteDesc.map(_.escapedStaticPartitionKVs).getOrElse(Seq.empty)

  /**
   * The staging root directory for InsertIntoHadoopFsRelation operation.
   */
  @transient private var insertStagingDir: Path = null

  /**
   * The staging output path for InsertIntoHadoopFsRelation operation.
   */
  @transient private var stagingOutputPath: Path = null

  /**
   * Get the desired output path for the job. The output will be [[path]] when current operation
   * is not a InsertIntoHadoopFsRelation operation. Otherwise, we choose a sub path composed of
   * [[escapedStaticPartitionKVs]] under [[insertStagingDir]] over [[path]] to mark this operation
   * and we can detect whether there is a operation conflict with current by checking the existence
   * of relative output path.
   *
   * @return Path the desired output path.
   */
  protected def getOutputPath(context: TaskAttemptContext): Path = {
    if (supportConcurrent) {
      val insertStagingPath = ".spark-staging-" + escapedStaticPartitionKVs.size
      insertStagingDir = new Path(path, insertStagingPath)
      val appId = SparkEnv.get.conf.getAppId
      val outputPath = new Path(path, Array(insertStagingPath,
        getEscapedStaticPartitionPath(escapedStaticPartitionKVs), appId, jobId)
        .mkString(File.separator))
      insertStagingDir.getFileSystem(context.getConfiguration).makeQualified(outputPath)
      outputPath
    } else {
      new Path(path)
    }
  }

  protected def setupCommitter(context: TaskAttemptContext): OutputCommitter = {
    if (supportConcurrent) {
      stagingOutputPath = getOutputPath(context)
      context.getConfiguration.set(FileOutputFormat.OUTDIR, stagingOutputPath.toString)
      logDebug("Set file output committer algorithm version to 2 implicitly," +
        " for that the task output would be committed to staging output path firstly," +
        " which is equivalent to algorithm 1.")
      context.getConfiguration.setInt(FileOutputCommitter.FILEOUTPUTCOMMITTER_ALGORITHM_VERSION, 2)
    }

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

    val stagingDir: Path = committer match {
      case _ if dynamicPartitionOverwrite =>
        assert(dir.isDefined,
          "The dataset to be written must be partitioned when dynamicPartitionOverwrite is true.")
        partitionPaths += dir.get
        this.stagingDir
      // For FileOutputCommitter it has its own staging path called "work path".
      case f: FileOutputCommitter =>
        new Path(Option(f.getWorkPath).map(_.toString).getOrElse(path))
      case _ => new Path(path)
    }

    dir.map { d =>
      new Path(new Path(stagingDir, d), filename).toString
    }.getOrElse {
      new Path(stagingDir, filename).toString
    }
  }

  override def newTaskTempFileAbsPath(
      taskContext: TaskAttemptContext, absoluteDir: String, ext: String): String = {
    val filename = getFilename(taskContext, ext)
    val absOutputPath = new Path(absoluteDir, filename).toString

    // Include a UUID here to prevent file collisions for one task writing to different dirs.
    // In principle we could include hash(absoluteDir) instead but this is simpler.
    val tmpOutputPath = new Path(stagingDir, UUID.randomUUID().toString() + "-" + filename).toString

    addedAbsPathFiles(tmpOutputPath) = absOutputPath
    tmpOutputPath
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
  }

  override def commitJob(jobContext: JobContext, taskCommits: Seq[TaskCommitMessage]): Unit = {
    committer.commitJob(jobContext)

    if (hasValidPath) {
      val (allAbsPathFiles, allPartitionPaths) =
        taskCommits.map(_.obj.asInstanceOf[(Map[String, String], Set[String])]).unzip
      val fs = stagingDir.getFileSystem(jobContext.getConfiguration)

      val filesToMove = allAbsPathFiles.foldLeft(Map[String, String]())(_ ++ _)
      logDebug(s"Committing files staged for absolute locations $filesToMove")
      if (dynamicPartitionOverwrite) {
        val absPartitionPaths = filesToMove.values.map(new Path(_).getParent).toSet
        logDebug(s"Clean up absolute partition directories for overwriting: $absPartitionPaths")
        absPartitionPaths.foreach(fs.delete(_, true))
      }
      for ((src, dst) <- filesToMove) {
        fs.rename(new Path(src), new Path(dst))
      }

      if (dynamicPartitionOverwrite) {
        val partitionPaths = allPartitionPaths.foldLeft(Set[String]())(_ ++ _)
        logDebug(s"Clean up default partition directories for overwriting: $partitionPaths")
        for (part <- partitionPaths) {
          val finalPartPath = new Path(path, part)
          if (!fs.delete(finalPartPath, true) && !fs.exists(finalPartPath.getParent)) {
            // According to the official hadoop FileSystem API spec, delete op should assume
            // the destination is no longer present regardless of return value, thus we do not
            // need to double check if finalPartPath exists before rename.
            // Also in our case, based on the spec, delete returns false only when finalPartPath
            // does not exist. When this happens, we need to take action if parent of finalPartPath
            // also does not exist(e.g. the scenario described on SPARK-23815), because
            // FileSystem API spec on rename op says the rename dest(finalPartPath) must have
            // a parent that exists, otherwise we may get unexpected result on the rename.
            fs.mkdirs(finalPartPath.getParent)
          }
          fs.rename(new Path(stagingDir, part), finalPartPath)
        }
      } else if (supportConcurrent) {
        // For InsertIntoHadoopFsRelation operation, the result has been committed to staging
        // output path, merge it to destination path.
        FileCommitProtocol.mergePaths(committer.asInstanceOf[FileOutputCommitter], fs,
          fs.getFileStatus(stagingOutputPath), new Path(path), jobContext)
      }

      if (supportConcurrent) {
        // For InsertIntoHadoopFsRelation operation, try to delete its staging output path.
        deleteStagingInsertOutputPath(fs, insertStagingDir, stagingOutputPath,
          escapedStaticPartitionKVs)
      }

      fs.delete(stagingDir, true)
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
        val fs = stagingDir.getFileSystem(jobContext.getConfiguration)
        fs.delete(stagingDir, true)
        deleteStagingInsertOutputPath(fs, insertStagingDir, stagingOutputPath,
          escapedStaticPartitionKVs)
      }
    } catch {
      case e: IOException =>
        logWarning(s"Exception while aborting ${jobContext.getJobID}", e)
    }
  }

  override def setupTask(taskContext: TaskAttemptContext): Unit = {
    committer = setupCommitter(taskContext)
    committer.setupTask(taskContext)
    addedAbsPathFiles = mutable.Map[String, String]()
    partitionPaths = mutable.Set[String]()
  }

  override def commitTask(taskContext: TaskAttemptContext): TaskCommitMessage = {
    val attemptId = taskContext.getTaskAttemptID
    logTrace(s"Commit task ${attemptId}")
    SparkHadoopMapRedUtil.commitTask(
      committer, taskContext, attemptId.getJobID.getId, attemptId.getTaskID.getId)
    new TaskCommitMessage(addedAbsPathFiles.toMap -> partitionPaths.toSet)
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
      committer.abortTask(taskContext)
    } catch {
      case e: IOException =>
        logWarning(s"Exception while aborting ${taskContext.getTaskAttemptID}", e)
    }
    // best effort cleanup of other staged files
    try {
      for ((src, _) <- addedAbsPathFiles) {
        val tmp = new Path(src)
        tmp.getFileSystem(taskContext.getConfiguration).delete(tmp, false)
      }
    } catch {
      case e: IOException =>
        logWarning(s"Exception while aborting ${taskContext.getTaskAttemptID}", e)
    }
  }
}

object  HadoopMapReduceCommitProtocol extends Logging {

  /**
   * Get a path according to specified partition key-value pairs.
   */
  def getEscapedStaticPartitionPath(staticPartitionKVs: Iterable[(String, String)]): String = {
    staticPartitionKVs.map{kv =>
      kv._1 + "=" + kv._2
    }.mkString(File.separator)
  }

  /**
   * Delete the staging output path of current InsertIntoHadoopFsRelation operation. This output
   * path is used to mark a InsertIntoHadoopFsRelation operation and we can detect conflict when
   * there are several operations write same partition or a non-partitioned table concurrently.
   *
   * The output path is a multi level path and is composed of specified partition key value pairs
   * formatted `.spark-staging-${depth}/p1=v1/p2=v2/.../pn=vn/appId/jobId`. When deleting the
   * staging output path, delete the last level with recursive firstly. Then try to delete upper
   * level without recursive, if success, then delete upper level with same way, until delete the
   * insertStagingDir.
   */
   def deleteStagingInsertOutputPath(
       fs: FileSystem,
       insertStagingDir: Path,
       stagingOutputDir: Path,
       escapedStaticPartitionKVs: Seq[(String, String)]): Unit = {
     if (insertStagingDir == null || stagingOutputDir ==null || !fs.isDirectory(stagingOutputDir)) {
       return
     }

     // Firstly, delete the staging output dir with recursive, because it is unique.
     fs.delete(stagingOutputDir, true)

     var currentLevelPath = stagingOutputDir.getParent
     var complete: Boolean = false
     while (!complete && currentLevelPath != insertStagingDir) {
       try {
         fs.delete(currentLevelPath, false)
         currentLevelPath = currentLevelPath.getParent
       } catch {
         case e: Exception =>
           logWarning(s"Exception occurred when deleting dir: $currentLevelPath.", e)
           complete = true
       }
     }

     try {
       fs.delete(insertStagingDir, false)
     } catch {
       case e: Exception =>
         logWarning(s"Exception occurred when deleting dir: $insertStagingDir.", e)
     }
  }
}
