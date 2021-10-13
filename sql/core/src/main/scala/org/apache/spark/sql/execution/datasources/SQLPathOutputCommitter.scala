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

package org.apache.spark.sql.execution.datasources

import java.io.{FileNotFoundException, IOException}
import java.net.URI
import java.text.SimpleDateFormat
import java.util.{Date, Locale, Random}

import com.google.common.base.Preconditions
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path, PathFilter}
import org.apache.hadoop.mapreduce.{JobContext, JobStatus, TaskAttemptContext, TaskAttemptID}
import org.apache.hadoop.mapreduce.lib.output.PathOutputCommitter
import org.apache.hadoop.util.{DurationInfo, Progressable}

import org.apache.spark.internal.Logging

class SQLPathOutputCommitter(
    stagingDir: Path,
    outputPath: Path,
    context: TaskAttemptContext)
  extends PathOutputCommitter(outputPath, context) with Logging {

  val PENDING_DIR_NAME = "_temporary"
  val SUCCEEDED_FILE_NAME = "_SUCCESS"
  val SUCCESSFUL_JOB_OUTPUT_DIR_MARKER = "mapreduce.fileoutputcommitter.marksuccessfuljobs"
  val FILEOUTPUTCOMMITTER_ALGORITHM_VERSION = "mapreduce.fileoutputcommitter.algorithm.version"
  val FILEOUTPUTCOMMITTER_ALGORITHM_VERSION_DEFAULT = 2
  val FILEOUTPUTCOMMITTER_CLEANUP_SKIPPED = "mapreduce.fileoutputcommitter.cleanup.skipped"
  val FILEOUTPUTCOMMITTER_CLEANUP_SKIPPED_DEFAULT = false
  val FILEOUTPUTCOMMITTER_CLEANUP_FAILURES_IGNORED =
    "mapreduce.fileoutputcommitter.cleanup-failures.ignored"
  val FILEOUTPUTCOMMITTER_CLEANUP_FAILURES_IGNORED_DEFAULT = false
  val FILEOUTPUTCOMMITTER_FAILURE_ATTEMPTS = "mapreduce.fileoutputcommitter.failures.attempts"
  val FILEOUTPUTCOMMITTER_FAILURE_ATTEMPTS_DEFAULT = 1
  val FILEOUTPUTCOMMITTER_TASK_CLEANUP_ENABLED =
    "mapreduce.fileoutputcommitter.task.cleanup.enabled"
  val FILEOUTPUTCOMMITTER_TASK_CLEANUP_ENABLED_DEFAULT = false
  private val workPath: Path = if (getOutputPath != null) {
    Preconditions.checkNotNull(getTaskAttemptPath(context, getOutputPath),
      "Null task attempt path in %s and output path %s", context, outputPath)
  } else {
    null
  }
  private val algorithmVersion = context.getConfiguration.getInt(
    FILEOUTPUTCOMMITTER_ALGORITHM_VERSION, FILEOUTPUTCOMMITTER_ALGORITHM_VERSION_DEFAULT)
  private val skipCleanup = context.getConfiguration.getBoolean(
    FILEOUTPUTCOMMITTER_CLEANUP_SKIPPED, FILEOUTPUTCOMMITTER_CLEANUP_SKIPPED_DEFAULT)
  private val ignoreCleanupFailures = context.getConfiguration.getBoolean(
    FILEOUTPUTCOMMITTER_CLEANUP_FAILURES_IGNORED,
    FILEOUTPUTCOMMITTER_CLEANUP_FAILURES_IGNORED_DEFAULT)

  private class CommittedTaskFilter extends PathFilter {
    def accept(path: Path): Boolean = !(PENDING_DIR_NAME == path.getName)
  }

  override def getOutputPath: Path =
    outputPath.getFileSystem(context.getConfiguration).makeQualified(outputPath)

  def getPendingJobAttemptsPath(): Path = {
    getPendingJobAttemptsPath(stagingDir)
  }

  def getPendingJobAttemptsPath(out: Path): Path = {
    new Path(out, PENDING_DIR_NAME)
  }

  private def getAppAttemptId(context: JobContext) = {
    context.getConfiguration.getInt("mapreduce.job.application.attempt.id", 0)
  }

  def getJobAttemptPath(context: JobContext): Path = {
    getJobAttemptPath(context, this.stagingDir)
  }

  def getJobAttemptPath(context: JobContext, out: Path): Path = {
    getJobAttemptPath(getAppAttemptId(context), out)
  }

  protected def getJobAttemptPath(appAttemptId: Int): Path = {
    getJobAttemptPath(appAttemptId, this.stagingDir)
  }

  private def getJobAttemptPath(appAttemptId: Int, out: Path) = {
    new Path(getPendingJobAttemptsPath(out), String.valueOf(appAttemptId))
  }

  private def getPendingTaskAttemptsPath(context: JobContext): Path = {
    getPendingTaskAttemptsPath(context, this.stagingDir)
  }

  private def getPendingTaskAttemptsPath(context: JobContext, out: Path) = {
    new Path(getJobAttemptPath(context, out), "_temporary")
  }

  def getTaskAttemptPath(context: TaskAttemptContext): Path = {
    new Path(this.getPendingTaskAttemptsPath(context), String.valueOf(context.getTaskAttemptID))
  }

  def getTaskAttemptPath(context: TaskAttemptContext, out: Path): Path = {
    new Path(getPendingTaskAttemptsPath(context, out), String.valueOf(context.getTaskAttemptID))
  }

  def getCommittedTaskPath(context: TaskAttemptContext): Path = {
    this.getCommittedTaskPath(getAppAttemptId(context), context)
  }

  def getCommittedTaskPath(context: TaskAttemptContext, out: Path): Path = {
    getCommittedTaskPath(getAppAttemptId(context), context, out)
  }

  protected def getCommittedTaskPath(appAttemptId: Int, context: TaskAttemptContext) = {
    new Path(this.getJobAttemptPath(appAttemptId),
      String.valueOf(context.getTaskAttemptID.getTaskID))
  }

  private def getCommittedTaskPath(
      appAttemptId: Int,
      context: TaskAttemptContext,
      out: Path): Path = {
    new Path(getJobAttemptPath(appAttemptId, out),
      String.valueOf(context.getTaskAttemptID.getTaskID.toString))
  }

  @throws[IOException]
  private def getAllCommittedTaskPaths(context: JobContext) = {
    val jobAttemptPath = this.getJobAttemptPath(context)
    val fs = jobAttemptPath.getFileSystem(context.getConfiguration)
    fs.listStatus(jobAttemptPath, new CommittedTaskFilter)
  }

  @throws[IOException]
  override def setupJob(context: JobContext): Unit = {
    if (hasOutputPath) {
      val jobAttemptPath = getJobAttemptPath(context)
      val fs = jobAttemptPath.getFileSystem(context.getConfiguration)
      if (!fs.mkdirs(jobAttemptPath)) {
        logError("Mkdirs failed to create " + jobAttemptPath)
      }
    } else {
      logWarning("Output Path is null in setupJob()")
    }
  }


  @throws[IOException]
  override def commitJob(context: JobContext): Unit = {
    val maxAttemptsOnFailure = if (this.isCommitJobRepeatable(context)) {
      context.getConfiguration.getInt(
        FILEOUTPUTCOMMITTER_FAILURE_ATTEMPTS, FILEOUTPUTCOMMITTER_FAILURE_ATTEMPTS_DEFAULT)
    } else {
      1
    }
    var attempt = 0
    var jobCommitNotFinished = true

    while (jobCommitNotFinished) {
      try {
        this.commitJobInternal(context)
        jobCommitNotFinished = false
      } catch {
        case var6: Exception =>
          attempt += 1
          if (attempt >= maxAttemptsOnFailure) throw var6
          logWarning("Exception get thrown in job commit, retry (" + attempt + ") time.", var6)
      }
    }
  }

  @throws[IOException]
  protected def commitJobInternal(context: JobContext): Unit = {
    if (!this.hasOutputPath) {
      logWarning("Output Path is null in commitJob()")
    } else {
      val finalOutput = this.getOutputPath
      val fs = finalOutput.getFileSystem(context.getConfiguration)
      if (this.algorithmVersion == 1) {
        val committedTaskPaths = this.getAllCommittedTaskPaths(context)
        committedTaskPaths.foreach { path =>
          this.mergePaths(fs, path, finalOutput, context)
        }
      }
      if (this.skipCleanup) {
        logInfo("Skip cleanup the _temporary folders under job's output directory in commitJob.")
      } else {
        try this.cleanupJob(context)
        catch {
          case var8: IOException =>
            if (!this.ignoreCleanupFailures) throw var8
            logError("Error in cleanup job, manually cleanup is needed.", var8)
        }
      }
      if (context.getConfiguration.getBoolean(SUCCESSFUL_JOB_OUTPUT_DIR_MARKER, true)) {
        val markerPath = new Path(this.outputPath, "_SUCCESS")
        if (this.isCommitJobRepeatable(context)) {
          fs.create(markerPath, true).close()
        } else {
          fs.create(markerPath).close()
        }
      }
    }
  }

  @throws[IOException]
  private def mergePaths(fs: FileSystem, from: FileStatus, to: Path, context: JobContext): Unit = {
    val d: DurationInfo =
      new DurationInfo(log, false, "Merging data from %s to %s", Array[AnyRef](from, to))
    var var6: Throwable = null
    try {
      this.reportProgress(context)
      var toStat: FileStatus = null
      try {
        toStat = fs.getFileStatus(to)
      } catch {
        case _: FileNotFoundException =>
          toStat = null
      }
      if (from.isFile) {
        if (toStat != null && !fs.delete(to, true)) {
          throw new IOException("Failed to delete " + to)
        }
        if (!fs.rename(from.getPath, to)) {
          throw new IOException("Failed to rename " + from + " to " + to)
        }
      } else {
        if (from.isDirectory) {
          if (toStat != null) {
            if (!toStat.isDirectory) {
              if (!fs.delete(to, true)) {
                throw new IOException("Failed to delete " + to)
              }
              this.renameOrMerge(fs, from, to, context)
            } else {
              val var8: Array[FileStatus] = fs.listStatus(from.getPath)
              val var9: Int = var8.length
              for (var10 <- 0 until var9) {
                val subFrom: FileStatus = var8(var10)
                val subTo: Path = new Path(to, subFrom.getPath.getName)
                this.mergePaths(fs, subFrom, subTo, context)
              }
            }
          } else {
            this.renameOrMerge(fs, from, to, context)
          }
        }
      }
    } catch {
      case var22: Throwable =>
        var6 = var22
        throw var22
    } finally {
      if (d != null) {
        if (var6 != null) {
          try d.close()
          catch {
            case var20: Throwable =>
              var6.addSuppressed(var20)
          }
        }
        else {
          d.close()
        }
      }
    }
  }

  private def reportProgress(context: JobContext): Unit = {
    if (context.isInstanceOf[Progressable]) {
      (context.asInstanceOf[Progressable]).progress()
    }
  }

  @throws[IOException]
  private def renameOrMerge(
      fs: FileSystem,
      from: FileStatus,
      to: Path,
      context: JobContext): Unit = {
    if (this.algorithmVersion == 1) {
      if (!(fs.rename(from.getPath, to))) {
        throw new IOException("Failed to rename " + from + " to " + to)
      }
    }
    else {
      fs.mkdirs(to)
      val var5: Array[FileStatus] = fs.listStatus(from.getPath)
      val var6: Int = var5.length
      for (var7 <- 0 until var6) {
        val subFrom: FileStatus = var5(var7)
        val subTo: Path = new Path(to, subFrom.getPath.getName)
        this.mergePaths(fs, subFrom, subTo, context)
      }
    }
  }

  /** @deprecated */
  @deprecated
  @throws[IOException]
  override def cleanupJob(context: JobContext): Unit = {
    if (this.hasOutputPath) {
      val pendingJobAttemptsPath: Path = this.getPendingJobAttemptsPath
      val fs: FileSystem = pendingJobAttemptsPath.getFileSystem(context.getConfiguration)
      try fs.delete(pendingJobAttemptsPath, true)
      catch {
        case var5: FileNotFoundException =>
          if (!(this.isCommitJobRepeatable(context))) {
            throw var5
          }
      }
    } else {
      logWarning("Output Path is null in cleanupJob()")
    }
  }

  @throws[IOException]
  override def abortJob(context: JobContext, state: JobStatus.State): Unit = {
    this.cleanupJob(context)
  }

  @throws[IOException]
  override def setupTask(context: TaskAttemptContext): Unit = {
  }

  @throws[IOException]
  override def commitTask(context: TaskAttemptContext): Unit = {
    this.commitTask(context, null.asInstanceOf[Path])
  }

  @throws[IOException]
  def commitTask(context: TaskAttemptContext, taskAttemptPath: Path): Unit = {
    val attemptId: TaskAttemptID = context.getTaskAttemptID
    var currentTaskAttemptPath = taskAttemptPath
    if (this.hasOutputPath) {
      context.progress()
      if (currentTaskAttemptPath == null) {
        currentTaskAttemptPath = this.getTaskAttemptPath(context)
      }
      val fs: FileSystem = currentTaskAttemptPath.getFileSystem(context.getConfiguration)
      var taskAttemptDirStatus: FileStatus = null
      try {
        taskAttemptDirStatus = fs.getFileStatus(currentTaskAttemptPath)
      } catch {
        case _: FileNotFoundException =>
          taskAttemptDirStatus = null
      }
      if (taskAttemptDirStatus != null) {
        if (this.algorithmVersion == 1) {
          val committedTaskPath = this.getCommittedTaskPath(context)
          if (fs.exists(committedTaskPath) && !fs.delete(committedTaskPath, true)) {
            throw new IOException("Could not delete " + committedTaskPath)
          }
          if (!fs.rename(currentTaskAttemptPath, committedTaskPath)) {
            throw new IOException(
              "Could not rename " + currentTaskAttemptPath + " to " + committedTaskPath)
          }
          logInfo("Saved output of task '" + attemptId + "' to " + committedTaskPath)
        } else {
          this.mergePaths(fs, taskAttemptDirStatus, this.outputPath, context)
          logInfo("Saved output of task '" + attemptId + "' to " + this.outputPath)
          if (context.getConfiguration.getBoolean(
            "mapreduce.fileoutputcommitter.task.cleanup.enabled", false)) {
            logDebug(String.format(
              "Deleting the temporary directory of '%s': '%s'", attemptId, currentTaskAttemptPath))
            if (!fs.delete(currentTaskAttemptPath, true)) {
              logWarning("Could not delete " + currentTaskAttemptPath)
            }
          }
        }
      } else {
        logWarning("No Output found for " + attemptId)
      }
    } else {
      logWarning("Output Path is null in commitTask()")
    }
  }

  @throws[IOException]
  override def abortTask(context: TaskAttemptContext): Unit = {
    this.abortTask(context, null.asInstanceOf[Path])
  }

  @throws[IOException]
  def abortTask(context: TaskAttemptContext, taskAttemptPath: Path): Unit = {
    var currentTaskAttemptPath = taskAttemptPath
    if (this.hasOutputPath) {
      context.progress()
      if (currentTaskAttemptPath == null) {
        currentTaskAttemptPath = this.getTaskAttemptPath(context)
      }
      val fs: FileSystem = currentTaskAttemptPath.getFileSystem(context.getConfiguration)
      if (!fs.delete(currentTaskAttemptPath, true)) {
        logWarning("Could not delete " + currentTaskAttemptPath)
      }
    } else {
      logWarning("Output Path is null in abortTask()")
    }
  }

  @throws[IOException]
  override def needsTaskCommit(context: TaskAttemptContext): Boolean = {
    this.needsTaskCommit(context, null.asInstanceOf[Path])
  }

  @throws[IOException]
  def needsTaskCommit(context: TaskAttemptContext, taskAttemptPath: Path): Boolean = {
    var currentTaskAttemptPath = taskAttemptPath
    if (this.hasOutputPath) {
      if (currentTaskAttemptPath == null) {
        currentTaskAttemptPath = this.getTaskAttemptPath(context)
      }
      val fs: FileSystem = currentTaskAttemptPath.getFileSystem(context.getConfiguration)
      fs.exists(currentTaskAttemptPath)
    } else {
      false
    }
  }

  @deprecated override def isRecoverySupported: Boolean = {
    true
  }

  @throws[IOException]
  override def isCommitJobRepeatable(context: JobContext): Boolean = {
    this.algorithmVersion == 2
  }

  @throws[IOException]
  override def recoverTask(context: TaskAttemptContext): Unit = {
    if (this.hasOutputPath) {
      context.progress()
      val attemptId: TaskAttemptID = context.getTaskAttemptID
      val previousAttempt: Int = getAppAttemptId(context) - 1
      if (previousAttempt < 0) {
        throw new IOException("Cannot recover task output for first attempt...")
      }
      val previousCommittedTaskPath: Path = this.getCommittedTaskPath(previousAttempt, context)
      val fs: FileSystem = previousCommittedTaskPath.getFileSystem(context.getConfiguration)
      if (log.isDebugEnabled) {
        logDebug("Trying to recover task from " + previousCommittedTaskPath)
      }
      if (this.algorithmVersion == 1) {
        if (fs.exists(previousCommittedTaskPath)) {
          val committedTaskPath: Path = this.getCommittedTaskPath(context)
          if (!fs.delete(committedTaskPath, true) && fs.exists(committedTaskPath)) {
            throw new IOException("Could not delete " + committedTaskPath)
          }
          val committedParent: Path = committedTaskPath.getParent
          fs.mkdirs(committedParent)
          if (!fs.rename(previousCommittedTaskPath, committedTaskPath)) {
            throw new IOException(
              "Could not rename " + previousCommittedTaskPath + " to " + committedTaskPath)
          }
        }
        else {
          logWarning(attemptId + " had no output to recover.")
        }
      }
      else {
        try {
          val from: FileStatus = fs.getFileStatus(previousCommittedTaskPath)
          logInfo("Recovering task for upgrading scenario, moving files from " +
            previousCommittedTaskPath + " to " + this.outputPath)
          this.mergePaths(fs, from, this.outputPath, context)
        } catch {
          case var8: FileNotFoundException =>

        }
        logInfo("Done recovering task " + attemptId)
      }
    }
    else {
      logWarning("Output Path is null in recoverTask()")
    }
  }

  override def getWorkPath: Path = workPath

  override def toString: String = {
    val sb: StringBuilder = new StringBuilder("FileOutputCommitter{")
    sb.append(super.toString).append("; ")
    sb.append("outputPath=").append(this.outputPath)
    sb.append(", workPath=").append(this.workPath)
    sb.append(", algorithmVersion=").append(this.algorithmVersion)
    sb.append(", skipCleanup=").append(this.skipCleanup)
    sb.append(", ignoreCleanupFailures=").append(this.ignoreCleanupFailures)
    sb.append('}')
    sb.toString
  }
}

object SQLPathOutputCommitter extends Logging {
  def newVersionExternalTempPath(
      jobId: String,
      path: Path,
      hadoopConf: Configuration,
      stagingDir: String): Path = {
    val extURI = path.toUri
    if (extURI.getScheme == "viewfs") {
      getExtTmpPathRelTo(path.getParent, hadoopConf, stagingDir, jobId)
    } else {
      new Path(getExternalScratchDir(extURI, hadoopConf, stagingDir, jobId), "-ext-10000")
    }
  }


  private def getExtTmpPathRelTo(
      path: Path,
      hadoopConf: Configuration,
      stagingDir: String,
      jobId: String): Path = {
    new Path(getStagingDir(path, hadoopConf, stagingDir, jobId), "-ext-10000") // Hive uses 10000
  }

  private def getExternalScratchDir(
      extURI: URI,
      hadoopConf: Configuration,
      stagingDir: String,
      jobId: String): Path = {
      getStagingDir(
      new Path(extURI.getScheme, extURI.getAuthority, extURI.getPath),
      hadoopConf,
      stagingDir,
      jobId: String)
  }

  private def getStagingDir(
      inputPath: Path,
      hadoopConf: Configuration,
      stagingDir: String,
      jobId: String): Path = {
    val inputPathName: String = inputPath.toString
    val fs: FileSystem = inputPath.getFileSystem(hadoopConf)
    var stagingPathName: String =
      if (inputPathName.indexOf(stagingDir) == -1) {
        new Path(inputPathName, stagingDir).toString
      } else {
        inputPathName.substring(0, inputPathName.indexOf(stagingDir) + stagingDir.length)
      }

    // SPARK-20594: This is a walk-around fix to resolve a Hive bug. Hive requires that the
    // staging directory needs to avoid being deleted when users set hive.exec.stagingdir
    // under the table directory.
    if (isSubDir(new Path(stagingPathName), inputPath, fs) &&
      !stagingPathName.stripPrefix(inputPathName).stripPrefix("/").startsWith(".")) {
      logDebug(s"The staging dir '$stagingPathName' should be a child directory starts " +
        "with '.' to avoid being deleted if we set hive.exec.stagingdir under the table " +
        "directory.")
      stagingPathName = new Path(inputPathName, ".hive-staging").toString
    }

    val dir = fs.makeQualified(
      new Path(stagingPathName + "_" + executionId + "-" + jobId))
    logDebug("Created staging dir = " + dir + " for path = " + inputPath)
    dir
  }

  // HIVE-14259 removed FileUtils.isSubDir(). Adapted it from Hive 1.2's FileUtils.isSubDir().
  private def isSubDir(p1: Path, p2: Path, fs: FileSystem): Boolean = {
    val path1 = fs.makeQualified(p1).toString + Path.SEPARATOR
    val path2 = fs.makeQualified(p2).toString + Path.SEPARATOR
    path1.startsWith(path2)
  }

  private def executionId: String = {
    val rand: Random = new Random
    val format = new SimpleDateFormat("yyyy-MM-dd_HH-mm-ss_SSS", Locale.US)
    "spark_" + format.format(new Date) + "_" + Math.abs(rand.nextLong)
  }
}
