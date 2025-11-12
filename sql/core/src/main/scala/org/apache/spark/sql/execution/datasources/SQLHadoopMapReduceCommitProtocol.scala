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

import java.io.IOException

import scala.collection.mutable

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.mapreduce.{JobContext, OutputCommitter, TaskAttemptContext}
import org.apache.hadoop.mapreduce.lib.output.{FileOutputCommitter, FileOutputFormat}

import org.apache.spark.internal.Logging
import org.apache.spark.internal.LogKeys.CLASS_NAME
import org.apache.spark.internal.io.{FileNameSpec, HadoopMapReduceCommitProtocol}
import org.apache.spark.internal.io.FileCommitProtocol.{DYNAMIC_PARTITION_OVERWRITE, TaskCommitMessage}
import org.apache.spark.mapred.SparkHadoopMapRedUtil
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.internal.SQLConf

/**
 * A variant of [[HadoopMapReduceCommitProtocol]] that allows specifying the actual
 * Hadoop output committer using an option specified in SQLConf.
 */
class SQLHadoopMapReduceCommitProtocol(
    jobId: String,
    path: String,
    mode: String = "")
  extends HadoopMapReduceCommitProtocol(jobId, path, mode)
    with Serializable with Logging {

  @transient private var addedAbsPath: mutable.Map[String, String] = null

  // For Hive serde, we just need to put the results into the staging directory and
  // wait for the load partition, so mode is None.
  protected val saveMode: Option[SaveMode] = mode match {
    case null | "" => None
    case m if m.equalsIgnoreCase(DYNAMIC_PARTITION_OVERWRITE) => Some(SaveMode.Overwrite)
    case m => Some(SaveMode.valueOf(m))
  }

  override def setupTask(taskContext: TaskAttemptContext): Unit = {
    super.setupTask(taskContext)
    addedAbsPath = mutable.Map[String, String]()
  }

  override protected def setupCommitter(context: TaskAttemptContext): OutputCommitter = {
    saveMode.map(_ => context.getConfiguration().set(FileOutputFormat.OUTDIR, stagingDir.toString))
    var committer = super.setupCommitter(context)

    val configuration = context.getConfiguration
    val clazz =
      configuration.getClass(SQLConf.OUTPUT_COMMITTER_CLASS.key, null, classOf[OutputCommitter])

    if (clazz != null) {
      logInfo(log"Using user defined output committer class " +
        log"${MDC(CLASS_NAME, clazz.getCanonicalName)}")

      // Every output format based on org.apache.hadoop.mapreduce.lib.output.OutputFormat
      // has an associated output committer. To override this output committer,
      // we will first try to use the output committer set in SQLConf.OUTPUT_COMMITTER_CLASS.
      // If a data source needs to override the output committer, it needs to set the
      // output committer in prepareForWrite method.
      if (classOf[FileOutputCommitter].isAssignableFrom(clazz)) {
        // The specified output committer is a FileOutputCommitter.
        // So, we will use the FileOutputCommitter-specified constructor.
        val ctor = clazz.getDeclaredConstructor(classOf[Path], classOf[TaskAttemptContext])
        val committerOutputPath = committer match {
          case f: FileOutputCommitter => f.getOutputPath()
          case _ => new Path(path)
        }
        committer = ctor.newInstance(committerOutputPath, context)
      } else {
        // The specified output committer is just an OutputCommitter.
        // So, we will use the no-argument constructor.
        val ctor = clazz.getDeclaredConstructor()
        committer = ctor.newInstance()
      }
    }
    logInfo(log"Using output committer class " +
      log"${MDC(CLASS_NAME, committer.getClass.getCanonicalName)}")
    committer
  }

  override def newTaskTempFile(taskContext: TaskAttemptContext,
                               partDir: Option[String],
                               customPath: Option[String],
                               spec: FileNameSpec): String = {
    // For hive serde, we should not move staging file to final location.
    // We should wait for load partition to do that. So there is no need to
    // add to addedAbsPath.
    saveMode.map { _ =>
      val srcDir = partDir.map { d =>
        new Path(stagingDir, d).toString
      }.getOrElse {
        stagingDir.toString
      }
      // For custom partition directory
      val dstDir = customPath.getOrElse {
        partDir.map { d =>
          new Path(path, d).toString
        }.getOrElse {
          path
        }
      }
      addedAbsPath(srcDir) = dstDir
    }

    super.newTaskTempFile(
      taskContext,
      partDir,
      customPath,
      spec)
  }

  override def commitTask(taskContext: TaskAttemptContext): TaskCommitMessage = {
    val attemptId = taskContext.getTaskAttemptID
    logTrace(s"Commit task ${attemptId}")
    SparkHadoopMapRedUtil.commitTask(
      committer, taskContext, attemptId.getJobID.getId, attemptId.getTaskID.getId)
    new TaskCommitMessage(addedAbsPath.toMap)
  }

  private def isSubDir(qualifiedPathParent: Path, qualifiedPathChild: Path): Boolean = {
    Iterator
      .iterate(qualifiedPathChild)(_.getParent)
      .takeWhile(_ != null)
      .exists(_.equals(qualifiedPathParent))
  }

  private def isDataPath(path: Path): Boolean = {
    val name = path.getName
    !(name.equals(FileOutputCommitter.SUCCEEDED_FILE_NAME) || name.startsWith("."))
  }

  def moveStagedFilesToFinalLocation(fs: FileSystem, stagingPath: Path, finalPath: Path): Unit = {
    for (file <- fs.listStatus(stagingPath).map(_.getPath)) {
      val dest = new Path(finalPath, file.getName)
      if(isDataPath(file) && !fs.rename(file, dest)) {
        throw new IOException(s"Failed to rename($file, $dest)")
      }
    }
  }

  override def commitJob(jobContext: JobContext, taskCommits: Seq[TaskCommitMessage]): Unit = {
    committer.commitJob(jobContext)

    if (hasValidPath) {
      val allAbsPath = taskCommits.map(_.obj.asInstanceOf[Map[String, String]])
      val fs = stagingDir.getFileSystem(jobContext.getConfiguration)

      val dirToMove = allAbsPath.foldLeft(Map[String, String]())(_ ++ _)
      logDebug(s"Committing directory staged for absolute locations $dirToMove")

      for ((src, dst) <- dirToMove) {
        val stagingPath = new Path(src)
        val finalPath = new Path(dst)
        assert(!isSubDir(stagingPath, finalPath), "staging path cannot be a parent of final path")
        val finalPathEmpty = fs.exists(finalPath) match {
          // final path must be directory if it exists
          case true => fs.listStatus(finalPath).map(_.getPath).filter(isDataPath(_)).isEmpty
          case false => true
        }
        saveMode match {
          case Some(SaveMode.ErrorIfExists) if !finalPathEmpty =>
            throw new IOException(s"Path $finalPath already exists.")
          case Some(SaveMode.Ignore) if !finalPathEmpty =>
            logInfo(s"Skip for existing Path $finalPath.")
          case None => logInfo(s"Skip for hive serde mode.")
          case Some(SaveMode.Append) if !finalPathEmpty =>
            // Move staged files to their final locations
            moveStagedFilesToFinalLocation(fs, stagingPath, finalPath)
          case _ =>
            if (isSubDir(finalPath, stagingDir)) {
              // For non-partitioned data, the stagingDir will be a subdirectory of finalPath.
              // Firstly, delete all path except non-data path(including staging dir) under
              // finalPath
              for (fileStatus <- fs.listStatus(finalPath)) {
                val childPath = fileStatus.getPath
                if (isDataPath(childPath) && !isSubDir(childPath, stagingPath)) {
                  fs.delete(childPath, true)
                }
              }
              // Secondly, move all files from stagingDir to finalPath
              moveStagedFilesToFinalLocation(fs, stagingPath, finalPath)
            } else {
              // According to the official hadoop FileSystem API spec, delete op should assume
              // the destination is no longer present regardless of return value, thus we do not
              // need to double check if finalPartPath exists before rename.
              // Also in our case, based on the spec, delete returns false only when finalPartPath
              // does not exist. When this happens, we need to take action if parent of
              // finalPartPath also does not exist(e.g. the scenario described on SPARK-23815),
              // because FileSystem API spec on rename op says the rename dest(finalPartPath) must
              // have a parent that exists, otherwise we may get unexpected result on the rename.
              if (!fs.delete(finalPath, true) && !fs.exists(finalPath.getParent)) {
                fs.mkdirs(finalPath.getParent)
              }
              fs.rename(stagingPath, finalPath)
            }
        }
      }
      fs.delete(stagingDir, true)
    }
  }

  override def abortTask(taskContext: TaskAttemptContext): Unit = {
    try {
      committer.abortTask(taskContext)
    } catch {
      case e: IOException =>
        logWarning(s"Exception while aborting ${taskContext.getTaskAttemptID}", e)
    }
    // best effort cleanup of other staged files
    try {
      for ((src, _) <- addedAbsPath) {
        val tmp = new Path(src)
        tmp.getFileSystem(taskContext.getConfiguration).delete(tmp, true)
      }
    } catch {
      case e: IOException =>
        logWarning(s"Exception while aborting ${taskContext.getTaskAttemptID}", e)
    }
  }
}
