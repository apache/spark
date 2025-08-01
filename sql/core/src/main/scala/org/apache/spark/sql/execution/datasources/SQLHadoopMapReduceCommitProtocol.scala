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

import java.io.FileNotFoundException

import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.{JobContext, OutputCommitter, TaskAttemptContext}
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter

import org.apache.spark.internal.{Logging, MDC}
import org.apache.spark.internal.LogKeys.CLASS_NAME
import org.apache.spark.internal.io.{FileCommitProtocol, HadoopMapReduceCommitProtocol}
import org.apache.spark.internal.io.FileCommitProtocol.SPARK_WRITE_JOB_ID
import org.apache.spark.sql.internal.SQLConf

/**
 * A variant of [[HadoopMapReduceCommitProtocol]] that allows specifying the actual
 * Hadoop output committer using an option specified in SQLConf.
 */
class SQLHadoopMapReduceCommitProtocol(
    jobId: String,
    path: String,
    dynamicPartitionOverwrite: Boolean = false)
  extends HadoopMapReduceCommitProtocol(jobId, path, dynamicPartitionOverwrite)
    with Serializable with Logging {

  override protected def setupCommitter(context: TaskAttemptContext): OutputCommitter = {
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
        val committerOutputPath = if (dynamicPartitionOverwrite) stagingDir else new Path(path)
        committer = ctor.newInstance(committerOutputPath, context)
      } else {
        // The specified output committer is just an OutputCommitter.
        // So, we will use the no-argument constructor.
        val ctor = clazz.getDeclaredConstructor()
        committer = ctor.newInstance()
      }
    } else if (committer.getClass == classOf[FileOutputCommitter]) {
      // If both OUTPUT_COMMITTER_CLASS is not specified by user and FileOutputCommitter is not
      // overridden by the data source, we will use the default UniqueJobPathOutputCommitter to
      // make the job temporary output path unique.
      committer = new UniqueJobPathOutputCommitter(new Path(path), context)
    }
    logInfo(log"Using output committer class " +
      log"${MDC(CLASS_NAME, committer.getClass.getCanonicalName)}")
    committer
  }
}

/**
 * An implementation of [[FileOutputCommitter]] that uses the Spark write ID to create
 * a staging directory for job attempts instead of static `_temporary` as root.
 *
 * This class can be set to spark.sql.sources.outputCommitterClass for direct use.
 */
class UniqueJobPathOutputCommitter(outputPath: Path, context: TaskAttemptContext)
  extends FileOutputCommitter(outputPath, context)
  with Logging {

  private def getSparkWriteId: String = {
    context.getConfiguration.get(SPARK_WRITE_JOB_ID, FileOutputCommitter.PENDING_DIR_NAME)
  }

  private def getPendingJobAttemptsPath: Path = {
    FileCommitProtocol.getStagingDir(this.getOutputPath, getSparkWriteId)
  }

  override def getJobAttemptPath(context: JobContext): Path = {
    val appAttemptId = context.getConfiguration.getInt("mapreduce.job.application.attempt.id", 0)
    getJobAttemptPath(appAttemptId)
  }

  /**
   * Returns the path for the job attempt directory, which modifies the parent'output path from
   *   OutputPath/_temporary/{{ appAttemptId }}/
   * to
   *   OutputPath/.spark-staging-{{ sparkWriteId }}/{{ appAttemptId }}/
   *
   * @param appAttemptId The application attempt ID.
   * @return The path for the job attempt directory.
   */
  override protected def getJobAttemptPath(appAttemptId: Int): Path = {
    new Path(getPendingJobAttemptsPath, String.valueOf(appAttemptId))
  }

  override def cleanupJob(context: JobContext): Unit = {
    if (hasOutputPath) {
      val pendingJobAttemptsPath = getPendingJobAttemptsPath
      val fs = pendingJobAttemptsPath.getFileSystem(context.getConfiguration)
      try {
        fs.delete(pendingJobAttemptsPath, true)
      } catch {
        case _: FileNotFoundException if isCommitJobRepeatable(context) =>
      }
    } else {
      logWarning("Output Path is null in cleanupJob()")
    }
  }

  private def getPendingTaskAttemptsPath(context: TaskAttemptContext): Path = {
    new Path(getJobAttemptPath(context), FileOutputCommitter.PENDING_DIR_NAME)
  }

  override def getTaskAttemptPath(context: TaskAttemptContext): Path = {
    val taskAttemptId = String.valueOf(context.getTaskAttemptID)
    new Path(getPendingTaskAttemptsPath(context), taskAttemptId)
  }

  override def abortTask(context: TaskAttemptContext): Unit = {
    abortTask(context, getTaskAttemptPath(context))
  }

  override def needsTaskCommit(context: TaskAttemptContext): Boolean = {
    needsTaskCommit(context, getTaskAttemptPath(context))
  }

  override def getCommittedTaskPath(context: TaskAttemptContext): Path = {
    new Path(getJobAttemptPath(context), String.valueOf(context.getTaskAttemptID.getTaskID))
  }

  override def getCommittedTaskPath(appAttemptId: Int, context: TaskAttemptContext): Path = {
    new Path(getJobAttemptPath(appAttemptId), String.valueOf(context.getTaskAttemptID.getTaskID))
  }
}
