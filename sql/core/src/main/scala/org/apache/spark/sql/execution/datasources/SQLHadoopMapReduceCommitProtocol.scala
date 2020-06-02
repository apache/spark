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

import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.{OutputCommitter, TaskAttemptContext}
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter

import org.apache.spark.SparkException
import org.apache.spark.internal.Logging
import org.apache.spark.internal.io.FileCommitProtocol.TaskCommitMessage
import org.apache.spark.internal.io.HadoopMapReduceCommitProtocol
import org.apache.spark.sql.internal.SQLConf

/**
 * A variant of [[HadoopMapReduceCommitProtocol]] that allows specifying the actual
 * Hadoop output committer using an option specified in SQLConf.
 */
class SQLHadoopMapReduceCommitProtocol(
    jobId: String,
    path: String,
    dynamicPartitionOverwrite: Boolean = false,
    restrictions: Map[String, Object] = Map.empty)
  extends HadoopMapReduceCommitProtocol(jobId, path, dynamicPartitionOverwrite)
    with Serializable with Logging {

  private val maxDynamicPartitions = restrictions.get(
    SQLConf.DYNAMIC_PARTITION_MAX_PARTITIONS.key) match {
      case Some(value) => value.asInstanceOf[Int]
      case None => Int.MaxValue
    }

  private val maxDynamicPartitionsPerTask = restrictions.get(
    SQLConf.DYNAMIC_PARTITION_MAX_PARTITIONS_PER_TASK.key) match {
      case Some(value) => value.asInstanceOf[Int]
      case None => Int.MaxValue
  }

  private val maxCreatedFilesInDynamicPartition = restrictions.get(
    SQLConf.DYNAMIC_PARTITION_MAX_CREATED_FILES.key) match {
    case Some(value) => value.asInstanceOf[Int]
    case None => Int.MaxValue
  }

  // They are only used in driver
  private var totalPartitions: Set[String] = Set.empty
  private var totalCreatedFiles: Int = 0

  override protected def setupCommitter(context: TaskAttemptContext): OutputCommitter = {
    var committer = super.setupCommitter(context)

    val configuration = context.getConfiguration
    val clazz =
      configuration.getClass(SQLConf.OUTPUT_COMMITTER_CLASS.key, null, classOf[OutputCommitter])

    if (clazz != null) {
      logInfo(s"Using user defined output committer class ${clazz.getCanonicalName}")

      // Every output format based on org.apache.hadoop.mapreduce.lib.output.OutputFormat
      // has an associated output committer. To override this output committer,
      // we will first try to use the output committer set in SQLConf.OUTPUT_COMMITTER_CLASS.
      // If a data source needs to override the output committer, it needs to set the
      // output committer in prepareForWrite method.
      if (classOf[FileOutputCommitter].isAssignableFrom(clazz)) {
        // The specified output committer is a FileOutputCommitter.
        // So, we will use the FileOutputCommitter-specified constructor.
        val ctor = clazz.getDeclaredConstructor(classOf[Path], classOf[TaskAttemptContext])
        committer = ctor.newInstance(new Path(path), context)
      } else {
        // The specified output committer is just an OutputCommitter.
        // So, we will use the no-argument constructor.
        val ctor = clazz.getDeclaredConstructor()
        committer = ctor.newInstance()
      }
    }
    logInfo(s"Using output committer class ${committer.getClass.getCanonicalName}")
    committer
  }

  /**
   * Called on the driver after a task commits. This can be used to access task commit messages
   * before the job has finished. These same task commit messages will be passed to commitJob()
   * if the entire job succeeds.
   * Override it to check dynamic partition limitation on driver side.
   */
  override def onTaskCommit(taskCommit: TaskCommitMessage): Unit = {
    logDebug(s"onTaskCommit($taskCommit)")
    if (hasValidPath) {
      val (addedAbsPathFiles, allPartitionPaths) =
        taskCommit.obj.asInstanceOf[(Map[String, String], Set[String])]
      val partitionsPerTask = allPartitionPaths.size
      if (partitionsPerTask > maxDynamicPartitionsPerTask) {
        throw new SparkException(s"Task tried to create $partitionsPerTask dynamic partitions," +
          s" which is more than $maxDynamicPartitionsPerTask. To solve this" +
          s" try to increase ${SQLConf.DYNAMIC_PARTITION_MAX_PARTITIONS_PER_TASK.key}")
      }
      totalPartitions ++= allPartitionPaths
      val totalPartitionNum = totalPartitions.size
      if (totalPartitionNum > maxDynamicPartitions) {
        throw new SparkException(s"Total number of dynamic partitions created is" +
          s" $totalPartitionNum, which is more than $maxDynamicPartitions." +
          s" To solve this try to increase ${SQLConf.DYNAMIC_PARTITION_MAX_PARTITIONS.key}")
      }
      totalCreatedFiles += partitionsPerTask
      if (totalCreatedFiles > maxCreatedFilesInDynamicPartition) {
        throw new SparkException(s"Total number of created files now is" +
          s" $totalCreatedFiles, which exceeds $maxCreatedFilesInDynamicPartition." +
          s" To solve this try to increase ${SQLConf.DYNAMIC_PARTITION_MAX_CREATED_FILES.key}")
      }
    }
  }
}
