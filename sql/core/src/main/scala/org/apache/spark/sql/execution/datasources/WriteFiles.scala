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

import java.util.Date

import org.apache.spark.{SparkException, TaskContext}
import org.apache.spark.internal.io.{FileCommitProtocol, SparkHadoopWriterUtils}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, UnaryNode}
import org.apache.spark.sql.connector.write.WriterCommitMessage
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}
import org.apache.spark.sql.execution.datasources.FileFormatWriter.ConcurrentOutputWriterSpec
import org.apache.spark.sql.internal.WriteSpec

/**
 * The write files spec holds all information of [[V1WriteCommand]] if its provider is
 * [[FileFormat]].
 */
case class WriteFilesSpec(
    description: WriteJobDescription,
    committer: FileCommitProtocol,
    concurrentOutputWriterSpecFunc: SparkPlan => Option[ConcurrentOutputWriterSpec])
  extends WriteSpec

/**
 * During Optimizer, [[V1Writes]] injects the [[WriteFiles]] between [[V1WriteCommand]] and query.
 * [[WriteFiles]] must be the root plan as the child of [[V1WriteCommand]].
 */
case class WriteFiles(child: LogicalPlan) extends UnaryNode {
  override def output: Seq[Attribute] = child.output
  override protected def withNewChildInternal(newChild: LogicalPlan): WriteFiles =
    copy(child = newChild)
}

/**
 * Responsible for writing files.
 */
case class WriteFilesExec(child: SparkPlan) extends UnaryExecNode {
  override def output: Seq[Attribute] = Seq.empty

  override protected def doExecuteWrite(writeSpec: WriteSpec): RDD[WriterCommitMessage] = {
    assert(writeSpec.isInstanceOf[WriteFilesSpec])
    val writeFilesSpec: WriteFilesSpec = writeSpec.asInstanceOf[WriteFilesSpec]

    val rdd = child.execute()
    // SPARK-23271 If we are attempting to write a zero partition rdd, create a dummy single
    // partition rdd to make sure we at least set up one write task to write the metadata.
    val rddWithNonEmptyPartitions = if (rdd.partitions.length == 0) {
      session.sparkContext.parallelize(Array.empty[InternalRow], 1)
    } else {
      rdd
    }

    val concurrentOutputWriterSpec = writeFilesSpec.concurrentOutputWriterSpecFunc(child)
    val description = writeFilesSpec.description
    val committer = writeFilesSpec.committer
    val jobTrackerID = SparkHadoopWriterUtils.createJobTrackerID(new Date())
    rddWithNonEmptyPartitions.mapPartitionsInternal { iterator =>
      val sparkStageId = TaskContext.get().stageId()
      val sparkPartitionId = TaskContext.get().partitionId()
      val sparkAttemptNumber = TaskContext.get().taskAttemptId().toInt & Int.MaxValue

      val ret = FileFormatWriter.executeTask(
        description,
        jobTrackerID,
        sparkStageId,
        sparkPartitionId,
        sparkAttemptNumber,
        committer,
        iterator,
        concurrentOutputWriterSpec
      )

      Iterator(ret)
    }
  }

  override protected def doExecute(): RDD[InternalRow] = {
    throw SparkException.internalError(s"$nodeName does not support doExecute")
  }

  override protected def stringArgs: Iterator[Any] = Iterator(child)

  override protected def withNewChildInternal(newChild: SparkPlan): WriteFilesExec =
    copy(child = newChild)
}
