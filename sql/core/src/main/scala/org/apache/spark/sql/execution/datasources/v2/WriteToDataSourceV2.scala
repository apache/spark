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

package org.apache.spark.sql.execution.datasources.v2

import org.apache.spark.{SparkEnv, SparkException, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.streaming.StreamExecution
import org.apache.spark.sql.execution.streaming.continuous.{CommitPartitionEpoch, ContinuousExecution, EpochCoordinatorRef, SetWriterPartitions}
import org.apache.spark.sql.sources.v2.streaming.writer.ContinuousWriter
import org.apache.spark.sql.sources.v2.writer._
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.Utils

/**
 * The logical plan for writing data into data source v2.
 */
case class WriteToDataSourceV2(writer: DataSourceV2Writer, query: LogicalPlan) extends LogicalPlan {
  override def children: Seq[LogicalPlan] = Seq(query)
  override def output: Seq[Attribute] = Nil
}

/**
 * The physical plan for writing data into data source v2.
 */
case class WriteToDataSourceV2Exec(writer: DataSourceV2Writer, query: SparkPlan) extends SparkPlan {
  override def children: Seq[SparkPlan] = Seq(query)
  override def output: Seq[Attribute] = Nil

  override protected def doExecute(): RDD[InternalRow] = {
    val writeTask = writer match {
      case w: SupportsWriteInternalRow => w.createInternalRowWriterFactory()
      case _ => new InternalRowDataWriterFactory(writer.createWriterFactory(), query.schema)
    }

    val rdd = query.execute()
    val messages = new Array[WriterCommitMessage](rdd.partitions.length)

    logInfo(s"Start processing data source writer: $writer. " +
      s"The input RDD has ${messages.length} partitions.")

    try {
      val runTask = writer match {
        case w: ContinuousWriter =>
          EpochCoordinatorRef.get(
            sparkContext.getLocalProperty(ContinuousExecution.RUN_ID_KEY), sparkContext.env)
            .askSync[Unit](SetWriterPartitions(rdd.getNumPartitions))

          (context: TaskContext, iter: Iterator[InternalRow]) =>
            DataWritingSparkTask.runContinuous(writeTask, context, iter)
        case _ =>
          (context: TaskContext, iter: Iterator[InternalRow]) =>
            DataWritingSparkTask.run(writeTask, context, iter)
      }

      sparkContext.runJob(
        rdd,
        runTask,
        rdd.partitions.indices,
        (index, message: WriterCommitMessage) => messages(index) = message
      )

      if (!writer.isInstanceOf[ContinuousWriter]) {
        logInfo(s"Data source writer $writer is committing.")
        writer.commit(messages)
        logInfo(s"Data source writer $writer committed.")
      }
    } catch {
      case _: InterruptedException if writer.isInstanceOf[ContinuousWriter] =>
        // Interruption is how continuous queries are ended, so accept and ignore the exception.
      case cause: Throwable =>
        logError(s"Data source writer $writer is aborting.")
        try {
          writer.abort(messages)
        } catch {
          case t: Throwable =>
            logError(s"Data source writer $writer failed to abort.")
            cause.addSuppressed(t)
            throw new SparkException("Writing job failed.", cause)
        }
        logError(s"Data source writer $writer aborted.")
        throw new SparkException("Writing job aborted.", cause)
    }

    sparkContext.emptyRDD
  }
}

object DataWritingSparkTask extends Logging {
  def run(
      writeTask: DataWriterFactory[InternalRow],
      context: TaskContext,
      iter: Iterator[InternalRow]): WriterCommitMessage = {
    val dataWriter = writeTask.createDataWriter(context.partitionId(), context.attemptNumber())

    // write the data and commit this writer.
    Utils.tryWithSafeFinallyAndFailureCallbacks(block = {
      iter.foreach(dataWriter.write)
      logInfo(s"Writer for partition ${context.partitionId()} is committing.")
      val msg = dataWriter.commit()
      logInfo(s"Writer for partition ${context.partitionId()} committed.")
      msg
    })(catchBlock = {
      // If there is an error, abort this writer
      logError(s"Writer for partition ${context.partitionId()} is aborting.")
      dataWriter.abort()
      logError(s"Writer for partition ${context.partitionId()} aborted.")
    })
  }

  def runContinuous(
      writeTask: DataWriterFactory[InternalRow],
      context: TaskContext,
      iter: Iterator[InternalRow]): WriterCommitMessage = {
    val dataWriter = writeTask.createDataWriter(context.partitionId(), context.attemptNumber())
    val epochCoordinator = EpochCoordinatorRef.get(
      context.getLocalProperty(ContinuousExecution.RUN_ID_KEY),
      SparkEnv.get)
    val currentMsg: WriterCommitMessage = null
    var currentEpoch = context.getLocalProperty(ContinuousExecution.START_EPOCH_KEY).toLong

    do {
      // write the data and commit this writer.
      Utils.tryWithSafeFinallyAndFailureCallbacks(block = {
        try {
          iter.foreach(dataWriter.write)
          logInfo(s"Writer for partition ${context.partitionId()} is committing.")
          val msg = dataWriter.commit()
          logInfo(s"Writer for partition ${context.partitionId()} committed.")
          epochCoordinator.send(
            CommitPartitionEpoch(context.partitionId(), currentEpoch, msg)
          )
          currentEpoch += 1
        } catch {
          case _: InterruptedException =>
            // Continuous shutdown always involves an interrupt. Just finish the task.
        }
      })(catchBlock = {
        // If there is an error, abort this writer
        logError(s"Writer for partition ${context.partitionId()} is aborting.")
        dataWriter.abort()
        logError(s"Writer for partition ${context.partitionId()} aborted.")
      })
    } while (!context.isInterrupted())

    currentMsg
  }
}

class InternalRowDataWriterFactory(
    rowWriterFactory: DataWriterFactory[Row],
    schema: StructType) extends DataWriterFactory[InternalRow] {

  override def createDataWriter(partitionId: Int, attemptNumber: Int): DataWriter[InternalRow] = {
    new InternalRowDataWriter(
      rowWriterFactory.createDataWriter(partitionId, attemptNumber),
      RowEncoder.apply(schema).resolveAndBind())
  }
}

class InternalRowDataWriter(rowWriter: DataWriter[Row], encoder: ExpressionEncoder[Row])
  extends DataWriter[InternalRow] {

  override def write(record: InternalRow): Unit = rowWriter.write(encoder.fromRow(record))

  override def commit(): WriterCommitMessage = rowWriter.commit()

  override def abort(): Unit = rowWriter.abort()
}
