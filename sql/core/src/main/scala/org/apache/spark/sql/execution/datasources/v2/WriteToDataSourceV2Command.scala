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

import org.apache.spark.{SparkException, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.command.RunnableCommand
import org.apache.spark.sql.sources.v2.writer._
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.Utils

case class WriteToDataSourceV2Command(writer: DataSourceV2Writer, query: LogicalPlan)
  extends RunnableCommand {

  override protected def innerChildren: Seq[QueryPlan[_]] = Seq(query)

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val writeTask = writer match {
      case w: SupportsWriteInternalRow => w.createInternalRowWriterFactory()
      case _ => new RowToInternalRowDataWriterFactory(writer.createWriterFactory(), query.schema)
    }

    val rdd = Dataset.ofRows(sparkSession, query).queryExecution.toRdd
    val messages = new Array[WriterCommitMessage](rdd.partitions.length)

    logInfo(s"Start processing data source writer: $writer. " +
      s"The input RDD has ${messages.length} partitions.")

    try {
      sparkSession.sparkContext.runJob(
        rdd,
        (context: TaskContext, iter: Iterator[InternalRow]) =>
          DataWritingSparkTask.run(writeTask, context, iter),
        rdd.partitions.indices,
        (index, message: WriterCommitMessage) => messages(index) = message
      )

      logInfo(s"Data source writer $writer is committing.")
      writer.commit(messages)
      logInfo(s"Data source writer $writer committed.")
    } catch {
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

    Nil
  }
}

object DataWritingSparkTask extends Logging {
  def run(
      writeTask: DataWriterFactory[InternalRow],
      context: TaskContext,
      iter: Iterator[InternalRow]): WriterCommitMessage = {
    val dataWriter =
      writeTask.createWriter(context.stageId(), context.partitionId(), context.attemptNumber())

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
}

class RowToInternalRowDataWriterFactory(
    rowWriterFactory: DataWriterFactory[Row],
    schema: StructType) extends DataWriterFactory[InternalRow] {

  override def createWriter(
      stageId: Int,
      partitionId: Int,
      attemptNumber: Int): DataWriter[InternalRow] = {
    new RowToInternalRowDataWriter(
      rowWriterFactory.createWriter(stageId, partitionId, attemptNumber),
      RowEncoder.apply(schema).resolveAndBind())
  }
}

class RowToInternalRowDataWriter(rowWriter: DataWriter[Row], encoder: ExpressionEncoder[Row])
  extends DataWriter[InternalRow] {

  override def write(record: InternalRow): Unit = rowWriter.write(encoder.fromRow(record))

  override def commit(): WriterCommitMessage = rowWriter.commit()

  override def abort(): Unit = rowWriter.abort()
}
