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

package org.apache.spark.sql.execution.streaming.sources

import javax.annotation.concurrent.GuardedBy

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.control.NonFatal

import org.apache.spark.internal.Logging
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.{LeafNode, Statistics}
import org.apache.spark.sql.catalyst.plans.logical.statsEstimation.EstimationUtils
import org.apache.spark.sql.catalyst.streaming.InternalOutputModes.{Append, Complete, Update}
import org.apache.spark.sql.execution.streaming.{MemorySinkBase, Sink}
import org.apache.spark.sql.sources.v2.{DataSourceOptions, DataSourceV2, StreamWriteSupport}
import org.apache.spark.sql.sources.v2.writer._
import org.apache.spark.sql.sources.v2.writer.streaming.StreamWriter
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.StructType

/**
 * A sink that stores the results in memory. This [[Sink]] is primarily intended for use in unit
 * tests and does not provide durability.
 */
class MemorySinkV2 extends DataSourceV2 with StreamWriteSupport with MemorySinkBase with Logging {
  override def createStreamWriter(
      queryId: String,
      schema: StructType,
      mode: OutputMode,
      options: DataSourceOptions): StreamWriter = {
    new MemoryStreamWriter(this, mode, schema)
  }

  private case class AddedData(batchId: Long, data: Array[Row])

  /** An order list of batches that have been written to this [[Sink]]. */
  @GuardedBy("this")
  private val batches = new ArrayBuffer[AddedData]()

  /** Returns all rows that are stored in this [[Sink]]. */
  def allData: Seq[Row] = synchronized {
    batches.flatMap(_.data)
  }

  def latestBatchId: Option[Long] = synchronized {
    batches.lastOption.map(_.batchId)
  }

  def latestBatchData: Seq[Row] = synchronized {
    batches.lastOption.toSeq.flatten(_.data)
  }

  def dataSinceBatch(sinceBatchId: Long): Seq[Row] = synchronized {
    batches.filter(_.batchId > sinceBatchId).flatMap(_.data)
  }

  def toDebugString: String = synchronized {
    batches.map { case AddedData(batchId, data) =>
      val dataStr = try data.mkString(" ") catch {
        case NonFatal(e) => "[Error converting to string]"
      }
      s"$batchId: $dataStr"
    }.mkString("\n")
  }

  def write(batchId: Long, outputMode: OutputMode, newRows: Array[Row]): Unit = {
    val notCommitted = synchronized {
      latestBatchId.isEmpty || batchId > latestBatchId.get
    }
    if (notCommitted) {
      logDebug(s"Committing batch $batchId to $this")
      outputMode match {
        case Append | Update =>
          val rows = AddedData(batchId, newRows)
          synchronized { batches += rows }

        case Complete =>
          val rows = AddedData(batchId, newRows)
          synchronized {
            batches.clear()
            batches += rows
          }

        case _ =>
          throw new IllegalArgumentException(
            s"Output mode $outputMode is not supported by MemorySinkV2")
      }
    } else {
      logDebug(s"Skipping already committed batch: $batchId")
    }
  }

  def clear(): Unit = synchronized {
    batches.clear()
  }

  override def toString(): String = "MemorySinkV2"
}

case class MemoryWriterCommitMessage(partition: Int, data: Seq[Row])
  extends WriterCommitMessage {}

class MemoryStreamWriter(val sink: MemorySinkV2, outputMode: OutputMode, schema: StructType)
  extends StreamWriter {

  override def createWriterFactory: MemoryWriterFactory = MemoryWriterFactory(outputMode, schema)

  override def commit(epochId: Long, messages: Array[WriterCommitMessage]): Unit = {
    val newRows = messages.flatMap {
      case message: MemoryWriterCommitMessage => message.data
    }
    sink.write(epochId, outputMode, newRows)
  }

  override def abort(epochId: Long, messages: Array[WriterCommitMessage]): Unit = {
    // Don't accept any of the new input.
  }
}

case class MemoryWriterFactory(outputMode: OutputMode, schema: StructType)
  extends DataWriterFactory[InternalRow] {

  override def createDataWriter(
      partitionId: Int,
      taskId: Long,
      epochId: Long): DataWriter[InternalRow] = {
    new MemoryDataWriter(partitionId, outputMode, schema)
  }
}

class MemoryDataWriter(partition: Int, outputMode: OutputMode, schema: StructType)
  extends DataWriter[InternalRow] with Logging {

  private val data = mutable.Buffer[Row]()

  private val encoder = RowEncoder(schema).resolveAndBind()

  override def write(row: InternalRow): Unit = {
    data.append(encoder.fromRow(row))
  }

  override def commit(): MemoryWriterCommitMessage = {
    val msg = MemoryWriterCommitMessage(partition, data.clone())
    data.clear()
    msg
  }

  override def abort(): Unit = {}
}


/**
 * Used to query the data that has been written into a [[MemorySinkV2]].
 */
case class MemoryPlanV2(sink: MemorySinkV2, override val output: Seq[Attribute]) extends LeafNode {
  private val sizePerRow = EstimationUtils.getSizePerRow(output)

  override def computeStats(): Statistics = Statistics(sizePerRow * sink.allData.size)
}

