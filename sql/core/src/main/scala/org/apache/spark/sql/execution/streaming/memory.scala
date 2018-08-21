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

package org.apache.spark.sql.execution.streaming

import java.{util => ju}
import java.util.Optional
import java.util.concurrent.atomic.AtomicInteger
import javax.annotation.concurrent.GuardedBy

import scala.collection.JavaConverters._
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.util.control.NonFatal

import org.apache.spark.internal.Logging
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.encoderFor
import org.apache.spark.sql.catalyst.expressions.{Attribute, UnsafeRow}
import org.apache.spark.sql.catalyst.plans.logical.{LeafNode, LogicalPlan, Statistics}
import org.apache.spark.sql.catalyst.plans.logical.statsEstimation.EstimationUtils
import org.apache.spark.sql.catalyst.streaming.InternalOutputModes._
import org.apache.spark.sql.sources.v2.reader.{InputPartition, InputPartitionReader}
import org.apache.spark.sql.sources.v2.reader.streaming.{MicroBatchReader, Offset => OffsetV2}
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.Utils

object MemoryStream {
  protected val currentBlockId = new AtomicInteger(0)
  protected val memoryStreamId = new AtomicInteger(0)

  def apply[A : Encoder](implicit sqlContext: SQLContext): MemoryStream[A] =
    new MemoryStream[A](memoryStreamId.getAndIncrement(), sqlContext)
}

/**
 * A base class for memory stream implementations. Supports adding data and resetting.
 */
abstract class MemoryStreamBase[A : Encoder](sqlContext: SQLContext) extends BaseStreamingSource {
  protected val encoder = encoderFor[A]
  protected val attributes = encoder.schema.toAttributes

  def toDS(): Dataset[A] = {
    Dataset[A](sqlContext.sparkSession, logicalPlan)
  }

  def toDF(): DataFrame = {
    Dataset.ofRows(sqlContext.sparkSession, logicalPlan)
  }

  def addData(data: A*): Offset = {
    addData(data.toTraversable)
  }

  def readSchema(): StructType = encoder.schema

  protected def logicalPlan: LogicalPlan

  def addData(data: TraversableOnce[A]): Offset
}

/**
 * A [[Source]] that produces value stored in memory as they are added by the user.  This [[Source]]
 * is intended for use in unit tests as it can only replay data when the object is still
 * available.
 */
case class MemoryStream[A : Encoder](id: Int, sqlContext: SQLContext)
    extends MemoryStreamBase[A](sqlContext) with MicroBatchReader with Logging {

  protected val logicalPlan: LogicalPlan =
    StreamingExecutionRelation(this, attributes)(sqlContext.sparkSession)
  protected val output = logicalPlan.output

  /**
   * All batches from `lastCommittedOffset + 1` to `currentOffset`, inclusive.
   * Stored in a ListBuffer to facilitate removing committed batches.
   */
  @GuardedBy("this")
  protected val batches = new ListBuffer[Array[UnsafeRow]]

  @GuardedBy("this")
  protected var currentOffset: LongOffset = new LongOffset(-1)

  @GuardedBy("this")
  protected var startOffset = new LongOffset(-1)

  @GuardedBy("this")
  private var endOffset = new LongOffset(-1)

  /**
   * Last offset that was discarded, or -1 if no commits have occurred. Note that the value
   * -1 is used in calculations below and isn't just an arbitrary constant.
   */
  @GuardedBy("this")
  protected var lastOffsetCommitted : LongOffset = new LongOffset(-1)

  def addData(data: TraversableOnce[A]): Offset = {
    val objects = data.toSeq
    val rows = objects.iterator.map(d => encoder.toRow(d).copy().asInstanceOf[UnsafeRow]).toArray
    logDebug(s"Adding: $objects")
    this.synchronized {
      currentOffset = currentOffset + 1
      batches += rows
      currentOffset
    }
  }

  override def toString: String = s"MemoryStream[${Utils.truncatedString(output, ",")}]"

  override def setOffsetRange(start: Optional[OffsetV2], end: Optional[OffsetV2]): Unit = {
    synchronized {
      startOffset = start.orElse(LongOffset(-1)).asInstanceOf[LongOffset]
      endOffset = end.orElse(currentOffset).asInstanceOf[LongOffset]
    }
  }

  override def deserializeOffset(json: String): OffsetV2 = LongOffset(json.toLong)

  override def getStartOffset: OffsetV2 = synchronized {
    if (startOffset.offset == -1) null else startOffset
  }

  override def getEndOffset: OffsetV2 = synchronized {
    if (endOffset.offset == -1) null else endOffset
  }

  override def planInputPartitions(): ju.List[InputPartition[InternalRow]] = {
    synchronized {
      // Compute the internal batch numbers to fetch: [startOrdinal, endOrdinal)
      val startOrdinal = startOffset.offset.toInt + 1
      val endOrdinal = endOffset.offset.toInt + 1

      // Internal buffer only holds the batches after lastCommittedOffset.
      val newBlocks = synchronized {
        val sliceStart = startOrdinal - lastOffsetCommitted.offset.toInt - 1
        val sliceEnd = endOrdinal - lastOffsetCommitted.offset.toInt - 1
        assert(sliceStart <= sliceEnd, s"sliceStart: $sliceStart sliceEnd: $sliceEnd")
        batches.slice(sliceStart, sliceEnd)
      }

      logDebug(generateDebugString(newBlocks.flatten, startOrdinal, endOrdinal))

      newBlocks.map { block =>
        new MemoryStreamInputPartition(block): InputPartition[InternalRow]
      }.asJava
    }
  }

  private def generateDebugString(
      rows: Seq[UnsafeRow],
      startOrdinal: Int,
      endOrdinal: Int): String = {
    val fromRow = encoder.resolveAndBind().fromRow _
    s"MemoryBatch [$startOrdinal, $endOrdinal]: " +
        s"${rows.map(row => fromRow(row)).mkString(", ")}"
  }

  override def commit(end: OffsetV2): Unit = synchronized {
    def check(newOffset: LongOffset): Unit = {
      val offsetDiff = (newOffset.offset - lastOffsetCommitted.offset).toInt

      if (offsetDiff < 0) {
        sys.error(s"Offsets committed out of order: $lastOffsetCommitted followed by $end")
      }

      batches.trimStart(offsetDiff)
      lastOffsetCommitted = newOffset
    }

    LongOffset.convert(end) match {
      case Some(lo) => check(lo)
      case None => sys.error(s"MemoryStream.commit() received an offset ($end) " +
        "that did not originate with an instance of this class")
    }
  }

  override def stop() {}

  def reset(): Unit = synchronized {
    batches.clear()
    startOffset = LongOffset(-1)
    endOffset = LongOffset(-1)
    currentOffset = new LongOffset(-1)
    lastOffsetCommitted = new LongOffset(-1)
  }
}


class MemoryStreamInputPartition(records: Array[UnsafeRow])
  extends InputPartition[InternalRow] {
  override def createPartitionReader(): InputPartitionReader[InternalRow] = {
    new InputPartitionReader[InternalRow] {
      private var currentIndex = -1

      override def next(): Boolean = {
        // Return true as long as the new index is in the array.
        currentIndex += 1
        currentIndex < records.length
      }

      override def get(): UnsafeRow = records(currentIndex)

      override def close(): Unit = {}
    }
  }
}

/** A common trait for MemorySinks with methods used for testing */
trait MemorySinkBase extends BaseStreamingSink {
  def allData: Seq[Row]
  def latestBatchData: Seq[Row]
  def dataSinceBatch(sinceBatchId: Long): Seq[Row]
  def latestBatchId: Option[Long]
}

/**
 * A sink that stores the results in memory. This [[Sink]] is primarily intended for use in unit
 * tests and does not provide durability.
 */
class MemorySink(val schema: StructType, outputMode: OutputMode) extends Sink
  with MemorySinkBase with Logging {

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

  def latestBatchData: Seq[Row] = synchronized { batches.lastOption.toSeq.flatten(_.data) }

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

  override def addBatch(batchId: Long, data: DataFrame): Unit = {
    val notCommitted = synchronized {
      latestBatchId.isEmpty || batchId > latestBatchId.get
    }
    if (notCommitted) {
      logDebug(s"Committing batch $batchId to $this")
      outputMode match {
        case Append | Update =>
          val rows = AddedData(batchId, data.collect())
          synchronized { batches += rows }

        case Complete =>
          val rows = AddedData(batchId, data.collect())
          synchronized {
            batches.clear()
            batches += rows
          }

        case _ =>
          throw new IllegalArgumentException(
            s"Output mode $outputMode is not supported by MemorySink")
      }
    } else {
      logDebug(s"Skipping already committed batch: $batchId")
    }
  }

  def clear(): Unit = synchronized {
    batches.clear()
  }

  override def toString(): String = "MemorySink"
}

/**
 * Used to query the data that has been written into a [[MemorySink]].
 */
case class MemoryPlan(sink: MemorySink, output: Seq[Attribute]) extends LeafNode {
  def this(sink: MemorySink) = this(sink, sink.schema.toAttributes)

  private val sizePerRow = EstimationUtils.getSizePerRow(sink.schema.toAttributes)

  override def computeStats(): Statistics = Statistics(sizePerRow * sink.allData.size)
}
