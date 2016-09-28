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

import java.util.concurrent.atomic.AtomicInteger
import javax.annotation.concurrent.GuardedBy

import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.util.control.NonFatal

import org.apache.spark.internal.Logging
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.encoders.encoderFor
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.LeafNode
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
 * A [[Source]] that produces value stored in memory as they are added by the user.  This [[Source]]
 * is primarily intended for use in unit tests as it can only replay data when the object is still
 * available.
 */
case class MemoryStream[A : Encoder](id: Int, sqlContext: SQLContext)
    extends Source with Logging {
  protected val encoder = encoderFor[A]
  protected val logicalPlan = StreamingExecutionRelation(this)
  protected val output = logicalPlan.output

  /**
   * All batches from `lastCommittedOffset + 1` to `currentOffset`, inclusive.
   * Stored in a ListBuffer to facilitate removing committed batches.
   */
  @GuardedBy("this")
  protected val batches = new ListBuffer[Dataset[A]]

  @GuardedBy("this")
  protected var currentOffset: LongOffset = new LongOffset(-1)

  /**
   * Last offset that was discarded, or -1 if no commits have occurred. Note that the value
   * -1 is used in calculations below and isn't just an arbitrary constant.
   */
  @GuardedBy("this")
  protected var lastOffsetCommitted : LongOffset = new LongOffset(-1)

  def schema: StructType = encoder.schema

  def toDS()(implicit sqlContext: SQLContext): Dataset[A] = {
    Dataset(sqlContext.sparkSession, logicalPlan)
  }

  def toDF()(implicit sqlContext: SQLContext): DataFrame = {
    Dataset.ofRows(sqlContext.sparkSession, logicalPlan)
  }

  def addData(data: A*): Offset = {
    addData(data.toTraversable)
  }

  def addData(data: TraversableOnce[A]): Offset = {
    import sqlContext.implicits._
    val ds = data.toVector.toDS()
    logDebug(s"Adding ds: $ds")
    this.synchronized {
      currentOffset = currentOffset + 1
      batches += ds
      currentOffset
    }
  }

  override def toString: String = s"MemoryStream[${Utils.truncatedString(output, ",")}]"

  override def lastCommittedOffset: Option[Offset] = synchronized {
    if (lastOffsetCommitted.offset == -1) {
      None
    } else {
      Some(lastOffsetCommitted)
    }
  }

  override def getOffset: Option[Offset] = synchronized {
    if (currentOffset.offset == -1) {
      None
    } else {
      Some(currentOffset)
    }
  }

  override def getBatch(start: Option[Offset], end: Offset): DataFrame = {
    // Compute the internal batch numbers to fetch: [startOrdinal, endOrdinal)
    val startOrdinal =
      start.map(_.asInstanceOf[LongOffset]).getOrElse(LongOffset(-1)).offset.toInt + 1
    val endOrdinal = end.asInstanceOf[LongOffset].offset.toInt + 1

    // Internal buffer only holds the batches after lastCommittedOffset.
    val newBlocks = synchronized {
      val sliceStart = startOrdinal - lastOffsetCommitted.offset.toInt - 1
      val sliceEnd = endOrdinal - lastOffsetCommitted.offset.toInt - 1
      batches.slice(sliceStart, sliceEnd)
    }

    logDebug(
      s"MemoryBatch [$startOrdinal, $endOrdinal]: ${newBlocks.flatMap(_.collect()).mkString(", ")}")
    newBlocks
      .map(_.toDF())
      .reduceOption(_ union _)
      .getOrElse {
        sys.error("No data selected!")
      }
  }

  override def commit(end: Offset): Unit = synchronized {
    if (end.isInstanceOf[LongOffset]) {
      val newOffset = end.asInstanceOf[LongOffset]
      val offsetDiff = (newOffset.offset - lastOffsetCommitted.offset).toInt

      if (offsetDiff < 0) {
        sys.error(s"Offsets committed out of order: $lastOffsetCommitted followed by $end")
      }

      batches.trimStart(offsetDiff)
      lastOffsetCommitted = newOffset
    } else {
      sys.error(s"MemoryStream.commit() received an offset ($end) that did not originate with " +
        s"an instance of this class")
    }
  }

  override def stop() {}
}

/**
 * A sink that stores the results in memory. This [[Sink]] is primarily intended for use in unit
 * tests and does not provide durability.
 */
class MemorySink(val schema: StructType, outputMode: OutputMode) extends Sink with Logging {

  private case class AddedData(batchId: Long, data: Array[Row])

  /** An order list of batches that have been written to this [[Sink]]. */
  @GuardedBy("this")
  private val batches = new ArrayBuffer[AddedData]()

  /** Returns all rows that are stored in this [[Sink]]. */
  def allData: Seq[Row] = synchronized {
    batches.map(_.data).flatten
  }

  def latestBatchId: Option[Long] = synchronized {
    batches.lastOption.map(_.batchId)
  }

  def latestBatchData: Seq[Row] = synchronized { batches.lastOption.toSeq.flatten(_.data) }

  def toDebugString: String = synchronized {
    batches.map { case AddedData(batchId, data) =>
      val dataStr = try data.mkString(" ") catch {
        case NonFatal(e) => "[Error converting to string]"
      }
      s"$batchId: $dataStr"
    }.mkString("\n")
  }

  override def addBatch(batchId: Long, data: DataFrame): Unit = synchronized {
    if (latestBatchId.isEmpty || batchId > latestBatchId.get) {
      logDebug(s"Committing batch $batchId to $this")
      outputMode match {
        case InternalOutputModes.Append | InternalOutputModes.Update =>
          batches.append(AddedData(batchId, data.collect()))

        case InternalOutputModes.Complete =>
          batches.clear()
          batches += AddedData(batchId, data.collect())

        case _ =>
          throw new IllegalArgumentException(
            s"Output mode $outputMode is not supported by MemorySink")
      }
    } else {
      logDebug(s"Skipping already committed batch: $batchId")
    }
  }
}

/**
 * Used to query the data that has been written into a [[MemorySink]].
 */
case class MemoryPlan(sink: MemorySink, output: Seq[Attribute]) extends LeafNode {
  def this(sink: MemorySink) = this(sink, sink.schema.toAttributes)
}
