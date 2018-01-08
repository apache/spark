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

import java.util.Optional

import scala.collection.JavaConverters._
import scala.collection.mutable

import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.execution.streaming.{RateStreamOffset, ValueRunTimeMsPair}
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.sources.v2.{DataSourceV2, DataSourceV2Options}
import org.apache.spark.sql.sources.v2.reader._
import org.apache.spark.sql.sources.v2.streaming.MicroBatchReadSupport
import org.apache.spark.sql.sources.v2.streaming.reader.{MicroBatchReader, Offset}
import org.apache.spark.sql.types.{LongType, StructField, StructType, TimestampType}
import org.apache.spark.util.{ManualClock, SystemClock}

/**
 * This is a temporary register as we build out v2 migration. Microbatch read support should
 * be implemented in the same register as v1.
 */
class RateSourceProviderV2 extends DataSourceV2 with MicroBatchReadSupport with DataSourceRegister {
  override def createMicroBatchReader(
      schema: Optional[StructType],
      checkpointLocation: String,
      options: DataSourceV2Options): MicroBatchReader = {
    new RateStreamMicroBatchReader(options)
  }

  override def shortName(): String = "ratev2"
}

class RateStreamMicroBatchReader(options: DataSourceV2Options)
  extends MicroBatchReader {
  implicit val defaultFormats: DefaultFormats = DefaultFormats

  val clock = {
    // The option to use a manual clock is provided only for unit testing purposes.
    if (options.get("useManualClock").orElse("false").toBoolean) new ManualClock
    else new SystemClock
  }

  private val numPartitions =
    options.get(RateStreamSourceV2.NUM_PARTITIONS).orElse("5").toInt
  private val rowsPerSecond =
    options.get(RateStreamSourceV2.ROWS_PER_SECOND).orElse("6").toLong

  // The interval (in milliseconds) between rows in each partition.
  // e.g. if there are 4 global rows per second, and 2 partitions, each partition
  // should output rows every (1000 * 2 / 4) = 500 ms.
  private val msPerPartitionBetweenRows = (1000 * numPartitions) / rowsPerSecond

  override def readSchema(): StructType = {
    StructType(
      StructField("timestamp", TimestampType, false) ::
      StructField("value", LongType, false) :: Nil)
  }

  val creationTimeMs = clock.getTimeMillis()

  private var start: RateStreamOffset = _
  private var end: RateStreamOffset = _

  override def setOffsetRange(
      start: Optional[Offset],
      end: Optional[Offset]): Unit = {
    this.start = start.orElse(
      RateStreamSourceV2.createInitialOffset(numPartitions, creationTimeMs))
      .asInstanceOf[RateStreamOffset]

    this.end = end.orElse {
      val currentTime = clock.getTimeMillis()
      RateStreamOffset(
        this.start.partitionToValueAndRunTimeMs.map {
          case startOffset @ (part, ValueRunTimeMsPair(currentVal, currentReadTime)) =>
            // Calculate the number of rows we should advance in this partition (based on the
            // current time), and output a corresponding offset.
            val readInterval = currentTime - currentReadTime
            val numNewRows = readInterval / msPerPartitionBetweenRows
            if (numNewRows <= 0) {
              startOffset
            } else {
              (part, ValueRunTimeMsPair(
                  currentVal + (numNewRows * numPartitions),
                  currentReadTime + (numNewRows * msPerPartitionBetweenRows)))
            }
        }
      )
    }.asInstanceOf[RateStreamOffset]
  }

  override def getStartOffset(): Offset = {
    if (start == null) throw new IllegalStateException("start offset not set")
    start
  }
  override def getEndOffset(): Offset = {
    if (end == null) throw new IllegalStateException("end offset not set")
    end
  }

  override def deserializeOffset(json: String): Offset = {
    RateStreamOffset(Serialization.read[Map[Int, ValueRunTimeMsPair]](json))
  }

  override def createReadTasks(): java.util.List[ReadTask[Row]] = {
    val startMap = start.partitionToValueAndRunTimeMs
    val endMap = end.partitionToValueAndRunTimeMs
    endMap.keys.toSeq.map { part =>
      val ValueRunTimeMsPair(endVal, _) = endMap(part)
      val ValueRunTimeMsPair(startVal, startTimeMs) = startMap(part)

      val packedRows = mutable.ListBuffer[(Long, Long)]()
      var outVal = startVal + numPartitions
      var outTimeMs = startTimeMs
      while (outVal <= endVal) {
        packedRows.append((outTimeMs, outVal))
        outVal += numPartitions
        outTimeMs += msPerPartitionBetweenRows
      }

      RateStreamBatchTask(packedRows).asInstanceOf[ReadTask[Row]]
    }.toList.asJava
  }

  override def commit(end: Offset): Unit = {}
  override def stop(): Unit = {}
}

case class RateStreamBatchTask(vals: Seq[(Long, Long)]) extends ReadTask[Row] {
  override def createDataReader(): DataReader[Row] = new RateStreamBatchReader(vals)
}

class RateStreamBatchReader(vals: Seq[(Long, Long)]) extends DataReader[Row] {
  var currentIndex = -1

  override def next(): Boolean = {
    // Return true as long as the new index is in the seq.
    currentIndex += 1
    currentIndex < vals.size
  }

  override def get(): Row = {
    Row(
      DateTimeUtils.toJavaTimestamp(DateTimeUtils.fromMillis(vals(currentIndex)._1)),
      vals(currentIndex)._2)
  }

  override def close(): Unit = {}
}

object RateStreamSourceV2 {
  val NUM_PARTITIONS = "numPartitions"
  val ROWS_PER_SECOND = "rowsPerSecond"

  private[sql] def createInitialOffset(numPartitions: Int, creationTimeMs: Long) = {
    RateStreamOffset(
      Range(0, numPartitions).map { i =>
        // Note that the starting offset is exclusive, so we have to decrement the starting value
        // by the increment that will later be applied. The first row output in each
        // partition will have a value equal to the partition index.
        (i,
          ValueRunTimeMsPair(
            (i - numPartitions).toLong,
            creationTimeMs))
      }.toMap)
  }
}
