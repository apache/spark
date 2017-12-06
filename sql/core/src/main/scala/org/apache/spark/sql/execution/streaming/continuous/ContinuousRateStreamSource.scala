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

package org.apache.spark.sql.execution.streaming.continuous

import scala.collection.JavaConverters._

import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.execution.streaming._
import org.apache.spark.sql.sources.v2.{ContinuousReadSupport, DataSourceV2, DataSourceV2Options}
import org.apache.spark.sql.sources.v2.reader._
import org.apache.spark.sql.types.{LongType, StructField, StructType, TimestampType}

object ContinuousRateStreamSource {
  val NUM_PARTITIONS = "numPartitions"
  val ROWS_PER_SECOND = "rowsPerSecond"
}

case class RateStreamOffset(partitionToStartValue: Map[Int, Long]) extends Offset {
  implicit val defaultFormats: DefaultFormats = DefaultFormats
  override val json = Serialization.write(partitionToStartValue)
}

case class ContinuousRateStreamPartitionOffset(partition: Int, start: Long) extends PartitionOffset

class ContinuousRateStreamReader(options: DataSourceV2Options)
  extends ContinuousReader {
  implicit val defaultFormats: DefaultFormats = DefaultFormats

  val numPartitions = options.get(ContinuousRateStreamSource.NUM_PARTITIONS).orElse("5").toInt
  val rowsPerSecond = options.get(ContinuousRateStreamSource.ROWS_PER_SECOND).orElse("6").toLong

  override def mergeOffsets(offsets: Array[PartitionOffset]): Offset = {
    assert(offsets.length == numPartitions)
    val tuples = offsets.map {
      case ContinuousRateStreamPartitionOffset(p, s) => p -> s
    }
    RateStreamOffset(Map(tuples: _*))
  }

  override def deserialize(json: String): Offset = {
    RateStreamOffset(Serialization.read[Map[Int, Long]](json))
  }

  override def readSchema(): StructType = {
    StructType(
        StructField("timestamp", TimestampType, false) ::
        StructField("value", LongType, false) :: Nil)
  }

  private var offset: java.util.Optional[Offset] = _

  override def setOffset(offset: java.util.Optional[Offset]): Unit = {
    this.offset = offset
  }

  override def createReadTasks(): java.util.List[ReadTask[Row]] = {
    val partitionStartMap = Option(offset.orElse(null)).map {
      case o: RateStreamOffset => o.partitionToStartValue
      case s: SerializedOffset => Serialization.read[Map[Int, Long]](s.json)
      case _ => throw new IllegalArgumentException("invalid offset type for ContinuousRateSource")
    }
    if (partitionStartMap.exists(_.keySet.size > numPartitions)) {
      throw new IllegalArgumentException("Start offset contained too many partitions.")
    }
    val perPartitionRate = rowsPerSecond.toDouble / numPartitions.toDouble

    Range(0, numPartitions).map { n =>
      // If the offset doesn't have a value for this partition, start from the beginning.
      val start = partitionStartMap.flatMap(_.get(n)).getOrElse(0L + n)
      // Have each partition advance by numPartitions each row, with starting points staggered
      // by their partition index.
      RateStreamReadTask(start, n, numPartitions, perPartitionRate)
        .asInstanceOf[ReadTask[Row]]
    }.asJava
  }

  override def commit(end: Offset): Unit = {}
  override def stop(): Unit = {}

}

case class RateStreamReadTask(
    startValue: Long, partitionIndex: Int, increment: Long, rowsPerSecond: Double)
  extends ReadTask[Row] {
  override def createDataReader(): DataReader[Row] =
    new RateStreamDataReader(startValue, partitionIndex, increment, rowsPerSecond.toLong)
}

class RateStreamDataReader(
    startValue: Long, partitionIndex: Int, increment: Long, rowsPerSecond: Long)
  extends ContinuousDataReader[Row] {
  private var nextReadTime = 0L
  private var numReadRows = 0L

  private var currentValue = startValue
  private var currentRow: Row = null

  override def next(): Boolean = {
    // Set the timestamp for the first time.
    if (currentRow == null) nextReadTime = System.currentTimeMillis() + 1000

    if (numReadRows == rowsPerSecond) {
      // Sleep until we reach the next second.

      try {
        while (System.currentTimeMillis < nextReadTime) {
          Thread.sleep(nextReadTime - System.currentTimeMillis)
        }
      } catch {
        case _: InterruptedException =>
          // Someone's trying to end the task; just let them.
          return false
      }
      numReadRows = 0
      nextReadTime += 1000
    }

    currentRow = Row(
      DateTimeUtils.toJavaTimestamp(DateTimeUtils.fromMillis(System.currentTimeMillis)),
      currentValue)
    currentValue += increment
    numReadRows += 1

    true
  }

  override def get: Row = currentRow

  override def close(): Unit = {}

  // We use the value corresponding to partition 0 as the offset.
  override def getOffset(): PartitionOffset =
    ContinuousRateStreamPartitionOffset(partitionIndex, currentValue)
}
