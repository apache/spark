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
import org.apache.spark.sql.execution.streaming.{RateSourceProvider, RateStreamOffset, ValueRunTimeMsPair}
import org.apache.spark.sql.execution.streaming.sources.RateStreamSourceV2
import org.apache.spark.sql.sources.v2.{ContinuousReadSupport, DataSourceV2, DataSourceV2Options}
import org.apache.spark.sql.sources.v2.reader._
import org.apache.spark.sql.types.{LongType, StructField, StructType, TimestampType}

case class ContinuousRateStreamPartitionOffset(
   partition: Int, currentValue: Long, currentTimeMs: Long) extends PartitionOffset

class ContinuousRateStreamReader(options: DataSourceV2Options)
  extends ContinuousReader {
  implicit val defaultFormats: DefaultFormats = DefaultFormats

  val creationTime = System.currentTimeMillis()

  val numPartitions = options.get(RateStreamSourceV2.NUM_PARTITIONS).orElse("5").toInt
  val rowsPerSecond = options.get(RateStreamSourceV2.ROWS_PER_SECOND).orElse("6").toLong
  val perPartitionRate = rowsPerSecond.toDouble / numPartitions.toDouble

  override def mergeOffsets(offsets: Array[PartitionOffset]): Offset = {
    assert(offsets.length == numPartitions)
    val tuples = offsets.map {
      case ContinuousRateStreamPartitionOffset(i, currVal, nextRead) =>
        (i, ValueRunTimeMsPair(currVal, nextRead))
    }
    RateStreamOffset(Map(tuples: _*))
  }

  override def deserializeOffset(json: String): Offset = {
    RateStreamOffset(Serialization.read[Map[Int, ValueRunTimeMsPair]](json))
  }

  override def readSchema(): StructType = RateSourceProvider.SCHEMA

  private var offset: Offset = _

  override def setOffset(offset: java.util.Optional[Offset]): Unit = {
    this.offset = offset.orElse(RateStreamSourceV2.createInitialOffset(numPartitions, creationTime))
  }

  override def getStartOffset(): Offset = offset

  override def createReadTasks(): java.util.List[ReadTask[Row]] = {
    val partitionStartMap = offset match {
      case off: RateStreamOffset => off.partitionToValueAndRunTimeMs
      case off =>
        throw new IllegalArgumentException(
          s"invalid offset type ${off.getClass()} for ContinuousRateSource")
    }
    if (partitionStartMap.keySet.size != numPartitions) {
      throw new IllegalArgumentException(
        s"The previous run contained ${partitionStartMap.keySet.size} partitions, but" +
        s" $numPartitions partitions are currently configured. The numPartitions option" +
        " cannot be changed.")
    }

    Range(0, numPartitions).map { i =>
      val start = partitionStartMap(i)
      // Have each partition advance by numPartitions each row, with starting points staggered
      // by their partition index.
      RateStreamReadTask(
        start.value,
        start.runTimeMs,
        i,
        numPartitions,
        perPartitionRate)
        .asInstanceOf[ReadTask[Row]]
    }.asJava
  }

  override def commit(end: Offset): Unit = {}
  override def stop(): Unit = {}

}

case class RateStreamReadTask(
    startValue: Long,
    startTimeMs: Long,
    partitionIndex: Int,
    increment: Long,
    rowsPerSecond: Double)
  extends ReadTask[Row] {
  override def createDataReader(): DataReader[Row] =
    new RateStreamDataReader(startValue, startTimeMs, partitionIndex, increment, rowsPerSecond)
}

class RateStreamDataReader(
    startValue: Long,
    startTimeMs: Long,
    partitionIndex: Int,
    increment: Long,
    rowsPerSecond: Double)
  extends ContinuousDataReader[Row] {
  private var nextReadTime: Long = startTimeMs
  private val readTimeIncrement: Long = (1000 / rowsPerSecond).toLong

  private var currentValue = startValue
  private var currentRow: Row = null

  override def next(): Boolean = {
    currentValue += increment
    nextReadTime += readTimeIncrement

    try {
      while (System.currentTimeMillis < nextReadTime) {
        Thread.sleep(nextReadTime - System.currentTimeMillis)
      }
    } catch {
      case _: InterruptedException =>
        // Someone's trying to end the task; just let them.
        return false
    }

    currentRow = Row(
      DateTimeUtils.toJavaTimestamp(DateTimeUtils.fromMillis(nextReadTime)),
      currentValue)

    true
  }

  override def get: Row = currentRow

  override def close(): Unit = {}

  override def getOffset(): PartitionOffset =
    ContinuousRateStreamPartitionOffset(partitionIndex, currentValue, nextReadTime)
}
