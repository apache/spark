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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.execution.streaming.{RateStreamOffset, ValueRunTimeMsPair}
import org.apache.spark.sql.execution.streaming.sources.RateStreamProvider
import org.apache.spark.sql.sources.v2.DataSourceOptions
import org.apache.spark.sql.sources.v2.reader._
import org.apache.spark.sql.sources.v2.reader.streaming.{ContinuousInputPartitionReader, ContinuousReader, Offset, PartitionOffset}
import org.apache.spark.sql.types.StructType

case class RateStreamPartitionOffset(
   partition: Int, currentValue: Long, currentTimeMs: Long) extends PartitionOffset

class RateStreamContinuousReader(options: DataSourceOptions) extends ContinuousReader {
  implicit val defaultFormats: DefaultFormats = DefaultFormats

  val creationTime = System.currentTimeMillis()

  val numPartitions = options.get(RateStreamProvider.NUM_PARTITIONS).orElse("5").toInt
  val rowsPerSecond = options.get(RateStreamProvider.ROWS_PER_SECOND).orElse("6").toLong
  val perPartitionRate = rowsPerSecond.toDouble / numPartitions.toDouble

  override def mergeOffsets(offsets: Array[PartitionOffset]): Offset = {
    assert(offsets.length == numPartitions)
    val tuples = offsets.map {
      case RateStreamPartitionOffset(i, currVal, nextRead) =>
        (i, ValueRunTimeMsPair(currVal, nextRead))
    }
    RateStreamOffset(Map(tuples: _*))
  }

  override def deserializeOffset(json: String): Offset = {
    RateStreamOffset(Serialization.read[Map[Int, ValueRunTimeMsPair]](json))
  }

  override def readSchema(): StructType = RateStreamProvider.SCHEMA

  private var offset: Offset = _

  override def setStartOffset(offset: java.util.Optional[Offset]): Unit = {
    this.offset = offset.orElse(createInitialOffset(numPartitions, creationTime))
  }

  override def getStartOffset(): Offset = offset

  override def planInputPartitions(): java.util.List[InputPartition[InternalRow]] = {
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
      RateStreamContinuousInputPartition(
        start.value,
        start.runTimeMs,
        i,
        numPartitions,
        perPartitionRate)
        .asInstanceOf[InputPartition[InternalRow]]
    }.asJava
  }

  override def commit(end: Offset): Unit = {}
  override def stop(): Unit = {}

  private def createInitialOffset(numPartitions: Int, creationTimeMs: Long) = {
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

case class RateStreamContinuousInputPartition(
    startValue: Long,
    startTimeMs: Long,
    partitionIndex: Int,
    increment: Long,
    rowsPerSecond: Double)
  extends ContinuousInputPartition[InternalRow] {

  override def createContinuousReader(
      offset: PartitionOffset): InputPartitionReader[InternalRow] = {
    val rateStreamOffset = offset.asInstanceOf[RateStreamPartitionOffset]
    require(rateStreamOffset.partition == partitionIndex,
      s"Expected partitionIndex: $partitionIndex, but got: ${rateStreamOffset.partition}")
    new RateStreamContinuousInputPartitionReader(
      rateStreamOffset.currentValue,
      rateStreamOffset.currentTimeMs,
      partitionIndex,
      increment,
      rowsPerSecond)
  }

  override def createPartitionReader(): InputPartitionReader[InternalRow] =
    new RateStreamContinuousInputPartitionReader(
      startValue, startTimeMs, partitionIndex, increment, rowsPerSecond)
}

class RateStreamContinuousInputPartitionReader(
    startValue: Long,
    startTimeMs: Long,
    partitionIndex: Int,
    increment: Long,
    rowsPerSecond: Double)
  extends ContinuousInputPartitionReader[InternalRow] {
  private var nextReadTime: Long = startTimeMs
  private val readTimeIncrement: Long = (1000 / rowsPerSecond).toLong

  private var currentValue = startValue
  private var currentRow: InternalRow = null

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

    currentRow = InternalRow(
      DateTimeUtils.fromMillis(nextReadTime),
      currentValue)

    true
  }

  override def get: InternalRow = currentRow

  override def close(): Unit = {}

  override def getOffset(): PartitionOffset =
    RateStreamPartitionOffset(partitionIndex, currentValue, nextReadTime)
}
