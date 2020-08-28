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

import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.connector.read.InputPartition
import org.apache.spark.sql.connector.read.streaming.{ContinuousPartitionReader, ContinuousPartitionReaderFactory, ContinuousStream, Offset, PartitionOffset}
import org.apache.spark.sql.execution.streaming.{RateStreamOffset, ValueRunTimeMsPair}

case class RateStreamPartitionOffset(
   partition: Int, currentValue: Long, currentTimeMs: Long) extends PartitionOffset

class RateStreamContinuousStream(rowsPerSecond: Long, numPartitions: Int) extends ContinuousStream {
  implicit val defaultFormats: DefaultFormats = DefaultFormats

  val creationTime = System.currentTimeMillis()

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

  override def initialOffset: Offset = createInitialOffset(numPartitions, creationTime)

  override def planInputPartitions(start: Offset): Array[InputPartition] = {
    val partitionStartMap = start match {
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
    }.toArray
  }

  override def createContinuousReaderFactory(): ContinuousPartitionReaderFactory = {
    RateStreamContinuousReaderFactory
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
  extends InputPartition

object RateStreamContinuousReaderFactory extends ContinuousPartitionReaderFactory {
  override def createReader(partition: InputPartition): ContinuousPartitionReader[InternalRow] = {
    val p = partition.asInstanceOf[RateStreamContinuousInputPartition]
    new RateStreamContinuousPartitionReader(
      p.startValue, p.startTimeMs, p.partitionIndex, p.increment, p.rowsPerSecond)
  }
}

class RateStreamContinuousPartitionReader(
    startValue: Long,
    startTimeMs: Long,
    partitionIndex: Int,
    increment: Long,
    rowsPerSecond: Double)
  extends ContinuousPartitionReader[InternalRow] {
  private var nextReadTime: Long = startTimeMs
  private val readTimeIncrement: Long = (1000 / rowsPerSecond).toLong

  private var currentValue = startValue
  private var currentRow: InternalRow = null

  override def next(): Boolean = {
    currentValue += increment
    nextReadTime += readTimeIncrement

    try {
      var toWaitMs = nextReadTime - System.currentTimeMillis
      while (toWaitMs > 0) {
        Thread.sleep(toWaitMs)
        toWaitMs = nextReadTime - System.currentTimeMillis
      }
    } catch {
      case _: InterruptedException =>
        // Someone's trying to end the task; just let them.
        return false
    }

    currentRow = InternalRow(
      DateTimeUtils.millisToMicros(nextReadTime),
      currentValue)

    true
  }

  override def get: InternalRow = currentRow

  override def close(): Unit = {}

  override def getOffset(): PartitionOffset =
    RateStreamPartitionOffset(partitionIndex, currentValue, nextReadTime)
}
