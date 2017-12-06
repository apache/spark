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

import java.util.Optional

import scala.collection.JavaConverters._

import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.execution.streaming.continuous.{ContinuousRateStreamSource, RateStreamDataReader, RateStreamReadTask}
import org.apache.spark.sql.sources.v2.DataSourceV2Options
import org.apache.spark.sql.sources.v2.reader._
import org.apache.spark.sql.types.{LongType, StructField, StructType, TimestampType}
import org.apache.spark.util.SystemClock

class RateStreamV2Reader(options: DataSourceV2Options)
  extends MicroBatchReader {
  implicit val defaultFormats: DefaultFormats = DefaultFormats

  val clock = new SystemClock

  private val numPartitions =
    options.get(ContinuousRateStreamSource.NUM_PARTITIONS).orElse("5").toInt
  private val rowsPerSecond =
    options.get(ContinuousRateStreamSource.ROWS_PER_SECOND).orElse("6").toLong

  override def readSchema(): StructType = {
    StructType(
      StructField("timestamp", TimestampType, false) ::
      StructField("value", LongType, false) :: Nil)
  }

  val creationTimeMs = clock.getTimeMillis()

  private var start: Offset = _
  private var end: Offset = _

  override def setOffsetRange(start: Optional[Offset], end: Optional[Offset]): Unit = {
    this.start = start.orElse(LongOffset(creationTimeMs))
    this.end = end.orElse(LongOffset(clock.getTimeMillis()))
  }

  override def getStartOffset(): Offset = {
    if (start == null) throw new IllegalStateException("start offset not set")
    start
  }
  override def getEndOffset(): Offset = {
    if (end == null) throw new IllegalStateException("end offset not set")
    end
  }

  override def createReadTasks(): java.util.List[ReadTask[Row]] = {
    val startTime = LongOffset.convert(start).get.offset
    val numSeconds = (LongOffset.convert(end).get.offset - startTime) / 1000
    val firstValue = (startTime - creationTimeMs) / 1000

    (firstValue to firstValue + numSeconds * rowsPerSecond - 1)
      .groupBy(_ % numPartitions)
      .values
      .map(vals => RateStreamBatchTask(vals).asInstanceOf[ReadTask[Row]])
      .toList
      .asJava
  }

  override def commit(end: Offset): Unit = {}
  override def stop(): Unit = {}
}

case class RateStreamBatchTask(vals: Seq[Long]) extends ReadTask[Row] {
  override def createDataReader(): DataReader[Row] = new RateStreamBatchReader(vals)
}

class RateStreamBatchReader(vals: Seq[Long]) extends DataReader[Row] {
  var currentIndex = -1

  override def next(): Boolean = {
    // Return true as long as the new index is in the seq.
    currentIndex += 1
    currentIndex < vals.size
  }

  override def get(): Row = {
    Row(
      DateTimeUtils.toJavaTimestamp(DateTimeUtils.fromMillis(System.currentTimeMillis)),
      vals(currentIndex))
  }

  override def close(): Unit = {}
}
