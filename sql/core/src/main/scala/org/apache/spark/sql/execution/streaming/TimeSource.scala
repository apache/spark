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

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import org.apache.spark.sql.sources.{DataSourceRegister, StreamSourceProvider}
import org.apache.spark.sql.types._

class TimeStreamProvider extends StreamSourceProvider with DataSourceRegister {
  /** Returns the name and schema of the source that can be used to continually read data. */
  override def sourceSchema(
      sqlContext: SQLContext,
      schema: Option[StructType],
      providerName: String,
      parameters: Map[String, String]): (String, StructType) =
    ("time", StructType(StructField("timestamp", LongType) ::
      StructField("value", LongType) :: Nil))

  override def createSource(
                             sqlContext: SQLContext,
                             metadataPath: String,
                             schema: Option[StructType],
                             providerName: String,
                             parameters: Map[String, String]): Source = {
    new TimeStreamSource(
      sqlContext.sparkSession,
      parameters.get("tuplesPerSecond").map(_.toLong).getOrElse(1L),
      parameters.get("rampUpTimeSeconds").map(_.toLong).getOrElse(10L),
      parameters.get("wiggleRatio").map(_.toDouble).getOrElse(0.01))
  }

  /**
    * The string that represents the format that this data source provider uses. This is
    * overridden by children to provide a nice alias for the data source. For example:
    *
    * {{{
    *   override def shortName(): String = "parquet"
    * }}}
    *
    * @since 1.5.0
    */
  override def shortName(): String = "time"
}

class TimeStreamSource(
    spark: SparkSession,
    tuplesPerSecond: Long,
    rampUpTimeSeconds: Long,
    wiggleRatio: Double) extends Source with Logging {
  val startTime = System.currentTimeMillis()
  var lastTime = startTime
  import spark.implicits._

  /** Returns the schema of the data from this source */
  override def schema: StructType = StructType(StructField("timestamp", LongType) ::
    StructField("value", LongType) :: Nil)

  /** Returns the maximum available offset for this source. */
  override def getOffset: Option[Offset] = {
    val curTime = System.currentTimeMillis()
    if (curTime > lastTime) {
      lastTime = curTime
    }
    Some(LongOffset((curTime - startTime) / 1000))
  }

  private def addRateWiggle(second: Long): Long = {
    var wiggled: Long = (((second % 4) - 1) * tuplesPerSecond * wiggleRatio + second).toLong
    if (wiggled < 0) {
      wiggled = second
    }
    logError(s"\n\n wiggleBefore: $second - wiggleAfter: $wiggled\n\n")
    wiggled
  }

  /**
    * Returns the data that is between the offsets (`start`, `end`]. When `start` is `None` then
    * the batch should begin with the first available record. This method must always return the
    * same data for a particular `start` and `end` pair.
    */
  override def getBatch(start: Option[Offset], end: Offset): DataFrame = {
    val startSeconds = start.map(_.asInstanceOf[LongOffset].offset).getOrElse(0L)
    val endSeconds = end.asInstanceOf[LongOffset].offset
    val (rangeStart, rangeEnd) = if (rampUpTimeSeconds > endSeconds) {
      (math.rint(tuplesPerSecond * (startSeconds * 1.0 / rampUpTimeSeconds)).toLong * startSeconds,
        math.rint(tuplesPerSecond * (endSeconds * 1.0 / rampUpTimeSeconds)).toLong * endSeconds)
    } else if (startSeconds < rampUpTimeSeconds) {
      (math.rint(tuplesPerSecond * (startSeconds * 1.0 / rampUpTimeSeconds)).toLong * startSeconds,
        addRateWiggle(endSeconds * tuplesPerSecond))
    } else {
      (addRateWiggle(startSeconds * tuplesPerSecond),
        addRateWiggle(endSeconds * tuplesPerSecond))
    }
    logError(s"\n\nrangeStart: $rangeStart - rangeEnd: $rangeEnd - \n\n")
    val localStartTime = startTime
    val localPerSecond = tuplesPerSecond

    spark.range(rangeStart, rangeEnd, 1, 1).map { v =>
      val relative = v + (1 << (v % 4))
      (relative / localPerSecond + localStartTime / 1000, relative)
    }.toDF("timestamp", "value")
  }

  /** Stop this source and free any resources it has allocated. */
  override def stop(): Unit = {}
}