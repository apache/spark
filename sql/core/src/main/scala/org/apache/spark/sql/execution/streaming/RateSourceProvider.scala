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

import java.io._
import java.nio.charset.StandardCharsets
import java.util.concurrent.TimeUnit

import org.apache.commons.io.IOUtils

import org.apache.spark.internal.Logging
import org.apache.spark.network.util.JavaUtils
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.{CaseInsensitiveMap, DateTimeUtils}
import org.apache.spark.sql.sources.{DataSourceRegister, StreamSourceProvider}
import org.apache.spark.sql.types._
import org.apache.spark.util.{ManualClock, SystemClock}

/**
 *  A source that generates increment long values with timestamps. Each generated row has two
 *  columns: a timestamp column for the generated time and an auto increment long column starting
 *  with 0L.
 *
 *  This source supports the following options:
 *  - `rowsPerSecond` (e.g. 100, default: 1): How many rows should be generated per second.
 *  - `rampUpTime` (e.g. 5s, default: 0s): How long to ramp up before the generating speed
 *    becomes `rowsPerSecond`. Using finer granularities than seconds will be truncated to integer
 *    seconds.
 *  - `numPartitions` (e.g. 10, default: Spark's default parallelism): The partition number for the
 *    generated rows. The source will try its best to reach `rowsPerSecond`, but the query may
 *    be resource constrained, and `numPartitions` can be tweaked to help reach the desired speed.
 */
class RateSourceProvider extends StreamSourceProvider with DataSourceRegister {

  override def sourceSchema(
      sqlContext: SQLContext,
      schema: Option[StructType],
      providerName: String,
      parameters: Map[String, String]): (String, StructType) =
    (shortName(), RateSourceProvider.SCHEMA)

  override def createSource(
      sqlContext: SQLContext,
      metadataPath: String,
      schema: Option[StructType],
      providerName: String,
      parameters: Map[String, String]): Source = {
    val params = CaseInsensitiveMap(parameters)

    val rowsPerSecond = params.get("rowsPerSecond").map(_.toLong).getOrElse(1L)
    if (rowsPerSecond <= 0) {
      throw new IllegalArgumentException(
        s"Invalid value '${params("rowsPerSecond")}'. The option 'rowsPerSecond' " +
          "must be positive")
    }

    val rampUpTimeSeconds =
      params.get("rampUpTime").map(JavaUtils.timeStringAsSec(_)).getOrElse(0L)
    if (rampUpTimeSeconds < 0) {
      throw new IllegalArgumentException(
        s"Invalid value '${params("rampUpTime")}'. The option 'rampUpTime' " +
          "must not be negative")
    }

    val numPartitions = params.get("numPartitions").map(_.toInt).getOrElse(
      sqlContext.sparkContext.defaultParallelism)
    if (numPartitions <= 0) {
      throw new IllegalArgumentException(
        s"Invalid value '${params("numPartitions")}'. The option 'numPartitions' " +
          "must be positive")
    }

    new RateStreamSource(
      sqlContext,
      metadataPath,
      rowsPerSecond,
      rampUpTimeSeconds,
      numPartitions,
      params.get("useManualClock").map(_.toBoolean).getOrElse(false) // Only for testing
    )
  }
  override def shortName(): String = "rate"
}

object RateSourceProvider {
  val SCHEMA =
    StructType(StructField("timestamp", TimestampType) :: StructField("value", LongType) :: Nil)

  val VERSION = 1
}

class RateStreamSource(
    sqlContext: SQLContext,
    metadataPath: String,
    rowsPerSecond: Long,
    rampUpTimeSeconds: Long,
    numPartitions: Int,
    useManualClock: Boolean) extends Source with Logging {

  import RateSourceProvider._
  import RateStreamSource._

  val clock = if (useManualClock) new ManualClock else new SystemClock

  private val maxSeconds = Long.MaxValue / rowsPerSecond

  if (rampUpTimeSeconds > maxSeconds) {
    throw new ArithmeticException(
      s"Integer overflow. Max offset with $rowsPerSecond rowsPerSecond" +
        s" is $maxSeconds, but 'rampUpTimeSeconds' is $rampUpTimeSeconds.")
  }

  private val startTimeMs = {
    val metadataLog =
      new HDFSMetadataLog[LongOffset](sqlContext.sparkSession, metadataPath) {
        override def serialize(metadata: LongOffset, out: OutputStream): Unit = {
          val writer = new BufferedWriter(new OutputStreamWriter(out, StandardCharsets.UTF_8))
          writer.write("v" + VERSION + "\n")
          writer.write(metadata.json)
          writer.flush
        }

        override def deserialize(in: InputStream): LongOffset = {
          val content = IOUtils.toString(new InputStreamReader(in, StandardCharsets.UTF_8))
          // HDFSMetadataLog guarantees that it never creates a partial file.
          assert(content.length != 0)
          if (content(0) == 'v') {
            val indexOfNewLine = content.indexOf("\n")
            if (indexOfNewLine > 0) {
              val version = parseVersion(content.substring(0, indexOfNewLine), VERSION)
              LongOffset(SerializedOffset(content.substring(indexOfNewLine + 1)))
            } else {
              throw new IllegalStateException(
                s"Log file was malformed: failed to detect the log file version line.")
            }
          } else {
            throw new IllegalStateException(
              s"Log file was malformed: failed to detect the log file version line.")
          }
        }
      }

    metadataLog.get(0).getOrElse {
      val offset = LongOffset(clock.getTimeMillis())
      metadataLog.add(0, offset)
      logInfo(s"Start time: $offset")
      offset
    }.offset
  }

  /** When the system time runs backward, "lastTimeMs" will make sure we are still monotonic. */
  @volatile private var lastTimeMs = startTimeMs

  override def schema: StructType = RateSourceProvider.SCHEMA

  override def getOffset: Option[Offset] = {
    val now = clock.getTimeMillis()
    if (lastTimeMs < now) {
      lastTimeMs = now
    }
    Some(LongOffset(TimeUnit.MILLISECONDS.toSeconds(lastTimeMs - startTimeMs)))
  }

  override def getBatch(start: Option[Offset], end: Offset): DataFrame = {
    val startSeconds = start.flatMap(LongOffset.convert(_).map(_.offset)).getOrElse(0L)
    val endSeconds = LongOffset.convert(end).map(_.offset).getOrElse(0L)
    assert(startSeconds <= endSeconds, s"startSeconds($startSeconds) > endSeconds($endSeconds)")
    if (endSeconds > maxSeconds) {
      throw new ArithmeticException("Integer overflow. Max offset with " +
        s"$rowsPerSecond rowsPerSecond is $maxSeconds, but it's $endSeconds now.")
    }
    // Fix "lastTimeMs" for recovery
    if (lastTimeMs < TimeUnit.SECONDS.toMillis(endSeconds) + startTimeMs) {
      lastTimeMs = TimeUnit.SECONDS.toMillis(endSeconds) + startTimeMs
    }
    val rangeStart = valueAtSecond(startSeconds, rowsPerSecond, rampUpTimeSeconds)
    val rangeEnd = valueAtSecond(endSeconds, rowsPerSecond, rampUpTimeSeconds)
    logDebug(s"startSeconds: $startSeconds, endSeconds: $endSeconds, " +
      s"rangeStart: $rangeStart, rangeEnd: $rangeEnd")

    if (rangeStart == rangeEnd) {
      return sqlContext.internalCreateDataFrame(sqlContext.sparkContext.emptyRDD, schema)
    }

    val localStartTimeMs = startTimeMs + TimeUnit.SECONDS.toMillis(startSeconds)
    val relativeMsPerValue =
      TimeUnit.SECONDS.toMillis(endSeconds - startSeconds).toDouble / (rangeEnd - rangeStart)

    val rdd = sqlContext.sparkContext.range(rangeStart, rangeEnd, 1, numPartitions).map { v =>
      val relative = math.round((v - rangeStart) * relativeMsPerValue)
      InternalRow(DateTimeUtils.fromMillis(relative + localStartTimeMs), v)
    }
    sqlContext.internalCreateDataFrame(rdd, schema)
  }

  override def stop(): Unit = {}

  override def toString: String = s"RateSource[rowsPerSecond=$rowsPerSecond, " +
    s"rampUpTimeSeconds=$rampUpTimeSeconds, numPartitions=$numPartitions]"
}

object RateStreamSource {

  /** Calculate the end value we will emit at the time `seconds`. */
  def valueAtSecond(seconds: Long, rowsPerSecond: Long, rampUpTimeSeconds: Long): Long = {
    // E.g., rampUpTimeSeconds = 4, rowsPerSecond = 10
    // Then speedDeltaPerSecond = 2
    //
    // seconds   = 0 1 2  3  4  5  6
    // speed     = 0 2 4  6  8 10 10 (speedDeltaPerSecond * seconds)
    // end value = 0 2 6 12 20 30 40 (0 + speedDeltaPerSecond * seconds) * (seconds + 1) / 2
    val speedDeltaPerSecond = rowsPerSecond / (rampUpTimeSeconds + 1)
    if (seconds <= rampUpTimeSeconds) {
      // Calculate "(0 + speedDeltaPerSecond * seconds) * (seconds + 1) / 2" in a special way to
      // avoid overflow
      if (seconds % 2 == 1) {
        (seconds + 1) / 2 * speedDeltaPerSecond * seconds
      } else {
        seconds / 2 * speedDeltaPerSecond * (seconds + 1)
      }
    } else {
      // rampUpPart is just a special case of the above formula: rampUpTimeSeconds == seconds
      val rampUpPart = valueAtSecond(rampUpTimeSeconds, rowsPerSecond, rampUpTimeSeconds)
      rampUpPart + (seconds - rampUpTimeSeconds) * rowsPerSecond
    }
  }
}
