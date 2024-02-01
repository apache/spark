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

import java.util

import org.apache.spark.network.util.JavaUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.catalog.{SupportsRead, Table, TableCapability}
import org.apache.spark.sql.connector.read.{Scan, ScanBuilder}
import org.apache.spark.sql.connector.read.streaming.{ContinuousStream, MicroBatchStream}
import org.apache.spark.sql.execution.streaming.continuous.RateStreamContinuousStream
import org.apache.spark.sql.internal.connector.SimpleTableProvider
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.CaseInsensitiveStringMap

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
class RateStreamProvider extends SimpleTableProvider with DataSourceRegister {
  import RateStreamProvider._

  override def getTable(options: CaseInsensitiveStringMap): Table = {
    val rowsPerSecond = options.getLong(ROWS_PER_SECOND, 1)
    if (rowsPerSecond <= 0) {
      throw new IllegalArgumentException(
        s"Invalid value '$rowsPerSecond'. The option 'rowsPerSecond' must be positive")
    }

    val rampUpTimeSeconds = Option(options.get(RAMP_UP_TIME))
      .map(JavaUtils.timeStringAsSec)
      .getOrElse(0L)
    if (rampUpTimeSeconds < 0) {
      throw new IllegalArgumentException(
        s"Invalid value '$rampUpTimeSeconds'. The option 'rampUpTime' must not be negative")
    }

    val numPartitions = options.getInt(
      NUM_PARTITIONS, SparkSession.active.sparkContext.defaultParallelism)
    if (numPartitions <= 0) {
      throw new IllegalArgumentException(
        s"Invalid value '$numPartitions'. The option 'numPartitions' must be positive")
    }
    new RateStreamTable(rowsPerSecond, rampUpTimeSeconds, numPartitions)
  }

  override def shortName(): String = "rate"
}

class RateStreamTable(
    rowsPerSecond: Long,
    rampUpTimeSeconds: Long,
    numPartitions: Int)
  extends Table with SupportsRead {

  override def name(): String = {
    s"RateStream(rowsPerSecond=$rowsPerSecond, rampUpTimeSeconds=$rampUpTimeSeconds, " +
      s"numPartitions=$numPartitions)"
  }

  override def schema(): StructType = RateStreamProvider.SCHEMA

  override def capabilities(): util.Set[TableCapability] = {
    util.EnumSet.of(TableCapability.MICRO_BATCH_READ, TableCapability.CONTINUOUS_READ)
  }

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = () => new Scan {
    override def readSchema(): StructType = RateStreamProvider.SCHEMA

    override def toMicroBatchStream(checkpointLocation: String): MicroBatchStream =
      new RateStreamMicroBatchStream(
        rowsPerSecond, rampUpTimeSeconds, numPartitions, options, checkpointLocation)

    override def toContinuousStream(checkpointLocation: String): ContinuousStream =
      new RateStreamContinuousStream(rowsPerSecond, numPartitions)

    override def columnarSupportMode(): Scan.ColumnarSupportMode =
      Scan.ColumnarSupportMode.UNSUPPORTED
  }
}

object RateStreamProvider {
  val SCHEMA =
    StructType(Array(StructField("timestamp", TimestampType), StructField("value", LongType)))

  val VERSION = 1

  val NUM_PARTITIONS = "numPartitions"
  val ROWS_PER_SECOND = "rowsPerSecond"
  val RAMP_UP_TIME = "rampUpTime"

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
