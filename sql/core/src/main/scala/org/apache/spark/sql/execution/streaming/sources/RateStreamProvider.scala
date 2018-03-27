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

import org.apache.spark.network.util.JavaUtils
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.execution.streaming.continuous.RateStreamContinuousReader
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.sources.v2._
import org.apache.spark.sql.sources.v2.reader.streaming.{ContinuousReader, MicroBatchReader}
import org.apache.spark.sql.types._

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
class RateStreamProvider extends DataSourceV2
  with MicroBatchReadSupport with ContinuousReadSupport with DataSourceRegister {
  import RateStreamProvider._

  override def createMicroBatchReader(
      schema: Optional[StructType],
      checkpointLocation: String,
      options: DataSourceOptions): MicroBatchReader = {
      if (options.get(ROWS_PER_SECOND).isPresent) {
      val rowsPerSecond = options.get(ROWS_PER_SECOND).get().toLong
      if (rowsPerSecond <= 0) {
        throw new IllegalArgumentException(
          s"Invalid value '$rowsPerSecond'. The option 'rowsPerSecond' must be positive")
      }
    }

    if (options.get(RAMP_UP_TIME).isPresent) {
      val rampUpTimeSeconds =
        JavaUtils.timeStringAsSec(options.get(RAMP_UP_TIME).get())
      if (rampUpTimeSeconds < 0) {
        throw new IllegalArgumentException(
          s"Invalid value '$rampUpTimeSeconds'. The option 'rampUpTime' must not be negative")
      }
    }

    if (options.get(NUM_PARTITIONS).isPresent) {
      val numPartitions = options.get(NUM_PARTITIONS).get().toInt
      if (numPartitions <= 0) {
        throw new IllegalArgumentException(
          s"Invalid value '$numPartitions'. The option 'numPartitions' must be positive")
      }
    }

    if (schema.isPresent) {
      throw new AnalysisException("The rate source does not support a user-specified schema.")
    }

    new RateStreamMicroBatchReader(options, checkpointLocation)
  }

  override def createContinuousReader(
     schema: Optional[StructType],
     checkpointLocation: String,
     options: DataSourceOptions): ContinuousReader = new RateStreamContinuousReader(options)

  override def shortName(): String = "rate"
}

object RateStreamProvider {
  val SCHEMA =
    StructType(StructField("timestamp", TimestampType) :: StructField("value", LongType) :: Nil)

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
