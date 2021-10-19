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

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.catalog.{SupportsRead, Table, TableCapability}
import org.apache.spark.sql.connector.read.{Scan, ScanBuilder}
import org.apache.spark.sql.connector.read.streaming.{ContinuousStream, MicroBatchStream}
import org.apache.spark.sql.internal.connector.SimpleTableProvider
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types.{LongType, StructField, StructType, TimestampType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/**
 *  A source that generates increment long values with timestamps. Each generated row has two
 *  columns: a timestamp column for the generated time and an auto increment long column starting
 *  with 0L.
 *
 *  This source supports the following options:
 *  - `rowsPerEpoch` (e.g. 100): How many rows should be generated per epoch.
 *  - `numPartitions` (e.g. 10, default: Spark's default parallelism): The partition number for the
 *    generated rows.
 *  - `startTimestamp` (e.g. 1000, default: 0): starting value of generated time
 *  - `advanceMillisPerEpoch` (e.g. 1000, default: 1000): the amount of time being advanced in
 *    generated time on each epoch.
 *
 *  Unlike `rate` data source, this data source provides a consistent set of input rows per epoch
 *  regardless of query execution (configuration of trigger, query being lagging, etc.), say,
 *  batch 0 will produce 0~999 and batch 1 will produce 1000~1999, and so on. Same applies to the
 *  generated time.
 *  (Here `epoch` represents a micro-batch for micro-batch mode.)
 */
class RatePerEpochProvider extends SimpleTableProvider with DataSourceRegister {
  import RatePerEpochProvider._

  override def getTable(options: CaseInsensitiveStringMap): Table = {
    val rowsPerEpoch = options.getLong(ROWS_PER_EPOCH, 0)
    if (rowsPerEpoch <= 0) {
      throw new IllegalArgumentException(
        s"Invalid value '$rowsPerEpoch'. The option 'rowsPerEpoch' must be positive")
    }

    val numPartitions = options.getInt(
      NUM_PARTITIONS, SparkSession.active.sparkContext.defaultParallelism)
    if (numPartitions <= 0) {
      throw new IllegalArgumentException(
        s"Invalid value '$numPartitions'. The option 'numPartitions' must be positive")
    }

    val startTimestamp = options.getLong(START_TIMESTAMP, 0)
    if (startTimestamp < 0) {
      throw new IllegalArgumentException(
        s"Invalid value '$startTimestamp'. The option 'startTimestamp' must be non-negative")
    }

    val advanceMillisPerEpoch = options.getInt(ADVANCE_MILLIS_PER_EPOCH, 1000)
    if (advanceMillisPerEpoch < 0) {
      throw new IllegalArgumentException(
        s"Invalid value '$advanceMillisPerEpoch'. The option 'advanceMillisPerEpoch' " +
          "must be non-negative")
    }

    new RatePerEpochTable(rowsPerEpoch, numPartitions, startTimestamp, advanceMillisPerEpoch)
  }

  override def shortName(): String = "rate-epoch"
}

class RatePerEpochTable(
    rowsPerEpoch: Long,
    numPartitions: Int,
    startTimestamp: Long,
    advanceMillisPerEpoch: Int) extends Table with SupportsRead {
  override def name(): String = {
    s"RatePerEpoch(rowsPerEpoch=$rowsPerEpoch, numPartitions=$numPartitions," +
      s"startTimestamp=$startTimestamp, advanceMillisPerEpoch=$advanceMillisPerEpoch)"
  }

  override def schema(): StructType = RatePerEpochProvider.SCHEMA

  override def capabilities(): util.Set[TableCapability] = {
    util.EnumSet.of(TableCapability.MICRO_BATCH_READ)
  }

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = () => new Scan {
    override def readSchema(): StructType = RatePerEpochProvider.SCHEMA

    override def toMicroBatchStream(checkpointLocation: String): MicroBatchStream =
      new RatePerEpochMicroBatchStream(rowsPerEpoch, numPartitions, startTimestamp,
        advanceMillisPerEpoch, options)

    override def toContinuousStream(checkpointLocation: String): ContinuousStream = {
      throw new UnsupportedOperationException("continuous mode is not supported yet!")
    }
  }
}

object RatePerEpochProvider {
  val SCHEMA =
    StructType(StructField("timestamp", TimestampType) :: StructField("value", LongType) :: Nil)

  val VERSION = 1

  val NUM_PARTITIONS = "numPartitions"
  val ROWS_PER_EPOCH = "rowsPerEpoch"
  val START_TIMESTAMP = "startTimestamp"
  val ADVANCE_MILLIS_PER_EPOCH = "advanceMillisPerEpoch"
}
