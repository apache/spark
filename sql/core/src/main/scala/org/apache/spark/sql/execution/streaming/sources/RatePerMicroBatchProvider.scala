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

import org.apache.spark.SparkUnsupportedOperationException
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
 *  - `rowsPerBatch` (e.g. 100): How many rows should be generated per micro-batch.
 *  - `numPartitions` (e.g. 10, default: Spark's default parallelism): The partition number for the
 *    generated rows.
 *  - `startTimestamp` (e.g. 1000, default: 0): starting value of generated time
 *  - `advanceMillisPerBatch` (e.g. 1000, default: 1000): the amount of time being advanced in
 *    generated time on each micro-batch.
 *
 *  Unlike `rate` data source, this data source provides a consistent set of input rows per
 *  micro-batch regardless of query execution (configuration of trigger, query being lagging, etc.),
 *  say, batch 0 will produce 0~999 and batch 1 will produce 1000~1999, and so on. Same applies to
 *  the generated time.
 *
 *  As the name represents, this data source only supports micro-batch read.
 */
class RatePerMicroBatchProvider extends SimpleTableProvider with DataSourceRegister {
  import RatePerMicroBatchProvider._

  override def getTable(options: CaseInsensitiveStringMap): Table = {
    val rowsPerBatch = options.getLong(ROWS_PER_BATCH, 0)
    if (rowsPerBatch <= 0) {
      throw new IllegalArgumentException(
        s"Invalid value '$rowsPerBatch'. The option 'rowsPerBatch' must be positive")
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

    val advanceMillisPerBatch = options.getInt(ADVANCE_MILLIS_PER_BATCH, 1000)
    if (advanceMillisPerBatch < 0) {
      throw new IllegalArgumentException(
        s"Invalid value '$advanceMillisPerBatch'. The option 'advanceMillisPerBatch' " +
          "must be non-negative")
    }

    new RatePerMicroBatchTable(rowsPerBatch, numPartitions, startTimestamp,
      advanceMillisPerBatch)
  }

  override def shortName(): String = "rate-micro-batch"
}

class RatePerMicroBatchTable(
    rowsPerBatch: Long,
    numPartitions: Int,
    startTimestamp: Long,
    advanceMillisPerBatch: Int) extends Table with SupportsRead {
  override def name(): String = {
    s"RatePerMicroBatch(rowsPerBatch=$rowsPerBatch, numPartitions=$numPartitions," +
      s"startTimestamp=$startTimestamp, advanceMillisPerBatch=$advanceMillisPerBatch)"
  }

  override def schema(): StructType = RatePerMicroBatchProvider.SCHEMA

  override def capabilities(): util.Set[TableCapability] = {
    util.EnumSet.of(TableCapability.MICRO_BATCH_READ)
  }

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = () => new Scan {
    override def readSchema(): StructType = RatePerMicroBatchProvider.SCHEMA

    override def toMicroBatchStream(checkpointLocation: String): MicroBatchStream =
      new RatePerMicroBatchStream(rowsPerBatch, numPartitions, startTimestamp,
        advanceMillisPerBatch, options)

    override def toContinuousStream(checkpointLocation: String): ContinuousStream = {
      throw new SparkUnsupportedOperationException("_LEGACY_ERROR_TEMP_3167")
    }

    override def columnarSupportMode(): Scan.ColumnarSupportMode =
      Scan.ColumnarSupportMode.UNSUPPORTED
  }
}

object RatePerMicroBatchProvider {
  val SCHEMA =
    StructType(Array(StructField("timestamp", TimestampType), StructField("value", LongType)))

  val VERSION = 1

  val NUM_PARTITIONS = "numPartitions"
  val ROWS_PER_BATCH = "rowsPerBatch"
  val START_TIMESTAMP = "startTimestamp"
  val ADVANCE_MILLIS_PER_BATCH = "advanceMillisPerBatch"
}
