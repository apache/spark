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

package org.apache.spark.sql.streaming.sources

import java.util
import java.util.OptionalLong

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.optimizer.BuildLeft
import org.apache.spark.sql.connector.catalog.{SupportsRead, Table, TableCapability}
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReaderFactory, Scan, ScanBuilder, Statistics, SupportsReportStatistics}
import org.apache.spark.sql.connector.read.streaming.{MicroBatchStream, Offset, SupportsReportMicroBatchStatistics}
import org.apache.spark.sql.execution.datasources.v2.StreamingDataSourceV2ScanRelation
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, SortMergeJoinExec}
import org.apache.spark.sql.execution.streaming.runtime.{LongOffset, StreamingQueryWrapper}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.connector.SimpleTableProvider
import org.apache.spark.sql.streaming.StreamTest
import org.apache.spark.sql.types.{LongType, StructField, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/**
 * A stream with one micro-batch over the offsets (0, 10], whose partitions are empty: the tests
 * check how Spark plans the micro-batch, not what it reads.
 */
class FakeOffsetRangeStream extends MicroBatchStream {
  override def initialOffset(): Offset = LongOffset(0)
  override def latestOffset(): Offset = LongOffset(10)
  override def deserializeOffset(json: String): Offset = LongOffset(json.toLong)
  override def commit(end: Offset): Unit = {}
  override def stop(): Unit = {}
  override def planInputPartitions(start: Offset, end: Offset): Array[InputPartition] =
    Array.empty
  override def createReaderFactory(): PartitionReaderFactory = { _ =>
    throw new IllegalStateException("the stream has no partitions to read")
  }
}

/** A [[FakeOffsetRangeStream]] that reports 100 bytes and one row for each offset it reads. */
class FakeOffsetRangeStreamWithStatistics
    extends FakeOffsetRangeStream with SupportsReportMicroBatchStatistics {
  override def estimateStatistics(start: Offset, end: Offset): Statistics = {
    val rows = end.asInstanceOf[LongOffset].offset - start.asInstanceOf[LongOffset].offset
    FakeStatistics(bytes = rows * 100, rows = rows)
  }
}

case class FakeStatistics(bytes: Long, rows: Long) extends Statistics {
  override def sizeInBytes(): OptionalLong = OptionalLong.of(bytes)
  override def numRows(): OptionalLong = OptionalLong.of(rows)
}

/**
 * A source whose scan reports the statistics of the whole source, 1 GB, and whose stream reports
 * the statistics of each micro-batch when the option `microBatchStatistics` is true.
 */
class FakeReadWithMicroBatchStatistics extends SimpleTableProvider {
  override def getTable(options: CaseInsensitiveStringMap): Table = new Table with SupportsRead {
    override def name(): String = "fake"
    override def schema(): StructType = StructType(Seq(StructField("id", LongType)))
    override def capabilities(): util.Set[TableCapability] =
      util.EnumSet.of(TableCapability.MICRO_BATCH_READ)
    override def newScanBuilder(scanOptions: CaseInsensitiveStringMap): ScanBuilder = () =>
      new Scan with SupportsReportStatistics {
        override def readSchema(): StructType = schema()
        override def estimateStatistics(): Statistics =
          FakeStatistics(bytes = 1L << 30, rows = 1L << 20)
        override def toMicroBatchStream(checkpointLocation: String): MicroBatchStream =
          if (options.getBoolean("microBatchStatistics", false)) {
            new FakeOffsetRangeStreamWithStatistics
          } else {
            new FakeOffsetRangeStream
          }
      }
  }
}

class StreamingMicroBatchStatisticsSuite extends StreamTest {

  private def readStream(microBatchStatistics: Boolean): DataFrame =
    spark.readStream
      .format(classOf[FakeReadWithMicroBatchStatistics].getName)
      .option("microBatchStatistics", microBatchStatistics)
      .load()

  /** Runs the micro-batch of `df` and returns the last execution of the query. */
  private def runMicroBatch(df: DataFrame) = {
    val query = df.writeStream.format("noop").start()
    try {
      query.processAllAvailable()
      query.asInstanceOf[StreamingQueryWrapper].streamingQuery.lastExecution
    } finally {
      query.stop()
    }
  }

  for (microBatchStatistics <- Seq(true, false)) {
    test(s"statistics of a micro-batch - micro-batch statistics: $microBatchStatistics") {
      val execution = runMicroBatch(readStream(microBatchStatistics))
      val leaves = execution.optimizedPlan.collect {
        case relation: StreamingDataSourceV2ScanRelation => relation
      }
      assert(leaves.size == 1, execution.optimizedPlan)
      val stats = leaves.head.stats
      if (microBatchStatistics) {
        // The micro-batch reads the offsets (0, 10].
        assert(stats.sizeInBytes == 1000 && stats.rowCount.contains(10), stats)
      } else {
        assert(stats.sizeInBytes == (1L << 30) && stats.rowCount.contains(1L << 20), stats)
      }
    }

    test("a micro-batch that fits under the broadcast threshold is broadcast in a join - " +
        s"micro-batch statistics: $microBatchStatistics") {
      withSQLConf(
          SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
          SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "10000") {
        // The static side is larger than the threshold, so only the micro-batch can be broadcast.
        val static = spark.range(100000).toDF("id")
        val execution = runMicroBatch(readStream(microBatchStatistics).join(static, "id"))
        val plan = execution.executedPlan
        if (microBatchStatistics) {
          val joins = plan.collect { case join: BroadcastHashJoinExec => join }
          assert(joins.size == 1 && joins.head.buildSide == BuildLeft, plan)
        } else {
          assert(plan.collect { case join: SortMergeJoinExec => join }.size == 1, plan)
        }
      }
    }
  }
}
