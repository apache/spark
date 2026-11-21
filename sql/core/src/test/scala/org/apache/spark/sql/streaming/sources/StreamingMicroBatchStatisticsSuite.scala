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
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
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
 * A stream whose offsets go from 0 to `latest`, and whose partitions are empty: the tests check
 * how Spark plans the micro-batches, not what they read.
 */
class FakeOffsetRangeStream(latest: Long) extends MicroBatchStream {
  override def initialOffset(): Offset = LongOffset(0)
  override def latestOffset(): Offset = LongOffset(latest)
  override def deserializeOffset(json: String): Offset = LongOffset(json.toLong)
  override def commit(end: Offset): Unit = {}
  override def stop(): Unit = {}
  override def planInputPartitions(start: Offset, end: Offset): Array[InputPartition] =
    Array.empty
  override def createReaderFactory(): PartitionReaderFactory = { _ =>
    throw new IllegalStateException("the stream has no partitions to read")
  }
}

/** A [[FakeOffsetRangeStream]] that reports `statistics(start, end)` for each micro-batch. */
class FakeOffsetRangeStreamWithStatistics(latest: Long, statistics: (Long, Long) => Statistics)
  extends FakeOffsetRangeStream(latest) with SupportsReportMicroBatchStatistics {
  override def estimateStatistics(start: Offset, end: Offset): Statistics =
    statistics(start.asInstanceOf[LongOffset].offset, end.asInstanceOf[LongOffset].offset)
}

case class FakeStatistics(bytes: Long, rows: Long) extends Statistics {
  override def sizeInBytes(): OptionalLong = OptionalLong.of(bytes)
  override def numRows(): OptionalLong = OptionalLong.of(rows)
}

object FakeEmptyStatistics extends Statistics {
  override def sizeInBytes(): OptionalLong = OptionalLong.empty()
  override def numRows(): OptionalLong = OptionalLong.empty()
}

/**
 * A source whose scan reports the statistics of the whole source, 1 GB, and whose stream, with the
 * offsets from 0 to the option `latestOffset`, reports the statistics of each micro-batch as the
 * option `microBatchStatistics` says:
 *  - `none`: the stream does not implement [[SupportsReportMicroBatchStatistics]].
 *  - `known`: 100 bytes and one row for each offset.
 *  - `empty`: statistics with no values.
 *  - `null`: null.
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
        override def toMicroBatchStream(checkpointLocation: String): MicroBatchStream = {
          val latest = options.getLong("latestOffset", 10)
          options.get("microBatchStatistics") match {
            case "none" => new FakeOffsetRangeStream(latest)
            case "known" => new FakeOffsetRangeStreamWithStatistics(latest, (start, end) =>
              FakeStatistics(bytes = (end - start) * 100, rows = end - start))
            case "empty" =>
              new FakeOffsetRangeStreamWithStatistics(latest, (_, _) => FakeEmptyStatistics)
            case "null" => new FakeOffsetRangeStreamWithStatistics(latest, (_, _) => null)
          }
        }
      }
  }
}

class StreamingMicroBatchStatisticsSuite extends StreamTest {

  private val scanStatistics = (BigInt(1L << 30), Some(BigInt(1L << 20)))

  private def readStream(microBatchStatistics: String, latestOffset: Long = 10): DataFrame =
    spark.readStream
      .format(classOf[FakeReadWithMicroBatchStatistics].getName)
      .option("microBatchStatistics", microBatchStatistics)
      .option("latestOffset", latestOffset)
      .load()

  /** Runs the micro-batches of `df` and returns the last execution of the query. */
  private def runMicroBatches(df: DataFrame, checkpointLocation: Option[String] = None) = {
    val writer = df.writeStream.format("noop")
    val query = checkpointLocation.map(writer.option("checkpointLocation", _)).getOrElse(writer)
      .start()
    try {
      query.processAllAvailable()
      query.asInstanceOf[StreamingQueryWrapper].streamingQuery.lastExecution
    } finally {
      query.stop()
    }
  }

  /** Returns the relation that the last micro-batch of `df` reads. */
  private def lastMicroBatchRelation(
      df: DataFrame,
      checkpointLocation: Option[String] = None): StreamingDataSourceV2ScanRelation = {
    val execution = runMicroBatches(df, checkpointLocation)
    val leaves = execution.optimizedPlan.collect {
      case relation: StreamingDataSourceV2ScanRelation => relation
    }
    assert(leaves.size == 1, execution.optimizedPlan)
    leaves.head
  }

  private def sizeAndRows(relation: LogicalPlan) =
    (relation.stats.sizeInBytes, relation.stats.rowCount)

  for (microBatchStatistics <- Seq("none", "known", "empty", "null")) {
    test(s"statistics of a micro-batch - micro-batch statistics: $microBatchStatistics") {
      val relation = lastMicroBatchRelation(readStream(microBatchStatistics))
      if (microBatchStatistics == "known") {
        // The micro-batch reads the offsets (0, 10].
        assert(sizeAndRows(relation) == (BigInt(1000), Some(BigInt(10))))
        // Without an end offset, as in a real-time query, Spark uses the statistics of the scan.
        assert(sizeAndRows(relation.copy(endOffset = None)) == scanStatistics)
      } else {
        assert(sizeAndRows(relation) == scanStatistics)
      }
    }
  }

  test("each micro-batch reports the statistics of its own offset range, also after a restart") {
    withTempDir { checkpointDir =>
      val checkpoint = Some(checkpointDir.getCanonicalPath)
      val first = lastMicroBatchRelation(readStream("known", latestOffset = 10), checkpoint)
      assert(sizeAndRows(first) == (BigInt(1000), Some(BigInt(10))))
      // The restarted query reads the start offset of its micro-batch, 10, from the checkpoint.
      val second = lastMicroBatchRelation(readStream("known", latestOffset = 30), checkpoint)
      assert(sizeAndRows(second) == (BigInt(2000), Some(BigInt(20))))
    }
  }

  for (microBatchStatistics <- Seq("none", "known")) {
    test("a micro-batch that fits under the broadcast threshold is broadcast in a join - " +
        s"micro-batch statistics: $microBatchStatistics") {
      // Without AQE, the join strategy in the executed plan comes from the statistics of the
      // logical plan, which is what this test checks.
      withSQLConf(
          SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
          SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "10000") {
        // The static side is larger than the threshold, so only the micro-batch can be broadcast.
        val static = spark.range(100000).toDF("id")
        val execution = runMicroBatches(readStream(microBatchStatistics).join(static, "id"))
        val plan = execution.executedPlan
        if (microBatchStatistics == "known") {
          val joins = plan.collect { case join: BroadcastHashJoinExec => join }
          assert(joins.size == 1 && joins.head.buildSide == BuildLeft, plan)
        } else {
          assert(plan.collect { case join: SortMergeJoinExec => join }.size == 1, plan)
        }
      }
    }
  }
}
