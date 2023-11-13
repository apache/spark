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

package org.apache.spark.sql.streaming

import java.io.File

import org.apache.commons.io.FileUtils

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.plans.physical.UnspecifiedDistribution
import org.apache.spark.sql.execution.aggregate.BaseAggregateExec
import org.apache.spark.sql.execution.streaming.{MemoryStream, SessionWindowStateStoreRestoreExec, SessionWindowStateStoreSaveExec}
import org.apache.spark.sql.functions.{count, session_window}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming.util.StatefulOpClusteredDistributionTestHelper
import org.apache.spark.tags.SlowSQLTest
import org.apache.spark.util.Utils

@SlowSQLTest
class StreamingSessionWindowDistributionSuite extends StreamTest
  with StatefulOpClusteredDistributionTestHelper with Logging {

  import testImplicits._

  test("SPARK-38204: session window aggregation should require StatefulOpClusteredDistribution " +
    "from children") {

    withSQLConf(
      // exclude partial merging session to simplify test
      SQLConf.STREAMING_SESSION_WINDOW_MERGE_SESSIONS_IN_LOCAL_PARTITION.key -> "false") {

      val inputData = MemoryStream[(String, String, Long)]

      // Split the lines into words, treat words as sessionId of events
      val events = inputData.toDF()
        .select($"_1".as("value"), $"_2".as("userId"), $"_3".as("timestamp"))
        .withColumn("eventTime", $"timestamp".cast("timestamp"))
        .withWatermark("eventTime", "30 seconds")
        .selectExpr("explode(split(value, ' ')) AS sessionId", "userId", "eventTime")

      val sessionUpdates = events
        .repartition($"userId")
        .groupBy(session_window($"eventTime", "10 seconds") as Symbol("session"),
          $"sessionId", $"userId")
        .agg(count("*").as("numEvents"))
        .selectExpr("sessionId", "userId", "CAST(session.start AS LONG)",
          "CAST(session.end AS LONG)",
          "CAST(session.end AS LONG) - CAST(session.start AS LONG) AS durationMs",
          "numEvents")

      testStream(sessionUpdates, OutputMode.Append())(
        AddData(inputData,
          ("hello world spark streaming", "key1", 40L),
          ("world hello structured streaming", "key2", 41L)
        ),

        // skip checking the result, since we focus to verify the physical plan
        ProcessAllAvailable(),
        Execute { query =>
          val numPartitions = query.lastExecution.numStateStores

          val operators = query.lastExecution.executedPlan.collect {
            case s: SessionWindowStateStoreRestoreExec => s
            case s: SessionWindowStateStoreSaveExec => s
          }

          assert(operators.nonEmpty)
          operators.foreach { stateOp =>
            assert(requireStatefulOpClusteredDistribution(stateOp, Seq(Seq("sessionId", "userId")),
              numPartitions))
            assert(hasDesiredHashPartitioningInChildren(stateOp, Seq(Seq("sessionId", "userId")),
              numPartitions))
          }

          // Verify aggregations in between, except partial aggregation.
          // This includes MergingSessionsExec.
          val allAggregateExecs = query.lastExecution.executedPlan.collect {
            case a: BaseAggregateExec => a
          }

          val aggregateExecsWithoutPartialAgg = allAggregateExecs.filter {
            _.requiredChildDistribution.head != UnspecifiedDistribution
          }

          // We expect single partial aggregation since we disable partial merging sessions.
          // Remaining agg execs should have child producing expected output partitioning.
          assert(allAggregateExecs.length - 1 === aggregateExecsWithoutPartialAgg.length)

          // For aggregate execs, we make sure output partitioning of the children is same as
          // we expect, HashPartitioning with clustering keys & number of partitions.
          aggregateExecsWithoutPartialAgg.foreach { aggr =>
            assert(hasDesiredHashPartitioningInChildren(aggr, Seq(Seq("sessionId", "userId")),
              numPartitions))
          }
        }
      )
    }
  }

  test("SPARK-38204: session window aggregation should require ClusteredDistribution " +
    "from children if the query starts from checkpoint in 3.2") {

    withSQLConf(
      // exclude partial merging session to simplify test
      SQLConf.STREAMING_SESSION_WINDOW_MERGE_SESSIONS_IN_LOCAL_PARTITION.key -> "false") {

      val inputData = MemoryStream[(String, String, Long)]

      // Split the lines into words, treat words as sessionId of events
      val events = inputData.toDF()
        .select($"_1".as("value"), $"_2".as("userId"), $"_3".as("timestamp"))
        .withColumn("eventTime", $"timestamp".cast("timestamp"))
        .withWatermark("eventTime", "30 seconds")
        .selectExpr("explode(split(value, ' ')) AS sessionId", "userId", "eventTime")

      val sessionUpdates = events
        .repartition($"userId")
        .groupBy(session_window($"eventTime", "10 seconds") as Symbol("session"),
          $"sessionId", $"userId")
        .agg(count("*").as("numEvents"))
        .selectExpr("sessionId", "userId", "CAST(session.start AS LONG)",
          "CAST(session.end AS LONG)",
          "CAST(session.end AS LONG) - CAST(session.start AS LONG) AS durationMs",
          "numEvents")

      val resourceUri = this.getClass.getResource(
        "/structured-streaming/checkpoint-version-3.2.0-session-window-with-repartition/").toURI

      val checkpointDir = Utils.createTempDir().getCanonicalFile
      // Copy the checkpoint to a temp dir to prevent changes to the original.
      // Not doing this will lead to the test passing on the first run, but fail subsequent runs.
      FileUtils.copyDirectory(new File(resourceUri), checkpointDir)

      inputData.addData(
        ("hello world spark streaming", "key1", 40L),
        ("world hello structured streaming", "key2", 41L))

      testStream(sessionUpdates, OutputMode.Append())(
        StartStream(checkpointLocation = checkpointDir.getAbsolutePath,
          additionalConfs = Map(SQLConf.STATEFUL_OPERATOR_USE_STRICT_DISTRIBUTION.key -> "true")),

        // scalastyle:off line.size.limit
        /*
        Note: The checkpoint was generated using the following input in Spark version 3.2.0
        AddData(inputData,
          ("hello world spark streaming", "key1", 40L),
          ("world hello structured streaming", "key2", 41L)),
        // skip checking the result, since we focus to verify the physical plan
        ProcessAllAvailable()

        Note2: The following is the physical plan of the query in Spark version 3.2.0.

        WriteToDataSourceV2 org.apache.spark.sql.execution.streaming.sources.MicroBatchWrite@6649ee50, org.apache.spark.sql.execution.datasources.v2.DataSourceV2Strategy$$Lambda$2209/0x0000000840ebd440@f9f45c6
        +- *(3) HashAggregate(keys=[session_window#33-T30000ms, sessionId#21, userId#10], functions=[count(1)], output=[sessionId#21, userId#10, CAST(session.start AS BIGINT)#43L, CAST(session.end AS BIGINT)#44L, durationMs#38L, numEvents#32L])
           +- SessionWindowStateStoreSave [sessionId#21, userId#10], session_window#33: struct<start: timestamp, end: timestamp>, state info [ checkpoint = file:/tmp/spark-f8a951f5-c7c1-43b0-883d-9b893d672ee5/state, runId = 92681f36-1f0d-434e-8492-897e4e988bb3, opId = 0, ver = 1, numPartitions = 5], Append, 11000, 1
              +- MergingSessions List(ClusteredDistribution(ArrayBuffer(sessionId#21, userId#10),None)), [session_window#33-T30000ms, sessionId#21, userId#10], session_window#33: struct<start: timestamp, end: timestamp>, [merge_count(1)], [count(1)#30L], 3, [session_window#33-T30000ms, sessionId#21, userId#10, count#58L]
                 +- SessionWindowStateStoreRestore [sessionId#21, userId#10], session_window#33: struct<start: timestamp, end: timestamp>, state info [ checkpoint = file:/tmp/spark-f8a951f5-c7c1-43b0-883d-9b893d672ee5/state, runId = 92681f36-1f0d-434e-8492-897e4e988bb3, opId = 0, ver = 1, numPartitions = 5], 11000, 1
                    +- *(2) Sort [sessionId#21 ASC NULLS FIRST, userId#10 ASC NULLS FIRST, session_window#33-T30000ms ASC NULLS FIRST], false, 0
                       +- *(2) HashAggregate(keys=[session_window#33-T30000ms, sessionId#21, userId#10], functions=[partial_count(1)], output=[session_window#33-T30000ms, sessionId#21, userId#10, count#58L])
                          +- *(2) Project [named_struct(start, precisetimestampconversion(precisetimestampconversion(eventTime#15-T30000ms, TimestampType, LongType), LongType, TimestampType), end, precisetimestampconversion(precisetimestampconversion(eventTime#15-T30000ms + 10 seconds, TimestampType, LongType), LongType, TimestampType)) AS session_window#33-T30000ms, sessionId#21, userId#10]
                             +- Exchange hashpartitioning(userId#10, 5), REPARTITION_BY_COL, [id=#372]
                                +- *(1) Project [sessionId#21, userId#10, eventTime#15-T30000ms]
                                   +- *(1) Generate explode(split(value#9,  , -1)), [userId#10, eventTime#15-T30000ms], false, [sessionId#21]
                                      +- *(1) Filter (precisetimestampconversion(precisetimestampconversion(eventTime#15-T30000ms + 10 seconds, TimestampType, LongType), LongType, TimestampType) > precisetimestampconversion(precisetimestampconversion(eventTime#15-T30000ms, TimestampType, LongType), LongType, TimestampType))
                                         +- EventTimeWatermark eventTime#15: timestamp, 30 seconds
                                            +- LocalTableScan <empty>, [value#9, userId#10, eventTime#15]
        */
        // scalastyle:on line.size.limit

        AddData(inputData, ("spark streaming", "key1", 25L)),
        // skip checking the result, since we focus to verify the physical plan
        ProcessAllAvailable(),

        Execute { query =>
          val numPartitions = query.lastExecution.numStateStores

          val operators = query.lastExecution.executedPlan.collect {
            case s: SessionWindowStateStoreRestoreExec => s
            case s: SessionWindowStateStoreSaveExec => s
          }

          assert(operators.nonEmpty)
          operators.foreach { stateOp =>
            assert(requireClusteredDistribution(stateOp, Seq(Seq("sessionId", "userId")),
              Some(numPartitions)))
            assert(hasDesiredHashPartitioningInChildren(stateOp, Seq(Seq("userId")),
              numPartitions))
          }

          // Verify aggregations in between, except partial aggregation.
          // This includes MergingSessionsExec.
          val allAggregateExecs = query.lastExecution.executedPlan.collect {
            case a: BaseAggregateExec => a
          }

          val aggregateExecsWithoutPartialAgg = allAggregateExecs.filter {
            _.requiredChildDistribution.head != UnspecifiedDistribution
          }

          // We expect single partial aggregation since we disable partial merging sessions.
          // Remaining agg execs should have child producing expected output partitioning.
          assert(allAggregateExecs.length - 1 === aggregateExecsWithoutPartialAgg.length)

          // For aggregate execs, we make sure output partitioning of the children is same as
          // we expect, HashPartitioning with sub-clustering keys & number of partitions.
          aggregateExecsWithoutPartialAgg.foreach { aggr =>
            assert(hasDesiredHashPartitioningInChildren(aggr, Seq(Seq("userId")),
              numPartitions))
          }
        }
      )
    }
  }
}
