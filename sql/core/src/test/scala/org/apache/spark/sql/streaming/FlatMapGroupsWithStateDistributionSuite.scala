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

import org.apache.spark.sql.catalyst.streaming.InternalOutputModes.Update
import org.apache.spark.sql.execution.streaming.{FlatMapGroupsWithStateExec, MemoryStream}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming.GroupStateTimeout.ProcessingTimeTimeout
import org.apache.spark.sql.streaming.util.{StatefulOpClusteredDistributionTestHelper, StreamManualClock}
import org.apache.spark.util.Utils

class FlatMapGroupsWithStateDistributionSuite extends StreamTest
  with StatefulOpClusteredDistributionTestHelper {

  import testImplicits._

  test("SPARK-38204: flatMapGroupsWithState should require StatefulOpClusteredDistribution " +
    "from children - with initial state") {
    // function will return -1 on timeout and returns count of the state otherwise
    val stateFunc =
      (key: (String, String), values: Iterator[(String, String, Long)],
       state: GroupState[RunningCount]) => {

        if (state.hasTimedOut) {
          state.remove()
          Iterator((key, "-1"))
        } else {
          val count = state.getOption.map(_.count).getOrElse(0L) + values.size
          state.update(RunningCount(count))
          state.setTimeoutDuration("10 seconds")
          Iterator((key, count.toString))
        }
      }

    val clock = new StreamManualClock
    val inputData = MemoryStream[(String, String, Long)]
    val initialState = Seq(("c", "c", new RunningCount(2)))
      .toDS()
      .repartition($"_2")
      .groupByKey(a => (a._1, a._2)).mapValues(_._3)
    val result =
      inputData.toDF().toDF("key1", "key2", "time")
        .selectExpr("key1", "key2", "timestamp_seconds(time) as timestamp")
        .withWatermark("timestamp", "10 second")
        .as[(String, String, Long)]
        .repartition($"_1")
        .groupByKey(x => (x._1, x._2))
        .flatMapGroupsWithState(Update, ProcessingTimeTimeout(), initialState)(stateFunc)
        .select($"_1._1".as("key1"), $"_1._2".as("key2"), $"_2".as("cnt"))

    testStream(result, Update)(
      StartStream(Trigger.ProcessingTime("1 second"), triggerClock = clock),
      AddData(inputData, ("a", "a", 1L)),
      AdvanceManualClock(1 * 1000), // a and c are processed here for the first time.
      CheckNewAnswer(("a", "a", "1"), ("c", "c", "2")),
      Execute { query =>
        val numPartitions = query.lastExecution.numStateStores

        val flatMapGroupsWithStateExecs = query.lastExecution.executedPlan.collect {
          case f: FlatMapGroupsWithStateExec => f
        }

        assert(flatMapGroupsWithStateExecs.length === 1)
        assert(requireStatefulOpClusteredDistribution(
          flatMapGroupsWithStateExecs.head, Seq(Seq("_1", "_2"), Seq("_1", "_2")), numPartitions))
        assert(hasDesiredHashPartitioningInChildren(
          flatMapGroupsWithStateExecs.head, Seq(Seq("_1", "_2"), Seq("_1", "_2")), numPartitions))
      }
    )
  }

  test("SPARK-38204: flatMapGroupsWithState should require StatefulOpClusteredDistribution " +
    "from children - without initial state") {
    // function will return -1 on timeout and returns count of the state otherwise
    val stateFunc =
      (key: (String, String), values: Iterator[(String, String, Long)],
       state: GroupState[RunningCount]) => {

        if (state.hasTimedOut) {
          state.remove()
          Iterator((key, "-1"))
        } else {
          val count = state.getOption.map(_.count).getOrElse(0L) + values.size
          state.update(RunningCount(count))
          state.setTimeoutDuration("10 seconds")
          Iterator((key, count.toString))
        }
      }

    val clock = new StreamManualClock
    val inputData = MemoryStream[(String, String, Long)]
    val result =
      inputData.toDF().toDF("key1", "key2", "time")
        .selectExpr("key1", "key2", "timestamp_seconds(time) as timestamp")
        .withWatermark("timestamp", "10 second")
        .as[(String, String, Long)]
        .repartition($"_1")
        .groupByKey(x => (x._1, x._2))
        .flatMapGroupsWithState(Update, ProcessingTimeTimeout())(stateFunc)
        .select($"_1._1".as("key1"), $"_1._2".as("key2"), $"_2".as("cnt"))

    testStream(result, Update)(
      StartStream(Trigger.ProcessingTime("1 second"), triggerClock = clock),
      AddData(inputData, ("a", "a", 1L)),
      AdvanceManualClock(1 * 1000), // a is processed here for the first time.
      CheckNewAnswer(("a", "a", "1")),
      Execute { query =>
        val numPartitions = query.lastExecution.numStateStores

        val flatMapGroupsWithStateExecs = query.lastExecution.executedPlan.collect {
          case f: FlatMapGroupsWithStateExec => f
        }

        assert(flatMapGroupsWithStateExecs.length === 1)
        assert(requireStatefulOpClusteredDistribution(
          flatMapGroupsWithStateExecs.head, Seq(Seq("_1", "_2"), Seq("_1", "_2")), numPartitions))
        assert(hasDesiredHashPartitioningInChildren(
          flatMapGroupsWithStateExecs.head, Seq(Seq("_1", "_2"), Seq("_1", "_2")), numPartitions))
      }
    )
  }

  test("SPARK-38204: flatMapGroupsWithState should require ClusteredDistribution " +
    "from children if the query starts from checkpoint in 3.2.x - with initial state") {
    // function will return -1 on timeout and returns count of the state otherwise
    val stateFunc =
      (key: (String, String), values: Iterator[(String, String, Long)],
       state: GroupState[RunningCount]) => {

        if (state.hasTimedOut) {
          state.remove()
          Iterator((key, "-1"))
        } else {
          val count = state.getOption.map(_.count).getOrElse(0L) + values.size
          state.update(RunningCount(count))
          state.setTimeoutDuration("10 seconds")
          Iterator((key, count.toString))
        }
      }

    val clock = new StreamManualClock
    val inputData = MemoryStream[(String, String, Long)]
    val initialState = Seq(("c", "c", new RunningCount(2)))
      .toDS()
      .repartition($"_2")
      .groupByKey(a => (a._1, a._2)).mapValues(_._3)
    val result =
      inputData.toDF().toDF("key1", "key2", "time")
        .selectExpr("key1", "key2", "timestamp_seconds(time) as timestamp")
        .withWatermark("timestamp", "10 second")
        .as[(String, String, Long)]
        .repartition($"_1")
        .groupByKey(x => (x._1, x._2))
        .flatMapGroupsWithState(Update, ProcessingTimeTimeout(), initialState)(stateFunc)
        .select($"_1._1".as("key1"), $"_1._2".as("key2"), $"_2".as("cnt"))

    val resourceUri = this.getClass.getResource(
      "/structured-streaming/checkpoint-version-3.2.0-flatmapgroupswithstate1-repartition/").toURI

    val checkpointDir = Utils.createTempDir().getCanonicalFile
    // Copy the checkpoint to a temp dir to prevent changes to the original.
    // Not doing this will lead to the test passing on the first run, but fail subsequent runs.
    FileUtils.copyDirectory(new File(resourceUri), checkpointDir)

    inputData.addData(("a", "a", 1L))

    testStream(result, Update)(
      StartStream(Trigger.ProcessingTime("1 second"),
        checkpointLocation = checkpointDir.getAbsolutePath,
        triggerClock = clock,
        additionalConfs = Map(SQLConf.STATEFUL_OPERATOR_USE_STRICT_DISTRIBUTION.key -> "true")),

      // scalastyle:off line.size.limit
      /*
        Note: The checkpoint was generated using the following input in Spark version 3.2.0
        AddData(inputData, ("a", "a", 1L)),
        AdvanceManualClock(1 * 1000), // a and c are processed here for the first time.
        CheckNewAnswer(("a", "a", "1"), ("c", "c", "2")),

        Note2: The following is the physical plan of the query in Spark version 3.2.0.

        WriteToDataSourceV2 org.apache.spark.sql.execution.streaming.sources.MicroBatchWrite@253dd5ad, org.apache.spark.sql.execution.datasources.v2.DataSourceV2Strategy$$Lambda$2214/0x0000000840ead440@6ede0d42
        +- *(6) Project [_1#58._1 AS key1#63, _1#58._2 AS key2#64, _2#59 AS cnt#65]
           +- *(6) SerializeFromObject [if (isnull(knownnotnull(assertnotnull(input[0, scala.Tuple2, true]))._1)) null else named_struct(_1, staticinvoke(class org.apache.spark.unsafe.types.UTF8String, StringType, fromString, knownnotnull(knownnotnull(assertnotnull(input[0, scala.Tuple2, true]))._1)._1, true, false), _2, staticinvoke(class org.apache.spark.unsafe.types.UTF8String, StringType, fromString, knownnotnull(knownnotnull(assertnotnull(input[0, scala.Tuple2, true]))._1)._2, true, false)) AS _1#58, staticinvoke(class org.apache.spark.unsafe.types.UTF8String, StringType, fromString, knownnotnull(assertnotnull(input[0, scala.Tuple2, true]))._2, true, false) AS _2#59]
              +- FlatMapGroupsWithState org.apache.spark.sql.streaming.FlatMapGroupsWithStateSuite$$Lambda$1067/0x0000000840770440@3f2e51a9, newInstance(class scala.Tuple2), newInstance(class scala.Tuple3), newInstance(class org.apache.spark.sql.streaming.RunningCount), [_1#52, _2#53], [_1#22, _2#23], [key1#29, key2#30, timestamp#35-T10000ms], [count#25L], obj#57: scala.Tuple2, state info [ checkpoint = file:/tmp/streaming.metadata-d4f0d156-78b5-4129-97fb-361241ab03d8/state, runId = eb107298-692d-4336-bb76-6b11b34a0753, opId = 0, ver = 0, numPartitions = 5], class[count[0]: bigint], 2, Update, ProcessingTimeTimeout, 1000, 0, true
                 :- *(3) Sort [_1#52 ASC NULLS FIRST, _2#53 ASC NULLS FIRST], false, 0
                 :  +- Exchange hashpartitioning(_1#52, _2#53, 5), ENSURE_REQUIREMENTS, [id=#78]
                 :     +- AppendColumns org.apache.spark.sql.streaming.FlatMapGroupsWithStateSuite$$Lambda$1751/0x0000000840ccc040@41d4c0d8, newInstance(class scala.Tuple3), [staticinvoke(class org.apache.spark.unsafe.types.UTF8String, StringType, fromString, knownnotnull(assertnotnull(input[0, scala.Tuple2, true]))._1, true, false) AS _1#52, staticinvoke(class org.apache.spark.unsafe.types.UTF8String, StringType, fromString, knownnotnull(assertnotnull(input[0, scala.Tuple2, true]))._2, true, false) AS _2#53]
                 :        +- *(2) Project [key1#29, key2#30, timestamp#35-T10000ms]
                 :           +- Exchange hashpartitioning(_1#3, 5), REPARTITION_BY_COL, [id=#73]
                 :              +- EventTimeWatermark timestamp#35: timestamp, 10 seconds
                 :                 +- *(1) Project [_1#3 AS key1#29, _2#4 AS key2#30, timestamp_seconds(_3#5L) AS timestamp#35, _1#3]
                 :                    +- MicroBatchScan[_1#3, _2#4, _3#5L] MemoryStreamDataSource
                 +- *(5) Sort [_1#22 ASC NULLS FIRST, _2#23 ASC NULLS FIRST], false, 0
                    +- Exchange hashpartitioning(_1#22, _2#23, 5), ENSURE_REQUIREMENTS, [id=#85]
                       +- *(4) Project [count#25L, _1#22, _2#23]
                          +- AppendColumns org.apache.spark.sql.streaming.FlatMapGroupsWithStateSuite$$Lambda$1686/0x0000000840c9b840@6bb881d0, newInstance(class scala.Tuple3), [knownnotnull(assertnotnull(input[0, org.apache.spark.sql.streaming.RunningCount, true])).count AS count#25L]
                             +- AppendColumns org.apache.spark.sql.streaming.FlatMapGroupsWithStateSuite$$Lambda$1681/0x0000000840c98840@11355c7b, newInstance(class scala.Tuple3), [staticinvoke(class org.apache.spark.unsafe.types.UTF8String, StringType, fromString, knownnotnull(assertnotnull(input[0, scala.Tuple2, true]))._1, true, false) AS _1#22, staticinvoke(class org.apache.spark.unsafe.types.UTF8String, StringType, fromString, knownnotnull(assertnotnull(input[0, scala.Tuple2, true]))._2, true, false) AS _2#23]
                                +- Exchange hashpartitioning(_1#9, 5), REPARTITION_BY_COL, [id=#43]
                                   +- LocalTableScan [_1#9, _2#10, _3#11]
       */
      // scalastyle:on line.size.limit

      AddData(inputData, ("a", "b", 1L)),
      AdvanceManualClock(1 * 1000),
      CheckNewAnswer(("a", "b", "1")),

      Execute { query =>
        val numPartitions = query.lastExecution.numStateStores

        val flatMapGroupsWithStateExecs = query.lastExecution.executedPlan.collect {
          case f: FlatMapGroupsWithStateExec => f
        }

        assert(flatMapGroupsWithStateExecs.length === 1)
        assert(requireClusteredDistribution(flatMapGroupsWithStateExecs.head,
          Seq(Seq("_1", "_2"), Seq("_1", "_2")), Some(numPartitions)))
        assert(hasDesiredHashPartitioningInChildren(
          flatMapGroupsWithStateExecs.head, Seq(Seq("_1", "_2"), Seq("_1", "_2")), numPartitions))
      }
    )
  }

  test("SPARK-38204: flatMapGroupsWithState should require ClusteredDistribution " +
    "from children if the query starts from checkpoint in 3.2.x - without initial state") {
    // function will return -1 on timeout and returns count of the state otherwise
    val stateFunc =
      (key: (String, String), values: Iterator[(String, String, Long)],
       state: GroupState[RunningCount]) => {

        if (state.hasTimedOut) {
          state.remove()
          Iterator((key, "-1"))
        } else {
          val count = state.getOption.map(_.count).getOrElse(0L) + values.size
          state.update(RunningCount(count))
          state.setTimeoutDuration("10 seconds")
          Iterator((key, count.toString))
        }
      }

    val clock = new StreamManualClock
    val inputData = MemoryStream[(String, String, Long)]
    val result =
      inputData.toDF().toDF("key1", "key2", "time")
        .selectExpr("key1", "key2", "timestamp_seconds(time) as timestamp")
        .withWatermark("timestamp", "10 second")
        .as[(String, String, Long)]
        .repartition($"_1")
        .groupByKey(x => (x._1, x._2))
        .flatMapGroupsWithState(Update, ProcessingTimeTimeout())(stateFunc)
        .select($"_1._1".as("key1"), $"_1._2".as("key2"), $"_2".as("cnt"))

    val resourceUri = this.getClass.getResource(
      "/structured-streaming/checkpoint-version-3.2.0-flatmapgroupswithstate2-repartition/").toURI

    val checkpointDir = Utils.createTempDir().getCanonicalFile
    // Copy the checkpoint to a temp dir to prevent changes to the original.
    // Not doing this will lead to the test passing on the first run, but fail subsequent runs.
    FileUtils.copyDirectory(new File(resourceUri), checkpointDir)

    inputData.addData(("a", "a", 1L))

    testStream(result, Update)(
      StartStream(Trigger.ProcessingTime("1 second"),
        checkpointLocation = checkpointDir.getAbsolutePath,
        triggerClock = clock,
        additionalConfs = Map(SQLConf.STATEFUL_OPERATOR_USE_STRICT_DISTRIBUTION.key -> "true")),

      // scalastyle:off line.size.limit
      /*
        Note: The checkpoint was generated using the following input in Spark version 3.2.0
        AddData(inputData, ("a", "a", 1L)),
        AdvanceManualClock(1 * 1000), // a is processed here for the first time.
        CheckNewAnswer(("a", "a", "1")),

        Note2: The following is the physical plan of the query in Spark version 3.2.0 (convenience for checking backward compatibility)
        WriteToDataSourceV2 org.apache.spark.sql.execution.streaming.sources.MicroBatchWrite@20732f1b, org.apache.spark.sql.execution.datasources.v2.DataSourceV2Strategy$$Lambda$2205/0x0000000840ea5440@48e6c016
        +- *(5) Project [_1#39._1 AS key1#44, _1#39._2 AS key2#45, _2#40 AS cnt#46]
           +- *(5) SerializeFromObject [if (isnull(knownnotnull(assertnotnull(input[0, scala.Tuple2, true]))._1)) null else named_struct(_1, staticinvoke(class org.apache.spark.unsafe.types.UTF8String, StringType, fromString, knownnotnull(knownnotnull(assertnotnull(input[0, scala.Tuple2, true]))._1)._1, true, false), _2, staticinvoke(class org.apache.spark.unsafe.types.UTF8String, StringType, fromString, knownnotnull(knownnotnull(assertnotnull(input[0, scala.Tuple2, true]))._1)._2, true, false)) AS _1#39, staticinvoke(class org.apache.spark.unsafe.types.UTF8String, StringType, fromString, knownnotnull(assertnotnull(input[0, scala.Tuple2, true]))._2, true, false) AS _2#40]
              +- FlatMapGroupsWithState org.apache.spark.sql.streaming.FlatMapGroupsWithStateSuite$$Lambda$1065/0x0000000840770040@240e41f8, newInstance(class scala.Tuple2), newInstance(class scala.Tuple3), newInstance(class scala.Tuple2), [_1#32, _2#33], [_1#32, _2#33], [key1#9, key2#10, timestamp#15-T10000ms], [key1#9, key2#10, timestamp#15-T10000ms], obj#37: scala.Tuple2, state info [ checkpoint = file:/tmp/spark-6619d285-b0ca-42ab-8284-723a564e13b6/state, runId = b3383a6c-9976-483c-a463-7fc9e9ae3e1a, opId = 0, ver = 0, numPartitions = 5], class[count[0]: bigint], 2, Update, ProcessingTimeTimeout, 1000, 0, false
                 :- *(3) Sort [_1#32 ASC NULLS FIRST, _2#33 ASC NULLS FIRST], false, 0
                 :  +- Exchange hashpartitioning(_1#32, _2#33, 5), ENSURE_REQUIREMENTS, [id=#62]
                 :     +- AppendColumns org.apache.spark.sql.streaming.FlatMapGroupsWithStateSuite$$Lambda$1709/0x0000000840ca7040@351810cb, newInstance(class scala.Tuple3), [staticinvoke(class org.apache.spark.unsafe.types.UTF8String, StringType, fromString, knownnotnull(assertnotnull(input[0, scala.Tuple2, true]))._1, true, false) AS _1#32, staticinvoke(class org.apache.spark.unsafe.types.UTF8String, StringType, fromString, knownnotnull(assertnotnull(input[0, scala.Tuple2, true]))._2, true, false) AS _2#33]
                 :        +- *(2) Project [key1#9, key2#10, timestamp#15-T10000ms]
                 :           +- Exchange hashpartitioning(_1#3, 5), REPARTITION_BY_COL, [id=#57]
                 :              +- EventTimeWatermark timestamp#15: timestamp, 10 seconds
                 :                 +- *(1) Project [_1#3 AS key1#9, _2#4 AS key2#10, timestamp_seconds(_3#5L) AS timestamp#15, _1#3]
                 :                    +- MicroBatchScan[_1#3, _2#4, _3#5L] MemoryStreamDataSource
                 +- *(4) !Sort [_1#32 ASC NULLS FIRST, _2#33 ASC NULLS FIRST], false, 0
                    +- !Exchange hashpartitioning(_1#32, _2#33, 5), ENSURE_REQUIREMENTS, [id=#46]
                       +- LocalTableScan <empty>, [count#38L]
       */
      // scalastyle:on line.size.limit

      AddData(inputData, ("a", "b", 1L)),
      AdvanceManualClock(1 * 1000),
      CheckNewAnswer(("a", "b", "1")),

      Execute { query =>
        val numPartitions = query.lastExecution.numStateStores

        val flatMapGroupsWithStateExecs = query.lastExecution.executedPlan.collect {
          case f: FlatMapGroupsWithStateExec => f
        }

        assert(flatMapGroupsWithStateExecs.length === 1)
        assert(requireClusteredDistribution(flatMapGroupsWithStateExecs.head,
          Seq(Seq("_1", "_2"), Seq("_1", "_2")), Some(numPartitions)))
        assert(hasDesiredHashPartitioningInChildren(
          flatMapGroupsWithStateExecs.head, Seq(Seq("_1", "_2"), Seq("_1", "_2")), numPartitions))
      }
    )
  }

  test("SPARK-38204: flatMapGroupsWithState should require ClusteredDistribution " +
    "from children if the query starts from checkpoint in prior to 3.2") {
    // function will return -1 on timeout and returns count of the state otherwise
    val stateFunc =
      (key: (String, String), values: Iterator[(String, String, Long)],
       state: GroupState[RunningCount]) => {

        if (state.hasTimedOut) {
          state.remove()
          Iterator((key, "-1"))
        } else {
          val count = state.getOption.map(_.count).getOrElse(0L) + values.size
          state.update(RunningCount(count))
          state.setTimeoutDuration("10 seconds")
          Iterator((key, count.toString))
        }
      }

    val clock = new StreamManualClock
    val inputData = MemoryStream[(String, String, Long)]
    val result =
      inputData.toDF().toDF("key1", "key2", "time")
        .selectExpr("key1", "key2", "timestamp_seconds(time) as timestamp")
        .withWatermark("timestamp", "10 second")
        .as[(String, String, Long)]
        .repartition($"_1")
        .groupByKey(x => (x._1, x._2))
        .flatMapGroupsWithState(Update, ProcessingTimeTimeout())(stateFunc)
        .select($"_1._1".as("key1"), $"_1._2".as("key2"), $"_2".as("cnt"))

    val resourceUri = this.getClass.getResource(
      "/structured-streaming/checkpoint-version-3.1.0-flatmapgroupswithstate-repartition/").toURI

    val checkpointDir = Utils.createTempDir().getCanonicalFile
    // Copy the checkpoint to a temp dir to prevent changes to the original.
    // Not doing this will lead to the test passing on the first run, but fail subsequent runs.
    FileUtils.copyDirectory(new File(resourceUri), checkpointDir)

    inputData.addData(("a", "a", 1L))

    testStream(result, Update)(
      StartStream(Trigger.ProcessingTime("1 second"),
        checkpointLocation = checkpointDir.getAbsolutePath,
        triggerClock = clock,
        additionalConfs = Map(SQLConf.STATEFUL_OPERATOR_USE_STRICT_DISTRIBUTION.key -> "true")),

      // scalastyle:off line.size.limit
      /*
        Note: The checkpoint was generated using the following input in Spark version 3.2.0
        AddData(inputData, ("a", "a", 1L)),
        AdvanceManualClock(1 * 1000), // a is processed here for the first time.
        CheckNewAnswer(("a", "a", "1")),

        Note2: The following plans are the physical plans of the query in older Spark versions
        The physical plans around FlatMapGroupsWithStateExec are quite similar, especially
        shuffles being injected are same. That said, verifying with checkpoint being built with
        Spark 3.1.0 would verify the following versions as well.

        A. Spark 3.1.0
        WriteToDataSourceV2 org.apache.spark.sql.execution.streaming.sources.MicroBatchWrite@4505821b
        +- *(3) Project [_1#38._1 AS key1#43, _1#38._2 AS key2#44, _2#39 AS cnt#45]
           +- *(3) SerializeFromObject [if (isnull(knownnotnull(assertnotnull(input[0, scala.Tuple2, true]))._1)) null else named_struct(_1, staticinvoke(class org.apache.spark.unsafe.types.UTF8String, StringType, fromString, knownnotnull(knownnotnull(assertnotnull(input[0, scala.Tuple2, true]))._1)._1, true, false), _2, staticinvoke(class org.apache.spark.unsafe.types.UTF8String, StringType, fromString, knownnotnull(knownnotnull(assertnotnull(input[0, scala.Tuple2, true]))._1)._2, true, false)) AS _1#38, staticinvoke(class org.apache.spark.unsafe.types.UTF8String, StringType, fromString, knownnotnull(assertnotnull(input[0, scala.Tuple2, true]))._2, true, false) AS _2#39]
              +- FlatMapGroupsWithState org.apache.spark.sql.streaming.FlatMapGroupsWithStateSuite$$Lambda$1035/0x0000000840721840@64351072, newInstance(class scala.Tuple2), newInstance(class scala.Tuple3), [_1#32, _2#33], [key1#9, key2#10, timestamp#15-T10000ms], obj#37: scala.Tuple2, state info [ checkpoint = file:/tmp/spark-56397379-d014-48e0-a002-448c0621cfe8/state, runId = 4f9a129f-2b0c-4838-9d26-18171d94be7d, opId = 0, ver = 0, numPartitions = 5], class[count[0]: bigint], 2, Update, ProcessingTimeTimeout, 1000, 0
                 +- *(2) Sort [_1#32 ASC NULLS FIRST, _2#33 ASC NULLS FIRST], false, 0
                    +- Exchange hashpartitioning(_1#32, _2#33, 5), ENSURE_REQUIREMENTS, [id=#54]
                       +- AppendColumns org.apache.spark.sql.streaming.FlatMapGroupsWithStateSuite$$Lambda$1594/0x0000000840bc8840@857c80d, newInstance(class scala.Tuple3), [staticinvoke(class org.apache.spark.unsafe.types.UTF8String, StringType, fromString, knownnotnull(assertnotnull(input[0, scala.Tuple2, true]))._1, true, false) AS _1#32, staticinvoke(class org.apache.spark.unsafe.types.UTF8String, StringType, fromString, knownnotnull(assertnotnull(input[0, scala.Tuple2, true]))._2, true, false) AS _2#33]
                          +- Exchange hashpartitioning(key1#9, 5), REPARTITION, [id=#52]
                             +- EventTimeWatermark timestamp#15: timestamp, 10 seconds
                                +- *(1) Project [_1#3 AS key1#9, _2#4 AS key2#10, timestamp_seconds(_3#5L) AS timestamp#15]
                                   +- *(1) Project [_1#3, _2#4, _3#5L]
                                      +- MicroBatchScan[_1#3, _2#4, _3#5L] MemoryStreamDataSource

        B. Spark 3.0.0
        WriteToDataSourceV2 org.apache.spark.sql.execution.streaming.sources.MicroBatchWrite@32ae8206
        +- *(3) Project [_1#38._1 AS key1#43, _1#38._2 AS key2#44, _2#39 AS cnt#45]
           +- *(3) SerializeFromObject [if (isnull(knownnotnull(assertnotnull(input[0, scala.Tuple2, true]))._1)) null else named_struct(_1, staticinvoke(class org.apache.spark.unsafe.types.UTF8String, StringType, fromString, knownnotnull(knownnotnull(assertnotnull(input[0, scala.Tuple2, true]))._1)._1, true, false), _2, staticinvoke(class org.apache.spark.unsafe.types.UTF8String, StringType, fromString, knownnotnull(knownnotnull(assertnotnull(input[0, scala.Tuple2, true]))._1)._2, true, false)) AS _1#38, staticinvoke(class org.apache.spark.unsafe.types.UTF8String, StringType, fromString, knownnotnull(assertnotnull(input[0, scala.Tuple2, true]))._2, true, false) AS _2#39]
              +- FlatMapGroupsWithState org.apache.spark.sql.streaming.FlatMapGroupsWithStateSuite$$Lambda$972/0x0000000840721c40@3e8c825d, newInstance(class scala.Tuple2), newInstance(class scala.Tuple3), [_1#32, _2#33], [key1#9, key2#10, timestamp#15-T10000ms], obj#37: scala.Tuple2, state info [ checkpoint = file:/tmp/spark-dcd6753e-54c7-481c-aa21-f7fc677a29a4/state, runId = 4854d427-436c-4f4e-9e1d-577bcd9cc890, opId = 0, ver = 0, numPartitions = 5], class[count[0]: bigint], 2, Update, ProcessingTimeTimeout, 1000, 0
                 +- *(2) Sort [_1#32 ASC NULLS FIRST, _2#33 ASC NULLS FIRST], false, 0
                    +- Exchange hashpartitioning(_1#32, _2#33, 5), true, [id=#54]
                       +- AppendColumns org.apache.spark.sql.streaming.FlatMapGroupsWithStateSuite$$Lambda$1477/0x0000000840bb6040@627623e, newInstance(class scala.Tuple3), [staticinvoke(class org.apache.spark.unsafe.types.UTF8String, StringType, fromString, knownnotnull(assertnotnull(input[0, scala.Tuple2, true]))._1, true, false) AS _1#32, staticinvoke(class org.apache.spark.unsafe.types.UTF8String, StringType, fromString, knownnotnull(assertnotnull(input[0, scala.Tuple2, true]))._2, true, false) AS _2#33]
                          +- Exchange hashpartitioning(key1#9, 5), false, [id=#52]
                             +- EventTimeWatermark timestamp#15: timestamp, 10 seconds
                                +- *(1) Project [_1#3 AS key1#9, _2#4 AS key2#10, cast(_3#5L as timestamp) AS timestamp#15]
                                   +- *(1) Project [_1#3, _2#4, _3#5L]
                                      +- MicroBatchScan[_1#3, _2#4, _3#5L] MemoryStreamDataSource

        C. Spark 2.4.0
        *(3) Project [_1#32._1 AS key1#35, _1#32._2 AS key2#36, _2#33 AS cnt#37]
        +- *(3) SerializeFromObject [if (isnull(assertnotnull(input[0, scala.Tuple2, true])._1)) null else named_struct(_1, staticinvoke(class org.apache.spark.unsafe.types.UTF8String, StringType, fromString, assertnotnull(assertnotnull(input[0, scala.Tuple2, true])._1)._1, true, false), _2, staticinvoke(class org.apache.spark.unsafe.types.UTF8String, StringType, fromString, assertnotnull(assertnotnull(input[0, scala.Tuple2, true])._1)._2, true, false)) AS _1#32, staticinvoke(class org.apache.spark.unsafe.types.UTF8String, StringType, fromString, assertnotnull(input[0, scala.Tuple2, true])._2, true, false) AS _2#33]
           +- FlatMapGroupsWithState <function3>, newInstance(class scala.Tuple2), newInstance(class scala.Tuple3), [_1#26, _2#27], [key1#9, key2#10, timestamp#15-T10000ms], obj#31: scala.Tuple2, state info [ checkpoint = file:/tmp/spark-634482c9-a55a-4f4e-b352-babec98fb4fc/state, runId = dd65fff0-d901-4e0b-a1ad-8c09b69f33ba, opId = 0, ver = 0, numPartitions = 5], class[count[0]: bigint], 2, Update, ProcessingTimeTimeout, 1000, 0
              +- *(2) Sort [_1#26 ASC NULLS FIRST, _2#27 ASC NULLS FIRST], false, 0
                 +- Exchange hashpartitioning(_1#26, _2#27, 5)
                    +- AppendColumns <function1>, newInstance(class scala.Tuple3), [staticinvoke(class org.apache.spark.unsafe.types.UTF8String, StringType, fromString, assertnotnull(input[0, scala.Tuple2, true])._1, true, false) AS _1#26, staticinvoke(class org.apache.spark.unsafe.types.UTF8String, StringType, fromString, assertnotnull(input[0, scala.Tuple2, true])._2, true, false) AS _2#27]
                       +- Exchange hashpartitioning(key1#9, 5)
                          +- EventTimeWatermark timestamp#15: timestamp, interval 10 seconds
                             +- *(1) Project [_1#56 AS key1#9, _2#57 AS key2#10, cast(_3#58L as timestamp) AS timestamp#15]
                                +- *(1) Project [_1#56, _2#57, _3#58L]
                                   +- *(1) ScanV2 MemoryStreamDataSource$[_1#56, _2#57, _3#58L]
       */
      // scalastyle:on line.size.limit

      AddData(inputData, ("a", "b", 1L)),
      AdvanceManualClock(1 * 1000),
      CheckNewAnswer(("a", "b", "1")),

      Execute { query =>
        val numPartitions = query.lastExecution.numStateStores

        val flatMapGroupsWithStateExecs = query.lastExecution.executedPlan.collect {
          case f: FlatMapGroupsWithStateExec => f
        }

        assert(flatMapGroupsWithStateExecs.length === 1)
        assert(requireClusteredDistribution(flatMapGroupsWithStateExecs.head,
          Seq(Seq("_1", "_2"), Seq("_1", "_2")), Some(numPartitions)))
        assert(hasDesiredHashPartitioningInChildren(
          flatMapGroupsWithStateExecs.head, Seq(Seq("_1", "_2"), Seq("_1", "_2")), numPartitions))
      }
    )
  }
}
