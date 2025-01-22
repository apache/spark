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
import java.lang.{Integer => JInteger}
import java.sql.Timestamp
import java.util.{Locale, UUID}

import scala.util.Random

import org.apache.commons.io.FileUtils
import org.scalatest.BeforeAndAfter

import org.apache.spark.SparkUnsupportedOperationException
import org.apache.spark.scheduler.ExecutorCacheTaskLocation
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Expression}
import org.apache.spark.sql.catalyst.plans.physical.HashPartitioning
import org.apache.spark.sql.execution.datasources.v2.state.StateSourceOptions
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.execution.streaming.{MemoryStream, StatefulOperatorStateInfo, StreamingSymmetricHashJoinExec, StreamingSymmetricHashJoinHelper}
import org.apache.spark.sql.execution.streaming.state.{RocksDBStateStoreProvider, StateStore, StateStoreProviderId}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.tags.SlowSQLTest
import org.apache.spark.util.Utils

abstract class StreamingJoinSuite
  extends StreamTest with StateStoreMetricsTest with BeforeAndAfter {

  import testImplicits._

  before {
    SparkSession.setActiveSession(spark)  // set this before force initializing 'joinExec'
    spark.streams.stateStoreCoordinator   // initialize the lazy coordinator
  }

  after {
    StateStore.stop()
  }

  protected def setupStream(prefix: String, multiplier: Int): (MemoryStream[Int], DataFrame) = {
    val input = MemoryStream[Int]
    val df = input.toDF()
      .select(
        $"value" as "key",
        timestamp_seconds($"value")  as s"${prefix}Time",
        ($"value" * multiplier) as s"${prefix}Value")
      .withWatermark(s"${prefix}Time", "10 seconds")

    (input, df)
  }

  protected def setupWindowedJoin(joinType: String)
    : (MemoryStream[Int], MemoryStream[Int], DataFrame) = {

    val (input1, df1) = setupStream("left", 2)
    val (input2, df2) = setupStream("right", 3)
    val windowed1 = df1
      .select($"key", window($"leftTime", "10 second"), $"leftValue")
    val windowed2 = df2
      .select($"key", window($"rightTime", "10 second"), $"rightValue")
    val joined = windowed1.join(windowed2, Seq("key", "window"), joinType)
    val select = if (joinType == "left_semi") {
      joined.select($"key", $"window.end".cast("long"), $"leftValue")
    } else {
      joined.select($"key", $"window.end".cast("long"), $"leftValue",
        $"rightValue")
    }

    (input1, input2, select)
  }

  protected def setupWindowedJoinWithLeftCondition(joinType: String)
    : (MemoryStream[Int], MemoryStream[Int], DataFrame) = {

    val (leftInput, df1) = setupStream("left", 2)
    val (rightInput, df2) = setupStream("right", 3)
    // Use different schemas to ensure the null row is being generated from the correct side.
    val left = df1.select($"key", window($"leftTime", "10 second"),
      $"leftValue")
    val right = df2.select($"key", window($"rightTime", "10 second"),
      $"rightValue".cast("string"))

    val joined = left.join(
      right,
      left("key") === right("key")
        && left("window") === right("window")
        && $"leftValue" > 4,
      joinType)

    val select = if (joinType == "left_semi") {
      joined.select(left("key"), left("window.end").cast("long"), $"leftValue")
    } else if (joinType == "left_outer") {
      joined.select(left("key"), left("window.end").cast("long"), $"leftValue",
        $"rightValue")
    } else if (joinType == "right_outer") {
      joined.select(right("key"), right("window.end").cast("long"), $"leftValue",
        $"rightValue")
    } else {
      joined.select(left("key"), left("window.end").cast("long"), $"leftValue",
        right("key"), right("window.end").cast("long"), $"rightValue")
    }

    (leftInput, rightInput, select)
  }

  protected def setupWindowedJoinWithRightCondition(joinType: String)
    : (MemoryStream[Int], MemoryStream[Int], DataFrame) = {

    val (leftInput, df1) = setupStream("left", 2)
    val (rightInput, df2) = setupStream("right", 3)
    // Use different schemas to ensure the null row is being generated from the correct side.
    val left = df1.select($"key", window($"leftTime", "10 second"),
      $"leftValue")
    val right = df2.select($"key", window($"rightTime", "10 second"),
      $"rightValue".cast("string"))

    val joined = left.join(
      right,
      left("key") === right("key")
        && left("window") === right("window")
        && $"rightValue".cast("int") > 7,
      joinType)

    val select = if (joinType == "left_semi") {
      joined.select(left("key"), left("window.end").cast("long"), $"leftValue")
    } else if (joinType == "left_outer") {
      joined.select(left("key"), left("window.end").cast("long"), $"leftValue",
        $"rightValue")
    } else if (joinType == "right_outer") {
      joined.select(right("key"), right("window.end").cast("long"), $"leftValue",
        $"rightValue")
    } else {
      joined.select(left("key"), left("window.end").cast("long"), $"leftValue",
        right("key"), right("window.end").cast("long"), $"rightValue")
    }

    (leftInput, rightInput, select)
  }

  protected def setupJoinWithRangeCondition(
      joinType: String,
      watermark: String = "10 seconds",
      lowerBound: String = "interval 5 seconds",
      upperBound: String = "interval 5 seconds")
    : (MemoryStream[(Int, Int)], MemoryStream[(Int, Int)], DataFrame) = {

    val leftInput = MemoryStream[(Int, Int)]
    val rightInput = MemoryStream[(Int, Int)]

    val df1 = leftInput.toDF().toDF("leftKey", "time")
      .select($"leftKey", timestamp_seconds($"time") as "leftTime",
        ($"leftKey" * 2) as "leftValue")
      .withWatermark("leftTime", watermark)

    val df2 = rightInput.toDF().toDF("rightKey", "time")
      .select($"rightKey", timestamp_seconds($"time") as "rightTime",
        ($"rightKey" * 3) as "rightValue")
      .withWatermark("rightTime", watermark)

    val joined =
      df1.join(
        df2,
        expr("leftKey = rightKey AND " +
          s"leftTime BETWEEN rightTime - $lowerBound AND rightTime + $upperBound"),
        joinType)

    val select = if (joinType == "left_semi") {
      joined.select($"leftKey", $"leftTime".cast("int"))
    } else {
      joined.select($"leftKey", $"rightKey", $"leftTime".cast("int"),
        $"rightTime".cast("int"))
    }

    (leftInput, rightInput, select)
  }

  protected def setupSelfJoin(joinType: String)
    : (MemoryStream[(Int, Long)], DataFrame) = {

    val inputStream = MemoryStream[(Int, Long)]

    val df = inputStream.toDS()
      .select(col("_1").as("value"), timestamp_seconds($"_2").as("timestamp"))

    val leftStream = df.select(col("value").as("leftId"), col("timestamp").as("leftTime"))

    val rightStream = df
      // Introduce misses for ease of debugging
      .where(col("value") % 2 === 0)
      .select(col("value").as("rightId"), col("timestamp").as("rightTime"))

    val joined = leftStream
      .withWatermark("leftTime", "5 seconds")
      .join(
        rightStream.withWatermark("rightTime", "5 seconds"),
        expr("leftId = rightId AND rightTime >= leftTime AND " +
          "rightTime <= leftTime + interval 5 seconds"),
        joinType)

    val select = if (joinType == "left_semi") {
      joined.select(col("leftId"), col("leftTime").cast("int"))
    } else {
      joined.select(col("leftId"), col("leftTime").cast("int"),
        col("rightId"), col("rightTime").cast("int"))
    }

    (inputStream, select)
  }

  protected def assertStateStoreRows(
      opId: Long,
      joinSide: String,
      expectedRows: Seq[Row])(projFn: DataFrame => DataFrame): AssertOnQuery = Execute { q =>
    val checkpointLoc = q.resolvedCheckpointRoot

    // just make sure the query have no leftover data
    q.processAllAvailable()

    // By default, it reads the state store from latest committed batch.
    val stateStoreDf = spark.read.format("statestore")
      .option(StateSourceOptions.JOIN_SIDE, joinSide)
      .option(StateSourceOptions.PATH, checkpointLoc)
      .option(StateSourceOptions.OPERATOR_ID, opId)
      .load()

    val projectedDf = projFn(stateStoreDf)
    checkAnswer(projectedDf, expectedRows)
  }
}

@SlowSQLTest
class StreamingInnerJoinSuite extends StreamingJoinSuite {

  import testImplicits._
  test("stream stream inner join on non-time column") {
    val input1 = MemoryStream[Int]
    val input2 = MemoryStream[Int]

    val df1 = input1.toDF().select($"value" as "key", ($"value" * 2) as "leftValue")
    val df2 = input2.toDF().select($"value" as "key", ($"value" * 3) as "rightValue")
    val joined = df1.join(df2, "key")

    testStream(joined)(
      AddData(input1, 1),
      CheckAnswer(),
      AddData(input2, 1, 10),       // 1 arrived on input1 first, then input2, should join
      CheckNewAnswer((1, 2, 3)),
      AddData(input1, 10),          // 10 arrived on input2 first, then input1, should join
      CheckNewAnswer((10, 20, 30)),
      AddData(input2, 1),           // another 1 in input2 should join with 1 input1
      CheckNewAnswer((1, 2, 3)),
      StopStream,
      StartStream(),
      AddData(input1, 1), // multiple 1s should be kept in state causing multiple (1, 2, 3)
      CheckNewAnswer((1, 2, 3), (1, 2, 3)),
      StopStream,
      StartStream(),
      AddData(input1, 100),
      AddData(input2, 100),
      CheckNewAnswer((100, 200, 300))
    )
  }

  test("stream stream inner join on windows - without watermark") {
    val input1 = MemoryStream[Int]
    val input2 = MemoryStream[Int]

    val df1 = input1.toDF()
      .select($"value" as "key", timestamp_seconds($"value") as "timestamp",
        ($"value" * 2) as "leftValue")
      .select($"key", window($"timestamp", "10 second"), $"leftValue")

    val df2 = input2.toDF()
      .select($"value" as "key", timestamp_seconds($"value") as "timestamp",
        ($"value" * 3) as "rightValue")
      .select($"key", window($"timestamp", "10 second"), $"rightValue")

    val joined = df1.join(df2, Seq("key", "window"))
      .select($"key", $"window.end".cast("long"), $"leftValue", $"rightValue")

    testStream(joined)(
      AddData(input1, 1),
      CheckNewAnswer(),
      AddData(input2, 1),
      CheckNewAnswer((1, 10, 2, 3)),
      StopStream,
      StartStream(),
      AddData(input1, 25),
      CheckNewAnswer(),
      StopStream,
      StartStream(),
      AddData(input2, 25),
      CheckNewAnswer((25, 30, 50, 75)),
      AddData(input1, 1),
      CheckNewAnswer((1, 10, 2, 3)),      // State for 1 still around as there is no watermark
      StopStream,
      StartStream(),
      AddData(input1, 5),
      CheckNewAnswer(),
      AddData(input2, 5),
      CheckNewAnswer((5, 10, 10, 15))     // No filter by any watermark
    )
  }

  test("stream stream inner join on windows - with watermark") {
    val input1 = MemoryStream[Int]
    val input2 = MemoryStream[Int]

    val df1 = input1.toDF()
      .select($"value" as "key", timestamp_seconds($"value") as "timestamp",
        ($"value" * 2) as "leftValue")
      .withWatermark("timestamp", "10 seconds")
      .select($"key", window($"timestamp", "10 second"), $"leftValue")

    val df2 = input2.toDF()
      .select($"value" as "key", timestamp_seconds($"value") as "timestamp",
        ($"value" * 3) as "rightValue")
      .select($"key", window($"timestamp", "10 second"), $"rightValue")

    val joined = df1.join(df2, Seq("key", "window"))
      .select($"key", $"window.end".cast("long"), $"leftValue", $"rightValue")

    testStream(joined)(
      AddData(input1, 1),
      CheckAnswer(),
      assertNumStateRows(total = 1, updated = 1),

      AddData(input2, 1),
      CheckAnswer((1, 10, 2, 3)),
      assertNumStateRows(total = 2, updated = 1),
      StopStream,
      StartStream(),

      AddData(input1, 25),
      CheckNewAnswer(),   // watermark = 15, no-data-batch should remove 2 rows having window=[0,10]
      assertNumStateRows(total = 1, updated = 1),

      AddData(input2, 25),
      CheckNewAnswer((25, 30, 50, 75)),
      assertNumStateRows(total = 2, updated = 1),
      StopStream,
      StartStream(),

      AddData(input2, 1),
      CheckNewAnswer(),                             // Should not join as < 15 removed
      assertNumStateRows(total = 2, updated = 0),   // row not add as 1 < state key watermark = 15

      AddData(input1, 5),
      CheckNewAnswer(),                             // Same reason as above
      assertNumStateRows(total = 2, updated = 0, droppedByWatermark = 1)
    )
  }

  test("stream stream inner join with time range - with watermark - one side condition") {
    import org.apache.spark.sql.functions._

    val leftInput = MemoryStream[(Int, Int)]
    val rightInput = MemoryStream[(Int, Int)]

    val df1 = leftInput.toDF().toDF("leftKey", "time")
      .select($"leftKey", timestamp_seconds($"time") as "leftTime",
        ($"leftKey" * 2) as "leftValue")
      .withWatermark("leftTime", "10 seconds")

    val df2 = rightInput.toDF().toDF("rightKey", "time")
      .select($"rightKey", timestamp_seconds($"time") as "rightTime",
        ($"rightKey" * 3) as "rightValue")
      .withWatermark("rightTime", "10 seconds")

    val joined =
      df1.join(df2, expr("leftKey = rightKey AND leftTime < rightTime - interval 5 seconds"))
        .select($"leftKey", $"leftTime".cast("int"), $"rightTime".cast("int"))

    testStream(joined)(
      AddData(leftInput, (1, 5)),
      CheckAnswer(),
      AddData(rightInput, (1, 11)),
      CheckNewAnswer((1, 5, 11)),
      AddData(rightInput, (1, 10)),
      CheckNewAnswer(), // no match as leftTime 5 is not < rightTime 10 - 5
      assertNumStateRows(total = 3, updated = 3),

      // Increase event time watermark to 20s by adding data with time = 30s on both inputs
      AddData(leftInput, (1, 3), (1, 30)),
      CheckNewAnswer((1, 3, 10), (1, 3, 11)),
      assertNumStateRows(total = 5, updated = 2),
      AddData(rightInput, (0, 30)),
      CheckNewAnswer(),

      // event time watermark:    max event time - 10   ==>   30 - 10 = 20
      // so left side going to only receive data where leftTime > 20
      // right side state constraint:    20 < leftTime < rightTime - 5   ==>   rightTime > 25
      // right state where rightTime <= 25 will be cleared, (1, 11) and (1, 10) removed
      assertNumStateRows(total = 4, updated = 1),

      // New data to right input should match with left side (1, 3) and (1, 5), as left state should
      // not be cleared. But rows rightTime <= 20 should be filtered due to event time watermark and
      // state rows with rightTime <= 25 should be removed from state.
      // (1, 20) ==> filtered by event time watermark = 20
      // (1, 21) ==> passed filter, matched with left (1, 3) and (1, 5), not added to state
      //             as 21 < state watermark = 25
      // (1, 28) ==> passed filter, matched with left (1, 3) and (1, 5), added to state
      AddData(rightInput, (1, 20), (1, 21), (1, 28)),
      CheckNewAnswer((1, 3, 21), (1, 5, 21), (1, 3, 28), (1, 5, 28)),
      assertNumStateRows(total = 5, updated = 1, droppedByWatermark = 1),

      // New data to left input with leftTime <= 20 should be filtered due to event time watermark
      AddData(leftInput, (1, 20), (1, 21)),
      CheckNewAnswer((1, 21, 28)),
      assertNumStateRows(total = 6, updated = 1, droppedByWatermark = 1)
    )
  }

  test("stream stream inner join with time range - with watermark - two side conditions") {
    import org.apache.spark.sql.functions._

    val leftInput = MemoryStream[(Int, Int)]
    val rightInput = MemoryStream[(Int, Int)]

    val df1 = leftInput.toDF().toDF("leftKey", "time")
      .select($"leftKey", timestamp_seconds($"time") as "leftTime",
        ($"leftKey" * 2) as "leftValue")
      .withWatermark("leftTime", "20 seconds")

    val df2 = rightInput.toDF().toDF("rightKey", "time")
      .select($"rightKey", timestamp_seconds($"time") as "rightTime",
        ($"rightKey" * 3) as "rightValue")
      .withWatermark("rightTime", "30 seconds")

    val condition = expr(
      "leftKey = rightKey AND " +
        "leftTime BETWEEN rightTime - interval 10 seconds AND rightTime + interval 5 seconds")

    // This translates to leftTime <= rightTime + 5 seconds AND leftTime >= rightTime - 10 seconds
    // So given leftTime, rightTime has to be BETWEEN leftTime - 5 seconds AND leftTime + 10 seconds
    //
    //  =============== * ======================== * ============================== * ==> leftTime
    //                  |                          |                                |
    //     |<---- 5s -->|<------ 10s ------>|      |<------ 10s ------>|<---- 5s -->|
    //     |                                |                          |
    //  == * ============================== * =========>============== * ===============> rightTime
    //
    // E.g.
    //      if rightTime = 60, then it matches only leftTime = [50, 65]
    //      if leftTime = 20, then it match only with rightTime = [15, 30]
    //
    // State value predicates
    //   left side:
    //     values allowed:  leftTime >= rightTime - 10s   ==>   leftTime > eventTimeWatermark - 10
    //     drop state where leftTime < eventTime - 10
    //   right side:
    //     values allowed:  rightTime >= leftTime - 5s   ==>   rightTime > eventTimeWatermark - 5
    //     drop state where rightTime < eventTime - 5

    val joined =
      df1.join(df2, condition).select($"leftKey", $"leftTime".cast("int"),
        $"rightTime".cast("int"))

    testStream(joined)(
      // If leftTime = 20, then it match only with rightTime = [15, 30]
      AddData(leftInput, (1, 20)),
      CheckAnswer(),
      AddData(rightInput, (1, 14), (1, 15), (1, 25), (1, 26), (1, 30), (1, 31)),
      CheckNewAnswer((1, 20, 15), (1, 20, 25), (1, 20, 26), (1, 20, 30)),
      assertNumStateRows(total = 7, updated = 7),

      // If rightTime = 60, then it matches only leftTime = [50, 65]
      AddData(rightInput, (1, 60)),
      CheckNewAnswer(),                // matches with nothing on the left
      AddData(leftInput, (1, 49), (1, 50), (1, 65), (1, 66)),
      CheckNewAnswer((1, 50, 60), (1, 65, 60)),

      // Event time watermark = min(left: 66 - delay 20 = 46, right: 60 - delay 30 = 30) = 30
      // Left state value watermark = 30 - 10 = slightly less than 20 (since condition has <=)
      //    Should drop < 20 from left, i.e., none
      // Right state value watermark = 30 - 5 = slightly less than 25 (since condition has <=)
      //    Should drop < 25 from the right, i.e., 14 and 15
      assertNumStateRows(total = 10, updated = 5), // 12 - 2 removed

      AddData(leftInput, (1, 30), (1, 31)),     // 30 should not be processed or added to state
      CheckNewAnswer((1, 31, 26), (1, 31, 30), (1, 31, 31)),
      assertNumStateRows(total = 11, updated = 1, droppedByWatermark = 1),  // only 31 added

      // Advance the watermark
      AddData(rightInput, (1, 80)),
      CheckNewAnswer(),
      // Event time watermark = min(left: 66 - delay 20 = 46, right: 80 - delay 30 = 50) = 46
      // Left state value watermark = 46 - 10 = slightly less than 36 (since condition has <=)
      //    Should drop < 36 from left, i.e., 20, 31 (30 was not added)
      // Right state value watermark = 46 - 5 = slightly less than 41 (since condition has <=)
      //    Should drop < 41 from the right, i.e., 25, 26, 30, 31
      assertNumStateRows(total = 6, updated = 1),  // 12 - 6 removed

      AddData(rightInput, (1, 46), (1, 50)),     // 46 should not be processed or added to state
      CheckNewAnswer((1, 49, 50), (1, 50, 50)),
      assertNumStateRows(total = 7, updated = 1, droppedByWatermark = 1)   // 50 added
    )
  }

  testQuietly("stream stream inner join without equality predicate") {
    val input1 = MemoryStream[Int]
    val input2 = MemoryStream[Int]

    val df1 = input1.toDF()
      .select($"value" as "leftKey", ($"value" * 2) as "leftValue")
    val df2 = input2.toDF()
      .select($"value" as "rightKey", ($"value" * 3) as "rightValue")
    val joined = df1.join(df2, expr("leftKey < rightKey"))
    val e = intercept[Exception] {
      val q = joined.writeStream.format("memory").queryName("test").start()
      input1.addData(1)
      q.awaitTermination(10000)
    }
    assert(e.toString.contains("Stream-stream join without equality predicate is not supported"))
  }

  test("stream stream self join") {
    val input = MemoryStream[Int]
    val df = input.toDF()
    val join =
      df.select($"value" % 5 as "key", $"value").join(
        df.select($"value" % 5 as "key", $"value"), "key")

    testStream(join)(
      AddData(input, 1, 2),
      CheckAnswer((1, 1, 1), (2, 2, 2)),
      StopStream,
      StartStream(),
      AddData(input, 3, 6),
      /*
      (1, 1)     (1, 1)
      (2, 2)  x  (2, 2)  =  (1, 1, 1), (1, 1, 6), (2, 2, 2), (1, 6, 1), (1, 6, 6)
      (1, 6)     (1, 6)
      */
      CheckAnswer((3, 3, 3), (1, 1, 1), (1, 1, 6), (2, 2, 2), (1, 6, 1), (1, 6, 6)))
  }

  test("locality preferences of StateStoreAwareZippedRDD") {
    import StreamingSymmetricHashJoinHelper._

    withTempDir { tempDir =>
      val queryId = UUID.randomUUID
      val opId = 0
      val path =
        Utils.createDirectory(tempDir.getAbsolutePath, Random.nextFloat().toString).toString
      val stateInfo = StatefulOperatorStateInfo(path, queryId, opId, 0L, 5, None)

      implicit val sqlContext = spark.sqlContext
      val coordinatorRef = sqlContext.streams.stateStoreCoordinator
      val numPartitions = 5
      val storeNames = Seq("name1", "name2")

      val partitionAndStoreNameToLocation = {
        for (partIndex <- 0 until numPartitions; storeName <- storeNames) yield {
          (partIndex, storeName) -> s"host-$partIndex-$storeName"
        }
      }.toMap
      partitionAndStoreNameToLocation.foreach { case ((partIndex, storeName), hostName) =>
        val providerId = StateStoreProviderId(stateInfo, partIndex, storeName)
        coordinatorRef.reportActiveInstance(providerId, hostName, s"exec-$hostName", Seq.empty)
        require(
          coordinatorRef.getLocation(providerId) ===
            Some(ExecutorCacheTaskLocation(hostName, s"exec-$hostName").toString))
      }

      val rdd1 = spark.sparkContext.makeRDD(1 to 10, numPartitions)
      val rdd2 = spark.sparkContext.makeRDD((1 to 10).map(_.toString), numPartitions)
      val rdd = rdd1.stateStoreAwareZipPartitions(rdd2, stateInfo, storeNames, coordinatorRef) {
        (_, left, right) => left.zip(right)
      }
      require(rdd.partitions.length === numPartitions)
      for (partIndex <- 0 until numPartitions) {
        val expectedLocations = storeNames.map { storeName =>
          val hostName = partitionAndStoreNameToLocation((partIndex, storeName))
          ExecutorCacheTaskLocation(hostName, s"exec-$hostName").toString
        }.toSet
        assert(rdd.preferredLocations(rdd.partitions(partIndex)).toSet === expectedLocations)
      }
    }
  }

  test("join between three streams") {
    val input1 = MemoryStream[Int]
    val input2 = MemoryStream[Int]
    val input3 = MemoryStream[Int]

    val df1 = input1.toDF().select($"value" as "leftKey", ($"value" * 2) as "leftValue")
    val df2 = input2.toDF()
      .select($"value" as "middleKey", ($"value" * 3) as "middleValue")
    val df3 = input3.toDF()
      .select($"value" as "rightKey", ($"value" * 5) as "rightValue")

    val joined = df1.join(df2, expr("leftKey = middleKey")).join(df3, expr("rightKey = middleKey"))

    testStream(joined)(
      AddData(input1, 1, 5),
      AddData(input2, 1, 5, 10),
      AddData(input3, 5, 10),
      CheckNewAnswer((5, 10, 5, 15, 5, 25)))
  }

  test("streaming join should require StatefulOpClusteredDistribution from children") {
    val input1 = MemoryStream[Int]
    val input2 = MemoryStream[Int]

    val df1 = input1.toDF()
      .select($"value" as Symbol("a"), $"value" * 2 as Symbol("b"))
    val df2 = input2.toDF()
      .select($"value" as Symbol("a"), $"value" * 2 as Symbol("b"))
      .repartition($"b")
    val joined = df1.join(df2, Seq("a", "b")).select($"a")

    testStream(joined)(
      AddData(input1, 1.to(1000): _*),
      AddData(input2, 1.to(1000): _*),
      CheckAnswer(1.to(1000): _*),
      Execute { query =>
        // Verify the query plan
        def partitionExpressionsColumns(expressions: Seq[Expression]): Seq[String] = {
          expressions.flatMap {
            case ref: AttributeReference => Some(ref.name)
          }
        }

        val numPartitions = spark.sessionState.conf.getConf(SQLConf.SHUFFLE_PARTITIONS)

        assert(query.lastExecution.executedPlan.collect {
          case j @ StreamingSymmetricHashJoinExec(_, _, _, _, _, _, _, _, _,
            ShuffleExchangeExec(opA: HashPartitioning, _, _, _),
            ShuffleExchangeExec(opB: HashPartitioning, _, _, _))
              if partitionExpressionsColumns(opA.expressions) === Seq("a", "b")
                && partitionExpressionsColumns(opB.expressions) === Seq("a", "b")
                && opA.numPartitions == numPartitions && opB.numPartitions == numPartitions => j
        }.size == 1)
      })
  }

  test("SPARK-26187 restore the stream-stream inner join query from Spark 2.4") {
    val inputStream = MemoryStream[(Int, Long)]
    val df = inputStream.toDS()
      .select(col("_1").as("value"), timestamp_seconds($"_2").as("timestamp"))

    val leftStream = df.select(col("value").as("leftId"), col("timestamp").as("leftTime"))

    val rightStream = df
      // Introduce misses for ease of debugging
      .where(col("value") % 2 === 0)
      .select(col("value").as("rightId"), col("timestamp").as("rightTime"))

    val query = leftStream
      .withWatermark("leftTime", "5 seconds")
      .join(
        rightStream.withWatermark("rightTime", "5 seconds"),
        expr("rightId = leftId AND rightTime >= leftTime AND " +
          "rightTime <= leftTime + interval 5 seconds"),
        joinType = "inner")
      .select(col("leftId"), col("leftTime").cast("int"),
        col("rightId"), col("rightTime").cast("int"))

    val resourceUri = this.getClass.getResource(
      "/structured-streaming/checkpoint-version-2.4.0-streaming-join/").toURI
    val checkpointDir = Utils.createTempDir().getCanonicalFile
    // Copy the checkpoint to a temp dir to prevent changes to the original.
    // Not doing this will lead to the test passing on the first run, but fail subsequent runs.
    FileUtils.copyDirectory(new File(resourceUri), checkpointDir)
    inputStream.addData((1, 1L), (2, 2L), (3, 3L), (4, 4L), (5, 5L))

    testStream(query)(
      StartStream(checkpointLocation = checkpointDir.getAbsolutePath),
      /*
      Note: The checkpoint was generated using the following input in Spark version 2.4.0
      AddData(inputStream, (1, 1L), (2, 2L), (3, 3L), (4, 4L), (5, 5L)),
      // batch 1 - global watermark = 0
      // states
      // left: (1, 1L), (2, 2L), (3, 3L), (4, 4L), (5, 5L)
      // right: (2, 2L), (4, 4L)
      CheckNewAnswer((2, 2L, 2, 2L), (4, 4L, 4, 4L)),
      assertNumStateRows(7, 7),
      */
      AddData(inputStream, (6, 6L), (7, 7L), (8, 8L), (9, 9L), (10, 10L)),
      // batch 2: same result as above test
      CheckNewAnswer((6, 6L, 6, 6L), (8, 8L, 8, 8L), (10, 10L, 10, 10L)),
      assertNumStateRows(11, 6),
      Execute { query =>
        // Verify state format = 1
        val f = query.lastExecution.executedPlan.collect {
          case f: StreamingSymmetricHashJoinExec => f
        }
        assert(f.size == 1)
        assert(f.head.stateFormatVersion == 1)
      }
    )
  }

  test("SPARK-48687 - restore the stream-stream inner join query from Spark 3.5 and " +
   "changing the join condition (key schema) should fail the query") {
    // NOTE: We are also changing the schema of input compared to the checkpoint.
    // In the checkpoint we define the input schema as (Int, Long), which does not have name
    // in both left and right.
    val inputStream = MemoryStream[(Int, Long, String)]
    val df = inputStream.toDS()
      .select(col("_1").as("value"), timestamp_seconds($"_2").as("timestamp"),
        col("_3").as("name"))

    val leftStream = df.select(col("value").as("leftId"),
      col("timestamp").as("leftTime"), col("name").as("leftName"))

    val rightStream = df
      // Introduce misses for ease of debugging
      .where(col("value") % 2 === 0)
      .select(col("value").as("rightId"),
        col("timestamp").as("rightTime"), col("name").as("rightName"))

    val query = leftStream
      .withWatermark("leftTime", "5 seconds")
      .join(
        rightStream.withWatermark("rightTime", "5 seconds"),
        expr("rightId = leftId AND leftName = rightName AND rightTime >= leftTime AND " +
          "rightTime <= leftTime + interval 5 seconds"),
        joinType = "inner")
      .select(col("leftId"), col("leftTime").cast("int"),
        col("leftName"),
        col("rightId"), col("rightTime").cast("int"),
        col("rightName"))

    val resourceUri = this.getClass.getResource(
      "/structured-streaming/checkpoint-version-3.5.1-streaming-join/").toURI
    val checkpointDir = Utils.createTempDir().getCanonicalFile
    // Copy the checkpoint to a temp dir to prevent changes to the original.
    // Not doing this will lead to the test passing on the first run, but fail subsequent runs.
    FileUtils.copyDirectory(new File(resourceUri), checkpointDir)
    inputStream.addData((1, 1L, "a"), (2, 2L, "b"), (3, 3L, "c"), (4, 4L, "d"), (5, 5L, "e"))

    val ex = intercept[StreamingQueryException] {
      testStream(query)(
        StartStream(checkpointLocation = checkpointDir.getAbsolutePath),
        /*
        Note: The checkpoint was generated using the following input in Spark version 3.5.1
        The base query is different because it does not use the leftName/rightName columns
        as part of the join keys/condition that is used as part of the key schema.

        AddData(inputStream, (1, 1L), (2, 2L), (3, 3L), (4, 4L), (5, 5L)),
        // batch 1 - global watermark = 0
        // states
        // left: (1, 1L), (2, 2L), (3, 3L), (4, 4L), (5, 5L)
        // right: (2, 2L), (4, 4L)
        CheckNewAnswer((2, 2L, 2, 2L), (4, 4L, 4, 4L)),
        */
        AddData(inputStream, (6, 6L, "a"), (7, 7L, "a"), (8, 8L, "a"), (9, 9L, "a"),
          (10, 10L, "a")),
        CheckNewAnswer((6, 6L, "a", 6, 6L, "a"), (8, 8L, "a", 8, 8L, "a"),
          (10, 10L, "a", 10, 10L, "a"))
      )
    }

    checkError(
      ex.getCause.asInstanceOf[SparkUnsupportedOperationException],
      condition = "STATE_STORE_KEY_SCHEMA_NOT_COMPATIBLE",
      parameters = Map("storedKeySchema" -> ".*",
        "newKeySchema" -> ".*"),
      matchPVals = true
    )
  }

  test("SPARK-48687 - restore the stream-stream inner join query from Spark 3.5 and " +
   "changing the value schema should fail the query") {
    // NOTE: We are also changing the schema of input compared to the checkpoint.
    // In the checkpoint we define the input schema as (Int, Long), which does not have name
    // in both left and right.
    val inputStream = MemoryStream[(Int, Long, String)]
    val df = inputStream.toDS()
      .select(col("_1").as("value"), timestamp_seconds($"_2").as("timestamp"),
        col("_3").as("name"))

    val leftStream = df.select(col("value").as("leftId"),
      col("timestamp").as("leftTime"), col("name").as("leftName"))

    val rightStream = df
      // Introduce misses for ease of debugging
      .where(col("value") % 2 === 0)
      .select(col("value").as("rightId"),
        col("timestamp").as("rightTime"), col("name").as("rightName"))

    val query = leftStream
      .withWatermark("leftTime", "5 seconds")
      .join(
        rightStream.withWatermark("rightTime", "5 seconds"),
        expr("rightId = leftId AND rightTime >= leftTime AND " +
          "rightTime <= leftTime + interval 5 seconds"),
        joinType = "inner")
      .select(col("leftId"), col("leftTime").cast("int"),
        col("leftName"),
        col("rightId"), col("rightTime").cast("int"),
        col("rightName"))

    val resourceUri = this.getClass.getResource(
      "/structured-streaming/checkpoint-version-3.5.1-streaming-join/").toURI
    val checkpointDir = Utils.createTempDir().getCanonicalFile
    // Copy the checkpoint to a temp dir to prevent changes to the original.
    // Not doing this will lead to the test passing on the first run, but fail subsequent runs.
    FileUtils.copyDirectory(new File(resourceUri), checkpointDir)
    inputStream.addData((1, 1L, "a"), (2, 2L, "b"), (3, 3L, "c"), (4, 4L, "d"), (5, 5L, "e"))

    val ex = intercept[StreamingQueryException] {
      testStream(query)(
        StartStream(checkpointLocation = checkpointDir.getAbsolutePath),
        /*
        Note: The checkpoint was generated using the following input in Spark version 3.5.1
        The base query is different because it does not use the leftName/rightName columns
        as part of the generated output that is used as part of the value schema.

        AddData(inputStream, (1, 1L), (2, 2L), (3, 3L), (4, 4L), (5, 5L)),
        // batch 1 - global watermark = 0
        // states
        // left: (1, 1L), (2, 2L), (3, 3L), (4, 4L), (5, 5L)
        // right: (2, 2L), (4, 4L)
        CheckNewAnswer((2, 2L, 2, 2L), (4, 4L, 4, 4L)),
        */
        AddData(inputStream, (6, 6L, "a"), (7, 7L, "a"), (8, 8L, "a"), (9, 9L, "a"),
          (10, 10L, "a")),
        CheckNewAnswer((6, 6L, "a", 6, 6L, "a"), (8, 8L, "a", 8, 8L, "a"),
          (10, 10L, "a", 10, 10L, "a"))
      )
    }

    checkError(
      ex.getCause.asInstanceOf[SparkUnsupportedOperationException],
      condition = "STATE_STORE_VALUE_SCHEMA_NOT_COMPATIBLE",
      parameters = Map("storedValueSchema" -> ".*",
        "newValueSchema" -> ".*"),
      matchPVals = true
    )
  }

  test("SPARK-35896: metrics in StateOperatorProgress are output correctly") {
    val input1 = MemoryStream[Int]
    val input2 = MemoryStream[Int]

    val df1 = input1.toDF()
      .select($"value" as "key", timestamp_seconds($"value") as "timestamp",
        ($"value" * 2) as "leftValue")
      .withWatermark("timestamp", "10 seconds")
      .select($"key", window($"timestamp", "10 second"), $"leftValue")

    val df2 = input2.toDF()
      .select($"value" as "key", timestamp_seconds($"value") as "timestamp",
        ($"value" * 3) as "rightValue")
      .select($"key", window($"timestamp", "10 second"), $"rightValue")

    val joined = df1.join(df2, Seq("key", "window"))
      .select($"key", $"window.end".cast("long"), $"leftValue", $"rightValue")

    testStream(joined)(
      StartStream(additionalConfs = Map(SQLConf.SHUFFLE_PARTITIONS.key -> "3")),
      AddData(input1, 1),
      CheckAnswer(),
      assertStateOperatorProgressMetric(operatorName = "symmetricHashJoin",
        numShufflePartitions = 3, numStateStoreInstances = 3 * 4),

      AddData(input2, 1),
      CheckAnswer((1, 10, 2, 3)),
      assertNumStateRows(
        total = Seq(2), updated = Seq(1), droppedByWatermark = Seq(0), removed = Some(Seq(0))),

      AddData(input1, 25),
      CheckNewAnswer(),   // watermark = 15, no-data-batch should remove 2 rows having window=[0,10]
      assertNumStateRows(
        total = Seq(1), updated = Seq(1), droppedByWatermark = Seq(0), removed = Some(Seq(2))),

      AddData(input2, 25),
      CheckNewAnswer((25, 30, 50, 75)),
      assertNumStateRows(
        total = Seq(2), updated = Seq(1), droppedByWatermark = Seq(0), removed = Some(Seq(0)))
    )
  }

  test("joining non-nullable left join key with nullable right join key") {
    val input1 = MemoryStream[Int]
    val input2 = MemoryStream[JInteger]

    val joined = testForJoinKeyNullability(input1.toDF(), input2.toDF())
    testStream(joined)(
      AddData(input1, 1, 5),
      AddData(input2, JInteger.valueOf(1), JInteger.valueOf(5), JInteger.valueOf(10), null),
      CheckNewAnswer(Row(1, 1, 2, 3), Row(5, 5, 10, 15))
    )
  }

  test("joining nullable left join key with non-nullable right join key") {
    val input1 = MemoryStream[JInteger]
    val input2 = MemoryStream[Int]

    val joined = testForJoinKeyNullability(input1.toDF(), input2.toDF())
    testStream(joined)(
      AddData(input1, JInteger.valueOf(1), JInteger.valueOf(5), JInteger.valueOf(10), null),
      AddData(input2, 1, 5),
      CheckNewAnswer(Row(1, 1, 2, 3), Row(5, 5, 10, 15))
    )
  }

  test("joining nullable left join key with nullable right join key") {
    val input1 = MemoryStream[JInteger]
    val input2 = MemoryStream[JInteger]

    val joined = testForJoinKeyNullability(input1.toDF(), input2.toDF())
    testStream(joined)(
      AddData(input1, JInteger.valueOf(1), JInteger.valueOf(5), JInteger.valueOf(10), null),
      AddData(input2, JInteger.valueOf(1), JInteger.valueOf(5), null),
      CheckNewAnswer(
        Row(JInteger.valueOf(1), JInteger.valueOf(1), JInteger.valueOf(2), JInteger.valueOf(3)),
        Row(JInteger.valueOf(5), JInteger.valueOf(5), JInteger.valueOf(10), JInteger.valueOf(15)),
        Row(null, null, null, null))
    )
  }

  private def testForJoinKeyNullability(left: DataFrame, right: DataFrame): DataFrame = {
    val df1 = left.selectExpr("value as leftKey", "value * 2 as leftValue")
    val df2 = right.selectExpr("value as rightKey", "value * 3 as rightValue")

    df1.join(df2, expr("leftKey <=> rightKey"))
      .select("leftKey", "rightKey", "leftValue", "rightValue")
  }
}


@SlowSQLTest
class StreamingOuterJoinSuite extends StreamingJoinSuite {

  import testImplicits._
  import org.apache.spark.sql.functions._

  test("left outer early state exclusion on left") {
    val (leftInput, rightInput, joined) = setupWindowedJoinWithLeftCondition("left_outer")

    testStream(joined)(
      MultiAddData(leftInput, 1, 2, 3)(rightInput, 3, 4, 5),
      // The left rows with leftValue <= 4 should generate their outer join row now and
      // not get added to the state.
      CheckNewAnswer(Row(3, 10, 6, "9"), Row(1, 10, 2, null), Row(2, 10, 4, null)),
      assertNumStateRows(total = 4, updated = 4),
      // We shouldn't get more outer join rows when the watermark advances.
      MultiAddData(leftInput, 20)(rightInput, 21),
      CheckNewAnswer(),
      AddData(rightInput, 20),
      CheckNewAnswer((20, 30, 40, "60"))
    )
  }

  test("left outer early state exclusion on right") {
    val (leftInput, rightInput, joined) = setupWindowedJoinWithRightCondition("left_outer")

    testStream(joined)(
      MultiAddData(leftInput, 3, 4, 5)(rightInput, 1, 2, 3),
      // The right rows with rightValue <= 7 should never be added to the state.
      CheckNewAnswer(Row(3, 10, 6, "9")),     // rightValue = 9 > 7 hence joined and added to state
      assertNumStateRows(total = 4, updated = 4),
      // When the watermark advances, we get the outer join rows just as we would if they
      // were added but didn't match the full join condition.
      MultiAddData(leftInput, 20)(rightInput, 21),  // watermark = 10, no-data-batch computes nulls
      CheckNewAnswer(Row(4, 10, 8, null), Row(5, 10, 10, null)),
      AddData(rightInput, 20),
      CheckNewAnswer(Row(20, 30, 40, "60"))
    )
  }

  test("right outer early state exclusion on left") {
    val (leftInput, rightInput, joined) = setupWindowedJoinWithLeftCondition("right_outer")

    testStream(joined)(
      MultiAddData(leftInput, 1, 2, 3)(rightInput, 3, 4, 5),
      // The left rows with leftValue <= 4 should never be added to the state.
      CheckNewAnswer(Row(3, 10, 6, "9")),     // leftValue = 7 > 4 hence joined and added to state
      assertNumStateRows(total = 4, updated = 4),
      // When the watermark advances, we get the outer join rows just as we would if they
      // were added but didn't match the full join condition.
      MultiAddData(leftInput, 20)(rightInput, 21), // watermark = 10, no-data-batch computes nulls
      CheckNewAnswer(Row(4, 10, null, "12"), Row(5, 10, null, "15")),
      AddData(rightInput, 20),
      CheckNewAnswer(Row(20, 30, 40, "60"))
    )
  }

  test("right outer early state exclusion on right") {
    val (leftInput, rightInput, joined) = setupWindowedJoinWithRightCondition("right_outer")

    testStream(joined)(
      MultiAddData(leftInput, 3, 4, 5)(rightInput, 1, 2, 3),
      // The right rows with rightValue <= 7 should generate their outer join row now and
      // not get added to the state.
      CheckNewAnswer(Row(3, 10, 6, "9"), Row(1, 10, null, "3"), Row(2, 10, null, "6")),
      assertNumStateRows(total = 4, updated = 4),
      // We shouldn't get more outer join rows when the watermark advances.
      MultiAddData(leftInput, 20)(rightInput, 21),
      CheckNewAnswer(),
      AddData(rightInput, 20),
      CheckNewAnswer((20, 30, 40, "60"))
    )
  }

  test("windowed left outer join") {
    val (leftInput, rightInput, joined) = setupWindowedJoin("left_outer")

    testStream(joined)(
      // Test inner part of the join.
      MultiAddData(leftInput, 1, 2, 3, 4, 5)(rightInput, 3, 4, 5, 6, 7),
      CheckNewAnswer((3, 10, 6, 9), (4, 10, 8, 12), (5, 10, 10, 15)),

      MultiAddData(leftInput, 21)(rightInput, 22), // watermark = 11, no-data-batch computes nulls
      CheckNewAnswer(Row(1, 10, 2, null), Row(2, 10, 4, null)),
      assertNumStateRows(total = 2, updated = 12),

      AddData(leftInput, 22),
      CheckNewAnswer(Row(22, 30, 44, 66)),
      assertNumStateRows(total = 3, updated = 1)
    )
  }

  test("windowed right outer join") {
    val (leftInput, rightInput, joined) = setupWindowedJoin("right_outer")

    testStream(joined)(
      // Test inner part of the join.
      MultiAddData(leftInput, 1, 2, 3, 4, 5)(rightInput, 3, 4, 5, 6, 7),
      CheckNewAnswer((3, 10, 6, 9), (4, 10, 8, 12), (5, 10, 10, 15)),

      MultiAddData(leftInput, 21)(rightInput, 22), // watermark = 11, no-data-batch computes nulls
      CheckNewAnswer(Row(6, 10, null, 18), Row(7, 10, null, 21)),
      assertNumStateRows(total = 2, updated = 12),

      AddData(leftInput, 22),
      CheckNewAnswer(Row(22, 30, 44, 66)),
      assertNumStateRows(total = 3, updated = 1)
    )
  }

  Seq(
    ("left_outer", Row(3, null, 5, null)),
    ("right_outer", Row(null, 2, null, 5))
  ).foreach { case (joinType: String, outerResult) =>
    test(s"${joinType.replace("_", " ")} with watermark range condition") {
      val (leftInput, rightInput, joined) = setupJoinWithRangeCondition(joinType)

      testStream(joined)(
        AddData(leftInput, (1, 5), (3, 5)),
        CheckAnswer(),
        AddData(rightInput, (1, 10), (2, 5)),
        CheckNewAnswer((1, 1, 5, 10)),
        AddData(rightInput, (1, 11)),
        CheckNewAnswer(), // no match as left time is too low
        assertNumStateRows(total = 5, updated = 5),

        // Increase event time watermark to 20s by adding data with time = 30s on both inputs
        AddData(leftInput, (1, 7), (1, 30)),
        CheckNewAnswer((1, 1, 7, 10), (1, 1, 7, 11)),
        assertNumStateRows(total = 7, updated = 2),
        AddData(rightInput, (0, 30)), // watermark = 30 - 10 = 20, no-data-batch computes nulls
        CheckNewAnswer(outerResult),
        assertNumStateRows(total = 2, updated = 1)
      )

      Seq(
        ("10 minutes",
          "interval 3 minutes 30 seconds"),
        ("10 minutes",
          "interval '3:30' minute to second")).foreach { case (watermark, bound) =>
        val (leftInput2, rightInput2, joined2) =
          setupJoinWithRangeCondition(
            joinType,
            watermark,
            bound,
            bound)

        testStream(joined2)(
          AddData(leftInput2, (1, 210), (3, 5)),
          CheckAnswer(),
          AddData(rightInput2, (1, 300), (2, 5)),
          CheckNewAnswer((1, 1, 210, 300)),
          AddData(rightInput2, (1, 450)),
          CheckNewAnswer(),
          assertNumStateRows(total = 5, updated = 5),
          AddData(leftInput2, (1, 260), (1, 1800)),
          CheckNewAnswer((1, 1, 260, 300), (1, 1, 260, 450)),
          assertNumStateRows(total = 7, updated = 2),
          AddData(rightInput2, (0, 1800)),
          CheckNewAnswer(outerResult),
          assertNumStateRows(total = 2, updated = 1)
        )
      }
    }
  }

  // When the join condition isn't true, the outer null rows must be generated, even if the join
  // keys themselves have a match.
  test("left outer join with non-key condition violated") {
    val (leftInput, simpleLeftDf) = setupStream("left", 2)
    val (rightInput, simpleRightDf) = setupStream("right", 3)

    val left = simpleLeftDf
      .select($"key", window($"leftTime", "10 second"), $"leftValue")
    val right = simpleRightDf
      .select($"key", window($"rightTime", "10 second"), $"rightValue")

    val joined = left.join(
        right,
        left("key") === right("key") && left("window") === right("window") &&
          $"leftValue" > 10 &&
          ($"rightValue" < 300 || $"rightValue" > 1000),
        "left_outer")
      .select(left("key"), left("window.end").cast("long"), $"leftValue",
        $"rightValue")

    testStream(joined)(
      // leftValue <= 10 should generate outer join rows even though it matches right keys
      MultiAddData(leftInput, 1, 2, 3)(rightInput, 1, 2, 3),
      CheckNewAnswer(Row(1, 10, 2, null), Row(2, 10, 4, null), Row(3, 10, 6, null)),
      assertNumStateRows(total = 3, updated = 3), // only right 1, 2, 3 added

      MultiAddData(leftInput, 20)(rightInput, 21), // watermark = 10, no-data-batch cleared < 10
      CheckNewAnswer(),
      assertNumStateRows(total = 2, updated = 2),  // only 20 and 21 left in state

      AddData(rightInput, 20),
      CheckNewAnswer(Row(20, 30, 40, 60)),
      assertNumStateRows(total = 3, updated = 1),

      // leftValue and rightValue both satisfying condition should not generate outer join rows
      MultiAddData(leftInput, 40, 41)(rightInput, 40, 41), // watermark = 31
      CheckNewAnswer((40, 50, 80, 120), (41, 50, 82, 123)),
      assertNumStateRows(total = 4, updated = 4),   // only left 40, 41 + right 40,41 left in state

      MultiAddData(leftInput, 70)(rightInput, 71), // watermark = 60
      CheckNewAnswer(),
      assertNumStateRows(total = 2, updated = 2), // only 70, 71 left in state

      AddData(rightInput, 70),
      CheckNewAnswer((70, 80, 140, 210)),
      assertNumStateRows(total = 3, updated = 1),

      // rightValue between 300 and 1000 should generate outer join rows even though it matches left
      MultiAddData(leftInput, 101, 102, 103)(rightInput, 101, 102, 103), // watermark = 91
      CheckNewAnswer(),
      assertNumStateRows(total = 6, updated = 3), // only 101 - 103 left in state

      MultiAddData(leftInput, 1000)(rightInput, 1001),
      CheckNewAnswer(
        Row(101, 110, 202, null),
        Row(102, 110, 204, null),
        Row(103, 110, 206, null)),
      assertNumStateRows(total = 2, updated = 2)
    )
  }

  test("SPARK-26187 self left outer join should not return outer nulls for already matched rows") {
    val (inputStream, query) = setupSelfJoin("left_outer")

    testStream(query)(
      AddData(inputStream, (1, 1L), (2, 2L), (3, 3L), (4, 4L), (5, 5L)),
      // batch 1 - global watermark = 0
      // states
      // left: (1, 1L), (2, 2L), (3, 3L), (4, 4L), (5, 5L)
      // right: (2, 2L), (4, 4L)
      CheckNewAnswer((2, 2L, 2, 2L), (4, 4L, 4, 4L)),
      assertNumStateRows(7, 7),

      AddData(inputStream, (6, 6L), (7, 7L), (8, 8L), (9, 9L), (10, 10L)),
      // batch 2 - global watermark = 5
      // states
      // left: (1, 1L), (2, 2L), (3, 3L), (4, 4L), (5, 5L), (6, 6L), (7, 7L), (8, 8L),
      //       (9, 9L), (10, 10L)
      // right: (6, 6L), (8, 8L), (10, 10L)
      // states evicted
      // left: nothing (it waits for 5 seconds more than watermark due to join condition)
      // right: (2, 2L), (4, 4L)
      // NOTE: look for evicted rows in right which are not evicted from left - they were
      // properly joined in batch 1
      CheckNewAnswer((6, 6L, 6, 6L), (8, 8L, 8, 8L), (10, 10L, 10, 10L)),
      assertNumStateRows(13, 8),

      AddData(inputStream, (11, 11L), (12, 12L), (13, 13L), (14, 14L), (15, 15L)),
      // batch 3
      // - global watermark = 9 <= min(9, 10)
      // states
      // left: (4, 4L), (5, 5L), (6, 6L), (7, 7L), (8, 8L), (9, 9L), (10, 10L), (11, 11L),
      //       (12, 12L), (13, 13L), (14, 14L), (15, 15L)
      // right: (10, 10L), (12, 12L), (14, 14L)
      // states evicted
      // left: (1, 1L), (2, 2L), (3, 3L)
      // right: (6, 6L), (8, 8L)
      CheckNewAnswer(
        Row(12, 12L, 12, 12L), Row(14, 14L, 14, 14L),
        Row(1, 1L, null, null), Row(3, 3L, null, null)),
      assertNumStateRows(15, 7)
    )
  }

  test("SPARK-26187 self right outer join should not return outer nulls for already matched rows") {
    val inputStream = MemoryStream[(Int, Long)]

    val df = inputStream.toDS()
      .select(col("_1").as("value"), timestamp_seconds($"_2").as("timestamp"))

    // we're just flipping "left" and "right" from left outer join and apply right outer join

    val leftStream = df
      // Introduce misses for ease of debugging
      .where(col("value") % 2 === 0)
      .select(col("value").as("leftId"), col("timestamp").as("leftTime"))

    val rightStream = df.select(col("value").as("rightId"), col("timestamp").as("rightTime"))

    val query = leftStream
      .withWatermark("leftTime", "5 seconds")
      .join(
        rightStream.withWatermark("rightTime", "5 seconds"),
        expr("leftId = rightId AND leftTime >= rightTime AND " +
          "leftTime <= rightTime + interval 5 seconds"),
        joinType = "rightOuter")
      .select(col("leftId"), col("leftTime").cast("int"),
        col("rightId"), col("rightTime").cast("int"))

    // we can just flip left and right in the explanation of left outer query test
    // to assume the status of right outer query, hence skip explaining here
    testStream(query)(
      AddData(inputStream, (1, 1L), (2, 2L), (3, 3L), (4, 4L), (5, 5L)),
      CheckNewAnswer((2, 2L, 2, 2L), (4, 4L, 4, 4L)),
      assertNumStateRows(7, 7),

      AddData(inputStream, (6, 6L), (7, 7L), (8, 8L), (9, 9L), (10, 10L)),
      CheckNewAnswer((6, 6L, 6, 6L), (8, 8L, 8, 8L), (10, 10L, 10, 10L)),
      assertNumStateRows(13, 8),

      AddData(inputStream, (11, 11L), (12, 12L), (13, 13L), (14, 14L), (15, 15L)),
      CheckNewAnswer(
        Row(12, 12L, 12, 12L), Row(14, 14L, 14, 14L),
        Row(null, null, 1, 1L), Row(null, null, 3, 3L)),
      assertNumStateRows(15, 7)
    )
  }

  test("SPARK-26187 restore the stream-stream outer join query from Spark 2.4") {
    val inputStream = MemoryStream[(Int, Long)]
    val df = inputStream.toDS()
      .select(col("_1").as("value"), timestamp_seconds($"_2").as("timestamp"))

    val leftStream = df.select(col("value").as("leftId"), col("timestamp").as("leftTime"))

    val rightStream = df
      // Introduce misses for ease of debugging
      .where(col("value") % 2 === 0)
      .select(col("value").as("rightId"), col("timestamp").as("rightTime"))

    val query = leftStream
      .withWatermark("leftTime", "5 seconds")
      .join(
        rightStream.withWatermark("rightTime", "5 seconds"),
        expr("rightId = leftId AND rightTime >= leftTime AND " +
          "rightTime <= leftTime + interval 5 seconds"),
        joinType = "leftOuter")
      .select(col("leftId"), col("leftTime").cast("int"),
        col("rightId"), col("rightTime").cast("int"))

    val resourceUri = this.getClass.getResource(
      "/structured-streaming/checkpoint-version-2.4.0-streaming-join/").toURI
    val checkpointDir = Utils.createTempDir().getCanonicalFile
    // Copy the checkpoint to a temp dir to prevent changes to the original.
    // Not doing this will lead to the test passing on the first run, but fail subsequent runs.
    FileUtils.copyDirectory(new File(resourceUri), checkpointDir)
    inputStream.addData((1, 1L), (2, 2L), (3, 3L), (4, 4L), (5, 5L))

    /*
      Note: The checkpoint was generated using the following input in Spark version 2.4.0
      AddData(inputStream, (1, 1L), (2, 2L), (3, 3L), (4, 4L), (5, 5L)),
      // batch 1 - global watermark = 0
      // states
      // left: (1, 1L), (2, 2L), (3, 3L), (4, 4L), (5, 5L)
      // right: (2, 2L), (4, 4L)
      CheckNewAnswer((2, 2L, 2, 2L), (4, 4L, 4, 4L)),
      assertNumStateRows(7, 7),
      */

    // we just fail the query if the checkpoint was create from less than Spark 3.0
    val e = intercept[StreamingQueryException] {
      val writer = query.writeStream.format("console")
        .option("checkpointLocation", checkpointDir.getAbsolutePath).start()
      inputStream.addData((7, 7L), (8, 8L))
      eventually(timeout(streamingTimeout)) {
        assert(writer.exception.isDefined)
      }
      throw writer.exception.get
    }
    assert(e.getMessage.toLowerCase(Locale.ROOT)
      .contains("the query is using stream-stream leftouter join with state format version 1"))
  }

  test("SPARK-29438: ensure UNION doesn't lead stream-stream join to use shifted partition IDs") {
    def constructUnionDf(desiredPartitionsForInput1: Int)
        : (MemoryStream[Int], MemoryStream[Int], MemoryStream[Int], DataFrame) = {
      val input1 = MemoryStream[Int](desiredPartitionsForInput1)
      val df1 = input1.toDF()
        .select(
          $"value" as "key",
          $"value" as "leftValue",
          $"value" as "rightValue")
      val (input2, df2) = setupStream("left", 2)
      val (input3, df3) = setupStream("right", 3)

      val joined = df2
        .join(df3,
          df2("key") === df3("key") && df2("leftTime") === df3("rightTime"),
          "inner")
        .select(df2("key"), $"leftValue", $"rightValue")

      (input1, input2, input3, df1.union(joined))
    }

    withTempDir { tempDir =>
      val (input1, input2, input3, unionDf) = constructUnionDf(2)

      testStream(unionDf)(
        StartStream(checkpointLocation = tempDir.getAbsolutePath),
        MultiAddData(
          (input1, Seq(11, 12, 13)),
          (input2, Seq(11, 12, 13, 14, 15)),
          (input3, Seq(13, 14, 15, 16, 17))),
        CheckNewAnswer(Row(11, 11, 11), Row(12, 12, 12), Row(13, 13, 13), Row(13, 26, 39),
          Row(14, 28, 42), Row(15, 30, 45)),
        StopStream
      )

      // We're restoring the query with different number of partitions in left side of UNION,
      // which leads right side of union to have mismatched partition IDs if it relies on
      // TaskContext.partitionId(). SPARK-29438 fixes this issue to not rely on it.

      val (newInput1, newInput2, newInput3, newUnionDf) = constructUnionDf(3)

      newInput1.addData(11, 12, 13)
      newInput2.addData(11, 12, 13, 14, 15)
      newInput3.addData(13, 14, 15, 16, 17)

      testStream(newUnionDf)(
        StartStream(checkpointLocation = tempDir.getAbsolutePath),
        MultiAddData(
          (newInput1, Seq(21, 22, 23)),
          (newInput2, Seq(21, 22, 23, 24, 25)),
          (newInput3, Seq(23, 24, 25, 26, 27))),
        CheckNewAnswer(Row(21, 21, 21), Row(22, 22, 22), Row(23, 23, 23), Row(23, 46, 69),
          Row(24, 48, 72), Row(25, 50, 75))
      )
    }
  }

  test("SPARK-32148 stream-stream join regression on Spark 3.0.0") {
    val input1 = MemoryStream[(Timestamp, String, String)]
    val df1 = input1.toDF()
      .selectExpr("_1 as eventTime", "_2 as id", "_3 as comment")
      .withWatermark(s"eventTime", "2 minutes")

    val input2 = MemoryStream[(Timestamp, String, String)]
    val df2 = input2.toDF()
      .selectExpr("_1 as eventTime", "_2 as id", "_3 as name")
      .withWatermark(s"eventTime", "4 minutes")

    val joined = df1.as("left")
      .join(df2.as("right"),
        expr("""
               |left.id = right.id AND left.eventTime BETWEEN
               |  right.eventTime - INTERVAL 30 seconds AND
               |  right.eventTime + INTERVAL 30 seconds
             """.stripMargin),
        joinType = "leftOuter")

    val inputDataForInput1 = Seq(
      (Timestamp.valueOf("2020-01-01 00:00:00"), "abc", "has no join partner"),
      (Timestamp.valueOf("2020-01-02 00:00:00"), "abc", "joined with A"),
      (Timestamp.valueOf("2020-01-02 01:00:00"), "abc", "joined with B"))

    val inputDataForInput2 = Seq(
      (Timestamp.valueOf("2020-01-02 00:00:10"), "abc", "A"),
      (Timestamp.valueOf("2020-01-02 00:59:59"), "abc", "B"),
      (Timestamp.valueOf("2020-01-02 02:00:00"), "abc", "C"))

    val expectedOutput = Seq(
      (Timestamp.valueOf("2020-01-01 00:00:00"), "abc", "has no join partner", null, null, null),
      (Timestamp.valueOf("2020-01-02 00:00:00"), "abc", "joined with A",
        Timestamp.valueOf("2020-01-02 00:00:10"), "abc", "A"),
      (Timestamp.valueOf("2020-01-02 01:00:00"), "abc", "joined with B",
        Timestamp.valueOf("2020-01-02 00:59:59"), "abc", "B"))

    testStream(joined)(
      MultiAddData((input1, inputDataForInput1), (input2, inputDataForInput2)),
      CheckNewAnswer(expectedOutput.head, expectedOutput.tail: _*)
    )
  }

  test("left-outer: joining non-nullable left join key with nullable right join key") {
    val input1 = MemoryStream[(Int, Int)]
    val input2 = MemoryStream[(JInteger, Int)]

    val joined = testForLeftOuterJoinKeyNullability(input1.toDF(), input2.toDF())

    testStream(joined)(
      AddData(input1, (1, 1), (1, 2), (1, 3), (1, 4), (1, 5)),
      AddData(input2,
        (JInteger.valueOf(1), 3),
        (JInteger.valueOf(1), 4),
        (JInteger.valueOf(1), 5),
        (JInteger.valueOf(1), 6)
      ),
      CheckNewAnswer(
        Row(1, 1, 3, 3, 10, 6, 9),
        Row(1, 1, 4, 4, 10, 8, 12),
        Row(1, 1, 5, 5, 10, 10, 15)),
      AddData(input1, (1, 21)),
      // right-null join
      AddData(input2, (JInteger.valueOf(1), 22)), // watermark = 11, no-data-batch computes nulls
      CheckNewAnswer(
        Row(1, null, 1, null, 10, 2, null),
        Row(1, null, 2, null, 10, 4, null)
      )
    )
  }

  test("left-outer: joining nullable left join key with non-nullable right join key") {
    val input1 = MemoryStream[(JInteger, Int)]
    val input2 = MemoryStream[(Int, Int)]

    val joined = testForLeftOuterJoinKeyNullability(input1.toDF(), input2.toDF())

    testStream(joined)(
      AddData(input1,
        (JInteger.valueOf(1), 1),
        (null, 2),
        (JInteger.valueOf(1), 3),
        (JInteger.valueOf(1), 4),
        (JInteger.valueOf(1), 5)),
      AddData(input2, (1, 3), (1, 4), (1, 5), (1, 6)),
      CheckNewAnswer(
        Row(1, 1, 3, 3, 10, 6, 9),
        Row(1, 1, 4, 4, 10, 8, 12),
        Row(1, 1, 5, 5, 10, 10, 15)),
      // right-null join
      AddData(input1, (JInteger.valueOf(1), 21)),
      AddData(input2, (1, 22)), // watermark = 11, no-data-batch computes nulls
      CheckNewAnswer(
        Row(1, null, 1, null, 10, 2, null),
        Row(null, null, 2, null, 10, 4, null)
      )
    )
  }

  test("left-outer: joining nullable left join key with nullable right join key") {
    val input1 = MemoryStream[(JInteger, Int)]
    val input2 = MemoryStream[(JInteger, Int)]

    val joined = testForLeftOuterJoinKeyNullability(input1.toDF(), input2.toDF())

    testStream(joined)(
      AddData(input1,
        (JInteger.valueOf(1), 1),
        (null, 2),
        (JInteger.valueOf(1), 3),
        (null, 4),
        (JInteger.valueOf(1), 5)),
      AddData(input2,
        (JInteger.valueOf(1), 3),
        (null, 4),
        (JInteger.valueOf(1), 5),
        (JInteger.valueOf(1), 6)),
      CheckNewAnswer(
        Row(1, 1, 3, 3, 10, 6, 9),
        Row(null, null, 4, 4, 10, 8, 12),
        Row(1, 1, 5, 5, 10, 10, 15)),
      // right-null join
      AddData(input1, (JInteger.valueOf(1), 21)),
      AddData(input2, (JInteger.valueOf(1), 22)), // watermark = 11, no-data-batch computes nulls
      CheckNewAnswer(
        Row(1, null, 1, null, 10, 2, null),
        Row(null, null, 2, null, 10, 4, null)
      )
    )
  }

  private def testForLeftOuterJoinKeyNullability(left: DataFrame, right: DataFrame): DataFrame = {
    val df1 = left
      .selectExpr("_1 as leftKey1", "_2 as leftKey2", "timestamp_seconds(_2) as leftTime",
        "_2 * 2 as leftValue")
      .withWatermark("leftTime", "10 seconds")
    val df2 = right
      .selectExpr(
        "_1 as rightKey1", "_2 as rightKey2", "timestamp_seconds(_2) as rightTime",
        "_2 * 3 as rightValue")
      .withWatermark("rightTime", "10 seconds")

    val windowed1 = df1.select($"leftKey1", $"leftKey2",
      window($"leftTime", "10 second").as(Symbol("leftWindow")), $"leftValue")
    val windowed2 = df2.select($"rightKey1", $"rightKey2",
      window($"rightTime", "10 second").as(Symbol("rightWindow")), $"rightValue")
    windowed1.join(windowed2,
      expr("leftKey1 <=> rightKey1 AND leftKey2 = rightKey2 AND leftWindow = rightWindow"),
      "left_outer"
    ).select($"leftKey1", $"rightKey1", $"leftKey2", $"rightKey2",
      $"leftWindow.end".cast("long"), $"leftValue", $"rightValue")
  }

  test("SPARK-38684: outer join works correctly even if processing input rows and " +
    "evicting state rows for same grouping key happens in the same micro-batch") {

    // The test is to demonstrate the correctness issue in outer join before SPARK-38684.
    withSQLConf(
      SQLConf.STREAMING_NO_DATA_MICRO_BATCHES_ENABLED.key -> "false",
      SQLConf.STATE_STORE_PROVIDER_CLASS.key -> classOf[RocksDBStateStoreProvider].getName) {

      val input1 = MemoryStream[(Timestamp, String, String)]
      val df1 = input1.toDF()
        .selectExpr("_1 as eventTime", "_2 as id", "_3 as comment")
        .withWatermark("eventTime", "0 second")

      val input2 = MemoryStream[(Timestamp, String, String)]
      val df2 = input2.toDF()
        .selectExpr("_1 as eventTime", "_2 as id", "_3 as comment")
        .withWatermark("eventTime", "0 second")

      val joined = df1.as("left")
        .join(df2.as("right"),
          expr("""
                 |left.id = right.id AND left.eventTime BETWEEN
                 |  right.eventTime - INTERVAL 30 seconds AND
                 |  right.eventTime + INTERVAL 30 seconds
             """.stripMargin),
          joinType = "leftOuter")

      testStream(joined)(
        MultiAddData(
          (input1, Seq((Timestamp.valueOf("2020-01-02 00:00:00"), "abc", "left in batch 1"))),
          (input2, Seq((Timestamp.valueOf("2020-01-02 00:01:00"), "abc", "right in batch 1")))
        ),
        CheckNewAnswer(),
        MultiAddData(
          (input1, Seq((Timestamp.valueOf("2020-01-02 01:00:00"), "abc", "left in batch 2"))),
          (input2, Seq((Timestamp.valueOf("2020-01-02 01:01:00"), "abc", "right in batch 2")))
        ),
        // watermark advanced to "2020-01-02 00:00:00"
        CheckNewAnswer(),
        AddData(input1, (Timestamp.valueOf("2020-01-02 01:30:00"), "abc", "left in batch 3")),
        // watermark advanced to "2020-01-02 01:00:00"
        CheckNewAnswer(
          (Timestamp.valueOf("2020-01-02 00:00:00"), "abc", "left in batch 1", null, null, null)
        ),
        // left side state should still contain "left in batch 2" and "left in batch 3"
        // we should see both rows in the left side since
        // - "left in batch 2" is going to be evicted in this batch
        // - "left in batch 3" is going to be matched with new row in right side
        AddData(input2,
          (Timestamp.valueOf("2020-01-02 01:30:10"), "abc", "match with left in batch 3")),
        // watermark advanced to "2020-01-02 01:01:00"
        CheckNewAnswer(
          (Timestamp.valueOf("2020-01-02 01:00:00"), "abc", "left in batch 2",
            null, null, null),
          (Timestamp.valueOf("2020-01-02 01:30:00"), "abc", "left in batch 3",
            Timestamp.valueOf("2020-01-02 01:30:10"), "abc", "match with left in batch 3")
        )
      )
    }
  }

  test("SPARK-49829 left-outer join, input being unmatched is between WM for late event and " +
    "WM for eviction") {

    withTempDir { checkpoint =>
      // This config needs to be set, otherwise no-data batch will be triggered and after
      // no-data batch, WM for late event and WM for eviction would be same.
      withSQLConf(SQLConf.STREAMING_NO_DATA_MICRO_BATCHES_ENABLED.key -> "false") {
        val memoryStream1 = MemoryStream[(String, Int)]
        val memoryStream2 = MemoryStream[(String, Int)]

        val data1 = memoryStream1.toDF()
          .selectExpr("_1 AS key", "timestamp_seconds(_2) AS eventTime")
          .withWatermark("eventTime", "0 seconds")
        val data2 = memoryStream2.toDF()
          .selectExpr("_1 AS key", "timestamp_seconds(_2) AS eventTime")
          .withWatermark("eventTime", "0 seconds")

        val joinedDf = data1.join(data2, Seq("key", "eventTime"), "leftOuter")
          .selectExpr("key", "CAST(eventTime AS long) AS eventTime")

        def assertLeftRows(expected: Seq[Row]): AssertOnQuery = {
          assertStateStoreRows(0L, "left", expected) { df =>
            df.selectExpr("value.key", "CAST(value.eventTime AS long)")
          }
        }

        def assertRightRows(expected: Seq[Row]): AssertOnQuery = {
          assertStateStoreRows(0L, "right", expected) { df =>
            df.selectExpr("value.key", "CAST(value.eventTime AS long)")
          }
        }

        testStream(joinedDf)(
          StartStream(checkpointLocation = checkpoint.getCanonicalPath),
          // batch 0
          // WM: late record = 0, eviction = 0
          MultiAddData(
            (memoryStream1, Seq(("a", 1), ("b", 2))),
            (memoryStream2, Seq(("b", 2), ("c", 1)))
          ),
          CheckNewAnswer(("b", 2)),
          assertLeftRows(Seq(Row("a", 1), Row("b", 2))),
          assertRightRows(Seq(Row("b", 2), Row("c", 1))),
          // batch 1
          // WM: late record = 0, eviction = 2
          // Before Spark introduces multiple stateful operator, WM for late record was same as
          // WM for eviction, hence ("d", 1) was treated as late record.
          // With the multiple state operator, ("d", 1) is added in batch 1 but also evicted in
          // batch 1. Note that the eviction is happening with state watermark: for this join,
          // state watermark = state eviction under join condition. Before SPARK-49829, this
          // wasn't producing unmatched row, and it is fixed.
          AddData(memoryStream1, ("d", 1)),
          CheckNewAnswer(("a", 1), ("d", 1)),
          assertLeftRows(Seq()),
          assertRightRows(Seq())
        )
      }
    }
  }
}

@SlowSQLTest
class StreamingFullOuterJoinSuite extends StreamingJoinSuite {

  test("windowed full outer join") {
    val (leftInput, rightInput, joined) = setupWindowedJoin("full_outer")

    testStream(joined)(
      MultiAddData(leftInput, 1, 2, 3, 4, 5)(rightInput, 3, 4, 5, 6, 7),
      CheckNewAnswer(Row(3, 10, 6, 9), Row(4, 10, 8, 12), Row(5, 10, 10, 15)),
      // states
      // left: 1, 2, 3, 4 ,5
      // right: 3, 4, 5, 6, 7
      assertNumStateRows(total = 10, updated = 10),
      MultiAddData(leftInput, 21)(rightInput, 22),
      // Watermark = 11, should remove rows having window=[0,10].
      CheckNewAnswer(Row(1, 10, 2, null), Row(2, 10, 4, null), Row(6, 10, null, 18),
        Row(7, 10, null, 21)),
      // states
      // left: 21
      // right: 22
      //
      // states evicted
      // left: 1, 2, 3, 4 ,5 (below watermark)
      // right: 3, 4, 5, 6, 7 (below watermark)
      assertNumStateRows(total = 2, updated = 2),
      AddData(leftInput, 22),
      CheckNewAnswer(Row(22, 30, 44, 66)),
      // states
      // left: 21, 22
      // right: 22
      assertNumStateRows(total = 3, updated = 1),
      StopStream,
      StartStream(),

      AddData(leftInput, 1),
      // Row not add as 1 < state key watermark = 12.
      CheckNewAnswer(),
      // states
      // left: 21, 22
      // right: 22
      assertNumStateRows(total = 3, updated = 0, droppedByWatermark = 1),
      AddData(rightInput, 5),
      // Row not add as 5 < state key watermark = 12.
      CheckNewAnswer(),
      // states
      // left: 21, 22
      // right: 22
      assertNumStateRows(total = 3, updated = 0, droppedByWatermark = 1)
    )
  }

  test("full outer early state exclusion on left") {
    val (leftInput, rightInput, joined) = setupWindowedJoinWithLeftCondition("full_outer")

    testStream(joined)(
      MultiAddData(leftInput, 1, 2, 3)(rightInput, 3, 4, 5),
      // The left rows with leftValue <= 4 should generate their outer join rows now and
      // not get added to the state.
      CheckNewAnswer(Row(1, 10, 2, null, null, null), Row(2, 10, 4, null, null, null),
        Row(3, 10, 6, 3, 10, "9")),
      // states
      // left: 3
      // right: 3, 4, 5
      assertNumStateRows(total = 4, updated = 4),
      // Generate outer join result for all non-matched rows when the watermark advances.
      MultiAddData(leftInput, 20)(rightInput, 21),
      CheckNewAnswer(Row(null, null, null, 4, 10, "12"), Row(null, null, null, 5, 10, "15")),
      // states
      // left: 20
      // right: 21
      //
      // states evicted
      // left: 3 (below watermark)
      // right: 3, 4, 5 (below watermark)
      assertNumStateRows(total = 2, updated = 2),
      AddData(rightInput, 20),
      CheckNewAnswer(Row(20, 30, 40, 20, 30, "60")),
      // states
      // left: 20
      // right: 21, 20
      assertNumStateRows(total = 3, updated = 1)
    )
  }

  test("full outer early state exclusion on right") {
    val (leftInput, rightInput, joined) = setupWindowedJoinWithRightCondition("full_outer")

    testStream(joined)(
      MultiAddData(leftInput, 3, 4, 5)(rightInput, 1, 2, 3),
      // The right rows with rightValue <= 7 should generate their outer join rows now,
      // and never be added to the state.
      // The right row with rightValue = 9 > 7, hence joined and added to state.
      CheckNewAnswer(Row(null, null, null, 1, 10, "3"), Row(null, null, null, 2, 10, "6"),
        Row(3, 10, 6, 3, 10, "9")),
      // states
      // left: 3, 4, 5
      // right: 3
      assertNumStateRows(total = 4, updated = 4),
      // Generate outer join result for all non-matched rows when the watermark advances.
      MultiAddData(leftInput, 20)(rightInput, 21),
      CheckNewAnswer(Row(4, 10, 8, null, null, null), Row(5, 10, 10, null, null, null)),
      // states
      // left: 20
      // right: 21
      //
      // states evicted
      // left: 3, 4, 5 (below watermark)
      // right: 3 (below watermark)
      assertNumStateRows(total = 2, updated = 2),
      AddData(rightInput, 20),
      CheckNewAnswer(Row(20, 30, 40, 20, 30, "60")),
      // states
      // left: 20
      // right: 21, 20
      assertNumStateRows(total = 3, updated = 1)
    )
  }

  test("full outer join with watermark range condition") {
    val (leftInput, rightInput, joined) = setupJoinWithRangeCondition("full_outer")

    testStream(joined)(
      AddData(leftInput, (1, 5), (3, 5)),
      CheckNewAnswer(),
      // states
      // left: (1, 5), (3, 5)
      // right: nothing
      assertNumStateRows(total = 2, updated = 2),
      AddData(rightInput, (1, 10), (2, 5)),
      // Match left row in the state.
      CheckNewAnswer(Row(1, 1, 5, 10)),
      // states
      // left: (1, 5), (3, 5)
      // right: (1, 10), (2, 5)
      assertNumStateRows(total = 4, updated = 2),
      AddData(rightInput, (1, 9)),
      // Match left row in the state.
      CheckNewAnswer(Row(1, 1, 5, 9)),
      // states
      // left: (1, 5), (3, 5)
      // right: (1, 10), (2, 5), (1, 9)
      assertNumStateRows(total = 5, updated = 1),
      // Increase event time watermark to 20s by adding data with time = 30s on both inputs.
      AddData(leftInput, (1, 7), (1, 30)),
      CheckNewAnswer(Row(1, 1, 7, 9), Row(1, 1, 7, 10)),
      // states
      // left: (1, 5), (3, 5), (1, 7), (1, 30)
      // right: (1, 10), (2, 5), (1, 9)
      assertNumStateRows(total = 7, updated = 2),
      // Watermark = 30 - 10 = 20, no matched row.
      // Generate outer join result for all non-matched rows when the watermark advances.
      AddData(rightInput, (0, 30)),
      CheckNewAnswer(Row(3, null, 5, null), Row(null, 2, null, 5)),
      // states
      // left: (1, 30)
      // right: (0, 30)
      //
      // states evicted
      // left: (1, 5), (3, 5), (1, 5) (below watermark = 20)
      // right: (1, 10), (2, 5), (1, 9) (below watermark = 20)
      assertNumStateRows(total = 2, updated = 1)
    )
  }

  test("self full outer join") {
    val (inputStream, query) = setupSelfJoin("full_outer")

    testStream(query)(
      AddData(inputStream, (1, 1L), (2, 2L), (3, 3L), (4, 4L), (5, 5L)),
      CheckNewAnswer(Row(2, 2L, 2, 2L), Row(4, 4L, 4, 4L)),
      // batch 1 - global watermark = 0
      // states
      // left: (1, 1L), (2, 2L), (3, 3L), (4, 4L), (5, 5L)
      // right: (2, 2L), (4, 4L)
      assertNumStateRows(total = 7, updated = 7),
      AddData(inputStream, (6, 6L), (7, 7L), (8, 8L), (9, 9L), (10, 10L)),
      CheckNewAnswer(Row(6, 6L, 6, 6L), Row(8, 8L, 8, 8L), Row(10, 10L, 10, 10L)),
      // batch 2 - global watermark = 5
      // states
      // left: (1, 1L), (2, 2L), (3, 3L), (4, 4L), (5, 5L), (6, 6L), (7, 7L), (8, 8L),
      //       (9, 9L), (10, 10L)
      // right: (6, 6L), (8, 8L), (10, 10L)
      //
      // states evicted
      // left: nothing (it waits for 5 seconds more than watermark due to join condition)
      // right: (2, 2L), (4, 4L)
      assertNumStateRows(total = 13, updated = 8),
      AddData(inputStream, (11, 11L), (12, 12L), (13, 13L), (14, 14L), (15, 15L)),
      CheckNewAnswer(Row(12, 12L, 12, 12L), Row(14, 14L, 14, 14L), Row(1, 1L, null, null),
        Row(3, 3L, null, null)),
      // batch 3 - global watermark = 9
      // states
      // left: (4, 4L), (5, 5L), (6, 6L), (7, 7L), (8, 8L), (9, 9L), (10, 10L), (11, 11L),
      //       (12, 12L), (13, 13L), (14, 14L), (15, 15L)
      // right: (10, 10L), (12, 12L), (14, 14L)
      //
      // states evicted
      // left: (1, 1L), (2, 2L), (3, 3L)
      // right: (6, 6L), (8, 8L)
      assertNumStateRows(total = 15, updated = 7)
    )
  }
}

@SlowSQLTest
class StreamingLeftSemiJoinSuite extends StreamingJoinSuite {

  import testImplicits._

  test("windowed left semi join") {
    val (leftInput, rightInput, joined) = setupWindowedJoin("left_semi")

    testStream(joined)(
      MultiAddData(leftInput, 1, 2, 3, 4, 5)(rightInput, 3, 4, 5, 6, 7),
      CheckNewAnswer(Row(3, 10, 6), Row(4, 10, 8), Row(5, 10, 10)),
      // states
      // left: 1, 2, 3, 4 ,5
      // right: 3, 4, 5, 6, 7
      assertNumStateRows(total = 10, updated = 10),
      MultiAddData(leftInput, 21)(rightInput, 22),
      // Watermark = 11, should remove rows having window=[0,10].
      CheckNewAnswer(),
      // states
      // left: 21
      // right: 22
      //
      // states evicted
      // left: 1, 2, 3, 4 ,5 (below watermark)
      // right: 3, 4, 5, 6, 7 (below watermark)
      assertNumStateRows(total = 2, updated = 2),
      AddData(leftInput, 22),
      CheckNewAnswer(Row(22, 30, 44)),
      // Unlike inner/outer joins, given left input row matches with right input row,
      // we don't buffer the matched left input row to the state store.
      //
      // states
      // left: 21
      // right: 22
      assertNumStateRows(total = 2, updated = 0),
      StopStream,
      StartStream(),

      AddData(leftInput, 1),
      // Row not add as 1 < state key watermark = 12.
      CheckNewAnswer(),
      // states
      // left: 21
      // right: 22
      assertNumStateRows(total = 2, updated = 0, droppedByWatermark = 1),
      AddData(rightInput, 5),
      // Row not add as 5 < state key watermark = 12.
      CheckNewAnswer(),
      // states
      // left: 21
      // right: 22
      assertNumStateRows(total = 2, updated = 0, droppedByWatermark = 1)
    )
  }

  test("left semi early state exclusion on left") {
    val (leftInput, rightInput, joined) = setupWindowedJoinWithLeftCondition("left_semi")

    testStream(joined)(
      MultiAddData(leftInput, 1, 2, 3)(rightInput, 3, 4, 5),
      // The left rows with leftValue <= 4 should not generate their semi join rows and
      // not get added to the state.
      CheckNewAnswer(Row(3, 10, 6)),
      // states
      // left: 3
      // right: 3, 4, 5
      assertNumStateRows(total = 4, updated = 4),
      // We shouldn't get more semi join rows when the watermark advances.
      MultiAddData(leftInput, 20)(rightInput, 21),
      CheckNewAnswer(),
      // states
      // left: 20
      // right: 21
      //
      // states evicted
      // left: 3 (below watermark)
      // right: 3, 4, 5 (below watermark)
      assertNumStateRows(total = 2, updated = 2),
      AddData(rightInput, 20),
      CheckNewAnswer((20, 30, 40)),
      // states
      // left: 20
      // right: 21, 20
      assertNumStateRows(total = 3, updated = 1)
    )
  }

  test("left semi early state exclusion on right") {
    val (leftInput, rightInput, joined) = setupWindowedJoinWithRightCondition("left_semi")

    testStream(joined)(
      MultiAddData(leftInput, 3, 4, 5)(rightInput, 1, 2, 3),
      // The right rows with rightValue <= 7 should never be added to the state.
      // The right row with rightValue = 9 > 7, hence joined and added to state.
      CheckNewAnswer(Row(3, 10, 6)),
      // states
      // left: 3, 4, 5
      // right: 3
      assertNumStateRows(total = 4, updated = 4),
      // We shouldn't get more semi join rows when the watermark advances.
      MultiAddData(leftInput, 20)(rightInput, 21),
      CheckNewAnswer(),
      // states
      // left: 20
      // right: 21
      //
      // states evicted
      // left: 3, 4, 5 (below watermark)
      // right: 3 (below watermark)
      assertNumStateRows(total = 2, updated = 2),
      AddData(rightInput, 20),
      CheckNewAnswer((20, 30, 40)),
      // states
      // left: 20
      // right: 21, 20
      assertNumStateRows(total = 3, updated = 1)
    )
  }

  test("left semi join with watermark range condition") {
    val (leftInput, rightInput, joined) = setupJoinWithRangeCondition("left_semi")

    testStream(joined)(
      AddData(leftInput, (1, 5), (3, 5)),
      CheckNewAnswer(),
      // states
      // left: (1, 5), (3, 5)
      // right: nothing
      assertNumStateRows(total = 2, updated = 2),
      AddData(rightInput, (1, 10), (2, 5)),
      // Match left row in the state.
      CheckNewAnswer((1, 5)),
      // states
      // left: (1, 5), (3, 5)
      // right: (1, 10), (2, 5)
      assertNumStateRows(total = 4, updated = 2),
      AddData(rightInput, (1, 9)),
      // No match as left row is already matched.
      CheckNewAnswer(),
      // states
      // left: (1, 5), (3, 5)
      // right: (1, 10), (2, 5), (1, 9)
      assertNumStateRows(total = 5, updated = 1),
      // Increase event time watermark to 20s by adding data with time = 30s on both inputs.
      AddData(leftInput, (1, 7), (1, 30)),
      CheckNewAnswer((1, 7)),
      // states
      // left: (1, 5), (3, 5), (1, 30)
      // right: (1, 10), (2, 5), (1, 9)
      assertNumStateRows(total = 6, updated = 1),
      // Watermark = 30 - 10 = 20, no matched row.
      AddData(rightInput, (0, 30)),
      CheckNewAnswer(),
      // states
      // left: (1, 30)
      // right: (0, 30)
      //
      // states evicted
      // left: (1, 5), (3, 5) (below watermark = 20)
      // right: (1, 10), (2, 5), (1, 9) (below watermark = 20)
      assertNumStateRows(total = 2, updated = 1)
    )
  }

  test("self left semi join") {
    val (inputStream, query) = setupSelfJoin("left_semi")

    testStream(query)(
      AddData(inputStream, (1, 1L), (2, 2L), (3, 3L), (4, 4L), (5, 5L)),
      CheckNewAnswer((2, 2), (4, 4)),
      // batch 1 - global watermark = 0
      // states
      // left: (2, 2L), (4, 4L)
      //       (left rows with value % 2 != 0 is filtered per [[PushPredicateThroughJoin]])
      // right: (2, 2L), (4, 4L)
      //       (right rows with value % 2 != 0 is filtered per [[PushPredicateThroughJoin]])
      assertNumStateRows(total = 4, updated = 4),
      AddData(inputStream, (6, 6L), (7, 7L), (8, 8L), (9, 9L), (10, 10L)),
      CheckNewAnswer((6, 6), (8, 8), (10, 10)),
      // batch 2 - global watermark = 5
      // states
      // left: (2, 2L), (4, 4L), (6, 6L), (8, 8L), (10, 10L)
      // right: (6, 6L), (8, 8L), (10, 10L)
      //
      // states evicted
      // left: nothing (it waits for 5 seconds more than watermark due to join condition)
      // right: (2, 2L), (4, 4L)
      assertNumStateRows(total = 8, updated = 6),
      AddData(inputStream, (11, 11L), (12, 12L), (13, 13L), (14, 14L), (15, 15L)),
      CheckNewAnswer((12, 12), (14, 14)),
      // batch 3 - global watermark = 9
      // states
      // left: (4, 4L), (6, 6L), (8, 8L), (10, 10L), (12, 12L), (14, 14L)
      // right: (10, 10L), (12, 12L), (14, 14L)
      //
      // states evicted
      // left: (2, 2L)
      // right: (6, 6L), (8, 8L)
      assertNumStateRows(total = 9, updated = 4)
    )
  }

  test("SPARK-49829 two chained stream-stream left outer joins among three input streams") {
    withSQLConf(SQLConf.STREAMING_NO_DATA_MICRO_BATCHES_ENABLED.key -> "false") {
      val memoryStream1 = MemoryStream[(Long, Int)]
      val memoryStream2 = MemoryStream[(Long, Int)]
      val memoryStream3 = MemoryStream[(Long, Int)]

      val data1 = memoryStream1.toDF()
        .selectExpr("timestamp_seconds(_1) AS eventTime", "_2 AS v1")
        .withWatermark("eventTime", "0 seconds")
      val data2 = memoryStream2.toDF()
        .selectExpr("timestamp_seconds(_1) AS eventTime", "_2 AS v2")
        .withWatermark("eventTime", "0 seconds")
      val data3 = memoryStream3.toDF()
        .selectExpr("timestamp_seconds(_1) AS eventTime", "_2 AS v3")
        .withWatermark("eventTime", "0 seconds")

      val join = data1
        .join(data2, Seq("eventTime"), "leftOuter")
        .join(data3, Seq("eventTime"), "leftOuter")
        .selectExpr("CAST(eventTime AS long) AS eventTime", "v1", "v2", "v3")

      def assertLeftRowsFor1stJoin(expected: Seq[Row]): AssertOnQuery = {
        assertStateStoreRows(1L, "left", expected) { df =>
          df.selectExpr("CAST(value.eventTime AS long)", "value.v1")
        }
      }

      def assertRightRowsFor1stJoin(expected: Seq[Row]): AssertOnQuery = {
        assertStateStoreRows(1L, "right", expected) { df =>
          df.selectExpr("CAST(value.eventTime AS long)", "value.v2")
        }
      }

      def assertLeftRowsFor2ndJoin(expected: Seq[Row]): AssertOnQuery = {
        assertStateStoreRows(0L, "left", expected) { df =>
          df.selectExpr("CAST(value.eventTime AS long)", "value.v1", "value.v2")
        }
      }

      def assertRightRowsFor2ndJoin(expected: Seq[Row]): AssertOnQuery = {
        assertStateStoreRows(0L, "right", expected) { df =>
          df.selectExpr("CAST(value.eventTime AS long)", "value.v3")
        }
      }

      testStream(join)(
        // batch 0
        // WM: late event = 0, eviction = 0
        MultiAddData(
          (memoryStream1, Seq((20L, 1))),
          (memoryStream2, Seq((20L, 1))),
          (memoryStream3, Seq((20L, 1)))
        ),
        CheckNewAnswer((20, 1, 1, 1)),
        assertLeftRowsFor1stJoin(Seq(Row(20, 1))),
        assertRightRowsFor1stJoin(Seq(Row(20, 1))),
        assertLeftRowsFor2ndJoin(Seq(Row(20, 1, 1))),
        assertRightRowsFor2ndJoin(Seq(Row(20, 1))),
        // batch 1
        // WM: late event = 0, eviction = 20
        MultiAddData(
          (memoryStream1, Seq((21L, 2))),
          (memoryStream2, Seq((21L, 2)))
        ),
        CheckNewAnswer(),
        assertLeftRowsFor1stJoin(Seq(Row(21, 2))),
        assertRightRowsFor1stJoin(Seq(Row(21, 2))),
        assertLeftRowsFor2ndJoin(Seq(Row(21, 2, 2))),
        assertRightRowsFor2ndJoin(Seq()),
        // batch 2
        // WM: late event = 20, eviction = 20 (slowest: inputStream3)
        MultiAddData(
          (memoryStream1, Seq((22L, 3))),
          (memoryStream3, Seq((22L, 3)))
        ),
        CheckNewAnswer(),
        assertLeftRowsFor1stJoin(Seq(Row(21, 2), Row(22, 3))),
        assertRightRowsFor1stJoin(Seq(Row(21, 2))),
        assertLeftRowsFor2ndJoin(Seq(Row(21, 2, 2))),
        assertRightRowsFor2ndJoin(Seq(Row(22, 3))),
        // batch 3
        // WM: late event = 20, eviction = 21 (slowest: inputStream2)
        AddData(memoryStream1, (23L, 4)),
        CheckNewAnswer(Row(21, 2, 2, null)),
        assertLeftRowsFor1stJoin(Seq(Row(22, 3), Row(23, 4))),
        assertRightRowsFor1stJoin(Seq()),
        assertLeftRowsFor2ndJoin(Seq()),
        assertRightRowsFor2ndJoin(Seq(Row(22, 3))),
        // batch 4
        // WM: late event = 21, eviction = 21 (slowest: inputStream2)
        MultiAddData(
          (memoryStream1, Seq((24L, 5))),
          (memoryStream2, Seq((24L, 5))),
          (memoryStream3, Seq((24L, 5)))
        ),
        CheckNewAnswer(Row(24, 5, 5, 5)),
        assertLeftRowsFor1stJoin(Seq(Row(22, 3), Row(23, 4), Row(24, 5))),
        assertRightRowsFor1stJoin(Seq(Row(24, 5))),
        assertLeftRowsFor2ndJoin(Seq(Row(24, 5, 5))),
        assertRightRowsFor2ndJoin(Seq(Row(22, 3), Row(24, 5))),
        // batch 5
        // WM: late event = 21, eviction = 24
        // just trigger a new batch with arbitrary data as the original test relies on no-data
        // batch, and we need to check with remaining unmatched outputs
        AddData(memoryStream1, (100L, 6)),
        // Before SPARK-49829, the test fails because (23, 4, null, null) wasn't produced.
        // (The assertion of state for left inputs & right inputs weren't included on the test
        // before SPARK-49829.)
        CheckNewAnswer(Row(22, 3, null, 3), Row(23, 4, null, null))
      )

      /*
      // The collection of the above new answers is the same with below in original test:
      val expected = Array(
        Row(Timestamp.valueOf("2024-02-10 10:20:00"), 1, 1, 1),
        Row(Timestamp.valueOf("2024-02-10 10:21:00"), 2, 2, null),
        Row(Timestamp.valueOf("2024-02-10 10:22:00"), 3, null, 3),
        Row(Timestamp.valueOf("2024-02-10 10:23:00"), 4, null, null),
        Row(Timestamp.valueOf("2024-02-10 10:24:00"), 5, 5, 5),
      )
       */
    }
  }
}
