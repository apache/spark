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

import java.util.UUID

import scala.util.Random

import org.scalatest.BeforeAndAfter

import org.apache.spark.scheduler.ExecutorCacheTaskLocation
import org.apache.spark.sql.{AnalysisException, DataFrame, Row, SparkSession}
import org.apache.spark.sql.catalyst.analysis.StreamingJoinHelper
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, AttributeSet, Literal}
import org.apache.spark.sql.catalyst.plans.logical.{EventTimeWatermark, Filter}
import org.apache.spark.sql.catalyst.trees.TreeNode
import org.apache.spark.sql.execution.{FileSourceScanExec, LogicalRDD}
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution.streaming.{MemoryStream, StatefulOperatorStateInfo, StreamingSymmetricHashJoinHelper}
import org.apache.spark.sql.execution.streaming.state.{StateStore, StateStoreProviderId}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.util.Utils


class StreamingInnerJoinSuite extends StreamTest with StateStoreMetricsTest with BeforeAndAfter {

  before {
    SparkSession.setActiveSession(spark)  // set this before force initializing 'joinExec'
    spark.streams.stateStoreCoordinator   // initialize the lazy coordinator
  }

  after {
    StateStore.stop()
  }

  import testImplicits._
  test("stream stream inner join on non-time column") {
    val input1 = MemoryStream[Int]
    val input2 = MemoryStream[Int]

    val df1 = input1.toDF.select('value as "key", ('value * 2) as "leftValue")
    val df2 = input2.toDF.select('value as "key", ('value * 3) as "rightValue")
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

    val df1 = input1.toDF
      .select('value as "key", 'value.cast("timestamp") as "timestamp", ('value * 2) as "leftValue")
      .select('key, window('timestamp, "10 second"), 'leftValue)

    val df2 = input2.toDF
      .select('value as "key", 'value.cast("timestamp") as "timestamp",
        ('value * 3) as "rightValue")
      .select('key, window('timestamp, "10 second"), 'rightValue)

    val joined = df1.join(df2, Seq("key", "window"))
      .select('key, $"window.end".cast("long"), 'leftValue, 'rightValue)

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

    val df1 = input1.toDF
      .select('value as "key", 'value.cast("timestamp") as "timestamp", ('value * 2) as "leftValue")
      .withWatermark("timestamp", "10 seconds")
      .select('key, window('timestamp, "10 second"), 'leftValue)

    val df2 = input2.toDF
      .select('value as "key", 'value.cast("timestamp") as "timestamp",
        ('value * 3) as "rightValue")
      .select('key, window('timestamp, "10 second"), 'rightValue)

    val joined = df1.join(df2, Seq("key", "window"))
      .select('key, $"window.end".cast("long"), 'leftValue, 'rightValue)

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
      assertNumStateRows(total = 2, updated = 0)
    )
  }

  test("stream stream inner join with time range - with watermark - one side condition") {
    import org.apache.spark.sql.functions._

    val leftInput = MemoryStream[(Int, Int)]
    val rightInput = MemoryStream[(Int, Int)]

    val df1 = leftInput.toDF.toDF("leftKey", "time")
      .select('leftKey, 'time.cast("timestamp") as "leftTime", ('leftKey * 2) as "leftValue")
      .withWatermark("leftTime", "10 seconds")

    val df2 = rightInput.toDF.toDF("rightKey", "time")
      .select('rightKey, 'time.cast("timestamp") as "rightTime", ('rightKey * 3) as "rightValue")
      .withWatermark("rightTime", "10 seconds")

    val joined =
      df1.join(df2, expr("leftKey = rightKey AND leftTime < rightTime - interval 5 seconds"))
        .select('leftKey, 'leftTime.cast("int"), 'rightTime.cast("int"))

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
      assertNumStateRows(total = 5, updated = 1),

      // New data to left input with leftTime <= 20 should be filtered due to event time watermark
      AddData(leftInput, (1, 20), (1, 21)),
      CheckNewAnswer((1, 21, 28)),
      assertNumStateRows(total = 6, updated = 1)
    )
  }

  test("stream stream inner join with time range - with watermark - two side conditions") {
    import org.apache.spark.sql.functions._

    val leftInput = MemoryStream[(Int, Int)]
    val rightInput = MemoryStream[(Int, Int)]

    val df1 = leftInput.toDF.toDF("leftKey", "time")
      .select('leftKey, 'time.cast("timestamp") as "leftTime", ('leftKey * 2) as "leftValue")
      .withWatermark("leftTime", "20 seconds")

    val df2 = rightInput.toDF.toDF("rightKey", "time")
      .select('rightKey, 'time.cast("timestamp") as "rightTime", ('rightKey * 3) as "rightValue")
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
      df1.join(df2, condition).select('leftKey, 'leftTime.cast("int"), 'rightTime.cast("int"))

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
      assertNumStateRows(total = 11, updated = 1),  // only 31 added

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
      assertNumStateRows(total = 7, updated = 1)   // 50 added
    )
  }

  testQuietly("stream stream inner join without equality predicate") {
    val input1 = MemoryStream[Int]
    val input2 = MemoryStream[Int]

    val df1 = input1.toDF.select('value as "leftKey", ('value * 2) as "leftValue")
    val df2 = input2.toDF.select('value as "rightKey", ('value * 3) as "rightValue")
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
    val df = input.toDF
    val join =
      df.select('value % 5 as "key", 'value).join(
        df.select('value % 5 as "key", 'value), "key")

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
      val path = Utils.createDirectory(tempDir.getAbsolutePath, Random.nextFloat.toString).toString
      val stateInfo = StatefulOperatorStateInfo(path, queryId, opId, 0L, 5)

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
        coordinatorRef.reportActiveInstance(providerId, hostName, s"exec-$hostName")
        require(
          coordinatorRef.getLocation(providerId) ===
            Some(ExecutorCacheTaskLocation(hostName, s"exec-$hostName").toString))
      }

      val rdd1 = spark.sparkContext.makeRDD(1 to 10, numPartitions)
      val rdd2 = spark.sparkContext.makeRDD((1 to 10).map(_.toString), numPartitions)
      val rdd = rdd1.stateStoreAwareZipPartitions(rdd2, stateInfo, storeNames, coordinatorRef) {
        (left, right) => left.zip(right)
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

    val df1 = input1.toDF.select('value as "leftKey", ('value * 2) as "leftValue")
    val df2 = input2.toDF.select('value as "middleKey", ('value * 3) as "middleValue")
    val df3 = input3.toDF.select('value as "rightKey", ('value * 5) as "rightValue")

    val joined = df1.join(df2, expr("leftKey = middleKey")).join(df3, expr("rightKey = middleKey"))

    testStream(joined)(
      AddData(input1, 1, 5),
      AddData(input2, 1, 5, 10),
      AddData(input3, 5, 10),
      CheckNewAnswer((5, 10, 5, 15, 5, 25)))
  }

  test("streaming join should require HashClusteredDistribution from children") {
    val input1 = MemoryStream[Int]
    val input2 = MemoryStream[Int]

    val df1 = input1.toDF.select('value as 'a, 'value * 2 as 'b)
    val df2 = input2.toDF.select('value as 'a, 'value * 2 as 'b).repartition('b)
    val joined = df1.join(df2, Seq("a", "b")).select('a)

    testStream(joined)(
      AddData(input1, 1.to(1000): _*),
      AddData(input2, 1.to(1000): _*),
      CheckAnswer(1.to(1000): _*))
  }
}


class StreamingOuterJoinSuite extends StreamTest with StateStoreMetricsTest with BeforeAndAfter {

  import testImplicits._
  import org.apache.spark.sql.functions._

  before {
    SparkSession.setActiveSession(spark) // set this before force initializing 'joinExec'
    spark.streams.stateStoreCoordinator // initialize the lazy coordinator
  }

  after {
    StateStore.stop()
  }

  private def setupStream(prefix: String, multiplier: Int): (MemoryStream[Int], DataFrame) = {
    val input = MemoryStream[Int]
    val df = input.toDF
      .select(
        'value as "key",
        'value.cast("timestamp") as s"${prefix}Time",
        ('value * multiplier) as s"${prefix}Value")
      .withWatermark(s"${prefix}Time", "10 seconds")

    return (input, df)
  }

  private def setupWindowedJoin(joinType: String):
  (MemoryStream[Int], MemoryStream[Int], DataFrame) = {
    val (input1, df1) = setupStream("left", 2)
    val (input2, df2) = setupStream("right", 3)
    val windowed1 = df1.select('key, window('leftTime, "10 second"), 'leftValue)
    val windowed2 = df2.select('key, window('rightTime, "10 second"), 'rightValue)
    val joined = windowed1.join(windowed2, Seq("key", "window"), joinType)
      .select('key, $"window.end".cast("long"), 'leftValue, 'rightValue)

    (input1, input2, joined)
  }

  test("left outer early state exclusion on left") {
    val (leftInput, df1) = setupStream("left", 2)
    val (rightInput, df2) = setupStream("right", 3)
    // Use different schemas to ensure the null row is being generated from the correct side.
    val left = df1.select('key, window('leftTime, "10 second"), 'leftValue)
    val right = df2.select('key, window('rightTime, "10 second"), 'rightValue.cast("string"))

    val joined = left.join(
        right,
        left("key") === right("key")
          && left("window") === right("window")
          && 'leftValue > 4,
        "left_outer")
        .select(left("key"), left("window.end").cast("long"), 'leftValue, 'rightValue)

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
    val (leftInput, df1) = setupStream("left", 2)
    val (rightInput, df2) = setupStream("right", 3)
    // Use different schemas to ensure the null row is being generated from the correct side.
    val left = df1.select('key, window('leftTime, "10 second"), 'leftValue)
    val right = df2.select('key, window('rightTime, "10 second"), 'rightValue.cast("string"))

    val joined = left.join(
      right,
      left("key") === right("key")
        && left("window") === right("window")
        && 'rightValue.cast("int") > 7,
      "left_outer")
      .select(left("key"), left("window.end").cast("long"), 'leftValue, 'rightValue)

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
    val (leftInput, df1) = setupStream("left", 2)
    val (rightInput, df2) = setupStream("right", 3)
    // Use different schemas to ensure the null row is being generated from the correct side.
    val left = df1.select('key, window('leftTime, "10 second"), 'leftValue)
    val right = df2.select('key, window('rightTime, "10 second"), 'rightValue.cast("string"))

    val joined = left.join(
      right,
      left("key") === right("key")
        && left("window") === right("window")
        && 'leftValue > 4,
      "right_outer")
      .select(right("key"), right("window.end").cast("long"), 'leftValue, 'rightValue)

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
    val (leftInput, df1) = setupStream("left", 2)
    val (rightInput, df2) = setupStream("right", 3)
    // Use different schemas to ensure the null row is being generated from the correct side.
    val left = df1.select('key, window('leftTime, "10 second"), 'leftValue)
    val right = df2.select('key, window('rightTime, "10 second"), 'rightValue.cast("string"))

    val joined = left.join(
      right,
      left("key") === right("key")
        && left("window") === right("window")
        && 'rightValue.cast("int") > 7,
      "right_outer")
      .select(right("key"), right("window.end").cast("long"), 'leftValue, 'rightValue)

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
    test(s"${joinType.replaceAllLiterally("_", " ")} with watermark range condition") {
      import org.apache.spark.sql.functions._

      val leftInput = MemoryStream[(Int, Int)]
      val rightInput = MemoryStream[(Int, Int)]

      val df1 = leftInput.toDF.toDF("leftKey", "time")
        .select('leftKey, 'time.cast("timestamp") as "leftTime", ('leftKey * 2) as "leftValue")
        .withWatermark("leftTime", "10 seconds")

      val df2 = rightInput.toDF.toDF("rightKey", "time")
        .select('rightKey, 'time.cast("timestamp") as "rightTime", ('rightKey * 3) as "rightValue")
        .withWatermark("rightTime", "10 seconds")

      val joined =
        df1.join(
          df2,
          expr("leftKey = rightKey AND " +
            "leftTime BETWEEN rightTime - interval 5 seconds AND rightTime + interval 5 seconds"),
          joinType)
          .select('leftKey, 'rightKey, 'leftTime.cast("int"), 'rightTime.cast("int"))
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
    }
  }

  // When the join condition isn't true, the outer null rows must be generated, even if the join
  // keys themselves have a match.
  test("left outer join with non-key condition violated") {
    val (leftInput, simpleLeftDf) = setupStream("left", 2)
    val (rightInput, simpleRightDf) = setupStream("right", 3)

    val left = simpleLeftDf.select('key, window('leftTime, "10 second"), 'leftValue)
    val right = simpleRightDf.select('key, window('rightTime, "10 second"), 'rightValue)

    val joined = left.join(
        right,
        left("key") === right("key") && left("window") === right("window") &&
            'leftValue > 10 && ('rightValue < 300 || 'rightValue > 1000),
        "left_outer")
      .select(left("key"), left("window.end").cast("long"), 'leftValue, 'rightValue)

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
}

