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
import org.apache.spark.sql.execution.LogicalRDD
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
      CheckLastBatch((1, 2, 3)),
      AddData(input1, 10),          // 10 arrived on input2 first, then input1, should join
      CheckLastBatch((10, 20, 30)),
      AddData(input2, 1),           // another 1 in input2 should join with 1 input1
      CheckLastBatch((1, 2, 3)),
      StopStream,
      StartStream(),
      AddData(input1, 1), // multiple 1s should be kept in state causing multiple (1, 2, 3)
      CheckLastBatch((1, 2, 3), (1, 2, 3)),
      StopStream,
      StartStream(),
      AddData(input1, 100),
      AddData(input2, 100),
      CheckLastBatch((100, 200, 300))
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
      CheckLastBatch(),
      AddData(input2, 1),
      CheckLastBatch((1, 10, 2, 3)),
      StopStream,
      StartStream(),
      AddData(input1, 25),
      CheckLastBatch(),
      StopStream,
      StartStream(),
      AddData(input2, 25),
      CheckLastBatch((25, 30, 50, 75)),
      AddData(input1, 1),
      CheckLastBatch((1, 10, 2, 3)),      // State for 1 still around as there is no watermark
      StopStream,
      StartStream(),
      AddData(input1, 5),
      CheckLastBatch(),
      AddData(input2, 5),
      CheckLastBatch((5, 10, 10, 15))     // No filter by any watermark
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
      CheckLastBatch((1, 10, 2, 3)),
      assertNumStateRows(total = 2, updated = 1),
      StopStream,
      StartStream(),

      AddData(input1, 25),
      CheckLastBatch(), // since there is only 1 watermark operator, the watermark should be 15
      assertNumStateRows(total = 3, updated = 1),

      AddData(input2, 25),
      CheckLastBatch((25, 30, 50, 75)), // watermark = 15 should remove 2 rows having window=[0,10]
      assertNumStateRows(total = 2, updated = 1),
      StopStream,
      StartStream(),

      AddData(input2, 1),
      CheckLastBatch(),       // Should not join as < 15 removed
      assertNumStateRows(total = 2, updated = 0),  // row not add as 1 < state key watermark = 15

      AddData(input1, 5),
      CheckLastBatch(),       // Should not join or add to state as < 15 got filtered by watermark
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
      CheckLastBatch((1, 5, 11)),
      AddData(rightInput, (1, 10)),
      CheckLastBatch(), // no match as neither 5, nor 10 from leftTime is less than rightTime 10 - 5
      assertNumStateRows(total = 3, updated = 1),

      // Increase event time watermark to 20s by adding data with time = 30s on both inputs
      AddData(leftInput, (1, 3), (1, 30)),
      CheckLastBatch((1, 3, 10), (1, 3, 11)),
      assertNumStateRows(total = 5, updated = 2),
      AddData(rightInput, (0, 30)),
      CheckLastBatch(),
      assertNumStateRows(total = 6, updated = 1),

      // event time watermark:    max event time - 10   ==>   30 - 10 = 20
      // right side state constraint:    20 < leftTime < rightTime - 5   ==>   rightTime > 25

      // Run another batch with event time = 25 to clear right state where rightTime <= 25
      AddData(rightInput, (0, 30)),
      CheckLastBatch(),
      assertNumStateRows(total = 5, updated = 1),  // removed (1, 11) and (1, 10), added (0, 30)

      // New data to right input should match with left side (1, 3) and (1, 5), as left state should
      // not be cleared. But rows rightTime <= 20 should be filtered due to event time watermark and
      // state rows with rightTime <= 25 should be removed from state.
      // (1, 20) ==> filtered by event time watermark = 20
      // (1, 21) ==> passed filter, matched with left (1, 3) and (1, 5), not added to state
      //             as state watermark = 25
      // (1, 28) ==> passed filter, matched with left (1, 3) and (1, 5), added to state
      AddData(rightInput, (1, 20), (1, 21), (1, 28)),
      CheckLastBatch((1, 3, 21), (1, 5, 21), (1, 3, 28), (1, 5, 28)),
      assertNumStateRows(total = 6, updated = 1),

      // New data to left input with leftTime <= 20 should be filtered due to event time watermark
      AddData(leftInput, (1, 20), (1, 21)),
      CheckLastBatch((1, 21, 28)),
      assertNumStateRows(total = 7, updated = 1)
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
      CheckLastBatch((1, 20, 15), (1, 20, 25), (1, 20, 26), (1, 20, 30)),
      assertNumStateRows(total = 7, updated = 6),

      // If rightTime = 60, then it matches only leftTime = [50, 65]
      AddData(rightInput, (1, 60)),
      CheckLastBatch(),                // matches with nothing on the left
      AddData(leftInput, (1, 49), (1, 50), (1, 65), (1, 66)),
      CheckLastBatch((1, 50, 60), (1, 65, 60)),
      assertNumStateRows(total = 12, updated = 4),

      // Event time watermark = min(left: 66 - delay 20 = 46, right: 60 - delay 30 = 30) = 30
      // Left state value watermark = 30 - 10 = slightly less than 20 (since condition has <=)
      //    Should drop < 20 from left, i.e., none
      // Right state value watermark = 30 - 5 = slightly less than 25 (since condition has <=)
      //    Should drop < 25 from the right, i.e., 14 and 15
      AddData(leftInput, (1, 30), (1, 31)),     // 30 should not be processed or added to stat
      CheckLastBatch((1, 31, 26), (1, 31, 30), (1, 31, 31)),
      assertNumStateRows(total = 11, updated = 1),  // 12 - 2 removed + 1 added

      // Advance the watermark
      AddData(rightInput, (1, 80)),
      CheckLastBatch(),
      assertNumStateRows(total = 12, updated = 1),

      // Event time watermark = min(left: 66 - delay 20 = 46, right: 80 - delay 30 = 50) = 46
      // Left state value watermark = 46 - 10 = slightly less than 36 (since condition has <=)
      //    Should drop < 36 from left, i.e., 20, 31 (30 was not added)
      // Right state value watermark = 46 - 5 = slightly less than 41 (since condition has <=)
      //    Should drop < 41 from the right, i.e., 25, 26, 30, 31
      AddData(rightInput, (1, 50)),
      CheckLastBatch((1, 49, 50), (1, 50, 50)),
      assertNumStateRows(total = 7, updated = 1)  // 12 - 6 removed + 1 added
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
    assert(e.toString.contains("Stream stream joins without equality predicate is not supported"))
  }

  testQuietly("extract watermark from time condition") {
    val attributesToFindConstraintFor = Seq(
      AttributeReference("leftTime", TimestampType)(),
      AttributeReference("leftOther", IntegerType)())
    val metadataWithWatermark = new MetadataBuilder()
      .putLong(EventTimeWatermark.delayKey, 1000)
      .build()
    val attributesWithWatermark = Seq(
      AttributeReference("rightTime", TimestampType, metadata = metadataWithWatermark)(),
      AttributeReference("rightOther", IntegerType)())

    def watermarkFrom(
        conditionStr: String,
        rightWatermark: Option[Long] = Some(10000)): Option[Long] = {
      val conditionExpr = Some(conditionStr).map { str =>
        val plan =
          Filter(
            spark.sessionState.sqlParser.parseExpression(str),
            LogicalRDD(
              attributesToFindConstraintFor ++ attributesWithWatermark,
              spark.sparkContext.emptyRDD)(spark))
        plan.queryExecution.optimizedPlan.asInstanceOf[Filter].condition
      }
      StreamingJoinHelper.getStateValueWatermark(
        AttributeSet(attributesToFindConstraintFor), AttributeSet(attributesWithWatermark),
        conditionExpr, rightWatermark)
    }

    // Test comparison directionality. E.g. if leftTime < rightTime and rightTime > watermark,
    // then cannot define constraint on leftTime.
    assert(watermarkFrom("leftTime > rightTime") === Some(10000))
    assert(watermarkFrom("leftTime >= rightTime") === Some(9999))
    assert(watermarkFrom("leftTime < rightTime") === None)
    assert(watermarkFrom("leftTime <= rightTime") === None)
    assert(watermarkFrom("rightTime > leftTime") === None)
    assert(watermarkFrom("rightTime >= leftTime") === None)
    assert(watermarkFrom("rightTime < leftTime") === Some(10000))
    assert(watermarkFrom("rightTime <= leftTime") === Some(9999))

    // Test type conversions
    assert(watermarkFrom("CAST(leftTime AS LONG) > CAST(rightTime AS LONG)") === Some(10000))
    assert(watermarkFrom("CAST(leftTime AS LONG) < CAST(rightTime AS LONG)") === None)
    assert(watermarkFrom("CAST(leftTime AS DOUBLE) > CAST(rightTime AS DOUBLE)") === Some(10000))
    assert(watermarkFrom("CAST(leftTime AS LONG) > CAST(rightTime AS DOUBLE)") === Some(10000))
    assert(watermarkFrom("CAST(leftTime AS LONG) > CAST(rightTime AS FLOAT)") === Some(10000))
    assert(watermarkFrom("CAST(leftTime AS DOUBLE) > CAST(rightTime AS FLOAT)") === Some(10000))
    assert(watermarkFrom("CAST(leftTime AS STRING) > CAST(rightTime AS STRING)") === None)

    // Test with timestamp type + calendar interval on either side of equation
    // Note: timestamptype and calendar interval don't commute, so less valid combinations to test.
    assert(watermarkFrom("leftTime > rightTime + interval 1 second") === Some(11000))
    assert(watermarkFrom("leftTime + interval 2 seconds > rightTime ") === Some(8000))
    assert(watermarkFrom("leftTime > rightTime - interval 3 second") === Some(7000))
    assert(watermarkFrom("rightTime < leftTime - interval 3 second") === Some(13000))
    assert(watermarkFrom("rightTime - interval 1 second < leftTime - interval 3 second")
      === Some(12000))

    // Test with casted long type + constants on either side of equation
    // Note: long type and constants commute, so more combinations to test.
    // -- Constants on the right
    assert(watermarkFrom("CAST(leftTime AS LONG) > CAST(rightTime AS LONG) + 1") === Some(11000))
    assert(watermarkFrom("CAST(leftTime AS LONG) > CAST(rightTime AS LONG) - 1") === Some(9000))
    assert(watermarkFrom("CAST(leftTime AS LONG) > CAST((rightTime + interval 1 second) AS LONG)")
      === Some(11000))
    assert(watermarkFrom("CAST(leftTime AS LONG) > 2 + CAST(rightTime AS LONG)") === Some(12000))
    assert(watermarkFrom("CAST(leftTime AS LONG) > -0.5 + CAST(rightTime AS LONG)") === Some(9500))
    assert(watermarkFrom("CAST(leftTime AS LONG) - CAST(rightTime AS LONG) > 2") === Some(12000))
    assert(watermarkFrom("-CAST(rightTime AS DOUBLE) + CAST(leftTime AS LONG) > 0.1")
      === Some(10100))
    assert(watermarkFrom("0 > CAST(rightTime AS LONG) - CAST(leftTime AS LONG) + 0.2")
      === Some(10200))
    // -- Constants on the left
    assert(watermarkFrom("CAST(leftTime AS LONG) + 2 > CAST(rightTime AS LONG)") === Some(8000))
    assert(watermarkFrom("1 + CAST(leftTime AS LONG) > CAST(rightTime AS LONG)") === Some(9000))
    assert(watermarkFrom("CAST((leftTime  + interval 3 second) AS LONG) > CAST(rightTime AS LONG)")
      === Some(7000))
    assert(watermarkFrom("CAST(leftTime AS LONG) - 2 > CAST(rightTime AS LONG)") === Some(12000))
    assert(watermarkFrom("CAST(leftTime AS LONG) + 0.5 > CAST(rightTime AS LONG)") === Some(9500))
    assert(watermarkFrom("CAST(leftTime AS LONG) - CAST(rightTime AS LONG) - 2 > 0")
      === Some(12000))
    assert(watermarkFrom("-CAST(rightTime AS LONG) + CAST(leftTime AS LONG) - 0.1 > 0")
      === Some(10100))
    // -- Constants on both sides, mixed types
    assert(watermarkFrom("CAST(leftTime AS LONG) - 2.0 > CAST(rightTime AS LONG) + 1")
      === Some(13000))

    // Test multiple conditions, should return minimum watermark
    assert(watermarkFrom(
      "leftTime > rightTime - interval 3 second AND rightTime < leftTime + interval 2 seconds") ===
      Some(7000))  // first condition wins
    assert(watermarkFrom(
      "leftTime > rightTime - interval 3 second AND rightTime < leftTime + interval 4 seconds") ===
      Some(6000))  // second condition wins

    // Test invalid comparisons
    assert(watermarkFrom("cast(leftTime AS LONG) > leftOther") === None)      // non-time attributes
    assert(watermarkFrom("leftOther > rightOther") === None)                  // non-time attributes
    assert(watermarkFrom("leftOther > rightOther AND leftTime > rightTime") === Some(10000))
    assert(watermarkFrom("cast(rightTime AS DOUBLE) < rightOther") === None)  // non-time attributes
    assert(watermarkFrom("leftTime > rightTime + interval 1 month") === None) // month not allowed

    // Test static comparisons
    assert(watermarkFrom("cast(leftTime AS LONG) > 10") === Some(10000))

    // Test non-positive results
    assert(watermarkFrom("CAST(leftTime AS LONG) > CAST(rightTime AS LONG) - 10") === Some(0))
    assert(watermarkFrom("CAST(leftTime AS LONG) > CAST(rightTime AS LONG) - 100") === Some(-90000))
  }

  test("locality preferences of StateStoreAwareZippedRDD") {
    import StreamingSymmetricHashJoinHelper._

    withTempDir { tempDir =>
      val queryId = UUID.randomUUID
      val opId = 0
      val path = Utils.createDirectory(tempDir.getAbsolutePath, Random.nextString(10)).toString
      val stateInfo = StatefulOperatorStateInfo(path, queryId, opId, 0L)

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

  test("left stream batch outer join") {
    val stream = MemoryStream[Int]
      .toDF()
      .withColumn("timestamp", 'value.cast("timestamp"))
      .withWatermark("timestamp", "1 second")
    val joined =
      stream.join(Seq(1).toDF(), Seq("value"), "left_outer")

    // This test is in the suite just to confirm the validations below don't block this valid join.
    // We don't need to check results, just that the join can happen.
    testStream(joined)()
  }

  test("left batch stream outer join") {
    val stream = MemoryStream[Int]
      .toDF()
      .withColumn("timestamp", 'value.cast("timestamp"))
      .withWatermark("timestamp", "1 second")
    val joined =
      Seq(1).toDF().join(stream, Seq("value"), "left_outer")

    val thrown = intercept[AnalysisException] {
      testStream(joined)()
    }

    assert(thrown.getMessage.contains(
      "Left outer join with a streaming DataFrame/Dataset on the right and a static"))
  }

  test("right stream batch outer join") {
    val stream = MemoryStream[Int]
      .toDF()
      .withColumn("timestamp", 'value.cast("timestamp"))
      .withWatermark("timestamp", "1 second")
    val joined =
      stream.join(Seq(1).toDF(), Seq("value"), "right_outer")

    val thrown = intercept[AnalysisException] {
      testStream(joined)()
    }

    assert(thrown.getMessage.contains(
      "Right outer join with a streaming DataFrame/Dataset on the left and a static"))
  }

  test("left outer join with no watermark") {
    val joined =
      MemoryStream[Int].toDF().join(MemoryStream[Int].toDF(), Seq("value"), "left_outer")

    val thrown = intercept[AnalysisException] {
      testStream(joined)()
    }

    assert(thrown.getMessage.contains(
      "Stream-stream outer join between two streaming DataFrame/Datasets is not supported " +
        "without a watermark"))
  }

  test("right outer join with no watermark") {
    val joined =
      MemoryStream[Int].toDF().join(MemoryStream[Int].toDF(), Seq("value"), "right_outer")

    val thrown = intercept[AnalysisException] {
      testStream(joined)()
    }

    assert(thrown.getMessage.contains(
      "Stream-stream outer join between two streaming DataFrame/Datasets is not supported " +
        "without a watermark"))
  }

  test("windowed left outer join") {
    val (leftInput, rightInput, joined) = setupWindowedJoin("left_outer")

    testStream(joined)(
      // Test inner part of the join.
      AddData(leftInput, 1, 2, 3, 4, 5),
      AddData(rightInput, 3, 4, 5, 6, 7),
      CheckLastBatch((3, 10, 6, 9), (4, 10, 8, 12), (5, 10, 10, 15)),
      // Old state doesn't get dropped until the batch *after* it gets introduced, so the
      // nulls won't show up until the next batch after the watermark advances.
      AddData(leftInput, 21),
      AddData(rightInput, 22),
      CheckLastBatch(),
      AddData(leftInput, 22),
      CheckLastBatch(Row(22, 30, 44, 66), Row(1, 10, 2, null), Row(2, 10, 4, null))
    )
  }

  test("windowed right outer join") {
    val (leftInput, rightInput, joined) = setupWindowedJoin("right_outer")

    testStream(joined)(
      // Test inner part of the join.
      AddData(leftInput, 1, 2, 3, 4, 5),
      AddData(rightInput, 3, 4, 5, 6, 7),
      CheckLastBatch((3, 10, 6, 9), (4, 10, 8, 12), (5, 10, 10, 15)),
      // Old state doesn't get dropped until the batch *after* it gets introduced, so the
      // nulls won't show up until the next batch after the watermark advances.
      AddData(leftInput, 21),
      AddData(rightInput, 22),
      CheckLastBatch(),
      AddData(leftInput, 22),
      CheckLastBatch(Row(22, 30, 44, 66), Row(6, 10, null, 18), Row(7, 10, null, 21))
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
        CheckLastBatch((1, 1, 5, 10)),
        AddData(rightInput, (1, 11)),
        CheckLastBatch(), // no match as left time is too low
        assertNumStateRows(total = 5, updated = 1),

        // Increase event time watermark to 20s by adding data with time = 30s on both inputs
        AddData(leftInput, (1, 7), (1, 30)),
        CheckLastBatch((1, 1, 7, 10), (1, 1, 7, 11)),
        assertNumStateRows(total = 7, updated = 2),
        AddData(rightInput, (0, 30)),
        CheckLastBatch(),
        assertNumStateRows(total = 8, updated = 1),
        AddData(rightInput, (0, 30)),
        CheckLastBatch(outerResult))
    }
  }

  // When the join condition isn't true, the outer null rows must be generated, even if the join
  // keys themselves have a match.
  test("outer join with non-key condition violated on left") {
    val (leftInput, simpleLeftDf) = setupStream("left", 2)
    val (rightInput, simpleRightDf) = setupStream("right", 3)

    val left = simpleLeftDf.select('key, window('leftTime, "10 second"), 'leftValue)
    val right = simpleRightDf.select('key, window('rightTime, "10 second"), 'rightValue)

    val joined = left.join(
        right,
        left("key") === right("key") && left("window") === right("window") &&
            'leftValue > 20 && 'rightValue < 200,
        "left_outer")
      .select(left("key"), left("window.end").cast("long"), 'leftValue, 'rightValue)

    testStream(joined)(
      // leftValue <= 20 should generate outer join rows even though it matches right keys
      AddData(leftInput, 1, 2, 3),
      AddData(rightInput, 1, 2, 3),
      CheckLastBatch(),
      AddData(leftInput, 30),
      AddData(rightInput, 31),
      CheckLastBatch(),
      AddData(rightInput, 32),
      CheckLastBatch(Row(1, 10, 2, null), Row(2, 10, 4, null), Row(3, 10, 6, null))
    )
  }

  test("outer join with non-key condition which is met") {
    val (leftInput, simpleLeftDf) = setupStream("left", 2)
    val (rightInput, simpleRightDf) = setupStream("right", 3)

    val left = simpleLeftDf.select('key, window('leftTime, "10 second"), 'leftValue)
    val right = simpleRightDf.select('key, window('rightTime, "10 second"), 'rightValue)

    val joined = left.join(
      right,
      left("key") === right("key") && left("window") === right("window") &&
        'leftValue > 20 && 'rightValue < 200,
      "left_outer")
      .select(left("key"), left("window.end").cast("long"), 'leftValue, 'rightValue)

    testStream(joined)(
      // both values between 20 and 200 should not generate outer join rows
      AddData(leftInput, 40, 50),
      AddData(rightInput, 40, 50),
      CheckLastBatch((40, 50, 80, 120), (50, 60, 100, 150)),
      AddData(leftInput, 70),
      AddData(rightInput, 71),
      CheckLastBatch(),
      AddData(leftInput, 72),
      AddData(rightInput, 73),
      CheckLastBatch()
    )
  }

  test("outer join with non-key condition violated on right") {
    val (leftInput, simpleLeftDf) = setupStream("left", 2)
    val (rightInput, simpleRightDf) = setupStream("right", 3)

    val left = simpleLeftDf.select('key, window('leftTime, "10 second"), 'leftValue)
    val right = simpleRightDf.select('key, window('rightTime, "10 second"), 'rightValue)

    val joined = left.join(
      right,
      left("key") === right("key") && left("window") === right("window") && 'rightValue > 20,
      "left_outer")
      .select(left("key"), left("window.end").cast("long"), 'leftValue, 'rightValue)

    testStream(joined)(
      // rightValue < 20 should generate outer join rows even though it matches left keys
      AddData(leftInput, 4, 5),
      AddData(rightInput, 4, 5),
      CheckLastBatch(),
      AddData(leftInput, 100),
      AddData(rightInput, 101),
      CheckLastBatch(),
      AddData(leftInput, 102),
      AddData(rightInput, 103),
      CheckLastBatch(Row(4, 10, 8, null), Row(5, 10, 10, null))
    )
  }
}

