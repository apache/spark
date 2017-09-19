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

import org.apache.hadoop.conf.Configuration
import org.scalatest.BeforeAndAfter

import org.apache.spark.scheduler.ExecutorCacheTaskLocation
import org.apache.spark.sql.LocalSparkSession.withSparkSession
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, BoundReference, Expression, GenericInternalRow, LessThanOrEqual, Literal, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.catalyst.expressions.codegen.{GeneratePredicate, Predicate}
import org.apache.spark.sql.catalyst.plans.logical.{EventTimeWatermark, Filter}
import org.apache.spark.sql.catalyst.util.quietly
import org.apache.spark.sql.execution.LogicalRDD
import org.apache.spark.sql.execution.streaming.{MemoryStream, StatefulOperatorStateInfo, StreamingSymmetricHashJoinHelper}
import org.apache.spark.sql.execution.streaming.StreamingSymmetricHashJoinHelper.LeftSide
import org.apache.spark.sql.execution.streaming.state.{StateStore, StateStoreConf, StateStoreId, StateStoreProviderId, SymmetricHashJoinStateManager}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.util.Utils


class StreamingJoinSuite extends StreamTest with BeforeAndAfter {

  before {
    SparkSession.setActiveSession(spark)  // set this before force initializing 'joinExec'
    spark.streams.stateStoreCoordinator   // initialize the lazy coordinator
  }

  after {
    StateStore.stop()
  }

  import testImplicits._

  test("SymmetricHashJoinStateManager") {

    val watermarkMetadata = new MetadataBuilder().putLong(EventTimeWatermark.delayKey, 10).build()
    val inputValueSchema = new StructType()
      .add(StructField("time", IntegerType, metadata = watermarkMetadata))
      .add(StructField("value", BooleanType))
    val inputValueAttribs = inputValueSchema.toAttributes
    val inputValueAttribWithWatermark = inputValueAttribs(0)
    val joinKeyExprs = Seq[Expression](Literal(false), inputValueAttribWithWatermark, Literal(10.0))

    val inputValueGen = UnsafeProjection.create(inputValueAttribs.map(_.dataType).toArray)
    val joinKeyGen = UnsafeProjection.create(joinKeyExprs.map(_.dataType).toArray)

    def toInputValue(i: Int): UnsafeRow = {
      inputValueGen.apply(new GenericInternalRow(Array[Any](i, false)))
    }

    def toJoinKeyRow(i: Int): UnsafeRow = {
      joinKeyGen.apply(new GenericInternalRow(Array[Any](false, i, 10.0)))
    }

    def toKeyInt(joinKeyRow: UnsafeRow): Int = joinKeyRow.getInt(1)

    def toValueInt(inputValueRow: UnsafeRow): Int = inputValueRow.getInt(0)

    withJoinStateManager(inputValueAttribs, joinKeyExprs) { manager =>
      def append(key: Int, value: Int): Unit = {
        manager.append(toJoinKeyRow(key), toInputValue(value))
      }

      def get(key: Int): Seq[Int] = manager.get(toJoinKeyRow(key)).map(toValueInt).toSeq.sorted

      /** Remove keys (and corresponding values) where `time <= threshold` */
      def removeByKey(threshold: Long): Unit = {
        val expr =
          LessThanOrEqual(
            BoundReference(
              1, inputValueAttribWithWatermark.dataType, inputValueAttribWithWatermark.nullable),
            Literal(threshold))
        manager.removeByKeyCondition(GeneratePredicate.generate(expr).eval _)
      }

      /** Remove values where `time <= threshold` */
      def removeByValue(watermark: Long): Unit = {
        val expr = LessThanOrEqual(inputValueAttribWithWatermark, Literal(watermark))
        manager.removeByPredicateOnValues(
          GeneratePredicate.generate(expr, inputValueAttribs).eval _)
      }

      assert(get(20) === Seq.empty)     // initially empty
      append(20, 2)
      assert(get(20) === Seq(2))        // should first value correctly

      append(20, 3)
      assert(get(20) === Seq(2, 3))     // should append new values
      append(20, 3)
      assert(get(20) === Seq(2, 3, 3))  // should append another copy if same value added again

      assert(get(30) === Seq.empty)
      append(30, 1)
      assert(get(30) === Seq(1))
      assert(get(20) === Seq(2, 3, 3))  // add another key-value should not affect existing ones

      removeByKey(25)
      assert(get(20) === Seq.empty)
      assert(get(30) === Seq(1))        // should remove 20, not 30

      removeByKey(30)
      assert(get(30) === Seq.empty)     // should remove 30

      def appendAndTest(key: Int, values: Int*): Unit = {
        values.foreach { value => append(key, value)}
        require(get(key) === values)
      }

      appendAndTest(40, 100, 200, 300)
      appendAndTest(50, 125)
      appendAndTest(60, 275)              // prepare for testing removeByValue

      removeByValue(125)
      assert(get(40) === Seq(200, 300))
      assert(get(50) === Seq.empty)
      assert(get(60) === Seq(275))        // should remove only some values, not all

      append(40, 50)
      assert(get(40) === Seq(50, 200, 300))
      append(40, 100)
      removeByValue(200)
      assert(get(40) === Seq(300))
      assert(get(60) === Seq(275))        // should remove only some values, not all

      removeByValue(300)
      assert(get(40) === Seq.empty)
      assert(get(60) === Seq.empty)       // should remove all values now
    }
  }

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
      AddData(input1, 25),
      CheckLastBatch(),
      AddData(input2, 25),
      CheckLastBatch((25, 30, 50, 75)),
      AddData(input1, 1),
      CheckLastBatch((1, 10, 2, 3)),      // State for 1 still around as there is not watermark
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
      AddData(input2, 1),
      CheckLastBatch((1, 10, 2, 3)),
      AddData(input1, 25),
      CheckLastBatch(),
      AddData(input2, 25),
      CheckLastBatch((25, 30, 50, 75)),   // sets the watermark to 25 - 10 = 15
      AddData(input2, 1),
      CheckLastBatch(),       // Does not join with previous 1 because state has been removed
      AddData(input1, 5),
      CheckLastBatch(),
      AddData(input2, 5),
      CheckLastBatch()        // Does not join as new data < 15 is ignored
    )
  }

  test("stream stream inner join with time range - with watermark - one side condition") {
    import org.apache.spark.sql.functions._

    val input1 = MemoryStream[(Int, Int)]
    val input2 = MemoryStream[(Int, Int)]

    val df1 = input1.toDF.toDF("leftKey", "time")
      .select('leftKey, 'time.cast("timestamp") as "leftTime", ('leftKey * 2) as "leftValue")
      .withWatermark("leftTime", "10 seconds")

    val df2 = input2.toDF.toDF("rightKey", "time")
      .select('rightKey, 'time.cast("timestamp") as "rightTime", ('rightKey * 3) as "rightValue")
      .withWatermark("rightTime", "10 seconds")

    val joined =
      df1.join(df2, expr("leftKey = rightKey AND leftTime < rightTime - interval 5 seconds"))
        .select('leftKey, 'leftTime.cast("int"), 'rightTime.cast("int"))

    testStream(joined)(
      AddData(input1, (1, 5)),
      CheckAnswer(),
      AddData(input2, (1, 11)),
      CheckLastBatch((1, 5, 11)),
      AddData(input2, (1, 10)),
      CheckLastBatch(),           // left time 5s is not less than right time 10s - 5s

      // Increase event time watermark to 20s by adding data with time = 30s on both inputs
      AddData(input1, (1, 3), (1, 30)),
      AddData(input2, (0, 30)),
      CheckLastBatch((1, 3, 10), (1, 3, 11)),
      // watermark:    max event time - 10s   ==>   30s - 10s = 20s
      // state constraint:    20s < leftTime < rightTime - 5s   ==>   rightTime > 25s

      // clear state rightTime < 25s
      AddData(input2),
      CheckLastBatch(),

      // input with time <= 20s ignored due to watermark
      AddData(input2, (1, 20), (1, 21), (1, 28)),
      CheckLastBatch((1, 3, 21), (1, 5, 21), (1, 3, 28), (1, 5, 28)),

      // input with time <= 20s ignored due to watermark
      AddData(input1, (1, 20), (1, 21)),
      CheckLastBatch((1, 21, 28))
    )
  }

  test("extract watermark from time condition") {
    val attributesToFindConstraintFor = Seq(
      AttributeReference("leftTime", TimestampType)(),
      AttributeReference("leftOther", StringType)())
    val metadataWithWatermark = new MetadataBuilder()
      .putLong(EventTimeWatermark.delayKey, 1000)
      .build()
    val attributesWithWatermark = Seq(
      AttributeReference("rightTime", TimestampType, metadata = metadataWithWatermark)(),
      AttributeReference("rightOther", StringType)())

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
      StreamingSymmetricHashJoinHelper.getStateValueWatermark(
        attributesToFindConstraintFor, attributesWithWatermark, conditionExpr, rightWatermark)
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

  def withJoinStateManager(
      inputValueAttribs: Seq[Attribute],
      joinKeyExprs: Seq[Expression])(f: SymmetricHashJoinStateManager => Unit): Unit = {

    withTempDir { file =>
      val storeConf = new StateStoreConf()
      val stateInfo = StatefulOperatorStateInfo(file.getAbsolutePath, UUID.randomUUID, 0, 0)
      val manager = new SymmetricHashJoinStateManager(
        LeftSide, inputValueAttribs, joinKeyExprs, Some(stateInfo), storeConf, new Configuration)
      try { f(manager) } finally { manager.abortIfNeeded() }
    }
    StateStore.stop()
  }
}
