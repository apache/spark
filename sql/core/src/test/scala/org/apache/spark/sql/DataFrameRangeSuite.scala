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

package org.apache.spark.sql

import scala.concurrent.duration._
import scala.math.abs
import scala.util.Random

import org.scalatest.concurrent.Eventually

import org.apache.spark.{SparkException, TaskContext}
import org.apache.spark.scheduler.{SparkListener, SparkListenerJobStart}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSQLContext


class DataFrameRangeSuite extends QueryTest with SharedSQLContext with Eventually {
  import testImplicits._

  test("SPARK-7150 range api") {
    // numSlice is greater than length
    val res1 = spark.range(0, 10, 1, 15).select("id")
    assert(res1.count == 10)
    assert(res1.agg(sum("id")).as("sumid").collect() === Seq(Row(45)))

    val res2 = spark.range(3, 15, 3, 2).select("id")
    assert(res2.count == 4)
    assert(res2.agg(sum("id")).as("sumid").collect() === Seq(Row(30)))

    val res3 = spark.range(1, -2).select("id")
    assert(res3.count == 0)

    // start is positive, end is negative, step is negative
    val res4 = spark.range(1, -2, -2, 6).select("id")
    assert(res4.count == 2)
    assert(res4.agg(sum("id")).as("sumid").collect() === Seq(Row(0)))

    // start, end, step are negative
    val res5 = spark.range(-3, -8, -2, 1).select("id")
    assert(res5.count == 3)
    assert(res5.agg(sum("id")).as("sumid").collect() === Seq(Row(-15)))

    // start, end are negative, step is positive
    val res6 = spark.range(-8, -4, 2, 1).select("id")
    assert(res6.count == 2)
    assert(res6.agg(sum("id")).as("sumid").collect() === Seq(Row(-14)))

    val res7 = spark.range(-10, -9, -20, 1).select("id")
    assert(res7.count == 0)

    val res8 = spark.range(Long.MinValue, Long.MaxValue, Long.MaxValue, 100).select("id")
    assert(res8.count == 3)
    assert(res8.agg(sum("id")).as("sumid").collect() === Seq(Row(-3)))

    val res9 = spark.range(Long.MaxValue, Long.MinValue, Long.MinValue, 100).select("id")
    assert(res9.count == 2)
    assert(res9.agg(sum("id")).as("sumid").collect() === Seq(Row(Long.MaxValue - 1)))

    // only end provided as argument
    val res10 = spark.range(10).select("id")
    assert(res10.count == 10)
    assert(res10.agg(sum("id")).as("sumid").collect() === Seq(Row(45)))

    val res11 = spark.range(-1).select("id")
    assert(res11.count == 0)

    // using the default slice number
    val res12 = spark.range(3, 15, 3).select("id")
    assert(res12.count == 4)
    assert(res12.agg(sum("id")).as("sumid").collect() === Seq(Row(30)))

    // difference between range start and end does not fit in a 64-bit integer
    val n = 9L * 1000 * 1000 * 1000 * 1000 * 1000 * 1000
    val res13 = spark.range(-n, n, n / 9).select("id")
    assert(res13.count == 18)

    // range with non aggregation operation
    val res14 = spark.range(0, 100, 2).toDF.filter("50 <= id")
    val len14 = res14.collect.length
    assert(len14 == 25)

    val res15 = spark.range(100, -100, -2).toDF.filter("id <= 0")
    val len15 = res15.collect.length
    assert(len15 == 50)

    val res16 = spark.range(-1500, 1500, 3).toDF.filter("0 <= id")
    val len16 = res16.collect.length
    assert(len16 == 500)

    val res17 = spark.range(10, 0, -1, 1).toDF.sortWithinPartitions("id")
    assert(res17.collect === (1 to 10).map(i => Row(i)).toArray)
  }

  test("Range with randomized parameters") {
    val MAX_NUM_STEPS = 10L * 1000

    val seed = System.currentTimeMillis()
    val random = new Random(seed)

    def randomBound(): Long = {
      val n = if (random.nextBoolean()) {
        random.nextLong() % (Long.MaxValue / (100 * MAX_NUM_STEPS))
      } else {
        random.nextLong() / 2
      }
      if (random.nextBoolean()) n else -n
    }

    for (l <- 1 to 10) {
      val start = randomBound()
      val end = randomBound()
      val numSteps = (abs(random.nextLong()) % MAX_NUM_STEPS) + 1
      val stepAbs = (abs(end - start) / numSteps) + 1
      val step = if (start < end) stepAbs else -stepAbs
      val partitions = random.nextInt(20) + 1

      val expCount = (start until end by step).size
      val expSum = (start until end by step).sum

      for (codegen <- List(false, true)) {
        withSQLConf(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> codegen.toString()) {
          val res = spark.range(start, end, step, partitions).toDF("id").
            agg(count("id"), sum("id")).collect()

          withClue(s"seed = $seed start = $start end = $end step = $step partitions = " +
              s"$partitions codegen = $codegen") {
            assert(!res.isEmpty)
            assert(res.head.getLong(0) == expCount)
            if (expCount > 0) {
              assert(res.head.getLong(1) == expSum)
            }
          }
        }
      }
    }
  }

  test("Cancelling stage in a query with Range.") {
    val listener = new SparkListener {
      override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
        eventually(timeout(10.seconds)) {
          assert(DataFrameRangeSuite.stageToKill > 0)
        }
        sparkContext.cancelStage(DataFrameRangeSuite.stageToKill)
      }
    }

    sparkContext.addSparkListener(listener)
    for (codegen <- Seq(true, false)) {
      withSQLConf(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> codegen.toString()) {
        DataFrameRangeSuite.stageToKill = -1
        val ex = intercept[SparkException] {
          spark.range(1000000000L).map { x =>
            DataFrameRangeSuite.stageToKill = TaskContext.get().stageId()
            x
          }.toDF("id").agg(sum("id")).collect()
        }
        ex.getCause() match {
          case null =>
            assert(ex.getMessage().contains("cancelled"))
          case cause: SparkException =>
            assert(cause.getMessage().contains("cancelled"))
          case cause: Throwable =>
            fail("Expected the cause to be SparkException, got " + cause.toString() + " instead.")
        }
      }
      eventually(timeout(20.seconds)) {
        assert(sparkContext.statusTracker.getExecutorInfos.map(_.numRunningTasks()).sum == 0)
      }
    }
  }

  test("SPARK-20430 Initialize Range parameters in a driver side") {
    withSQLConf(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "false") {
      checkAnswer(sql("SELECT * FROM range(3)"), Row(0) :: Row(1) :: Row(2) :: Nil)
    }
  }

  test("SPARK-21041 SparkSession.range()'s behavior is inconsistent with SparkContext.range()") {
    val start = java.lang.Long.MAX_VALUE - 3
    val end = java.lang.Long.MIN_VALUE + 2
    Seq("false", "true").foreach { value =>
      withSQLConf(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> value) {
        assert(spark.range(start, end, 1).collect.length == 0)
        assert(spark.range(start, start, 1).collect.length == 0)
      }
    }
  }
}

object DataFrameRangeSuite {
  @volatile var stageToKill = -1
}
