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

package org.apache.spark.sql.execution.joins

import java.sql.Date

import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, AdaptiveSparkPlanHelper,
  BroadcastQueryStageExec}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

class BroadcastNearestByJoinExecSuite extends QueryTest with SharedSparkSession
    with AdaptiveSparkPlanHelper {

  import testImplicits._

  private def withStreamingHeap(f: => Unit): Unit = {
    withSQLConf(
      SQLConf.NEAREST_BY_BROADCAST_ENABLED.key -> "true") {
      f
    }
  }

  test("empty right table - INNER returns nothing") {
    withStreamingHeap {
      val left = spark.range(5).toDF("id").withColumn("x", col("id").cast("double"))
      val right = spark.range(0).toDF("rid").withColumn("y", lit(0.0))
      val result = left.nearestByJoin(right, abs(col("x") - col("y")),
        numResults = 3, mode = "exact", direction = "distance")
      val plan = result.queryExecution.executedPlan
      assert(plan.treeString.contains("BroadcastNearestByJoin"),
        "Expected BroadcastNearestByJoinExec in plan but got:\n" + plan.treeString)
      assert(result.count() == 0)
    }
  }

  test("empty right table - LEFT OUTER returns left with nulls") {
    withStreamingHeap {
      val left = spark.range(3).toDF("id").withColumn("x", col("id").cast("double"))
      val right = spark.range(0).toDF("rid").withColumn("y", lit(0.0))
      val result = left.nearestByJoin(right, abs(col("x") - col("y")),
        numResults = 2, mode = "exact", direction = "distance", joinType = "left_outer")
      val plan = result.queryExecution.executedPlan
      assert(plan.treeString.contains("BroadcastNearestByJoin"),
        "Expected BroadcastNearestByJoinExec in plan but got:\n" + plan.treeString)
      assert(result.count() == 3)
      result.collect().foreach { row =>
        assert(row.isNullAt(2)) // rid is null
        assert(row.isNullAt(3)) // y is null
      }
    }
  }

  test("k=1 returns single nearest") {
    withStreamingHeap {
      val left = Seq((1, 10.0), (2, 20.0)).toDF("id", "x")
      val right = Seq((10, 9.0), (11, 15.0), (12, 21.0)).toDF("rid", "y")
      val result = left.nearestByJoin(right, abs(col("x") - col("y")),
        numResults = 1, mode = "exact", direction = "distance")
        .orderBy("id")
      val plan = result.queryExecution.executedPlan
      assert(plan.treeString.contains("BroadcastNearestByJoin"),
        "Expected BroadcastNearestByJoinExec in plan but got:\n" + plan.treeString)
      checkAnswer(result, Seq(
        Row(1, 10.0, 10, 9.0),  // nearest to 10.0 is 9.0
        Row(2, 20.0, 12, 21.0)  // nearest to 20.0 is 21.0
      ))
    }
  }

  test("k > right table size returns all right rows per left row") {
    withStreamingHeap {
      val left = Seq((1, 5.0)).toDF("id", "x")
      val right = Seq((10, 1.0), (11, 2.0)).toDF("rid", "y")
      val result = left.nearestByJoin(right, abs(col("x") - col("y")),
        numResults = 10, mode = "exact", direction = "distance")
      val plan = result.queryExecution.executedPlan
      assert(plan.treeString.contains("BroadcastNearestByJoin"),
        "Expected BroadcastNearestByJoinExec in plan but got:\n" + plan.treeString)
      // Only 2 right rows exist, so we get 2 results
      assert(result.count() == 2)
      checkAnswer(result.orderBy("rid"), Seq(
        Row(1, 5.0, 10, 1.0),
        Row(1, 5.0, 11, 2.0)
      ))
    }
  }

  test("NaN ranking values participate in ordering") {
    withStreamingHeap {
      val left = Seq((1, 5.0)).toDF("id", "x")
      val right = Seq((10, Double.NaN), (11, 3.0), (12, 7.0)).toDF("rid", "y")
      val result = left.nearestByJoin(right, abs(col("x") - col("y")),
        numResults = 3, mode = "exact", direction = "distance")
      val plan = result.queryExecution.executedPlan
      assert(plan.treeString.contains("BroadcastNearestByJoin"),
        "Expected BroadcastNearestByJoinExec in plan but got:\n" + plan.treeString)
      // NaN participates in natural ordering (sorts after all non-NaN for distance)
      assert(result.count() == 3)
      checkAnswer(result.orderBy("rid"), Seq(
        Row(1, 5.0, 10, Double.NaN),
        Row(1, 5.0, 11, 3.0),
        Row(1, 5.0, 12, 7.0)
      ))
    }
  }

  test("null ranking values are excluded, not treated as 0.0") {
    withStreamingHeap {
      val left = Seq((1, 5.0)).toDF("id", "x")
      // y=null will produce null ranking expression (abs(5.0 - null) = null)
      val right = Seq((10, Some(3.0)), (11, None), (12, Some(7.0))).toDF("rid", "y")
      val result = left.nearestByJoin(right, abs(col("x") - col("y")),
        numResults = 3, mode = "exact", direction = "distance")
      val plan = result.queryExecution.executedPlan
      assert(plan.treeString.contains("BroadcastNearestByJoin"),
        "Expected BroadcastNearestByJoinExec in plan but got:\n" + plan.treeString)
      // null row excluded, only 2 results
      assert(result.count() == 2)
      checkAnswer(result.orderBy("rid"), Seq(
        Row(1, 5.0, 10, 3.0),
        Row(1, 5.0, 12, 7.0)
      ))
    }
  }

  test("asymmetric - right small left big, operator fires") {
    withStreamingHeap {
      val left = spark.range(1000).toDF("id").withColumn("x", col("id").cast("double"))
      val right = spark.range(50).toDF("rid").withColumn("y", col("rid").cast("double") * 20)
      val df = left.nearestByJoin(right, abs(col("x") - col("y")),
        numResults = 3, mode = "exact", direction = "distance")
      val plan = df.queryExecution.executedPlan.toString()
      assert(plan.contains("BroadcastNearestByJoin"),
        s"Expected BroadcastNearestByJoinExec in plan: $plan")
      assert(df.count() == 1000 * 3)
      // Spot check: id=0 (x=0.0) nearest to y values 0,20,40 -> rids 0,1,2
      val row0 = df.filter(col("id") === 0).orderBy(abs(col("x") - col("y"))).collect()
      assert(row0.length == 3)
      assert(row0(0).getAs[Long]("rid") == 0L) // y=0, distance=0
    }
  }

  test("asymmetric - right exceeds broadcast threshold, broadcast still used (flag ON)") {
    // When the broadcast flag is ON, BroadcastNearestByJoinExec is always used regardless
    // of right-side size. There is no size decision and no fallback; an oversized right
    // side will fail the query at runtime (SPARK-57091).
    withSQLConf(
      SQLConf.NEAREST_BY_BROADCAST_ENABLED.key -> "true",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "1") {
      val left = spark.range(10).toDF("id").withColumn("x", col("id").cast("double"))
      val right = spark.range(50).toDF("rid").withColumn("y", col("rid").cast("double"))
      val df = left.nearestByJoin(right, abs(col("x") - col("y")),
        numResults = 2, mode = "exact", direction = "distance")
      val plan = df.queryExecution.executedPlan.toString()
      assert(plan.contains("BroadcastNearestByJoin"),
        s"Expected BroadcastNearestByJoinExec in plan (flag ON always broadcasts): $plan")
      // Results still correct
      assert(df.count() == 10 * 2)
      val row0 = df.filter(col("id") === 0).orderBy(abs(col("x") - col("y"))).collect()
      assert(row0(0).getAs[Long]("rid") == 0L)
    }
  }

  test("asymmetric - left small right big, operator fires") {
    withStreamingHeap {
      val left = spark.range(10).toDF("id").withColumn("x", col("id").cast("double") * 50)
      val right = spark.range(500).toDF("rid").withColumn("y", col("rid").cast("double"))
      val df = left.nearestByJoin(right, abs(col("x") - col("y")),
        numResults = 5, mode = "exact", direction = "distance")
      val plan = df.queryExecution.executedPlan.toString()
      assert(plan.contains("BroadcastNearestByJoin"),
        s"Expected BroadcastNearestByJoinExec in plan: $plan")
      assert(df.count() == 10 * 5)
      // id=0 (x=0.0): nearest are y=0,1,2,3,4
      val row0 = df.filter(col("id") === 0).orderBy(abs(col("x") - col("y"))).collect()
      assert(row0(0).getAs[Long]("rid") == 0L)
      assert(row0(4).getAs[Long]("rid") == 4L)
    }
  }

  test("asymmetric - both sides moderate, operator fires") {
    withStreamingHeap {
      val left = spark.range(200).toDF("id").withColumn("x", col("id").cast("double"))
      val right = spark.range(200).toDF("rid").withColumn("y", col("rid").cast("double") + 0.5)
      val df = left.nearestByJoin(right, abs(col("x") - col("y")),
        numResults = 3, mode = "exact", direction = "distance")
      val plan = df.queryExecution.executedPlan.toString()
      assert(plan.contains("BroadcastNearestByJoin"),
        s"Expected BroadcastNearestByJoinExec in plan: $plan")
      assert(df.count() == 200 * 3)
      // id=100 (x=100.0): nearest y values are 99.5(rid=99), 100.5(rid=100), 101.5(rid=101)
      val row100 = df.filter(col("id") === 100).orderBy(abs(col("x") - col("y"))).collect()
      assert(row100.length == 3)
      assert(row100(0).getAs[Long]("rid") == 99L) // y=99.5, distance=0.5
    }
  }

  test("basic correctness - small dataset") {
    withStreamingHeap {
      val left = Seq((1, 0.0), (2, 10.0), (3, 20.0)).toDF("id", "x")
      val right = Seq((100, 1.0), (101, 9.0), (102, 11.0), (103, 19.0), (104, 25.0))
        .toDF("rid", "y")
      val result = left.nearestByJoin(right, abs(col("x") - col("y")),
        numResults = 2, mode = "exact", direction = "distance")
        .orderBy("id", "y")
      // id=1 (x=0.0): nearest are y=1.0 (d=1), y=9.0 (d=9)
      // id=2 (x=10.0): nearest are y=9.0 (d=1), y=11.0 (d=1)
      // id=3 (x=20.0): nearest are y=19.0 (d=1), y=25.0 (d=5)
      checkAnswer(result, Seq(
        Row(1, 0.0, 100, 1.0),
        Row(1, 0.0, 101, 9.0),
        Row(2, 10.0, 101, 9.0),
        Row(2, 10.0, 102, 11.0),
        Row(3, 20.0, 103, 19.0),
        Row(3, 20.0, 104, 25.0)
      ))
    }
  }

  test("similarity direction - keeps largest ranking values") {
    withStreamingHeap {
      // Higher ranking value = more similar; top-k should be the largest values
      val left = Seq((1, 5.0)).toDF("id", "x")
      val right = Seq((10, 1.0), (11, 3.0), (12, 8.0), (13, 10.0)).toDF("rid", "y")
      // Use y directly as ranking: higher y = more similar
      val result = left.nearestByJoin(right, col("y"),
        numResults = 2, mode = "exact", direction = "similarity")
        .orderBy(col("y").desc)
      val plan = result.queryExecution.executedPlan
      assert(plan.treeString.contains("BroadcastNearestByJoin"),
        "Expected BroadcastNearestByJoinExec in plan but got:\n" + plan.treeString)
      // Top-2 by largest y: rid=13 (y=10.0), rid=12 (y=8.0)
      checkAnswer(result, Seq(
        Row(1, 5.0, 13, 10.0),
        Row(1, 5.0, 12, 8.0)
      ))
    }
  }

  test("integer ranking expression - does not corrupt values") {
    withStreamingHeap {
      // x and y are IntegerType, so (x - y) produces IntegerType ranking.
      // Use negative ranking values to expose getDouble corruption:
      // int -1 stored as 0x00000000FFFFFFFF reads as a tiny positive double, not -1.0.
      val left = Seq((1, 5)).toDF("id", "x")
      val right = Seq((10, 6), (11, 3), (12, 100)).toDF("rid", "y")
      // ranking = x - y: (5-6)=-1, (5-3)=2, (5-100)=-95
      // direction=similarity means largest ranking wins, so top-2 = rid=11(2), rid=10(-1)
      val result = left.nearestByJoin(right, col("x") - col("y"),
        numResults = 2, mode = "exact", direction = "similarity")
        .orderBy((col("x") - col("y")).desc)
      val plan = result.queryExecution.executedPlan
      assert(plan.treeString.contains("BroadcastNearestByJoin"),
        "Expected BroadcastNearestByJoinExec in plan but got:\n" + plan.treeString)
      checkAnswer(result, Seq(
        Row(1, 5, 11, 3),   // ranking = 2 (largest)
        Row(1, 5, 10, 6)    // ranking = -1 (second largest)
      ))
    }
  }

  test("tie-breaking - equal distances produce correct count") {
    withStreamingHeap {
      // 4 right rows all at distance 1.0 from x=5.0; k=2
      val left = Seq((1, 5.0)).toDF("id", "x")
      val right = Seq((10, 4.0), (11, 6.0), (12, 4.0), (13, 6.0)).toDF("rid", "y")
      val result = left.nearestByJoin(right, abs(col("x") - col("y")),
        numResults = 2, mode = "exact", direction = "distance")
      val plan = result.queryExecution.executedPlan
      assert(plan.treeString.contains("BroadcastNearestByJoin"),
        "Expected BroadcastNearestByJoinExec in plan but got:\n" + plan.treeString)
      // All 4 are tied at distance 1.0; we should get exactly k=2 results
      assert(result.count() == 2)
      // Each result should have distance 1.0
      result.collect().foreach { row =>
        val y = row.getAs[Double]("y")
        assert(math.abs(5.0 - y) == 1.0)
      }
    }
  }

  // ==========================================================================
  // SPARK-57091: Tests for ranking comparison fix (TypeUtils.getInterpretedOrdering)
  // ==========================================================================

  test("SPARK-57091: DateType ranking - nearest by earliest date (distance)") {
    withStreamingHeap {
      // Date ranking: the old Cast(_, DoubleType) cannot cast dates to double,
      // producing null rankings and empty results for INNER join.
      val left = Seq((1, Date.valueOf("2024-06-15"))).toDF("id", "ref_date")
      val right = Seq(
        (10, Date.valueOf("2024-06-10")),
        (11, Date.valueOf("2024-06-20")),
        (12, Date.valueOf("2024-12-01"))
      ).toDF("rid", "event_date")
      // Use event_date as ranking; direction=distance means earliest dates win
      val result = left.nearestByJoin(right, col("event_date"),
        numResults = 2, mode = "exact", direction = "distance")
        .orderBy("event_date")
      val plan = result.queryExecution.executedPlan
      assert(plan.treeString.contains("BroadcastNearestByJoin"),
        "Expected BroadcastNearestByJoinExec in plan but got:\n" + plan.treeString)
      checkAnswer(result, Seq(
        Row(1, Date.valueOf("2024-06-15"), 10, Date.valueOf("2024-06-10")),
        Row(1, Date.valueOf("2024-06-15"), 11, Date.valueOf("2024-06-20"))
      ))
    }
  }

  test("SPARK-57091: DateType ranking - similarity picks latest date") {
    withStreamingHeap {
      // Same DateType scenario but direction=similarity (largest value wins = latest date).
      // The old Cast(_, DoubleType) cannot cast dates, yielding empty results.
      val left = Seq((1, Date.valueOf("2024-06-15"))).toDF("id", "ref_date")
      val right = Seq(
        (10, Date.valueOf("2024-01-01")),
        (11, Date.valueOf("2024-06-20")),
        (12, Date.valueOf("2024-12-01"))
      ).toDF("rid", "event_date")
      val result = left.nearestByJoin(right, col("event_date"),
        numResults = 2, mode = "exact", direction = "similarity")
        .orderBy(col("event_date").desc)
      val plan = result.queryExecution.executedPlan
      assert(plan.treeString.contains("BroadcastNearestByJoin"),
        "Expected BroadcastNearestByJoinExec in plan but got:\n" + plan.treeString)
      // Largest (latest) dates: 2024-12-01, 2024-06-20
      checkAnswer(result, Seq(
        Row(1, Date.valueOf("2024-06-15"), 12, Date.valueOf("2024-12-01")),
        Row(1, Date.valueOf("2024-06-15"), 11, Date.valueOf("2024-06-20"))
      ))
    }
  }

  test("SPARK-57091: Long ranking past 2^53 - distinguishes values equal as Double") {
    withStreamingHeap {
      // Two Long values that differ by 1 but are equal when cast to Double:
      // Long.MAX_VALUE - 1 and Long.MAX_VALUE both cast to the same Double (9.223372036854776E18)
      // The old Cast(_, DoubleType) approach would see them as equal and pick arbitrarily.
      val v1 = Long.MaxValue - 1 // 9223372036854775806
      val v2 = Long.MaxValue     // 9223372036854775807
      // Verify they are indeed equal as Double (precondition)
      assert(v1.toDouble == v2.toDouble,
        "precondition: these Longs must be equal as Double to test the fix")

      val left = Seq((1, 0L)).toDF("id", "x")
      val right = Seq((10, v1), (11, v2)).toDF("rid", "y")
      // direction=distance: smallest y wins. v1 < v2 as Long but equal as Double.
      val result = left.nearestByJoin(right, col("y"),
        numResults = 1, mode = "exact", direction = "distance")
      val plan = result.queryExecution.executedPlan
      assert(plan.treeString.contains("BroadcastNearestByJoin"),
        "Expected BroadcastNearestByJoinExec in plan but got:\n" + plan.treeString)
      // With proper Long ordering, v1 (smaller) is chosen deterministically
      checkAnswer(result, Seq(Row(1, 0L, 10, v1)))
    }
  }

  test("SPARK-57091: Decimal ranking preserves precision beyond Double") {
    withStreamingHeap {
      // Decimals with 18 digits of precision: these are identical when cast to Double
      // but differ in the last digit as Decimal.
      val d1 = new java.math.BigDecimal("1.000000000000000001")
      val d2 = new java.math.BigDecimal("1.000000000000000002")
      // Verify they are equal as Double (precondition)
      assert(d1.doubleValue() == d2.doubleValue(),
        "precondition: these Decimals must be equal as Double to test the fix")

      val left = spark.createDataFrame(
        Seq((1, new java.math.BigDecimal("0")))).toDF("id", "x")
      val right = spark.createDataFrame(
        Seq((10, d1), (11, d2))).toDF("rid", "y")
      // direction=distance: smallest y wins. d1 < d2 as Decimal but equal as Double.
      val result = left.nearestByJoin(right, col("y"),
        numResults = 1, mode = "exact", direction = "distance")
      val plan = result.queryExecution.executedPlan
      assert(plan.treeString.contains("BroadcastNearestByJoin"),
        "Expected BroadcastNearestByJoinExec in plan but got:\n" + plan.treeString)
      checkAnswer(result, Seq(Row(1, new java.math.BigDecimal("0"), 10, d1)))
    }
  }

  test("SPARK-57091: output schema nullability - INNER join has all-nullable columns") {
    withStreamingHeap {
      // The fix ensures both left and right output attributes are nullable for INNER join.
      // With the old approach, left side would retain original nullability (non-nullable for
      // spark.range), causing schema mismatch with the logical plan.
      val left = spark.range(3).toDF("id").withColumn("x", col("id").cast("double"))
      val right = Seq((10, 1.0), (11, 2.0)).toDF("rid", "y")
      val result = left.nearestByJoin(right, abs(col("x") - col("y")),
        numResults = 1, mode = "exact", direction = "distance")
      val plan = result.queryExecution.executedPlan
      assert(plan.toString.contains("BroadcastNearestByJoin"),
        "expected BroadcastNearestByJoinExec in plan")
      // All output columns must be nullable
      plan.output.foreach { attr =>
        assert(attr.nullable,
          s"column '${attr.name}' should be nullable in INNER join output but was not")
      }
    }
  }

  test("SPARK-57091: gate off - broadcast operator does not fire, rewrite path used") {
    // When spark.sql.join.nearestBy.broadcast.enabled is false (default),
    // the BroadcastNearestByJoinExec must NOT appear and the rewrite path must be used.
    withSQLConf(
      SQLConf.NEAREST_BY_BROADCAST_ENABLED.key -> "false",
      SQLConf.CROSS_JOINS_ENABLED.key -> "true") {
      val left = Seq((1, 10.0), (2, 20.0)).toDF("id", "x")
      val right = Seq((10, 9.0), (11, 21.0)).toDF("rid", "y")
      val result = left.nearestByJoin(right, abs(col("x") - col("y")),
        numResults = 1, mode = "exact", direction = "distance")
      val plan = result.queryExecution.executedPlan.toString()
      assert(!plan.contains("BroadcastNearestByJoin"),
        "BroadcastNearestByJoinExec must NOT appear when gate is off")
      // Results are still correct via rewrite path
      checkAnswer(result.orderBy("id"), Seq(
        Row(1, 10.0, 10, 9.0),
        Row(2, 20.0, 11, 21.0)
      ))
    }
  }

  test("SPARK-57091: Long ranking - similarity direction orders correctly past 2^53") {
    withStreamingHeap {
      // v1 = 2^53 - 1, v2 = 2^53, v3 = 2^53 + 1. v2 and v3 are equal as Double
      // but v1 < v2 < v3 as Long. With k=1 and similarity (largest wins), proper
      // Long ordering deterministically keeps v3 because v3 > v2 strictly.
      // With a Double cast v2==v3, and the result would be non-deterministic.
      val v2 = (1L << 53)      // 9007199254740992
      val v3 = (1L << 53) + 1  // 9007199254740993
      assert(v2.toDouble == v3.toDouble,
        "precondition: v2 and v3 must be equal as Double")

      val left = Seq((1, 0L)).toDF("id", "x")
      // v3 is inserted first; with correct Long comparison the heap retains it
      // because v3 > v2 strictly, so v2 fails the retention check and is never offered.
      val right = Seq((12, v3), (11, v2)).toDF("rid", "y")
      val result = left.nearestByJoin(right, col("y"),
        numResults = 1, mode = "exact", direction = "similarity")
      val plan = result.queryExecution.executedPlan
      assert(plan.treeString.contains("BroadcastNearestByJoin"),
        "Expected BroadcastNearestByJoinExec in plan but got:\n" + plan.treeString)
      // With proper Long ordering, v3 (larger) must be kept
      checkAnswer(result, Seq(Row(1, 0L, 12, v3)))
    }
  }

  test("SPARK-57091: AQE ValidateSparkPlan accepts BroadcastNearestByJoinExec") {
    // Directly verify that ValidateSparkPlan does not reject a
    // BroadcastNearestByJoinExec whose right child is a BroadcastQueryStageExec.
    // Without the explicit `case b: BroadcastNearestByJoinExec` in ValidateSparkPlan,
    // the default case recurses into the BroadcastQueryStageExec child and throws
    // InvalidAQEPlanException.
    import org.apache.spark.sql.execution.adaptive.ValidateSparkPlan
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
      SQLConf.NEAREST_BY_BROADCAST_ENABLED.key -> "true") {
      val left = Seq((1, 10.0), (2, 20.0), (3, 30.0)).toDF("id", "x")
      val right = Seq((10, 9.0), (11, 15.0), (12, 21.0), (13, 29.0)).toDF("rid", "y")
      val df = left.nearestByJoin(right, abs(col("x") - col("y")),
        numResults = 2, mode = "exact", direction = "distance")
      val executedPlan = df.queryExecution.executedPlan
      // After execution, the plan tree has BroadcastNearestByJoinExec with
      // BroadcastQueryStageExec as its right child
      val adaptivePlan = executedPlan match {
        case aqe: AdaptiveSparkPlanExec =>
          df.collect() // force execution
          aqe.executedPlan
        case other => other
      }
      // Find the BroadcastNearestByJoinExec with its BroadcastQueryStageExec right child
      val nearestByOps = collect(adaptivePlan) {
        case b: BroadcastNearestByJoinExec => b
      }
      assert(nearestByOps.nonEmpty,
        "Expected BroadcastNearestByJoinExec in plan but got:\n" + adaptivePlan.treeString)
      val nb = nearestByOps.head
      assert(nb.right.isInstanceOf[BroadcastQueryStageExec],
        s"Expected BroadcastQueryStageExec as right child but got: ${nb.right.getClass.getName}")
      // Directly invoke ValidateSparkPlan on the subtree rooted at
      // BroadcastNearestByJoinExec. This must not throw InvalidAQEPlanException.
      // If the case in ValidateSparkPlan for our operator is missing, this throws.
      ValidateSparkPlan.apply(nb)
    }
  }

  test("SPARK-57091: StringType ranking - buffer retention with heap eviction") {
    withStreamingHeap {
      // Use StringType as the ranking column. UnsafeProjection reuses its output buffer,
      // so UTF8String values point into the mutable buffer. Without .copy(), earlier
      // heap entries get corrupted when the buffer is overwritten on subsequent iterations.
      // Data is ordered so that LATER rows have BETTER (smaller) ranking values and
      // DISPLACE earlier retained entries, exercising the variable-length .copy() path.
      val left = Seq((1, "ref")).toDF("id", "x")
      val right = Seq(
        (10, "hhh"), (11, "ggg"), (12, "fff"), (13, "eee"),
        (14, "ddd"), (15, "ccc"), (16, "bbb"), (17, "aaa")
      ).toDF("rid", "label")
      // direction=distance: smallest string wins (lexicographic). k=2 -> "aaa", "bbb"
      // The first entries in the heap are "hhh","ggg" which get displaced by later
      // better values, forcing .copy() of the retained ranking values.
      val result = left.nearestByJoin(right, col("label"),
        numResults = 2, mode = "exact", direction = "distance")
        .orderBy("label")
      val plan = result.queryExecution.executedPlan
      assert(plan.treeString.contains("BroadcastNearestByJoin"),
        "Expected BroadcastNearestByJoinExec in plan but got:\n" + plan.treeString)
      checkAnswer(result, Seq(
        Row(1, "ref", 17, "aaa"),
        Row(1, "ref", 16, "bbb")
      ))
    }
  }

  test("NaN ranking values under similarity direction") {
    withStreamingHeap {
      val left = Seq((1, 5.0)).toDF("id", "x")
      val right = Seq((10, Double.NaN), (11, 3.0), (12, 8.0), (13, 10.0)).toDF("rid", "y")
      // direction=similarity: largest ranking value wins. NaN is largest in Java ordering.
      val result = left.nearestByJoin(right, col("y"),
        numResults = 2, mode = "exact", direction = "similarity")
      val plan = result.queryExecution.executedPlan
      assert(plan.treeString.contains("BroadcastNearestByJoin"),
        "Expected BroadcastNearestByJoinExec in plan but got:\n" + plan.treeString)
      // NaN is greatest per Java Double ordering, so top-2 similarity = NaN, 10.0
      checkAnswer(result.orderBy(col("y").desc_nulls_last), Seq(
        Row(1, 5.0, 10, Double.NaN),
        Row(1, 5.0, 13, 10.0)
      ))
    }
  }

  test("null in non-ranking right column propagates correctly") {
    withStreamingHeap {
      val left = Seq((1, 5.0)).toDF("id", "x")
      // "label" column has nulls but is NOT the ranking column
      val right = Seq(
        (10, 3.0, Some("a")),
        (11, 7.0, None),
        (12, 100.0, Some("c"))
      ).toDF("rid", "y", "label")
      val result = left.nearestByJoin(right, abs(col("x") - col("y")),
        numResults = 2, mode = "exact", direction = "distance")
        .orderBy("rid")
      val plan = result.queryExecution.executedPlan
      assert(plan.treeString.contains("BroadcastNearestByJoin"),
        "Expected BroadcastNearestByJoinExec in plan but got:\n" + plan.treeString)
      // Top-2 nearest: rid=10 (d=2.0, label="a"), rid=11 (d=2.0, label=null)
      checkAnswer(result, Seq(
        Row(1, 5.0, 10, 3.0, "a"),
        Row(1, 5.0, 11, 7.0, null)
      ))
    }
  }

  test("SPARK-57091: non-deterministic ranking expression (rand()) does not throw") {
    withStreamingHeap {
      // A non-deterministic ranking expression must have its projection initialized
      // with the partition index before evaluation. Without initialization, rand() throws
      // "Nondeterministic expression ... has not been initialized".
      val left = spark.range(10).toDF("id").withColumn("x", col("id").cast("double"))
      val right = Seq((10, 1.0), (11, 2.0), (12, 3.0)).toDF("rid", "y")
      // Use rand() as ranking: non-deterministic, returns Double
      val result = left.nearestByJoin(right, rand(),
        numResults = 2, mode = "exact", direction = "similarity")
      val plan = result.queryExecution.executedPlan
      assert(plan.treeString.contains("BroadcastNearestByJoin"),
        "Expected BroadcastNearestByJoinExec in plan but got:\n" + plan.treeString)
      // Each of 10 left rows should get 2 right rows (k=2, right has 3 rows)
      assert(result.count() == 20)
    }
  }

  test("SPARK-57091: DSv2 right side does not throw INTERNAL_ERROR with flag ON") {
    // Regression: before the stats-free fix, reading right.stats.sizeInBytes in the
    // optimizer's FinishAnalysis batch triggered computeStats on a DSv2 source before
    // filter/partition pushdown completed, throwing [INTERNAL_ERROR]. With the fix,
    // the optimizer no longer reads stats -- the NearestByJoin node is left intact for
    // the planner, which unconditionally plans BroadcastNearestByJoinExec.
    withSQLConf(
      "spark.sql.catalog.testcat" ->
        classOf[org.apache.spark.sql.connector.catalog.InMemoryTableCatalog].getName,
      SQLConf.NEAREST_BY_BROADCAST_ENABLED.key -> "true") {
      spark.sql(
        """CREATE TABLE testcat.right_tbl (rid INT, y DOUBLE)
          |USING foo""".stripMargin)
      try {
        spark.sql("INSERT INTO testcat.right_tbl VALUES (10, 1.0), (11, 5.0), (12, 9.0)")
        val left = Seq((1, 3.0), (2, 7.0)).toDF("id", "x")
        val right = spark.table("testcat.right_tbl")
        val result = left.nearestByJoin(right, abs(col("x") - col("y")),
          numResults = 2, mode = "exact", direction = "distance")
        val plan = result.queryExecution.executedPlan
        assert(plan.treeString.contains("BroadcastNearestByJoin"),
          "Expected BroadcastNearestByJoinExec in plan but got:\n" + plan.treeString)
        // Verify correct results:
        // id=1(x=3.0) nearest y=1.0(d=2),5.0(d=2)
        // id=2(x=7.0) nearest y=5.0(d=2),9.0(d=2)
        assert(result.count() == 4)
        val row1 = result.filter(col("id") === 1).orderBy(abs(col("x") - col("y"))).collect()
        assert(row1.length == 2)
      } finally {
        spark.sql("DROP TABLE IF EXISTS testcat.right_tbl")
      }
    }
  }

  test("SPARK-57091: partitioned right table with flag ON uses BroadcastNearestByJoin") {
    // Regression: before the stats-free fix, partitioned file tables reported inflated
    // sizeInBytes before filter pushdown. With the unconditional broadcast contract,
    // the planner always plans BroadcastNearestByJoinExec when the flag is ON.
    withStreamingHeap {
      withTempPath { dir =>
        // Create partitioned parquet data
        val data = (1 to 100).map(i => (i % 5, i, i.toDouble))
        spark.createDataFrame(data).toDF("part", "rid", "y")
          .write.partitionBy("part").parquet(dir.getAbsolutePath)

        val right = spark.read.parquet(dir.getAbsolutePath).filter(col("part") === 0)
        val left = Seq((1, 10.0), (2, 50.0)).toDF("id", "x")
        val result = left.nearestByJoin(right, abs(col("x") - col("y")),
          numResults = 3, mode = "exact", direction = "distance")
        val plan = result.queryExecution.executedPlan
        assert(plan.treeString.contains("BroadcastNearestByJoin"),
          "Expected BroadcastNearestByJoinExec for partitioned right but got:\n" +
          plan.treeString)
        // part=0 has values 5,10,15,...,100 (20 rows). Each left row gets 3 nearest.
        assert(result.count() == 6)
      }
    }
  }

  // ==========================================================================
  // F7: crossJoin.enabled divergence -- operator path does not require it
  // ==========================================================================

  test("SPARK-57091: flag ON succeeds without crossJoin.enabled") {
    // When the broadcast flag is ON, no Join node is built so CheckCartesianProducts
    // does not apply. NEAREST BY should succeed even with crossJoin.enabled = false.
    withSQLConf(
      SQLConf.NEAREST_BY_BROADCAST_ENABLED.key -> "true",
      SQLConf.CROSS_JOINS_ENABLED.key -> "false") {
      val left = Seq((1, 10.0), (2, 20.0)).toDF("id", "x")
      val right = Seq((10, 9.0), (11, 15.0), (12, 21.0)).toDF("rid", "y")
      val result = left.nearestByJoin(right, abs(col("x") - col("y")),
        numResults = 2, mode = "exact", direction = "distance")
      val plan = result.queryExecution.executedPlan
      assert(plan.treeString.contains("BroadcastNearestByJoin"),
        "Expected BroadcastNearestByJoinExec in plan but got:\n" + plan.treeString)
      checkAnswer(result.filter(col("id") === 1).orderBy(abs(col("x") - col("y"))), Seq(
        Row(1, 10.0, 10, 9.0),
        Row(1, 10.0, 11, 15.0)
      ))
      checkAnswer(result.filter(col("id") === 2).orderBy(abs(col("x") - col("y"))), Seq(
        Row(2, 20.0, 12, 21.0),
        Row(2, 20.0, 11, 15.0)
      ))
    }
  }

  // ==========================================================================
  // EXPLAIN FORMATTED output
  // ==========================================================================

  test("SPARK-57091: EXPLAIN FORMATTED shows ranking, k, and direction") {
    withStreamingHeap {
      val left = Seq((1, 10.0)).toDF("id", "x")
      val right = Seq((10, 9.0)).toDF("rid", "y")
      val df = left.nearestByJoin(right, abs(col("x") - col("y")),
        numResults = 3, mode = "exact", direction = "distance")
      val explain = df.queryExecution.explainString(
        org.apache.spark.sql.execution.FormattedMode)
      assert(explain.contains("NumResults: 3"),
        s"EXPLAIN FORMATTED should contain 'NumResults: 3'\n$explain")
      assert(explain.contains("Direction: NearestByDistance"),
        s"EXPLAIN FORMATTED should contain 'Direction: NearestByDistance'\n$explain")
      assert(explain.contains("Ranking:"),
        s"EXPLAIN FORMATTED should contain 'Ranking:'\n$explain")
      assert(explain.contains("Join type: Inner"),
        s"EXPLAIN FORMATTED should contain 'Join type: Inner'\n$explain")
    }
  }

  // ==========================================================================
  // Cross-child Python UDF fallback to rewrite path
  // ==========================================================================

  test("SPARK-57091: cross-child Python UDF in ranking routes through rewrite path") {
    // When the ranking expression contains a scalar Python UDF whose references span both
    // left and right children, the operator path would crash in ExtractPythonUDFs with
    // "Invalid PythonUDF ... requires attributes from more than one child." The fix in
    // RewriteNearestByJoin detects this and falls back to the rewrite even when the
    // broadcast flag is ON.
    //
    // This test constructs a NearestByJoin with a mock PythonUDF referencing both sides
    // and verifies the optimized plan uses the rewrite (Join + Aggregate) rather than
    // BroadcastNearestByJoinExec. A null PythonFunction prevents actual execution but
    // is sufficient to exercise the optimizer guard.
    import org.apache.spark.api.python.PythonEvalType
    import org.apache.spark.sql.catalyst.expressions.PythonUDF
    import org.apache.spark.sql.catalyst.plans.logical.NearestByJoin
    import org.apache.spark.sql.catalyst.plans.{Inner, NearestByDistance}

    withSQLConf(
      SQLConf.NEAREST_BY_BROADCAST_ENABLED.key -> "true",
      SQLConf.CROSS_JOINS_ENABLED.key -> "true") {
      val left = Seq((1, 10.0), (2, 20.0)).toDF("id", "x")
      val right = Seq((10, 9.0), (11, 15.0), (12, 21.0)).toDF("rid", "y")
      val leftPlan = left.queryExecution.analyzed
      val rightPlan = right.queryExecution.analyzed

      // Construct a PythonUDF that references attributes from both children.
      val leftAttr = leftPlan.output.find(_.name == "x").get
      val rightAttr = rightPlan.output.find(_.name == "y").get
      val crossChildUDF = PythonUDF(
        "cross_child_distance", null,
        org.apache.spark.sql.types.DoubleType,
        Seq(leftAttr, rightAttr),
        PythonEvalType.SQL_BATCHED_UDF, udfDeterministic = true)

      val nearestBy = NearestByJoin(
        leftPlan, rightPlan, Inner, approx = false, numResults = 2,
        rankingExpression = crossChildUDF,
        direction = NearestByDistance)

      // Run the optimizer on this plan. The RewriteNearestByJoin rule should detect
      // the cross-child Python UDF and apply the rewrite despite the flag being ON.
      // The NearestByJoin is already resolved (children are analyzed plans from DataFrames).
      val optimized = spark.sessionState.optimizer.execute(nearestBy)

      // The optimized plan must NOT contain a NearestByJoin node (it should be rewritten).
      assert(!optimized.exists(_.isInstanceOf[NearestByJoin]),
        "NearestByJoin should have been rewritten due to cross-child Python UDF, " +
          "but it was left intact. Plan:\n" + optimized.treeString)
    }
  }

  // ==========================================================================
  // Parity test: rewrite vs operator produce identical results
  // ==========================================================================

  test("SPARK-57091: rewrite-vs-operator parity") {
    val left = Seq((1, 10.0), (2, 20.0), (3, 0.0)).toDF("id", "x")
    val right = Seq((10, 9.0), (11, 15.0), (12, 21.0), (13, 0.5), (14, 100.0))
      .toDF("rid", "y")

    // Case 1: INNER join, distance
    def innerDistance(flagOn: Boolean): Array[Row] = {
      withSQLConf(
        SQLConf.NEAREST_BY_BROADCAST_ENABLED.key -> flagOn.toString,
        SQLConf.CROSS_JOINS_ENABLED.key -> "true") {
        left.nearestByJoin(right, abs(col("x") - col("y")),
          numResults = 2, mode = "exact", direction = "distance")
          .orderBy("id", "rid").collect()
      }
    }
    assert(innerDistance(true).toSeq == innerDistance(false).toSeq,
      "INNER DISTANCE: operator and rewrite should produce identical results")

    // Case 2: INNER join, similarity
    def innerSimilarity(flagOn: Boolean): Array[Row] = {
      withSQLConf(
        SQLConf.NEAREST_BY_BROADCAST_ENABLED.key -> flagOn.toString,
        SQLConf.CROSS_JOINS_ENABLED.key -> "true") {
        left.nearestByJoin(right, abs(col("x") - col("y")),
          numResults = 2, mode = "exact", direction = "similarity")
          .orderBy("id", "rid").collect()
      }
    }
    assert(innerSimilarity(true).toSeq == innerSimilarity(false).toSeq,
      "INNER SIMILARITY: operator and rewrite should produce identical results")

    // Case 3: LEFT OUTER with right whose ranking values are ALL NULL
    val rightAllNull = Seq((10, None: Option[Double]), (11, None: Option[Double]))
      .toDF("rid", "y")
    def leftOuterAllNull(flagOn: Boolean): Array[Row] = {
      withSQLConf(
        SQLConf.NEAREST_BY_BROADCAST_ENABLED.key -> flagOn.toString,
        SQLConf.CROSS_JOINS_ENABLED.key -> "true") {
        left.nearestByJoin(rightAllNull, col("y"),
          numResults = 2, mode = "exact", direction = "distance",
          joinType = "left_outer")
          .orderBy("id").collect()
      }
    }
    assert(leftOuterAllNull(true).toSeq == leftOuterAllNull(false).toSeq,
      "LEFT OUTER ALL-NULL: operator and rewrite should produce identical results")

    // Case 4: Per-left-row result ordering (best-first).
    // With a single left row and no ties, the raw collect() order is deterministic
    // and best-first for both paths -- no orderBy needed.
    def orderedResults(flagOn: Boolean): Array[Row] = {
      withSQLConf(
        SQLConf.NEAREST_BY_BROADCAST_ENABLED.key -> flagOn.toString,
        SQLConf.CROSS_JOINS_ENABLED.key -> "true") {
        val singleLeft = Seq((1, 10.0)).toDF("id", "x")
        singleLeft.nearestByJoin(right, abs(col("x") - col("y")),
          numResults = 3, mode = "exact", direction = "distance")
          .collect()
      }
    }
    assert(orderedResults(true).toSeq == orderedResults(false).toSeq,
      "ORDERED RESULTS: operator and rewrite should produce identical best-first order")
  }

  // ==========================================================================
  // Per-partition HeapEntry pooling: allocation-bound invariant test
  // ==========================================================================

  test("SPARK-57091: HeapEntry allocations bounded by min(k, rightRows) per partition") {
    // This test verifies the per-partition pooling invariant: the total number of
    // HeapEntry allocations must be bounded by min(k, rightRows.length), NOT by
    // leftRows * k. With per-left-row allocation the count would be ~100 * 2 = 200;
    // with per-partition pooling it must be <= min(k=2, rightRows=5) = 2.
    withStreamingHeap {
      // Reset the allocation counter before the query.
      HeapEntry.resetAllocationCount()

      // 100 left rows, 5 right rows, k=2. Force a single partition so the entire
      // workload runs in one task with one pool.
      val left = spark.range(100).toDF("id").withColumn("x", col("id").cast("double"))
        .coalesce(1)
      val right = Seq((10, 1.0), (11, 2.0), (12, 3.0), (13, 4.0), (14, 5.0))
        .toDF("rid", "y")
      val result = left.nearestByJoin(right, abs(col("x") - col("y")),
        numResults = 2, mode = "exact", direction = "distance")
      val plan = result.queryExecution.executedPlan
      assert(plan.treeString.contains("BroadcastNearestByJoin"),
        "Expected BroadcastNearestByJoinExec in plan but got:\n" + plan.treeString)

      // Force execution to populate the counter.
      val rows = result.collect()
      assert(rows.length == 200, s"Expected 200 rows (100 left * k=2) but got ${rows.length}")

      // The invariant: total allocations bounded by min(k, rightRows.length) = min(2, 5) = 2
      // per partition. With a single partition, the counter must be <= 2.
      // Under the old per-left-row code this would be ~200 (100 left rows * k=2).
      val allocations = HeapEntry.allocationCount
      assert(allocations <= 2,
        s"HeapEntry allocations should be bounded by min(k=2, rightRows=5) = 2 per " +
          s"partition, but got $allocations (indicates per-left-row allocation, not pooling)")
    }
  }
}
