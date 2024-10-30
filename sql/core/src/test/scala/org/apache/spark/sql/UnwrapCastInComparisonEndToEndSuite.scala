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

import java.time.LocalDateTime

import org.apache.spark.sql.catalyst.expressions.IntegralLiteralTestUtils.{negativeInt, positiveInt}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.Decimal

class UnwrapCastInComparisonEndToEndSuite extends QueryTest with SharedSparkSession {
  import testImplicits._

  val t = "test_table"

  test("cases when literal is max") {
    withTable(t) {
      Seq[(Integer, java.lang.Short, java.lang.Float)](
        (1, 100.toShort, 3.14.toFloat),
        (2, Short.MaxValue, Float.NaN),
        (3, Short.MinValue, Float.PositiveInfinity),
        (4, 0.toShort, Float.MaxValue),
        (5, null, null))
        .toDF("c1", "c2", "c3").write.saveAsTable(t)
      val df = spark.table(t)

      val lit = Short.MaxValue.toInt
      checkAnswer(df.where(s"c2 > $lit").select("c1"), Seq.empty)
      checkAnswer(df.where(s"c2 >= $lit").select("c1"), Row(2))
      checkAnswer(df.where(s"c2 == $lit").select("c1"), Row(2))
      checkAnswer(df.where(s"c2 <=> $lit").select("c1"), Row(2))
      checkAnswer(df.where(s"c2 != $lit").select("c1"), Row(1) :: Row(3) :: Row(4) :: Nil)
      checkAnswer(df.where(s"c2 <= $lit").select("c1"), Row(1) :: Row(2) :: Row(3) :: Row(4) :: Nil)
      checkAnswer(df.where(s"c2 < $lit").select("c1"), Row(1) :: Row(3) :: Row(4) :: Nil)

      checkAnswer(df.where(s"c3 > double('nan')").select("c1"), Seq.empty)
      checkAnswer(df.where(s"c3 >= double('nan')").select("c1"), Row(2))
      checkAnswer(df.where(s"c3 == double('nan')").select("c1"), Row(2))
      checkAnswer(df.where(s"c3 <=> double('nan')").select("c1"), Row(2))
      checkAnswer(df.where(s"c3 != double('nan')").select("c1"), Row(1) :: Row(3) :: Row(4) :: Nil)
      checkAnswer(df.where(s"c3 <= double('nan')").select("c1"),
        Row(1) :: Row(2) :: Row(3) :: Row(4) :: Nil)
      checkAnswer(df.where(s"c3 < double('nan')").select("c1"), Row(1) :: Row(3) :: Row(4) :: Nil)
    }
  }

  test("cases when literal is > max") {
    withTable(t) {
      Seq[(Integer, java.lang.Short)](
        (1, 100.toShort),
        (2, Short.MaxValue),
        (3, null))
        .toDF("c1", "c2").write.saveAsTable(t)
      val df = spark.table(t)
      val lit = positiveInt
      checkAnswer(df.where(s"c2 > $lit").select("c1"), Seq.empty)
      checkAnswer(df.where(s"c2 >= $lit").select("c1"), Seq.empty)
      checkAnswer(df.where(s"c2 == $lit").select("c1"), Seq.empty)
      checkAnswer(df.where(s"c2 <=> $lit").select("c1"), Seq.empty)
      checkAnswer(df.where(s"c2 != $lit").select("c1"), Row(1) :: Row(2) :: Nil)
      checkAnswer(df.where(s"c2 <= $lit").select("c1"), Row(1) :: Row(2) :: Nil)
      checkAnswer(df.where(s"c2 < $lit").select("c1"), Row(1) :: Row(2) :: Nil)

      // No test for float case since NaN is greater than any other numeric value
    }
  }

  test("cases when literal is min") {
    withTable(t) {
      Seq[(Integer, java.lang.Short, java.lang.Float)](
        (1, 100.toShort, 3.14.toFloat),
        (2, Short.MinValue, Float.NegativeInfinity),
        (3, Short.MaxValue, Float.MinValue),
        (4, null, null))
        .toDF("c1", "c2", "c3").write.saveAsTable(t)
      val df = spark.table(t)

      val lit = Short.MinValue.toInt
      checkAnswer(df.where(s"c2 > $lit").select("c1"), Row(1) :: Row(3) :: Nil)
      checkAnswer(df.where(s"c2 >= $lit").select("c1"), Row(1) :: Row(2) :: Row(3) :: Nil)
      checkAnswer(df.where(s"c2 == $lit").select("c1"), Row(2))
      checkAnswer(df.where(s"c2 <=> $lit").select("c1"), Row(2))
      checkAnswer(df.where(s"c2 != $lit").select("c1"), Row(1) :: Row(3) :: Nil)
      checkAnswer(df.where(s"c2 <= $lit").select("c1"), Row(2))
      checkAnswer(df.where(s"c2 < $lit").select("c1"), Seq.empty)

      checkAnswer(df.where(s"c3 > double('-inf')").select("c1"), Row(1) :: Row(3) :: Nil)
      checkAnswer(df.where(s"c3 >= double('-inf')").select("c1"), Row(1) :: Row(2) :: Row(3) :: Nil)
      checkAnswer(df.where(s"c3 == double('-inf')").select("c1"), Row(2))
      checkAnswer(df.where(s"c3 <=> double('-inf')").select("c1"), Row(2))
      checkAnswer(df.where(s"c3 != double('-inf')").select("c1"), Row(1) :: Row(3) :: Nil)
      checkAnswer(df.where(s"c3 <= double('-inf')").select("c1"), Row(2) :: Nil)
      checkAnswer(df.where(s"c3 < double('-inf')").select("c1"), Seq.empty)
    }
  }

  test("cases when literal is < min") {
    val t = "test_table"
    withTable(t) {
      Seq[(Integer, java.lang.Short)](
        (1, 100.toShort),
        (2, Short.MinValue),
        (3, null))
        .toDF("c1", "c2").write.saveAsTable(t)
      val df = spark.table(t)

      val lit = negativeInt
      checkAnswer(df.where(s"c2 > $lit").select("c1"), Row(1) :: Row(2) :: Nil)
      checkAnswer(df.where(s"c2 >= $lit").select("c1"), Row(1) :: Row(2) :: Nil)
      checkAnswer(df.where(s"c2 == $lit").select("c1"), Seq.empty)
      checkAnswer(df.where(s"c2 <=> $lit").select("c1"), Seq.empty)
      checkAnswer(df.where(s"c2 != $lit").select("c1"), Row(1) :: Row(2) :: Nil)
      checkAnswer(df.where(s"c2 <= $lit").select("c1"), Seq.empty)
      checkAnswer(df.where(s"c2 < $lit").select("c1"), Seq.empty)
    }
  }

  test("cases when literal is within range (min, max)") {
    withTable(t) {
      Seq((1, 300.toShort), (2, 500.toShort)).toDF("c1", "c2").write.saveAsTable(t)
      val df = spark.table(t)

      checkAnswer(df.where("c2 < 200").select("c1"), Seq.empty)
      checkAnswer(df.where("c2 < 400").select("c1"), Row(1) :: Nil)
      checkAnswer(df.where("c2 < 600").select("c1"), Row(1) :: Row(2) :: Nil)

      checkAnswer(df.where("c2 <= 100").select("c1"), Seq.empty)
      checkAnswer(df.where("c2 <= 300").select("c1"), Row(1) :: Nil)
      checkAnswer(df.where("c2 <= 500").select("c1"), Row(1) :: Row(2) :: Nil)

      checkAnswer(df.where("c2 == 100").select("c1"), Seq.empty)
      checkAnswer(df.where("c2 == 300").select("c1"), Row(1) :: Nil)
      checkAnswer(df.where("c2 == 500").select("c1"), Row(2) :: Nil)

      checkAnswer(df.where("c2 <=> 100").select("c1"), Seq.empty)
      checkAnswer(df.where("c2 <=> 300").select("c1"), Row(1) :: Nil)
      checkAnswer(df.where("c2 <=> 500").select("c1"), Row(2) :: Nil)
      checkAnswer(df.where("c2 <=> null").select("c1"), Seq.empty)

      checkAnswer(df.where("c2 >= 200").select("c1"), Row(1) :: Row(2) :: Nil)
      checkAnswer(df.where("c2 >= 400").select("c1"), Row(2) :: Nil)
      checkAnswer(df.where("c2 >= 600").select("c1"), Seq.empty)

      checkAnswer(df.where("c2 > 100").select("c1"), Row(1) :: Row(2) :: Nil)
      checkAnswer(df.where("c2 > 300").select("c1"), Row(2) :: Nil)
      checkAnswer(df.where("c2 > 500").select("c1"), Seq.empty)
    }
  }

  test("cases when literal is within range (min, max) and has rounding up or down") {
    withTable(t) {
      Seq((1, 100, 3.14.toFloat, decimal(200.12)))
        .toDF("c1", "c2", "c3", "c4").write.saveAsTable(t)
      val df = spark.table(t)

      checkAnswer(df.where("c2 > 99.6").select("c1"), Row(1))
      checkAnswer(df.where("c2 > 100.4").select("c1"), Seq.empty)
      checkAnswer(df.where("c2 == 100.4").select("c1"), Seq.empty)
      checkAnswer(df.where("c2 <=> 100.4").select("c1"), Seq.empty)
      checkAnswer(df.where("c2 < 99.6").select("c1"), Seq.empty)
      checkAnswer(df.where("c2 < 100.4").select("c1"), Row(1))

      checkAnswer(df.where("c3 >= 3.14").select("c1"), Row(1))
      // float(3.14) is casted to double(3.140000104904175)
      checkAnswer(df.where("c3 >= 3.14000010").select("c1"), Row(1))
      checkAnswer(df.where("c3 == 3.14").select("c1"), Seq.empty)
      checkAnswer(df.where("c3 <=> 3.14").select("c1"), Seq.empty)
      checkAnswer(df.where("c3 < 3.14000010").select("c1"), Seq.empty)
      checkAnswer(df.where("c3 <= 3.14").select("c1"), Seq.empty)

      checkAnswer(df.where("c4 > cast(200.1199 as decimal(10, 4))").select("c1"), Row(1))
      checkAnswer(df.where("c4 >= cast(200.1201 as decimal(10, 4))").select("c1"), Seq.empty)
      checkAnswer(df.where("c4 == cast(200.1156 as decimal(10, 4))").select("c1"), Seq.empty)
      checkAnswer(df.where("c4 <=> cast(200.1201 as decimal(10, 4))").select("c1"), Seq.empty)
      checkAnswer(df.where("c4 <= cast(200.1201 as decimal(10, 4))").select("c1"), Row(1))
      checkAnswer(df.where("c4 < cast(200.1159 as decimal(10, 4))").select("c1"), Seq.empty)
    }
  }

  test("SPARK-36607: Support BooleanType in UnwrapCastInBinaryComparison") {
    // If ANSI mode is on, Spark disallows comparing Int with Boolean.
    if (!conf.ansiEnabled) {
      withTable(t) {
        Seq(Some(true), Some(false), None).toDF().write.saveAsTable(t)
        val df = spark.table(t)

        checkAnswer(df.where("value = -1"), Seq.empty)
        checkAnswer(df.where("value = 0"), Row(false))
        checkAnswer(df.where("value = 1"), Row(true))
        checkAnswer(df.where("value = 2"), Seq.empty)
        checkAnswer(df.where("value <=> -1"), Seq.empty)
        checkAnswer(df.where("value <=> 0"), Row(false))
        checkAnswer(df.where("value <=> 1"), Row(true))
        checkAnswer(df.where("value <=> 2"), Seq.empty)
      }
    }
  }

  test("SPARK-39476: Should not unwrap cast from Long to Double/Float") {
    withTable(t) {
      Seq((6470759586864300301L))
        .toDF("c1").write.saveAsTable(t)
      val df = spark.table(t)

      checkAnswer(
        df.where("cast(c1 as double) == cast(6470759586864300301L as double)")
          .select("c1"),
        Row(6470759586864300301L))

      checkAnswer(
        df.where("cast(c1 as float) == cast(6470759586864300301L as float)")
          .select("c1"),
        Row(6470759586864300301L))
    }
  }

  test("SPARK-39476: Should not unwrap cast from Integer to Float") {
    withTable(t) {
      Seq((33554435))
        .toDF("c1").write.saveAsTable(t)
      val df = spark.table(t)

      checkAnswer(
        df.where("cast(c1 as float) == cast(33554435 as float)")
          .select("c1"),
        Row(33554435))
    }
  }

  test("SPARK-42597: Support unwrap date type to timestamp type") {
    val ts1 = LocalDateTime.of(2023, 1, 1, 23, 59, 59, 99999000)
    val ts2 = LocalDateTime.of(2023, 1, 1, 23, 59, 59, 999998000)
    val ts3 = LocalDateTime.of(2023, 1, 2, 23, 59, 59, 8000)

    withTable(t) {
      Seq(ts1, ts2, ts3).toDF("ts").write.saveAsTable(t)
      val df = spark.table(t)

      checkAnswer(
        df.where("cast(ts as date) > date'2023-01-01'"), Seq(ts3).map(Row(_)))
      checkAnswer(
        df.where("cast(ts as date) >= date'2023-01-01'"), Seq(ts1, ts2, ts3).map(Row(_)))
      checkAnswer(
        df.where("cast(ts as date) < date'2023-01-02'"), Seq(ts1, ts2).map(Row(_)))
      checkAnswer(
        df.where("cast(ts as date) <= date'2023-01-02'"), Seq(ts1, ts2, ts3).map(Row(_)))
      checkAnswer(
        df.where("cast(ts as date) = date'2023-01-01'"), Seq(ts1, ts2).map(Row(_)))
    }
  }

  test("SPARK-46069: Support unwrap timestamp type to date type") {
    val d1 = java.sql.Date.valueOf("2023-01-01")
    val d2 = java.sql.Date.valueOf("2023-01-02")
    val d3 = java.sql.Date.valueOf("2023-01-03")

    withTable(t) {
      Seq(d1, d2, d3).toDF("dt").write.saveAsTable(t)
      val df = spark.table(t)

      val ts1 = "timestamp'2023-01-02 10:00:00'"
      checkAnswer(df.where(s"cast(dt as timestamp) > $ts1"), Seq(d3).map(Row(_)))
      checkAnswer(df.where(s"cast(dt as timestamp) >= $ts1"), Seq(d3).map(Row(_)))
      checkAnswer(df.where(s"cast(dt as timestamp) = $ts1"), Seq().map(Row(_)))
      checkAnswer(df.where(s"cast(dt as timestamp) < $ts1"), Seq(d1, d2).map(Row(_)))
      checkAnswer(df.where(s"cast(dt as timestamp) <= $ts1"), Seq(d1, d2).map(Row(_)))

      val ts2 = "timestamp'2023-01-02 00:00:00'"
      checkAnswer(df.where(s"cast(dt as timestamp) > $ts2"), Seq(d3).map(Row(_)))
      checkAnswer(df.where(s"cast(dt as timestamp) >= $ts2"), Seq(d2, d3).map(Row(_)))
      checkAnswer(df.where(s"cast(dt as timestamp) = $ts2"), Seq(d2).map(Row(_)))
      checkAnswer(df.where(s"cast(dt as timestamp) < $ts2"), Seq(d1).map(Row(_)))
      checkAnswer(df.where(s"cast(dt as timestamp) <= $ts2"), Seq(d1, d2).map(Row(_)))
    }
  }

  private def decimal(v: BigDecimal): Decimal = Decimal(v, 5, 2)
}
