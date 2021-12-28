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

  private def decimal(v: BigDecimal): Decimal = Decimal(v, 5, 2)
}
