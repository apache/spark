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

  test("cases when literal is max") {
    val t = "test_table"
    withTable(t) {
      Seq[(Integer, java.lang.Short, java.lang.Float)](
        (1, 100.toShort, 3.14.toFloat), (2, Short.MaxValue, Float.NaN), (3, null, null))
        .toDF("c1", "c2", "c3").write.saveAsTable(t)
      val df = spark.table(t)

      var lit = Short.MaxValue.toInt
      checkAnswer(df.select("c1").where(s"c2 > $lit"), Seq.empty)
      checkAnswer(df.select("c1").where(s"c2 >= $lit"), Row(2))
      checkAnswer(df.select("c1").where(s"c2 == $lit"), Row(2))
      checkAnswer(df.select("c1").where(s"c2 <=> $lit"), Row(2))
      checkAnswer(df.select("c1").where(s"c2 != $lit"), Row(1))
      checkAnswer(df.select("c1").where(s"c2 <= $lit"), Row(1) :: Row(2) :: Nil)
      checkAnswer(df.select("c1").where(s"c2 < $lit"), Row(1))

      checkAnswer(df.select("c1").where(s"c3 > double('nan')"), Seq.empty)
      checkAnswer(df.select("c1").where(s"c3 >= double('nan')"), Row(2))
      checkAnswer(df.select("c1").where(s"c3 == double('nan')"), Row(2))
      checkAnswer(df.select("c1").where(s"c3 <=> double('nan')"), Row(2))
      checkAnswer(df.select("c1").where(s"c3 != double('nan')"), Row(1))
      checkAnswer(df.select("c1").where(s"c3 <= double('nan')"), Row(1) :: Row(2) :: Nil)
      checkAnswer(df.select("c1").where(s"c3 < double('nan')"), Row(1))

      lit = positiveInt
      checkAnswer(df.select("c1").where(s"c2 > $lit"), Seq.empty)
      checkAnswer(df.select("c1").where(s"c2 >= $lit"), Seq.empty)
      checkAnswer(df.select("c1").where(s"c2 == $lit"), Seq.empty)
      checkAnswer(df.select("c1").where(s"c2 <=> $lit"), Seq.empty)
      checkAnswer(df.select("c1").where(s"c2 != $lit"), Row(1) :: Row(2) :: Nil)
      checkAnswer(df.select("c1").where(s"c2 <= $lit"), Row(1) :: Row(2) :: Nil)
      checkAnswer(df.select("c1").where(s"c2 < $lit"), Row(1) :: Row(2) :: Nil)
    }
  }

  test("cases when literal is min") {
    val t = "test_table"
    withTable(t) {
      Seq[(Integer, java.lang.Short, java.lang.Float)](
        (1, 100.toShort, 3.14.toFloat), (2, Short.MinValue, Float.NegativeInfinity),
        (3, null, null))
        .toDF("c1", "c2", "c3").write.saveAsTable(t)
      val df = spark.table(t)

      var lit = Short.MinValue.toInt
      checkAnswer(df.select("c1").where(s"c2 > $lit"), Row(1))
      checkAnswer(df.select("c1").where(s"c2 >= $lit"), Row(1) :: Row(2) :: Nil)
      checkAnswer(df.select("c1").where(s"c2 == $lit"), Row(2))
      checkAnswer(df.select("c1").where(s"c2 <=> $lit"), Row(2))
      checkAnswer(df.select("c1").where(s"c2 != $lit"), Row(1))
      checkAnswer(df.select("c1").where(s"c2 <= $lit"), Row(2))
      checkAnswer(df.select("c1").where(s"c2 < $lit"), Seq.empty)

      checkAnswer(df.select("c1").where(s"c3 > double('-inf')"), Row(1))
      checkAnswer(df.select("c1").where(s"c3 >= double('-inf')"), Row(1) :: Row(2) :: Nil)
      checkAnswer(df.select("c1").where(s"c3 == double('-inf')"), Row(2))
      checkAnswer(df.select("c1").where(s"c3 <=> double('-inf')"), Row(2))
      checkAnswer(df.select("c1").where(s"c3 != double('-inf')"), Row(1))
      checkAnswer(df.select("c1").where(s"c3 <= double('-inf')"), Row(2))
      checkAnswer(df.select("c1").where(s"c3 < double('-inf')"), Seq.empty)

      lit = negativeInt
      checkAnswer(df.select("c1").where(s"c2 > $lit"), Row(1) :: Row(2) :: Nil)
      checkAnswer(df.select("c1").where(s"c2 >= $lit"), Row(1) :: Row(2) :: Nil)
      checkAnswer(df.select("c1").where(s"c2 == $lit"), Seq.empty)
      checkAnswer(df.select("c1").where(s"c2 <=> $lit"), Seq.empty)
      checkAnswer(df.select("c1").where(s"c2 != $lit"), Row(1) :: Row(2) :: Nil)
      checkAnswer(df.select("c1").where(s"c2 <= $lit"), Seq.empty)
      checkAnswer(df.select("c1").where(s"c2 < $lit"), Seq.empty)
    }
  }

  test("cases when literal is within range (min, max)") {
    val t = "test_table"
    withTable(t) {
      Seq((1, 300.toShort), (2, 500.toShort)).toDF("c1", "c2").write.saveAsTable(t)
      val df = spark.table(t)

      checkAnswer(df.select("c1").where("c2 < 200"), Seq.empty)
      checkAnswer(df.select("c1").where("c2 < 400"), Row(1) :: Nil)
      checkAnswer(df.select("c1").where("c2 < 600"), Row(1) :: Row(2) :: Nil)

      checkAnswer(df.select("c1").where("c2 <= 100"), Seq.empty)
      checkAnswer(df.select("c1").where("c2 <= 300"), Row(1) :: Nil)
      checkAnswer(df.select("c1").where("c2 <= 500"), Row(1) :: Row(2) :: Nil)

      checkAnswer(df.select("c1").where("c2 == 100"), Seq.empty)
      checkAnswer(df.select("c1").where("c2 == 300"), Row(1) :: Nil)
      checkAnswer(df.select("c1").where("c2 == 500"), Row(2) :: Nil)

      checkAnswer(df.select("c1").where("c2 <=> 100"), Seq.empty)
      checkAnswer(df.select("c1").where("c2 <=> 300"), Row(1) :: Nil)
      checkAnswer(df.select("c1").where("c2 <=> 500"), Row(2) :: Nil)
      checkAnswer(df.select("c1").where("c2 <=> null"), Seq.empty)

      checkAnswer(df.select("c1").where("c2 >= 200"), Row(1) :: Row(2) :: Nil)
      checkAnswer(df.select("c1").where("c2 >= 400"), Row(2) :: Nil)
      checkAnswer(df.select("c1").where("c2 >= 600"), Seq.empty)

      checkAnswer(df.select("c1").where("c2 > 100"), Row(1) :: Row(2) :: Nil)
      checkAnswer(df.select("c1").where("c2 > 300"), Row(2) :: Nil)
      checkAnswer(df.select("c1").where("c2 > 500"), Seq.empty)
    }
  }

  test("cases when literal is within range (min, max) and rounding for ") {
    val t = "test_table"
    withTable(t) {
      Seq((1, 100, 3.14.toFloat, decimal(200.12)))
        .toDF("c1", "c2", "c3", "c4").write.saveAsTable(t)
      val df = spark.table(t)

      checkAnswer(df.select("c1").where("c2 > 99.6"), Row(1))
      checkAnswer(df.select("c1").where("c2 > 100.4"), Seq.empty)
      checkAnswer(df.select("c1").where("c2 == 100.4"), Seq.empty)
      checkAnswer(df.select("c1").where("c2 <=> 100.4"), Seq.empty)
      checkAnswer(df.select("c1").where("c2 < 99.6"), Seq.empty)
      checkAnswer(df.select("c1").where("c2 < 100.4"), Row(1))

      checkAnswer(df.select("c1").where("c3 >= 3.14"), Row(1))
      // float(3.14) is casted to double(3.140000104904175)
      checkAnswer(df.select("c1").where("c3 >= 3.14000010"), Row(1))
      checkAnswer(df.select("c1").where("c3 == 3.14"), Seq.empty)
      checkAnswer(df.select("c1").where("c3 <=> 3.14"), Seq.empty)
      checkAnswer(df.select("c1").where("c3 < 3.14000010"), Seq.empty)
      checkAnswer(df.select("c1").where("c3 <= 3.14"), Seq.empty)

      checkAnswer(df.select("c1").where("c4 > cast(200.1199 as decimal(10, 4))"), Row(1))
      checkAnswer(df.select("c1").where("c4 >= cast(200.1201 as decimal(10, 4))"), Seq.empty)
      checkAnswer(df.select("c1").where("c4 == cast(200.1156 as decimal(10, 4))"), Seq.empty)
      checkAnswer(df.select("c1").where("c4 <=> cast(200.1201 as decimal(10, 4))"), Seq.empty)
      checkAnswer(df.select("c1").where("c4 <= cast(200.1201 as decimal(10, 4))"), Row(1))
      checkAnswer(df.select("c1").where("c4 < cast(200.1159 as decimal(10, 4))"), Seq.empty)
    }
  }

  private def decimal(v: BigDecimal): Decimal = Decimal(v, 5, 2)
}
