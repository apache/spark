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

import org.apache.spark.sql.functions._
import org.apache.spark.sql.test.SharedSQLContext

case class T1(a: Int, b: Option[Int], c: Option[Int])

/**
 * This test suite takes https://sqlite.org/nulls.html as a reference.
 */
class NullHandlingSuite extends QueryTest with SharedSQLContext {
  import testImplicits._

  lazy val t1: DataFrame = Seq(T1(1, Some(0), Some(0)), T1(2, Some(0), Some(1)),
    T1(3, Some(1), Some(0)), T1(4, Some(1), Some(1)), T1(5, None, Some(0)),
    T1(6, None, Some(1)), T1(7, None, None)).toDF()
  lazy val zeros = Seq(0, 0.0f, 0.0d, BigDecimal(0))

  test("Adding anything to null gives null") {
    val actualResult = t1.select($"b" + $"c")
    val expectedResult = Seq(Row(0), Row(1), Row(1), Row(2), Row(null), Row(null), Row(null))
    checkAnswer(actualResult, expectedResult)
  }

  test("Multiplying null by zero gives null") {
    zeros.foreach { zero =>
      val actualResult = t1.select($"b" * zero)
      val expectedResult = Seq(Row(zero), Row(zero), Row(zero), Row(zero),
        Row(null), Row(null), Row(null))
      checkAnswer(actualResult, expectedResult)
    }
  }

  test("nulls are NOT distinct in SELECT DISTINCT") {
    val actualResult = t1.select($"b").distinct()
    val expectedResult = Seq(Row(0), Row(1), Row(null))
    checkAnswer(actualResult, expectedResult)
  }

  test("nulls are NOT distinct in UNION") {
    val actualResult = t1.select($"b").union(t1.select($"b")).distinct()
    val expectedResult = Seq(Row(0), Row(1), Row(null))
    checkAnswer(actualResult, expectedResult)
  }

  test("CASE WHEN null THEN 1 ELSE 0 END is 0") {
    zeros.foreach { zero =>
      // case when b<>0 then 1 else 0 end
      val actualResult = t1.select(when($"b" =!= zero, lit(1)).otherwise(lit(0)))
      val expectedResult = Seq(Row(0), Row(0), Row(1), Row(1), Row(0), Row(0), Row(0))
      checkAnswer(actualResult, expectedResult)

      // case when not b<>0 then 1 else 0 end
      val actualResult1 = t1.select(when(not($"b" =!= zero), lit(1)).otherwise(lit(0)))
      val expectedResult1 = Seq(Row(1), Row(1), Row(0), Row(0), Row(0), Row(0), Row(0))
      checkAnswer(actualResult1, expectedResult1)

      // case when b<>0 and c<>0 then 1 else 0 end
      val actualResult2 = t1.select(when($"b" =!= zero and $"c" =!= zero, lit(1)).otherwise(lit(0)))
      val expectedResult2 = Seq(Row(0), Row(0), Row(0), Row(1), Row(0), Row(0), Row(0))
      checkAnswer(actualResult2, expectedResult2)

      // case when not (b<>0 and c<>0) then 1 else 0 end
      val actualResult3 = t1.select(when(not($"b" =!= zero and $"c" =!= zero), lit(1))
        .otherwise(lit(0)))
      val expectedResult3 = Seq(Row(1), Row(1), Row(1), Row(0), Row(1), Row(0), Row(0))
      checkAnswer(actualResult3, expectedResult3)

      // case when b<>0 or c<>0 then 1 else 0 end
      val actualResult4 = t1.select(when($"b" =!= zero or $"c" =!= zero, lit(1)).otherwise(lit(0)))
      val expectedResult4 = Seq(Row(0), Row(1), Row(1), Row(1), Row(0), Row(1), Row(0))
      checkAnswer(actualResult4, expectedResult4)

      // case when not (b<>0 or c<>0) then 1 else 0 end
      val actualResult5 = t1.select(when(not($"b" =!= zero or $"c" =!= zero), lit(1))
        .otherwise(lit(0)))
      val expectedResult5 = Seq(Row(1), Row(0), Row(0), Row(0), Row(0), Row(0), Row(0))
      checkAnswer(actualResult5, expectedResult5)
    }
  }

  test("null with aggregate operators") {
    val actualResult = t1.select(count($"*"), count($"b"), sum($"b"), avg($"b"),
      min($"b"), max($"b"))
    val expectedResult = Seq(Row(7, 4, 2, 0.5, 0, 1))
    checkAnswer(actualResult, expectedResult)
  }

  test("Check the behavior of NULLs in WHERE clauses") {
    val actualResult = t1.where($"b" < 10).select($"a")
    val expectedResult = Seq(Row(1), Row(2), Row(3), Row(4))
    checkAnswer(actualResult, expectedResult)

    val actualResult1 = t1.where(not($"b" < 10)).select($"a")
    val expectedResult1 = Seq()
    checkAnswer(actualResult1, expectedResult1)

    val actualResult2 = t1.where($"b" < 10 or $"c" === 1).select($"a")
    val expectedResult2 = Seq(Row(1), Row(2), Row(3), Row(4), Row(6))
    checkAnswer(actualResult2, expectedResult2)

    val actualResult3 = t1.where(not($"b" < 10 or $"c" === 1)).select($"a")
    val expectedResult3 = Seq()
    checkAnswer(actualResult3, expectedResult3)

    val actualResult4 = t1.where($"b" < 10 and $"c" === 1).select($"a")
    val expectedResult4 = Seq(Row(2), Row(4))
    checkAnswer(actualResult4, expectedResult4)

    val actualResult5 = t1.where(not($"b" < 10 and $"c" === 1)).select($"a")
    val expectedResult5 = Seq(Row(1), Row(3), Row(5))
    checkAnswer(actualResult5, expectedResult5)
  }
}
