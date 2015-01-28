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

import org.apache.spark.sql.dsl._
import org.apache.spark.sql.test.TestSQLContext
import org.apache.spark.sql.types.{BooleanType, IntegerType, StructField, StructType}


class ColumnExpressionSuite extends QueryTest {
  import org.apache.spark.sql.TestData._

  // TODO: Add test cases for bitwise operations.

  test("star") {
    checkAnswer(testData.select($"*"), testData.collect().toSeq)
  }

  ignore("star qualified by data frame object") {
    // This is not yet supported.
    val df = testData.toDF
    checkAnswer(df.select(df("*")), df.collect().toSeq)
  }

  test("star qualified by table name") {
    checkAnswer(testData.as("testData").select($"testData.*"), testData.collect().toSeq)
  }

  test("+") {
    checkAnswer(
      testData2.select($"a" + 1),
      testData2.collect().toSeq.map(r => Row(r.getInt(0) + 1)))

    checkAnswer(
      testData2.select($"a" + $"b" + 2),
      testData2.collect().toSeq.map(r => Row(r.getInt(0) + r.getInt(1) + 2)))
  }

  test("-") {
    checkAnswer(
      testData2.select($"a" - 1),
      testData2.collect().toSeq.map(r => Row(r.getInt(0) - 1)))

    checkAnswer(
      testData2.select($"a" - $"b" - 2),
      testData2.collect().toSeq.map(r => Row(r.getInt(0) - r.getInt(1) - 2)))
  }

  test("*") {
    checkAnswer(
      testData2.select($"a" * 10),
      testData2.collect().toSeq.map(r => Row(r.getInt(0) * 10)))

    checkAnswer(
      testData2.select($"a" * $"b"),
      testData2.collect().toSeq.map(r => Row(r.getInt(0) * r.getInt(1))))
  }

  test("/") {
    checkAnswer(
      testData2.select($"a" / 2),
      testData2.collect().toSeq.map(r => Row(r.getInt(0).toDouble / 2)))

    checkAnswer(
      testData2.select($"a" / $"b"),
      testData2.collect().toSeq.map(r => Row(r.getInt(0).toDouble / r.getInt(1))))
  }


  test("%") {
    checkAnswer(
      testData2.select($"a" % 2),
      testData2.collect().toSeq.map(r => Row(r.getInt(0) % 2)))

    checkAnswer(
      testData2.select($"a" % $"b"),
      testData2.collect().toSeq.map(r => Row(r.getInt(0) % r.getInt(1))))
  }

  test("unary -") {
    checkAnswer(
      testData2.select(-$"a"),
      testData2.collect().toSeq.map(r => Row(-r.getInt(0))))
  }

  test("unary !") {
    checkAnswer(
      complexData.select(!$"b"),
      complexData.collect().toSeq.map(r => Row(!r.getBoolean(3))))
  }

  test("isNull") {
    checkAnswer(
      nullStrings.toDF.where($"s".isNull),
      nullStrings.collect().toSeq.filter(r => r.getString(1) eq null))
  }

  test("isNotNull") {
    checkAnswer(
      nullStrings.toDF.where($"s".isNotNull),
      nullStrings.collect().toSeq.filter(r => r.getString(1) ne null))
  }

  test("===") {
    checkAnswer(
      testData2.filter($"a" === 1),
      testData2.collect().toSeq.filter(r => r.getInt(0) == 1))

    checkAnswer(
      testData2.filter($"a" === $"b"),
      testData2.collect().toSeq.filter(r => r.getInt(0) == r.getInt(1)))
  }

  test("<=>") {
    checkAnswer(
      testData2.filter($"a" === 1),
      testData2.collect().toSeq.filter(r => r.getInt(0) == 1))

    checkAnswer(
      testData2.filter($"a" === $"b"),
      testData2.collect().toSeq.filter(r => r.getInt(0) == r.getInt(1)))
  }

  test("!==") {
    val nullData = TestSQLContext.applySchema(TestSQLContext.sparkContext.parallelize(
      Row(1, 1) ::
      Row(1, 2) ::
      Row(1, null) ::
      Row(null, null) :: Nil),
      StructType(Seq(StructField("a", IntegerType), StructField("b", IntegerType))))

    checkAnswer(
      nullData.filter($"b" <=> 1),
      Row(1, 1) :: Nil)

    checkAnswer(
      nullData.filter($"b" <=> null),
      Row(1, null) :: Row(null, null) :: Nil)

    checkAnswer(
      nullData.filter($"a" <=> $"b"),
      Row(1, 1) :: Row(null, null) :: Nil)
  }

  test(">") {
    checkAnswer(
      testData2.filter($"a" > 1),
      testData2.collect().toSeq.filter(r => r.getInt(0) > 1))

    checkAnswer(
      testData2.filter($"a" > $"b"),
      testData2.collect().toSeq.filter(r => r.getInt(0) > r.getInt(1)))
  }

  test(">=") {
    checkAnswer(
      testData2.filter($"a" >= 1),
      testData2.collect().toSeq.filter(r => r.getInt(0) >= 1))

    checkAnswer(
      testData2.filter($"a" >= $"b"),
      testData2.collect().toSeq.filter(r => r.getInt(0) >= r.getInt(1)))
  }

  test("<") {
    checkAnswer(
      testData2.filter($"a" < 2),
      testData2.collect().toSeq.filter(r => r.getInt(0) < 2))

    checkAnswer(
      testData2.filter($"a" < $"b"),
      testData2.collect().toSeq.filter(r => r.getInt(0) < r.getInt(1)))
  }

  test("<=") {
    checkAnswer(
      testData2.filter($"a" <= 2),
      testData2.collect().toSeq.filter(r => r.getInt(0) <= 2))

    checkAnswer(
      testData2.filter($"a" <= $"b"),
      testData2.collect().toSeq.filter(r => r.getInt(0) <= r.getInt(1)))
  }

  val booleanData = TestSQLContext.applySchema(TestSQLContext.sparkContext.parallelize(
    Row(false, false) ::
      Row(false, true) ::
      Row(true, false) ::
      Row(true, true) :: Nil),
    StructType(Seq(StructField("a", BooleanType), StructField("b", BooleanType))))

  test("&&") {
    checkAnswer(
      booleanData.filter($"a" && true),
      Row(true, false) :: Row(true, true) :: Nil)

    checkAnswer(
      booleanData.filter($"a" && false),
      Nil)

    checkAnswer(
      booleanData.filter($"a" && $"b"),
      Row(true, true) :: Nil)
  }

  test("||") {
    checkAnswer(
      booleanData.filter($"a" || true),
      booleanData.collect())

    checkAnswer(
      booleanData.filter($"a" || false),
      Row(true, false) :: Row(true, true) :: Nil)

    checkAnswer(
      booleanData.filter($"a" || $"b"),
      Row(false, true) :: Row(true, false) :: Row(true, true) :: Nil)
  }

  test("sqrt") {
    checkAnswer(
      testData.select(sqrt('key)).orderBy('key.asc),
      (1 to 100).map(n => Row(math.sqrt(n)))
    )

    checkAnswer(
      testData.select(sqrt('value), 'key).orderBy('key.asc, 'value.asc),
      (1 to 100).map(n => Row(math.sqrt(n), n))
    )

    checkAnswer(
      testData.select(sqrt(Literal(null))),
      (1 to 100).map(_ => Row(null))
    )
  }

  test("abs") {
    checkAnswer(
      testData.select(abs('key)).orderBy('key.asc),
      (1 to 100).map(n => Row(n))
    )

    checkAnswer(
      negativeData.select(abs('key)).orderBy('key.desc),
      (1 to 100).map(n => Row(n))
    )

    checkAnswer(
      testData.select(abs(Literal(null))),
      (1 to 100).map(_ => Row(null))
    )
  }

  test("upper") {
    checkAnswer(
      lowerCaseData.select(upper('l)),
      ('a' to 'd').map(c => Row(c.toString.toUpperCase))
    )

    checkAnswer(
      testData.select(upper('value), 'key),
      (1 to 100).map(n => Row(n.toString, n))
    )

    checkAnswer(
      testData.select(upper(Literal(null))),
      (1 to 100).map(n => Row(null))
    )
  }

  test("lower") {
    checkAnswer(
      upperCaseData.select(lower('L)),
      ('A' to 'F').map(c => Row(c.toString.toLowerCase))
    )

    checkAnswer(
      testData.select(lower('value), 'key),
      (1 to 100).map(n => Row(n.toString, n))
    )

    checkAnswer(
      testData.select(lower(Literal(null))),
      (1 to 100).map(n => Row(null))
    )
  }
}
