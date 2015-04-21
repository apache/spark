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
import org.apache.spark.sql.mathfunctions._
import org.apache.spark.sql.test.TestSQLContext
import org.apache.spark.sql.test.TestSQLContext.implicits._
import org.apache.spark.sql.types._

class ColumnExpressionSuite extends QueryTest {
  import org.apache.spark.sql.TestData._

  // TODO: Add test cases for bitwise operations.

  test("collect on column produced by a binary operator") {
    val df = Seq((1, 2, 3)).toDF("a", "b", "c")
    checkAnswer(df.select(df("a") + df("b")), Seq(Row(3)))
    checkAnswer(df.select(df("a") + df("b").as("c")), Seq(Row(3)))
  }

  test("star") {
    checkAnswer(testData.select($"*"), testData.collect().toSeq)
  }

  test("star qualified by data frame object") {
    val df = testData.toDF
    val goldAnswer = df.collect().toSeq
    checkAnswer(df.select(df("*")), goldAnswer)

    val df1 = df.select(df("*"), lit("abcd").as("litCol"))
    checkAnswer(df1.select(df("*")), goldAnswer)
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
    val nullData = TestSQLContext.createDataFrame(TestSQLContext.sparkContext.parallelize(
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

  val booleanData = TestSQLContext.createDataFrame(TestSQLContext.sparkContext.parallelize(
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
      testData.select(sqrt(lit(null))),
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
      testData.select(abs(lit(null))),
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
      testData.select(upper(lit(null))),
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
      testData.select(lower(lit(null))),
      (1 to 100).map(n => Row(null))
    )
  }

  test("lift alias out of cast") {
    compareExpressions(
      col("1234").as("name").cast("int").expr,
      col("1234").cast("int").as("name").expr)
  }

  test("columns can be compared") {
    assert('key.desc == 'key.desc)
    assert('key.desc != 'key.asc)
  }

  test("alias with metadata") {
    val metadata = new MetadataBuilder()
      .putString("originName", "value")
      .build()
    val schema = testData
      .select($"*", col("value").as("abc", metadata))
      .schema
    assert(schema("value").metadata === Metadata.empty)
    assert(schema("abc").metadata === metadata)
  }

  def testOneToOneMathFunction[@specialized(Int, Double, Float, Long) T]
      (c: Column => Column, f: T => T): Unit = {
    checkAnswer(
      doubleData.select(c('a)).orderBy('a.asc),
      (1 to 100).map(n => Row(f((n * 0.02 - 1).asInstanceOf[T])))
    )

    checkAnswer(
      doubleData.select(c('b)).orderBy('b.desc),
      (1 to 100).map(n => Row(f((-n * 0.02 + 1).asInstanceOf[T])))
    )

    checkAnswer(
      doubleData.select(c(lit(null))),
      (1 to 100).map(_ => Row(null))
    )
  }

  test("sin") {
    testOneToOneMathFunction(sin, math.sin)
  }

  test("asin") {
    testOneToOneMathFunction(asin, math.asin)
  }

  test("sinh") {
    testOneToOneMathFunction(sinh, math.sinh)
  }

  test("cos") {
    testOneToOneMathFunction(cos, math.cos)
  }

  test("acos") {
    testOneToOneMathFunction(acos, math.acos)
  }

  test("cosh") {
    testOneToOneMathFunction(cosh, math.cosh)
  }

  test("tan") {
    testOneToOneMathFunction(tan, math.tan)
  }

  test("atan") {
    testOneToOneMathFunction(atan, math.atan)
  }

  test("tanh") {
    testOneToOneMathFunction(tanh, math.tanh)
  }

  test("toDeg") {
    testOneToOneMathFunction(toDeg, math.toDegrees)
  }

  test("toRad") {
    testOneToOneMathFunction(toRad, math.toRadians)
  }

  test("cbrt") {
    testOneToOneMathFunction(cbrt, math.cbrt)
  }

  test("ceil") {
    testOneToOneMathFunction(ceil, math.ceil)
  }

  test("floor") {
    testOneToOneMathFunction(floor, math.floor)
  }

  test("rint") {
    testOneToOneMathFunction(rint, math.rint)
  }

  test("exp") {
    testOneToOneMathFunction(exp, math.exp)
  }

  test("expm1") {
    testOneToOneMathFunction(expm1, math.expm1)
  }

  test("signum") {
    testOneToOneMathFunction[Double](signum, math.signum)
  }

  test("isignum") {
    testOneToOneMathFunction[Int](isignum, math.signum)
  }

  test("fsignum") {
    testOneToOneMathFunction[Float](fsignum, math.signum)
  }

  test("lsignum") {
    testOneToOneMathFunction[Long](lsignum, math.signum)
  }

  def testTwoToOneMathFunction(
      c: (Column, Column) => Column,
      f: (Double, Double) => Double): Unit = {
    checkAnswer(
      nnDoubleData.select(c('a, 'a)).orderBy('a.asc),
      nnDoubleData.collect().toSeq.map(r => Row(f(r.getDouble(0), r.getDouble(0))))
    )

    checkAnswer(
      nnDoubleData.select(c('a, 'b)).orderBy('a.asc),
      nnDoubleData.collect().toSeq.map(r => Row(f(r.getDouble(0), r.getDouble(1))))
    )

    val nonNull = nullInts.collect().toSeq.filter(r => r.get(0) != null)

    checkAnswer(
      nullInts.select(c('a, 'a)).orderBy('a.asc),
      Row(null) +: nonNull.map(r => Row(f(r.getInt(0), r.getInt(0))))
    )
  }

  test("pow") {
    testTwoToOneMathFunction(pow, math.pow)
  }

  test("hypot") {
    testTwoToOneMathFunction(hypot, math.hypot)
  }

  test("atan2") {
    testTwoToOneMathFunction(atan2, math.atan2)
  }

  def testOneToOneNonNegativeMathFunction(c: Column => Column, f: Double => Double): Unit = {
    checkAnswer(
      testData.select(c('key)).orderBy('key.asc),
      (1 to 100).map(n => Row(f(n)))
    )

    if (f(-1) === math.log1p(-1)) {
      checkAnswer(
        negativeData.select(c('key)).orderBy('key.desc),
        Row(Double.NegativeInfinity) +: (2 to 100).map(n => Row(null))
      )
    } else {
      checkAnswer(
        negativeData.select(c('key)).orderBy('key.desc),
        (1 to 100).map(n => Row(null))
      )
    }

    checkAnswer(
      testData.select(c(lit(null))),
      (1 to 100).map(_ => Row(null))
    )
  }

  test("log") {
    testOneToOneNonNegativeMathFunction(log, math.log)
  }

  test("log10") {
    testOneToOneNonNegativeMathFunction(log10, math.log10)
  }

  test("log1p") {
    testOneToOneNonNegativeMathFunction(log1p, math.log1p)
  }
}
