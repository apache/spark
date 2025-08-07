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
package org.apache.spark.sql.connect

import java.io.ByteArrayOutputStream

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.{functions => fn, Column, ColumnName}
import org.apache.spark.sql.connect.test.ConnectFunSuite
import org.apache.spark.sql.types._

/**
 * Tests for client local Column behavior.
 */
class ColumnTestSuite extends ConnectFunSuite {
  test("equals & hashcode") {
    def expr: Column = fn.when(fn.col("a") < 10, "a").otherwise("b")
    val a = expr
    val b = expr
    val c = expr.as("nope")
    assert(a == a)
    assert(b == b)
    assert(c == c)
    assert(a == b)
    assert(b == a)
    assert(a != c)
    assert(c != a)
    assert(b != c)
    assert(c != b)
    assert(a.hashCode == b.hashCode)
    assert(a.hashCode != c.hashCode)
  }

  test("invalid when usage") {
    intercept[IllegalArgumentException] {
      fn.col("a").when(fn.lit(true), 2)
    }
    intercept[IllegalArgumentException] {
      fn.col("a").isNull.when(fn.lit(true), 2)
    }
    intercept[IllegalArgumentException] {
      fn.when(fn.col("a") < 10, 1)
        .otherwise(2)
        .when(fn.col("b") > 8, 3)
    }
  }

  test("invalid otherwise usage") {
    intercept[IllegalArgumentException] {
      fn.col("a").otherwise(2)
    }
    intercept[IllegalArgumentException] {
      fn.col("a").isNull.otherwise(2)
    }
    intercept[IllegalArgumentException] {
      fn.when(fn.col("a") < 10, 1)
        .otherwise(2)
        .otherwise(3)
    }
  }

  test("invalid withField usage") {
    intercept[IllegalArgumentException] {
      fn.col("c").withField(null, fn.lit(1))
    }
    intercept[IllegalArgumentException] {
      fn.col("c").withField("x", null)
    }
  }

  def testSame(
      name: String,
      f1: (Column, Column) => Column,
      f2: (Column, Column) => Column): Unit = test(name + " are the same") {
    val a = fn.col("a")
    val b = fn.col("b")
    assert(f1(a, b) == f2(a, b))
  }
  testSame("=== and equalTo", _ === _, _.equalTo(_))
  testSame("=!= and notEqual", _ =!= _, _.notEqual(_))
  testSame("> and gt", _ > _, _.gt(_))
  testSame("< and lt", _ < _, _.lt(_))
  testSame(">= and geq", _ >= _, _.geq(_))
  testSame("<= and leq", _ <= _, _.leq(_))
  testSame("<=> and eqNullSafe", _ <=> _, _.eqNullSafe(_))
  testSame("|| and or", _ || _, _.or(_))
  testSame("&& and and", _ && _, _.and(_))
  testSame("+ and plus", _ + _, _.plus(_))
  testSame("- and minus", _ - _, _.minus(_))
  testSame("* and multiply", _ * _, _.multiply(_))
  testSame("/ and divide", _ / _, _.divide(_))
  testSame("% and mod", _ % _, _.mod(_))

  test("isIn") {
    val a = fn.col("a")
    val values = Seq(1, 5, 6)
    assert(a.isin(values: _*) == a.isInCollection(values))
    assert(a.isin(values: _*) == a.isInCollection(values.asJava))
  }

  test("getItem/apply/getField are the same") {
    val a = fn.col("a")
    assert(a("x") == a.getItem("x"))
    assert(a("x") == a.getField("x"))
  }

  test("substr variations") {
    val a = fn.col("a")
    assert(a.substr(2, 10) == a.substr(fn.lit(2), fn.lit(10)))
  }

  test("startsWith variations") {
    val a = fn.col("a")
    assert(a.endsWith("p_") == a.endsWith(fn.lit("p_")))
  }

  test("endsWith variations") {
    val a = fn.col("a")
    assert(a.endsWith("world") == a.endsWith(fn.lit("world")))
  }

  test("alias/as/name are the same") {
    val a = fn.col("a")
    assert(a.as("x") == a.alias("x"))
    assert(a.as("x") == a.name("x"))
  }

  test("multi-alias variations") {
    val a = fn.col("a")
    assert(a.as("x" :: "y" :: Nil) == a.as(Array("x", "y")))
  }

  test("cast variations") {
    val a = fn.col("a")
    assert(a.cast("string") == a.cast(StringType))
  }

  test("desc and desc_nulls_last are the same") {
    val a = fn.col("a")
    assert(a.desc == a.desc_nulls_last)
  }

  test("asc and asc_nulls_first are the same") {
    val a = fn.col("a")
    assert(a.asc == a.asc_nulls_first)
  }

  private def captureStdOut(block: => Unit): String = {
    val capturedOut = new ByteArrayOutputStream()
    Console.withOut(capturedOut)(block)
    capturedOut.toString()
  }

  test("explain") {
    val x = fn.col("a") + fn.col("b")
    val explain1 = captureStdOut(x.explain(false))
    val explain2 = captureStdOut(x.explain(true))
    assert(explain1 != explain2)
    assert(explain1.strip() == "+(a, b)")
    assert(explain2.contains("UnresolvedFunction(+"))
    assert(explain2.contains("UnresolvedAttribute(List(a"))
    assert(explain2.contains("UnresolvedAttribute(List(b"))
  }

  private def testColName(dataType: DataType, f: ColumnName => StructField): Unit = {
    test("ColumnName " + dataType.catalogString) {
      val actual = f(new ColumnName("col"))
      val expected = StructField("col", dataType)
      assert(actual === expected)
    }
  }

  testColName(BooleanType, _.boolean)
  testColName(ByteType, _.byte)
  testColName(ShortType, _.short)
  testColName(IntegerType, _.int)
  testColName(LongType, _.long)
  testColName(FloatType, _.float)
  testColName(DoubleType, _.double)
  testColName(DecimalType.USER_DEFAULT, _.decimal)
  testColName(DecimalType(20, 10), _.decimal(20, 10))
  testColName(DateType, _.date)
  testColName(TimestampType, _.timestamp)
  testColName(StringType, _.string)
  testColName(BinaryType, _.binary)
  testColName(ArrayType(IntegerType), _.array(IntegerType))

  private val mapType = MapType(StringType, StringType)
  testColName(mapType, _.map(mapType))
  testColName(MapType(StringType, IntegerType), _.map(StringType, IntegerType))

  private val structType1 = new StructType().add("a", "int").add("b", "string")
  private val structType2 = structType1.add("c", "binary")
  testColName(structType1, _.struct(structType1))
  import org.apache.spark.util.ArrayImplicits._
  testColName(structType2, _.struct(structType2.fields.toImmutableArraySeq: _*))
}
