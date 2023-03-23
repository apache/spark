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

import java.io.ByteArrayOutputStream

import scala.collection.JavaConverters._

import org.apache.spark.sql.{functions => fn}
import org.apache.spark.sql.connect.client.util.ConnectFunSuite
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

  private def checkExplainInfo(col: Column, expected: String) = {
    val explain1 = captureStdOut(col.explain(false))
    val explain2 = captureStdOut(col.explain(true))
    assert(explain1 == expected)
    assert(explain2 == expected)
  }

  test("explain") {
    val colA = fn.col("a")
    val colB = fn.col("b")
    val colC = fn.col("c")

    val col1 = colA + colB
    checkExplainInfo(col1, "(a + b)\n")
    val col2 = col1 - fn.lit(1)
    checkExplainInfo(col2, "((a + b) - 1)\n")
    val col3 = colA * fn.lit(10) / colB % fn.lit(3)
    checkExplainInfo(col3, "(((a * 10) / b) % 3)\n")
    val col4 = col1.apply(1)
    checkExplainInfo(col4, "(a + b)[1]\n")
    val col5 = col1.unary_-
    checkExplainInfo(col5, "(- (a + b))\n")
    val col6 = col1.unary_!
    checkExplainInfo(col6, "(NOT (a + b))\n")
    val col7 = colA === fn.lit(1) || colB =!= fn.lit(2)
    checkExplainInfo(col7, "((a = 1) or (NOT (b = 2)))\n")
    val col8 = col1 > fn.lit(1) && col1 < fn.lit(9) || colC >= fn.lit(1) && colC <= fn.lit(8)
    checkExplainInfo(col8, "((((a + b) > 1) and ((a + b) < 9)) or ((c >= 1) and (c <= 8)))\n")
    val col9 = col1 <=> fn.lit(1)
    checkExplainInfo(col9, "((a + b) <=> 1)\n")
    val col10 = fn.when(col1 === 1, -1).otherwise(0)
    checkExplainInfo(col10, "CASE WHEN ((a + b) = 1) THEN -1 ELSE 0 END\n")
    val col11 = fn.when(col1 === 1, -1).when(col1 > 1, colC).otherwise(0)
    checkExplainInfo(
      col11,
      "CASE WHEN ((a + b) = 1) THEN -1 WHEN ((a + b) > 1) THEN c ELSE 0 END\n")
    val col12 = col1.between(fn.lit(1), fn.lit(3))
    checkExplainInfo(col12, "(((a + b) >= 1) and ((a + b) <= 3))\n")
    val col13 = col1.isNaN
    checkExplainInfo(col13, "isnan((a + b))\n")
    val col14 = colA.isNull && colB.isNotNull
    checkExplainInfo(col14, "((a IS NULL) and (b IS NOT NULL))\n")
    val col15 = col1.isin(2, 3)
    checkExplainInfo(col15, "((a + b) IN (2, 3))\n")
    val col16 = colA.like("Tom*") || colB.rlike("^P.*$") || colC.ilike("a")
    checkExplainInfo(col16, "(((a LIKE 'Tom*') or RLIKE(b, '^P.*$')) or ilike(c, 'a'))\n")
    val col17 = colA.withField("b", fn.lit(2)).dropFields("a")
    checkExplainInfo(col17, "update_fields(update_fields(a, WithField(2)), dropfield())\n")
    val col18 = colA.substr(2, 5)
    checkExplainInfo(col18, "substring(a, 2, 5)\n")
    val col19 = colA.contains("mer") and colB.startsWith("Ari") or colA.endsWith(colC)
    checkExplainInfo(col19, "((contains(a, 'mer') and startswith(b, 'Ari')) or endswith(a, c))\n")
    val col20 = col1.as("add_alias")
    checkExplainInfo(col20, "(a + b) AS add_alias\n")
    val col21 = col1.as("add_alias").as("key" :: "value" :: Nil)
    checkExplainInfo(col21, "multialias((a + b) AS add_alias)\n")
    val col22 = col1.cast(IntegerType).cast("int")
    checkExplainInfo(col22, "CAST(CAST((a + b) AS INT) AS INT)\n")
    val col23 = colA.desc.withField("b1", colB.asc)
    checkExplainInfo(col23, "update_fields(a DESC NULLS LAST, WithField(b ASC NULLS FIRST))\n")
    val col24 = colA.desc_nulls_first.withField("b1", colB.asc_nulls_last)
    checkExplainInfo(col24, "update_fields(a DESC NULLS FIRST, WithField(b ASC NULLS LAST))\n")
    val col25 = colA.bitwiseAND(colB.bitwiseOR(colC).bitwiseXOR(fn.lit(3)))
    checkExplainInfo(col25, "(a & ((b | c) ^ 3))\n")
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
  testColName(structType2, _.struct(structType2.fields: _*))
}
