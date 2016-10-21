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

import java.nio.charset.StandardCharsets

import org.apache.spark.sql.functions._
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types._

/**
 * Test suite for functions in [[org.apache.spark.sql.functions]].
 */
class DataFrameFunctionsSuite extends QueryTest with SharedSQLContext {
  import testImplicits._

  test("array with column name") {
    val df = Seq((0, 1)).toDF("a", "b")
    val row = df.select(array("a", "b")).first()

    val expectedType = ArrayType(IntegerType, containsNull = false)
    assert(row.schema(0).dataType === expectedType)
    assert(row.getAs[Seq[Int]](0) === Seq(0, 1))
  }

  test("array with column expression") {
    val df = Seq((0, 1)).toDF("a", "b")
    val row = df.select(array(col("a"), col("b") + col("b"))).first()

    val expectedType = ArrayType(IntegerType, containsNull = false)
    assert(row.schema(0).dataType === expectedType)
    assert(row.getSeq[Int](0) === Seq(0, 2))
  }

  test("map with column expressions") {
    val df = Seq(1 -> "a").toDF("a", "b")
    val row = df.select(map($"a" + 1, $"b")).first()

    val expectedType = MapType(IntegerType, StringType, valueContainsNull = true)
    assert(row.schema(0).dataType === expectedType)
    assert(row.getMap[Int, String](0) === Map(2 -> "a"))
  }

  test("struct with column name") {
    val df = Seq((1, "str")).toDF("a", "b")
    val row = df.select(struct("a", "b")).first()

    val expectedType = StructType(Seq(
      StructField("a", IntegerType, nullable = false),
      StructField("b", StringType)
    ))
    assert(row.schema(0).dataType === expectedType)
    assert(row.getAs[Row](0) === Row(1, "str"))
  }

  test("struct with column expression") {
    val df = Seq((1, "str")).toDF("a", "b")
    val row = df.select(struct((col("a") * 2).as("c"), col("b"))).first()

    val expectedType = StructType(Seq(
      StructField("c", IntegerType, nullable = false),
      StructField("b", StringType)
    ))
    assert(row.schema(0).dataType === expectedType)
    assert(row.getAs[Row](0) === Row(2, "str"))
  }

  test("struct with column expression to be automatically named") {
    val df = Seq((1, "str")).toDF("a", "b")
    val result = df.select(struct((col("a") * 2), col("b")))

    val expectedType = StructType(Seq(
      StructField("col1", IntegerType, nullable = false),
      StructField("b", StringType)
    ))
    assert(result.first.schema(0).dataType === expectedType)
    checkAnswer(result, Row(Row(2, "str")))
  }

  test("struct with literal columns") {
    val df = Seq((1, "str1"), (2, "str2")).toDF("a", "b")
    val result = df.select(struct((col("a") * 2), lit(5.0)))

    val expectedType = StructType(Seq(
      StructField("col1", IntegerType, nullable = false),
      StructField("col2", DoubleType, nullable = false)
    ))

    assert(result.first.schema(0).dataType === expectedType)
    checkAnswer(result, Seq(Row(Row(2, 5.0)), Row(Row(4, 5.0))))
  }

  test("struct with all literal columns") {
    val df = Seq((1, "str1"), (2, "str2")).toDF("a", "b")
    val result = df.select(struct(lit("v"), lit(5.0)))

    val expectedType = StructType(Seq(
      StructField("col1", StringType, nullable = false),
      StructField("col2", DoubleType, nullable = false)
    ))

    assert(result.first.schema(0).dataType === expectedType)
    checkAnswer(result, Seq(Row(Row("v", 5.0)), Row(Row("v", 5.0))))
  }

  test("constant functions") {
    checkAnswer(
      sql("SELECT E()"),
      Row(scala.math.E)
    )
    checkAnswer(
      sql("SELECT PI()"),
      Row(scala.math.Pi)
    )
  }

  test("bitwiseNOT") {
    checkAnswer(
      testData2.select(bitwiseNOT($"a")),
      testData2.collect().toSeq.map(r => Row(~r.getInt(0))))
  }

  test("bin") {
    val df = Seq[(Integer, Integer)]((12, null)).toDF("a", "b")
    checkAnswer(
      df.select(bin("a"), bin("b")),
      Row("1100", null))
    checkAnswer(
      df.selectExpr("bin(a)", "bin(b)"),
      Row("1100", null))
  }

  test("if function") {
    val df = Seq((1, 2)).toDF("a", "b")
    checkAnswer(
      df.selectExpr("if(a = 1, 'one', 'not_one')", "if(b = 1, 'one', 'not_one')"),
      Row("one", "not_one"))
  }

  test("misc md5 function") {
    val df = Seq(("ABC", Array[Byte](1, 2, 3, 4, 5, 6))).toDF("a", "b")
    checkAnswer(
      df.select(md5($"a"), md5($"b")),
      Row("902fbdd2b1df0c4f70b4a5d23525e932", "6ac1e56bc78f031059be7be854522c4c"))

    checkAnswer(
      df.selectExpr("md5(a)", "md5(b)"),
      Row("902fbdd2b1df0c4f70b4a5d23525e932", "6ac1e56bc78f031059be7be854522c4c"))
  }

  test("misc sha1 function") {
    val df = Seq(("ABC", "ABC".getBytes(StandardCharsets.UTF_8))).toDF("a", "b")
    checkAnswer(
      df.select(sha1($"a"), sha1($"b")),
      Row("3c01bdbb26f358bab27f267924aa2c9a03fcfdb8", "3c01bdbb26f358bab27f267924aa2c9a03fcfdb8"))

    val dfEmpty = Seq(("", "".getBytes(StandardCharsets.UTF_8))).toDF("a", "b")
    checkAnswer(
      dfEmpty.selectExpr("sha1(a)", "sha1(b)"),
      Row("da39a3ee5e6b4b0d3255bfef95601890afd80709", "da39a3ee5e6b4b0d3255bfef95601890afd80709"))
  }

  test("misc sha2 function") {
    val df = Seq(("ABC", Array[Byte](1, 2, 3, 4, 5, 6))).toDF("a", "b")
    checkAnswer(
      df.select(sha2($"a", 256), sha2($"b", 256)),
      Row("b5d4045c3f466fa91fe2cc6abe79232a1a57cdf104f7a26e716e0a1e2789df78",
        "7192385c3c0605de55bb9476ce1d90748190ecb32a8eed7f5207b30cf6a1fe89"))

    checkAnswer(
      df.selectExpr("sha2(a, 256)", "sha2(b, 256)"),
      Row("b5d4045c3f466fa91fe2cc6abe79232a1a57cdf104f7a26e716e0a1e2789df78",
        "7192385c3c0605de55bb9476ce1d90748190ecb32a8eed7f5207b30cf6a1fe89"))

    intercept[IllegalArgumentException] {
      df.select(sha2($"a", 1024))
    }
  }

  test("misc crc32 function") {
    val df = Seq(("ABC", Array[Byte](1, 2, 3, 4, 5, 6))).toDF("a", "b")
    checkAnswer(
      df.select(crc32($"a"), crc32($"b")),
      Row(2743272264L, 2180413220L))

    checkAnswer(
      df.selectExpr("crc32(a)", "crc32(b)"),
      Row(2743272264L, 2180413220L))
  }

  test("string function find_in_set") {
    val df = Seq(("abc,b,ab,c,def", "abc,b,ab,c,def")).toDF("a", "b")

    checkAnswer(
      df.selectExpr("find_in_set('ab', a)", "find_in_set('x', b)"),
      Row(3, 0))
  }

  test("conditional function: least") {
    checkAnswer(
      testData2.select(least(lit(-1), lit(0), col("a"), col("b"))).limit(1),
      Row(-1)
    )
    checkAnswer(
      sql("SELECT least(a, 2) as l from testData2 order by l"),
      Seq(Row(1), Row(1), Row(2), Row(2), Row(2), Row(2))
    )
  }

  test("conditional function: greatest") {
    checkAnswer(
      testData2.select(greatest(lit(2), lit(3), col("a"), col("b"))).limit(1),
      Row(3)
    )
    checkAnswer(
      sql("SELECT greatest(a, 2) as g from testData2 order by g"),
      Seq(Row(2), Row(2), Row(2), Row(2), Row(3), Row(3))
    )
  }

  test("pmod") {
    val intData = Seq((7, 3), (-7, 3)).toDF("a", "b")
    checkAnswer(
      intData.select(pmod('a, 'b)),
      Seq(Row(1), Row(2))
    )
    checkAnswer(
      intData.select(pmod('a, lit(3))),
      Seq(Row(1), Row(2))
    )
    checkAnswer(
      intData.select(pmod(lit(-7), 'b)),
      Seq(Row(2), Row(2))
    )
    checkAnswer(
      intData.selectExpr("pmod(a, b)"),
      Seq(Row(1), Row(2))
    )
    checkAnswer(
      intData.selectExpr("pmod(a, 3)"),
      Seq(Row(1), Row(2))
    )
    checkAnswer(
      intData.selectExpr("pmod(-7, b)"),
      Seq(Row(2), Row(2))
    )
    val doubleData = Seq((7.2, 4.1)).toDF("a", "b")
    checkAnswer(
      doubleData.select(pmod('a, 'b)),
      Seq(Row(3.1000000000000005)) // same as hive
    )
    checkAnswer(
      doubleData.select(pmod(lit(2), lit(Int.MaxValue))),
      Seq(Row(2))
    )
  }

  test("sort_array function") {
    val df = Seq(
      (Array[Int](2, 1, 3), Array("b", "c", "a")),
      (Array[Int](), Array[String]()),
      (null, null)
    ).toDF("a", "b")
    checkAnswer(
      df.select(sort_array($"a"), sort_array($"b")),
      Seq(
        Row(Seq(1, 2, 3), Seq("a", "b", "c")),
        Row(Seq[Int](), Seq[String]()),
        Row(null, null))
    )
    checkAnswer(
      df.select(sort_array($"a", false), sort_array($"b", false)),
      Seq(
        Row(Seq(3, 2, 1), Seq("c", "b", "a")),
        Row(Seq[Int](), Seq[String]()),
        Row(null, null))
    )
    checkAnswer(
      df.selectExpr("sort_array(a)", "sort_array(b)"),
      Seq(
        Row(Seq(1, 2, 3), Seq("a", "b", "c")),
        Row(Seq[Int](), Seq[String]()),
        Row(null, null))
    )
    checkAnswer(
      df.selectExpr("sort_array(a, true)", "sort_array(b, false)"),
      Seq(
        Row(Seq(1, 2, 3), Seq("c", "b", "a")),
        Row(Seq[Int](), Seq[String]()),
        Row(null, null))
    )

    val df2 = Seq((Array[Array[Int]](Array(2), Array(1), Array(2, 4), null), "x")).toDF("a", "b")
    checkAnswer(
      df2.selectExpr("sort_array(a, true)", "sort_array(a, false)"),
      Seq(
        Row(
          Seq[Seq[Int]](null, Seq(1), Seq(2), Seq(2, 4)),
          Seq[Seq[Int]](Seq(2, 4), Seq(2), Seq(1), null)))
    )

    val df3 = Seq(("xxx", "x")).toDF("a", "b")
    assert(intercept[AnalysisException] {
      df3.selectExpr("sort_array(a)").collect()
    }.getMessage().contains("only supports array input"))
  }

  test("array size function") {
    val df = Seq(
      (Seq[Int](1, 2), "x"),
      (Seq[Int](), "y"),
      (Seq[Int](1, 2, 3), "z"),
      (null, "empty")
    ).toDF("a", "b")
    checkAnswer(
      df.select(size($"a")),
      Seq(Row(2), Row(0), Row(3), Row(-1))
    )
    checkAnswer(
      df.selectExpr("size(a)"),
      Seq(Row(2), Row(0), Row(3), Row(-1))
    )
  }

  test("map size function") {
    val df = Seq(
      (Map[Int, Int](1 -> 1, 2 -> 2), "x"),
      (Map[Int, Int](), "y"),
      (Map[Int, Int](1 -> 1, 2 -> 2, 3 -> 3), "z"),
      (null, "empty")
    ).toDF("a", "b")
    checkAnswer(
      df.select(size($"a")),
      Seq(Row(2), Row(0), Row(3), Row(-1))
    )
    checkAnswer(
      df.selectExpr("size(a)"),
      Seq(Row(2), Row(0), Row(3), Row(-1))
    )
  }

  test("map_keys/map_values function") {
    val df = Seq(
      (Map[Int, Int](1 -> 100, 2 -> 200), "x"),
      (Map[Int, Int](), "y"),
      (Map[Int, Int](1 -> 100, 2 -> 200, 3 -> 300), "z")
    ).toDF("a", "b")
    checkAnswer(
      df.selectExpr("map_keys(a)"),
      Seq(Row(Seq(1, 2)), Row(Seq.empty), Row(Seq(1, 2, 3)))
    )
    checkAnswer(
      df.selectExpr("map_values(a)"),
      Seq(Row(Seq(100, 200)), Row(Seq.empty), Row(Seq(100, 200, 300)))
    )
  }

  test("array contains function") {
    val df = Seq(
      (Seq[Int](1, 2), "x"),
      (Seq[Int](), "x")
    ).toDF("a", "b")

    // Simple test cases
    checkAnswer(
      df.select(array_contains(df("a"), 1)),
      Seq(Row(true), Row(false))
    )
    checkAnswer(
      df.selectExpr("array_contains(a, 1)"),
      Seq(Row(true), Row(false))
    )

    // In hive, this errors because null has no type information
    intercept[AnalysisException] {
      df.select(array_contains(df("a"), null))
    }
    intercept[AnalysisException] {
      df.selectExpr("array_contains(a, null)")
    }
    intercept[AnalysisException] {
      df.selectExpr("array_contains(null, 1)")
    }

    checkAnswer(
      df.selectExpr("array_contains(array(array(1), null)[0], 1)"),
      Seq(Row(true), Row(true))
    )
    checkAnswer(
      df.selectExpr("array_contains(array(1, null), array(1, null)[0])"),
      Seq(Row(true), Row(true))
    )
  }
}
