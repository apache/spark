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

import java.io.File
import java.nio.charset.StandardCharsets
import java.sql.{Date, Timestamp}

import scala.util.Random

import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.{Alias, ArraysZip, AttributeReference, Expression, NamedExpression, UnaryExpression}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.plans.logical.OneRowRelation
import org.apache.spark.sql.catalyst.util.DateTimeTestUtils.{withDefaultTimeZone, UTC}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._

/**
 * Test suite for functions in [[org.apache.spark.sql.functions]].
 */
class DataFrameFunctionsSuite extends QueryTest with SharedSparkSession {
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

  test("map with arrays") {
    val df1 = Seq((Seq(1, 2), Seq("a", "b"))).toDF("k", "v")
    val expectedType = MapType(IntegerType, StringType, valueContainsNull = true)
    val row = df1.select(map_from_arrays($"k", $"v")).first()
    assert(row.schema(0).dataType === expectedType)
    assert(row.getMap[Int, String](0) === Map(1 -> "a", 2 -> "b"))
    checkAnswer(df1.select(map_from_arrays($"k", $"v")), Seq(Row(Map(1 -> "a", 2 -> "b"))))

    val df2 = Seq((Seq(1, 2), Seq(null, "b"))).toDF("k", "v")
    checkAnswer(df2.select(map_from_arrays($"k", $"v")), Seq(Row(Map(1 -> null, 2 -> "b"))))

    val df3 = Seq((null, null)).toDF("k", "v")
    checkAnswer(df3.select(map_from_arrays($"k", $"v")), Seq(Row(null)))

    val df4 = Seq((1, "a")).toDF("k", "v")
    intercept[AnalysisException] {
      df4.select(map_from_arrays($"k", $"v"))
    }

    val df5 = Seq((Seq("a", null), Seq(1, 2))).toDF("k", "v")
    val msg1 = intercept[Exception] {
      df5.select(map_from_arrays($"k", $"v")).collect
    }.getMessage
    assert(msg1.contains("Cannot use null as map key"))

    val df6 = Seq((Seq(1, 2), Seq("a"))).toDF("k", "v")
    val msg2 = intercept[Exception] {
      df6.select(map_from_arrays($"k", $"v")).collect
    }.getMessage
    assert(msg2.contains("The key array and value array of MapData must have the same length"))
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

  test("bitwise_not") {
    checkAnswer(
      testData2.select(bitwiseNOT($"a"), bitwise_not($"a")),
      testData2.collect().toSeq.map(r => Row(~r.getInt(0), ~r.getInt(0))))
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

  test("misc aes function") {
    val key16 = "abcdefghijklmnop"
    val key24 = "abcdefghijklmnop12345678"
    val key32 = "abcdefghijklmnop12345678ABCDEFGH"
    val encryptedText16 = "4Hv0UKCx6nfUeAoPZo1z+w=="
    val encryptedText24 = "NeTYNgA+PCQBN50DA//O2w=="
    val encryptedText32 = "9J3iZbIxnmaG+OIA9Amd+A=="
    val encryptedEmptyText16 = "jmTOhz8XTbskI/zYFFgOFQ=="
    val encryptedEmptyText24 = "9RDK70sHNzqAFRcpfGM5gQ=="
    val encryptedEmptyText32 = "j9IDsCvlYXtcVJUf4FAjQQ=="

    val df1 = Seq("Spark", "").toDF

    // Successful encryption
    Seq(
      (key16, encryptedText16, encryptedEmptyText16),
      (key24, encryptedText24, encryptedEmptyText24),
      (key32, encryptedText32, encryptedEmptyText32)).foreach {
      case (key, encryptedText, encryptedEmptyText) =>
        checkAnswer(
          df1.selectExpr(s"base64(aes_encrypt(value, '$key', 'ECB'))"),
          Seq(Row(encryptedText), Row(encryptedEmptyText)))
        checkAnswer(
          df1.selectExpr(s"base64(aes_encrypt(binary(value), '$key', 'ECB'))"),
          Seq(Row(encryptedText), Row(encryptedEmptyText)))
    }

    // Encryption failure - input or key is null
    Seq(key16, key24, key32).foreach { key =>
      checkAnswer(
        df1.selectExpr(s"aes_encrypt(cast(null as string), '$key')"),
        Seq(Row(null), Row(null)))
      checkAnswer(
        df1.selectExpr(s"aes_encrypt(cast(null as binary), '$key')"),
        Seq(Row(null), Row(null)))
      checkAnswer(
        df1.selectExpr(s"aes_encrypt(cast(null as string), binary('$key'))"),
        Seq(Row(null), Row(null)))
      checkAnswer(
        df1.selectExpr(s"aes_encrypt(cast(null as binary), binary('$key'))"),
        Seq(Row(null), Row(null)))
    }
    checkAnswer(
      df1.selectExpr("aes_encrypt(value, cast(null as string))"),
      Seq(Row(null), Row(null)))
    checkAnswer(
      df1.selectExpr("aes_encrypt(value, cast(null as binary))"),
      Seq(Row(null), Row(null)))

    val df2 = Seq(
      (encryptedText16, encryptedText24, encryptedText32),
      (encryptedEmptyText16, encryptedEmptyText24, encryptedEmptyText32)
    ).toDF("value16", "value24", "value32")

    // Successful decryption
    Seq(
      ("value16", key16),
      ("value24", key24),
      ("value32", key32)).foreach {
      case (colName, key) =>
        checkAnswer(
          df2.selectExpr(s"cast(aes_decrypt(unbase64($colName), '$key', 'ECB') as string)"),
          Seq(Row("Spark"), Row("")))
        checkAnswer(
          df2.selectExpr(s"cast(aes_decrypt(unbase64($colName), binary('$key'), 'ECB') as string)"),
          Seq(Row("Spark"), Row("")))
    }

    // Decryption failure - input or key is null
    Seq(key16, key24, key32).foreach { key =>
      checkAnswer(
        df2.selectExpr(s"aes_decrypt(cast(null as binary), '$key')"),
        Seq(Row(null), Row(null)))
      checkAnswer(
        df2.selectExpr(s"aes_decrypt(cast(null as binary), binary('$key'))"),
        Seq(Row(null), Row(null)))
    }
    Seq("value16", "value24", "value32").foreach { colName =>
      checkAnswer(
        df2.selectExpr(s"aes_decrypt($colName, cast(null as string))"),
        Seq(Row(null), Row(null)))
      checkAnswer(
        df2.selectExpr(s"aes_decrypt($colName, cast(null as binary))"),
        Seq(Row(null), Row(null)))
    }
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
      intData.select(pmod($"a", $"b")),
      Seq(Row(1), Row(2))
    )
    checkAnswer(
      intData.select(pmod($"a", lit(3))),
      Seq(Row(1), Row(2))
    )
    checkAnswer(
      intData.select(pmod(lit(-7), $"b")),
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
      doubleData.select(pmod($"a", $"b")),
      Seq(Row(3.1000000000000005)) // same as hive
    )
    checkAnswer(
      doubleData.select(pmod(lit(2), lit(Int.MaxValue))),
      Seq(Row(2))
    )
  }

  test("array_sort with lambda functions") {

    spark.udf.register("fAsc", (x: Int, y: Int) => {
      if (x < y) -1
      else if (x == y) 0
      else 1
    })

    spark.udf.register("fDesc", (x: Int, y: Int) => {
      if (x < y) 1
      else if (x == y) 0
      else -1
    })

    spark.udf.register("fString", (x: String, y: String) => {
      if (x == null && y == null) 0
      else if (x == null) 1
      else if (y == null) -1
      else if (x < y) 1
      else if (x == y) 0
      else -1
    })

    spark.udf.register("fStringLength", (x: String, y: String) => {
      if (x == null && y == null) 0
      else if (x == null) 1
      else if (y == null) -1
      else if (x.length < y.length) -1
      else if (x.length == y.length) 0
      else 1
    })

    val df1 = Seq(Array[Int](3, 2, 5, 1, 2)).toDF("a")
    checkAnswer(
      df1.selectExpr("array_sort(a, (x, y) -> fAsc(x, y))"),
      Seq(
        Row(Seq(1, 2, 2, 3, 5)))
    )

    checkAnswer(
      df1.selectExpr("array_sort(a, (x, y) -> fDesc(x, y))"),
      Seq(
        Row(Seq(5, 3, 2, 2, 1)))
    )

    val df2 = Seq(Array[String]("bc", "ab", "dc")).toDF("a")
    checkAnswer(
      df2.selectExpr("array_sort(a, (x, y) -> fString(x, y))"),
      Seq(
        Row(Seq("dc", "bc", "ab")))
    )

    val df3 = Seq(Array[String]("a", "abcd", "abc")).toDF("a")
    checkAnswer(
      df3.selectExpr("array_sort(a, (x, y) -> fStringLength(x, y))"),
      Seq(
        Row(Seq("a", "abc", "abcd")))
    )

    val df4 = Seq((Array[Array[Int]](Array(2, 3, 1), Array(4, 2, 1, 4),
      Array(1, 2)), "x")).toDF("a", "b")
    checkAnswer(
      df4.selectExpr("array_sort(a, (x, y) -> fAsc(cardinality(x), cardinality(y)))"),
      Seq(
        Row(Seq[Seq[Int]](Seq(1, 2), Seq(2, 3, 1), Seq(4, 2, 1, 4))))
    )

    val df5 = Seq(Array[String]("bc", null, "ab", "dc")).toDF("a")
    checkAnswer(
      df5.selectExpr("array_sort(a, (x, y) -> fString(x, y))"),
      Seq(
        Row(Seq("dc", "bc", "ab", null)))
    )

    spark.sql("drop temporary function fAsc")
    spark.sql("drop temporary function fDesc")
    spark.sql("drop temporary function fString")
    spark.sql("drop temporary function fStringLength")
  }

  test("SPARK-38130: array_sort with lambda of non-orderable items") {
    val df6 = Seq((Array[Map[String, Int]](Map("a" -> 1), Map("b" -> 2, "c" -> 3),
      Map()), "x")).toDF("a", "b")
    checkAnswer(
      df6.selectExpr("array_sort(a, (x, y) -> cardinality(x) - cardinality(y))"),
      Seq(
        Row(Seq[Map[String, Int]](Map(), Map("a" -> 1), Map("b" -> 2, "c" -> 3))))
    )
  }

  test("sort_array/array_sort functions") {
    val df = Seq(
      (Array[Int](2, 1, 3), Array("b", "c", "a")),
      (Array.empty[Int], Array.empty[String]),
      (null, null)
    ).toDF("a", "b")
    checkAnswer(
      df.select(sort_array($"a"), sort_array($"b")),
      Seq(
        Row(Seq(1, 2, 3), Seq("a", "b", "c")),
        Row(Seq.empty[Int], Seq.empty[String]),
        Row(null, null))
    )
    checkAnswer(
      df.select(sort_array($"a", false), sort_array($"b", false)),
      Seq(
        Row(Seq(3, 2, 1), Seq("c", "b", "a")),
        Row(Seq.empty[Int], Seq.empty[String]),
        Row(null, null))
    )
    checkAnswer(
      df.selectExpr("sort_array(a)", "sort_array(b)"),
      Seq(
        Row(Seq(1, 2, 3), Seq("a", "b", "c")),
        Row(Seq.empty[Int], Seq.empty[String]),
        Row(null, null))
    )
    checkAnswer(
      df.selectExpr("sort_array(a, true)", "sort_array(b, false)"),
      Seq(
        Row(Seq(1, 2, 3), Seq("c", "b", "a")),
        Row(Seq.empty[Int], Seq.empty[String]),
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

    checkAnswer(
      df.select(array_sort($"a"), array_sort($"b")),
      Seq(
        Row(Seq(1, 2, 3), Seq("a", "b", "c")),
        Row(Seq.empty[Int], Seq.empty[String]),
        Row(null, null))
    )
    checkAnswer(
      df.selectExpr("array_sort(a)", "array_sort(b)"),
      Seq(
        Row(Seq(1, 2, 3), Seq("a", "b", "c")),
        Row(Seq.empty[Int], Seq.empty[String]),
        Row(null, null))
    )

    checkAnswer(
      df2.selectExpr("array_sort(a)"),
      Seq(Row(Seq[Seq[Int]](Seq(1), Seq(2), Seq(2, 4), null)))
    )

    assert(intercept[AnalysisException] {
      df3.selectExpr("array_sort(a)").collect()
    }.getMessage().contains("argument 1 requires array type, however, 'a' is of string type"))
  }

  def testSizeOfArray(sizeOfNull: Any): Unit = {
    val df = Seq(
      (Seq[Int](1, 2), "x"),
      (Seq[Int](), "y"),
      (Seq[Int](1, 2, 3), "z"),
      (null, "empty")
    ).toDF("a", "b")

    checkAnswer(df.select(size($"a")), Seq(Row(2), Row(0), Row(3), Row(sizeOfNull)))
    checkAnswer(df.selectExpr("size(a)"), Seq(Row(2), Row(0), Row(3), Row(sizeOfNull)))
    checkAnswer(df.selectExpr("cardinality(a)"), Seq(Row(2L), Row(0L), Row(3L), Row(sizeOfNull)))
  }

  test("array size function - legacy") {
    if (!conf.ansiEnabled) {
      withSQLConf(SQLConf.LEGACY_SIZE_OF_NULL.key -> "true") {
        testSizeOfArray(sizeOfNull = -1)
      }
    }
  }

  test("array size function") {
    withSQLConf(SQLConf.LEGACY_SIZE_OF_NULL.key -> "false") {
      testSizeOfArray(sizeOfNull = null)
    }
    // size(null) should return null under ansi mode.
    withSQLConf(
      SQLConf.LEGACY_SIZE_OF_NULL.key -> "true",
      SQLConf.ANSI_ENABLED.key -> "true") {
      testSizeOfArray(sizeOfNull = null)
    }
  }

  test("dataframe arrays_zip function") {
    val df1 = Seq((Seq(9001, 9002, 9003), Seq(4, 5, 6))).toDF("val1", "val2")
    val df2 = Seq((Seq("a", "b"), Seq(true, false), Seq(10, 11))).toDF("val1", "val2", "val3")
    val df3 = Seq((Seq("a", "b"), Seq(4, 5, 6))).toDF("val1", "val2")
    val df4 = Seq((Seq("a", "b", null), Seq(4L))).toDF("val1", "val2")
    val df5 = Seq((Seq(-1), Seq(null), Seq(), Seq(null, null))).toDF("val1", "val2", "val3", "val4")
    val df6 = Seq((Seq(192.toByte, 256.toByte), Seq(1.1), Seq(), Seq(null, null)))
      .toDF("v1", "v2", "v3", "v4")
    val df7 = Seq((Seq(Seq(1, 2, 3), Seq(4, 5)), Seq(1.1, 2.2))).toDF("v1", "v2")
    val df8 = Seq((Seq(Array[Byte](1.toByte, 5.toByte)), Seq(null))).toDF("v1", "v2")

    val expectedValue1 = Row(Seq(Row(9001, 4), Row(9002, 5), Row(9003, 6)))
    checkAnswer(df1.select(arrays_zip($"val1", $"val2")), expectedValue1)
    checkAnswer(df1.selectExpr("arrays_zip(val1, val2)"), expectedValue1)

    val expectedValue2 = Row(Seq(Row("a", true, 10), Row("b", false, 11)))
    checkAnswer(df2.select(arrays_zip($"val1", $"val2", $"val3")), expectedValue2)
    checkAnswer(df2.selectExpr("arrays_zip(val1, val2, val3)"), expectedValue2)

    val expectedValue3 = Row(Seq(Row("a", 4), Row("b", 5), Row(null, 6)))
    checkAnswer(df3.select(arrays_zip($"val1", $"val2")), expectedValue3)
    checkAnswer(df3.selectExpr("arrays_zip(val1, val2)"), expectedValue3)

    val expectedValue4 = Row(Seq(Row("a", 4L), Row("b", null), Row(null, null)))
    checkAnswer(df4.select(arrays_zip($"val1", $"val2")), expectedValue4)
    checkAnswer(df4.selectExpr("arrays_zip(val1, val2)"), expectedValue4)

    val expectedValue5 = Row(Seq(Row(-1, null, null, null), Row(null, null, null, null)))
    checkAnswer(df5.select(arrays_zip($"val1", $"val2", $"val3", $"val4")), expectedValue5)
    checkAnswer(df5.selectExpr("arrays_zip(val1, val2, val3, val4)"), expectedValue5)

    val expectedValue6 = Row(Seq(
      Row(192.toByte, 1.1, null, null), Row(256.toByte, null, null, null)))
    checkAnswer(df6.select(arrays_zip($"v1", $"v2", $"v3", $"v4")), expectedValue6)
    checkAnswer(df6.selectExpr("arrays_zip(v1, v2, v3, v4)"), expectedValue6)

    val expectedValue7 = Row(Seq(
      Row(Seq(1, 2, 3), 1.1), Row(Seq(4, 5), 2.2)))
    checkAnswer(df7.select(arrays_zip($"v1", $"v2")), expectedValue7)
    checkAnswer(df7.selectExpr("arrays_zip(v1, v2)"), expectedValue7)

    val expectedValue8 = Row(Seq(
      Row(Array[Byte](1.toByte, 5.toByte), null)))
    checkAnswer(df8.select(arrays_zip($"v1", $"v2")), expectedValue8)
    checkAnswer(df8.selectExpr("arrays_zip(v1, v2)"), expectedValue8)
  }

  testWithWholeStageCodegenOnAndOff("SPARK-24633: arrays_zip splits input " +
    "processing correctly") { _ =>
    val df = spark.range(1)
    val exprs = (0 to 5).map(x => array($"id" + lit(x)))
    checkAnswer(df.select(arrays_zip(exprs: _*)),
      Row(Seq(Row(0, 1, 2, 3, 4, 5))))
  }

  test("SPARK-35876: arrays_zip should retain field names") {
    withTempDir { dir =>
      val df = spark.sparkContext.parallelize(
        Seq((Seq(9001, 9002, 9003), Seq(4, 5, 6)))).toDF("val1", "val2")
      val qualifiedDF = df.as("foo")

      // Fields are UnresolvedAttribute
      val zippedDF1 = qualifiedDF.select(arrays_zip($"foo.val1", $"foo.val2") as "zipped")
      val maybeAlias1 = zippedDF1.queryExecution.logical.expressions.head
      assert(maybeAlias1.isInstanceOf[Alias])
      val maybeArraysZip1 = maybeAlias1.children.head
      assert(maybeArraysZip1.isInstanceOf[ArraysZip])
      assert(maybeArraysZip1.children.forall(_.isInstanceOf[UnresolvedAttribute]))
      val file1 = new File(dir, "arrays_zip1")
      zippedDF1.write.parquet(file1.getAbsolutePath)
      val restoredDF1 = spark.read.parquet(file1.getAbsolutePath)
      val fieldNames1 = restoredDF1.schema.head.dataType.asInstanceOf[ArrayType]
        .elementType.asInstanceOf[StructType].fieldNames
      assert(fieldNames1.toSeq === Seq("val1", "val2"))

      // Fields are resolved NamedExpression
      val zippedDF2 = df.select(arrays_zip(df("val1"), df("val2")) as "zipped")
      val maybeAlias2 = zippedDF2.queryExecution.logical.expressions.head
      assert(maybeAlias2.isInstanceOf[Alias])
      val maybeArraysZip2 = maybeAlias2.children.head
      assert(maybeArraysZip2.isInstanceOf[ArraysZip])
      assert(maybeArraysZip2.children.forall(
        e => e.isInstanceOf[AttributeReference] && e.resolved))
      val file2 = new File(dir, "arrays_zip2")
      zippedDF2.write.parquet(file2.getAbsolutePath)
      val restoredDF2 = spark.read.parquet(file2.getAbsolutePath)
      val fieldNames2 = restoredDF2.schema.head.dataType.asInstanceOf[ArrayType]
        .elementType.asInstanceOf[StructType].fieldNames
      assert(fieldNames2.toSeq === Seq("val1", "val2"))

      // Fields are unresolved NamedExpression
      val zippedDF3 = df.select(arrays_zip($"val1" as "val3", $"val2" as "val4") as "zipped")
      val maybeAlias3 = zippedDF3.queryExecution.logical.expressions.head
      assert(maybeAlias3.isInstanceOf[Alias])
      val maybeArraysZip3 = maybeAlias3.children.head
      assert(maybeArraysZip3.isInstanceOf[ArraysZip])
      assert(maybeArraysZip3.children.forall(e => e.isInstanceOf[Alias] && !e.resolved))
      val file3 = new File(dir, "arrays_zip3")
      zippedDF3.write.parquet(file3.getAbsolutePath)
      val restoredDF3 = spark.read.parquet(file3.getAbsolutePath)
      val fieldNames3 = restoredDF3.schema.head.dataType.asInstanceOf[ArrayType]
        .elementType.asInstanceOf[StructType].fieldNames
      assert(fieldNames3.toSeq === Seq("val3", "val4"))

      // Fields are neither UnresolvedAttribute nor NamedExpression
      val zippedDF4 = df.select(arrays_zip(array_sort($"val1"), array_sort($"val2")) as "zipped")
      val maybeAlias4 = zippedDF4.queryExecution.logical.expressions.head
      assert(maybeAlias4.isInstanceOf[Alias])
      val maybeArraysZip4 = maybeAlias4.children.head
      assert(maybeArraysZip4.isInstanceOf[ArraysZip])
      assert(maybeArraysZip4.children.forall {
        case _: UnresolvedAttribute | _: NamedExpression => false
        case _ => true
      })
      val file4 = new File(dir, "arrays_zip4")
      zippedDF4.write.parquet(file4.getAbsolutePath)
      val restoredDF4 = spark.read.parquet(file4.getAbsolutePath)
      val fieldNames4 = restoredDF4.schema.head.dataType.asInstanceOf[ArrayType]
        .elementType.asInstanceOf[StructType].fieldNames
      assert(fieldNames4.toSeq === Seq("0", "1"))
    }
  }

  def testSizeOfMap(sizeOfNull: Any): Unit = {
    val df = Seq(
      (Map[Int, Int](1 -> 1, 2 -> 2), "x"),
      (Map[Int, Int](), "y"),
      (Map[Int, Int](1 -> 1, 2 -> 2, 3 -> 3), "z"),
      (null, "empty")
    ).toDF("a", "b")

    checkAnswer(df.select(size($"a")), Seq(Row(2), Row(0), Row(3), Row(sizeOfNull)))
    checkAnswer(df.selectExpr("size(a)"), Seq(Row(2), Row(0), Row(3), Row(sizeOfNull)))
  }

  test("map size function - legacy") {
    if (!conf.ansiEnabled) {
      withSQLConf(SQLConf.LEGACY_SIZE_OF_NULL.key -> "true") {
        testSizeOfMap(sizeOfNull = -1: Int)
      }
    }
  }

  test("map size function") {
    withSQLConf(SQLConf.LEGACY_SIZE_OF_NULL.key -> "false") {
      testSizeOfMap(sizeOfNull = null)
    }
    // size(null) should return null under ansi mode.
    withSQLConf(
      SQLConf.LEGACY_SIZE_OF_NULL.key -> "true",
      SQLConf.ANSI_ENABLED.key -> "true") {
      testSizeOfMap(sizeOfNull = null)
    }
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

  test("map_entries") {
    // Primitive-type elements
    val idf = Seq(
      Map[Int, Int](1 -> 100, 2 -> 200, 3 -> 300),
      Map[Int, Int](),
      null
    ).toDF("m")
    val iExpected = Seq(
      Row(Seq(Row(1, 100), Row(2, 200), Row(3, 300))),
      Row(Seq.empty),
      Row(null)
    )

    def testPrimitiveType(): Unit = {
      checkAnswer(idf.select(map_entries($"m")), iExpected)
      checkAnswer(idf.selectExpr("map_entries(m)"), iExpected)
      checkAnswer(idf.selectExpr("map_entries(map(1, null, 2, null))"),
        Seq.fill(iExpected.length)(Row(Seq(Row(1, null), Row(2, null)))))
    }

    // Test with local relation, the Project will be evaluated without codegen
    testPrimitiveType()
    // Test with cached relation, the Project will be evaluated with codegen
    idf.cache()
    testPrimitiveType()

    // Non-primitive-type elements
    val sdf = Seq(
      Map[String, String]("a" -> "f", "b" -> "o", "c" -> "o"),
      Map[String, String]("a" -> null, "b" -> null),
      Map[String, String](),
      null
    ).toDF("m")
    val sExpected = Seq(
      Row(Seq(Row("a", "f"), Row("b", "o"), Row("c", "o"))),
      Row(Seq(Row("a", null), Row("b", null))),
      Row(Seq.empty),
      Row(null)
    )

    def testNonPrimitiveType(): Unit = {
      checkAnswer(sdf.select(map_entries($"m")), sExpected)
      checkAnswer(sdf.selectExpr("map_entries(m)"), sExpected)
    }

    // Test with local relation, the Project will be evaluated without codegen
    testNonPrimitiveType()
    // Test with cached relation, the Project will be evaluated with codegen
    sdf.cache()
    testNonPrimitiveType()
  }

  test("map_concat function") {
    val df1 = Seq(
      (Map[Int, Int](1 -> 100, 2 -> 200), Map[Int, Int](3 -> 300, 4 -> 400)),
      (Map[Int, Int](1 -> 100, 2 -> 200), Map[Int, Int](3 -> 300, 1 -> 400)),
      (null, Map[Int, Int](3 -> 300, 4 -> 400))
    ).toDF("map1", "map2")

    val expected1a = Seq(
      Row(Map(1 -> 100, 2 -> 200, 3 -> 300, 4 -> 400)),
      Row(Map(1 -> 400, 2 -> 200, 3 -> 300)),
      Row(null)
    )

    intercept[SparkException](df1.selectExpr("map_concat(map1, map2)").collect())
    intercept[SparkException](df1.select(map_concat($"map1", $"map2")).collect())
    withSQLConf(SQLConf.MAP_KEY_DEDUP_POLICY.key -> SQLConf.MapKeyDedupPolicy.LAST_WIN.toString) {
      checkAnswer(df1.selectExpr("map_concat(map1, map2)"), expected1a)
      checkAnswer(df1.select(map_concat($"map1", $"map2")), expected1a)
    }

    val expected1b = Seq(
      Row(Map(1 -> 100, 2 -> 200)),
      Row(Map(1 -> 100, 2 -> 200)),
      Row(null)
    )

    checkAnswer(df1.selectExpr("map_concat(map1)"), expected1b)
    checkAnswer(df1.select(map_concat($"map1")), expected1b)

    val df2 = Seq(
      (
        Map[Array[Int], Int](Array(1) -> 100, Array(2) -> 200),
        Map[String, Int]("3" -> 300, "4" -> 400)
      )
    ).toDF("map1", "map2")

    val expected2 = Seq(Row(Map()))

    checkAnswer(df2.selectExpr("map_concat()"), expected2)
    checkAnswer(df2.select(map_concat()), expected2)

    val df3 = {
      val schema = StructType(
        StructField("map1", MapType(StringType, IntegerType, true), false)  ::
        StructField("map2", MapType(StringType, IntegerType, false), false) :: Nil
      )
      val data = Seq(
        Row(Map[String, Any]("a" -> 1, "b" -> null), Map[String, Any]("c" -> 3, "d" -> 4)),
        Row(Map[String, Any]("a" -> 1, "b" -> 2), Map[String, Any]("c" -> 3, "d" -> 4))
      )
      spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
    }

    val expected3 = Seq(
      Row(Map[String, Any]("a" -> 1, "b" -> null, "c" -> 3, "d" -> 4)),
      Row(Map[String, Any]("a" -> 1, "b" -> 2, "c" -> 3, "d" -> 4))
    )

    checkAnswer(df3.selectExpr("map_concat(map1, map2)"), expected3)
    checkAnswer(df3.select(map_concat($"map1", $"map2")), expected3)

    val expectedMessage1 = "input to function map_concat should all be the same type"

    assert(intercept[AnalysisException] {
      df2.selectExpr("map_concat(map1, map2)").collect()
    }.getMessage().contains(expectedMessage1))

    assert(intercept[AnalysisException] {
      df2.select(map_concat($"map1", $"map2")).collect()
    }.getMessage().contains(expectedMessage1))

    val expectedMessage2 = "input to function map_concat should all be of type map"

    assert(intercept[AnalysisException] {
      df2.selectExpr("map_concat(map1, 12)").collect()
    }.getMessage().contains(expectedMessage2))

    assert(intercept[AnalysisException] {
      df2.select(map_concat($"map1", lit(12))).collect()
    }.getMessage().contains(expectedMessage2))
  }

  test("map_from_entries function") {
    // Test cases with primitive-type keys and values
    val idf = Seq(
      Seq((1, 10), (2, 20), (3, 10)),
      Seq((1, 10), null, (2, 20)),
      Seq.empty,
      null
    ).toDF("a")
    val iExpected = Seq(
      Row(Map(1 -> 10, 2 -> 20, 3 -> 10)),
      Row(null),
      Row(Map.empty),
      Row(null))

    def testPrimitiveType(): Unit = {
      checkAnswer(idf.select(map_from_entries($"a")), iExpected)
      checkAnswer(idf.selectExpr("map_from_entries(a)"), iExpected)
      checkAnswer(idf.selectExpr("map_from_entries(array(struct(1, null), struct(2, null)))"),
        Seq.fill(iExpected.length)(Row(Map(1 -> null, 2 -> null))))
    }

    // Test with local relation, the Project will be evaluated without codegen
    testPrimitiveType()
    // Test with cached relation, the Project will be evaluated with codegen
    idf.cache()
    testPrimitiveType()

    // Test cases with non-primitive-type keys and values
    val sdf = Seq(
      Seq(("a", "aa"), ("b", "bb"), ("c", "aa")),
      Seq(("a", "aa"), null, ("b", "bb")),
      Seq(("a", null), ("b", null)),
      Seq.empty,
      null
    ).toDF("a")
    val sExpected = Seq(
      Row(Map("a" -> "aa", "b" -> "bb", "c" -> "aa")),
      Row(null),
      Row(Map("a" -> null, "b" -> null)),
      Row(Map.empty),
      Row(null))

    def testNonPrimitiveType(): Unit = {
      checkAnswer(sdf.select(map_from_entries($"a")), sExpected)
      checkAnswer(sdf.selectExpr("map_from_entries(a)"), sExpected)
    }

    // Test with local relation, the Project will be evaluated without codegen
    testNonPrimitiveType()
    // Test with cached relation, the Project will be evaluated with codegen
    sdf.cache()
    testNonPrimitiveType()
  }

  test("array contains function") {
    val df = Seq(
      (Seq[Int](1, 2), "x", 1),
      (Seq[Int](), "x", 1)
    ).toDF("a", "b", "c")

    // Simple test cases
    checkAnswer(
      df.select(array_contains(df("a"), 1)),
      Seq(Row(true), Row(false))
    )
    checkAnswer(
      df.selectExpr("array_contains(a, 1)"),
      Seq(Row(true), Row(false))
    )
    checkAnswer(
      df.select(array_contains(df("a"), df("c"))),
      Seq(Row(true), Row(false))
    )
    checkAnswer(
      df.selectExpr("array_contains(a, c)"),
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

    checkAnswer(
      OneRowRelation().selectExpr("array_contains(array(1), 1.23D)"),
      Seq(Row(false))
    )

    checkAnswer(
      OneRowRelation().selectExpr("array_contains(array(1), 1.0D)"),
      Seq(Row(true))
    )

    checkAnswer(
      OneRowRelation().selectExpr("array_contains(array(1.0D), 1)"),
      Seq(Row(true))
    )

    checkAnswer(
      OneRowRelation().selectExpr("array_contains(array(1.23D), 1)"),
      Seq(Row(false))
    )

    checkAnswer(
      OneRowRelation().selectExpr("array_contains(array(array(1)), array(1.0D))"),
      Seq(Row(true))
    )

    checkAnswer(
      OneRowRelation().selectExpr("array_contains(array(array(1)), array(1.23D))"),
      Seq(Row(false))
    )

    if (!conf.ansiEnabled) {
      val e1 = intercept[AnalysisException] {
        OneRowRelation().selectExpr("array_contains(array(1), .01234567890123456790123456780)")
      }
      val errorMsg1 =
        s"""
           |Input to function array_contains should have been array followed by a
           |value with same element type, but it's [array<int>, decimal(38,29)].
       """.stripMargin.replace("\n", " ").trim()
      assert(e1.message.contains(errorMsg1))
    }

    val e2 = intercept[AnalysisException] {
      OneRowRelation().selectExpr("array_contains(array(1), 'foo')")
    }
    val errorMsg2 =
      s"""
         |Input to function array_contains should have been array followed by a
         |value with same element type, but it's [array<int>, string].
       """.stripMargin.replace("\n", " ").trim()
    assert(e2.message.contains(errorMsg2))
  }

  test("SPARK-29600: ArrayContains function may return incorrect result for DecimalType") {
    checkAnswer(
      sql("select array_contains(array(1.10), 1.1)"),
      Seq(Row(true))
    )

    checkAnswer(
      sql("SELECT array_contains(array(1.1), 1.10)"),
      Seq(Row(true))
    )

    checkAnswer(
      sql("SELECT array_contains(array(1.11), 1.1)"),
      Seq(Row(false))
    )
  }

  test("arrays_overlap function") {
    val df = Seq(
      (Seq[Option[Int]](Some(1), Some(2)), Seq[Option[Int]](Some(-1), Some(10))),
      (Seq[Option[Int]](Some(1), Some(2)), Seq[Option[Int]](Some(-1), None)),
      (Seq[Option[Int]](Some(3), Some(2)), Seq[Option[Int]](Some(1), Some(2)))
    ).toDF("a", "b")

    val answer = Seq(Row(false), Row(null), Row(true))

    checkAnswer(df.select(arrays_overlap(df("a"), df("b"))), answer)
    checkAnswer(df.selectExpr("arrays_overlap(a, b)"), answer)

    checkAnswer(
      Seq((Seq(1, 2, 3), Seq(2.0, 2.5))).toDF("a", "b").selectExpr("arrays_overlap(a, b)"),
      Row(true))

    intercept[AnalysisException] {
      sql("select arrays_overlap(array(1, 2, 3), array('a', 'b', 'c'))")
    }

    intercept[AnalysisException] {
      sql("select arrays_overlap(null, null)")
    }

    intercept[AnalysisException] {
      sql("select arrays_overlap(map(1, 2), map(3, 4))")
    }
  }

  test("slice function") {
    val df = Seq(
      Seq(1, 2, 3),
      Seq(4, 5)
    ).toDF("x")

    val answer = Seq(Row(Seq(2, 3)), Row(Seq(5)))

    checkAnswer(df.select(slice(df("x"), 2, 2)), answer)
    checkAnswer(df.select(slice(df("x"), lit(2), lit(2))), answer)
    checkAnswer(df.selectExpr("slice(x, 2, 2)"), answer)

    val answerNegative = Seq(Row(Seq(3)), Row(Seq(5)))
    checkAnswer(df.select(slice(df("x"), -1, 1)), answerNegative)
    checkAnswer(df.select(slice(df("x"), lit(-1), lit(1))), answerNegative)
    checkAnswer(df.selectExpr("slice(x, -1, 1)"), answerNegative)

    val answerStartExpr = Seq(Row(Seq(2)), Row(Seq(4)))
    checkAnswer(df.select(slice(df("x"), size($"x") - 1, lit(1))), answerStartExpr)
    checkAnswer(df.selectExpr("slice(x, size(x) - 1, 1)"), answerStartExpr)

    val answerLengthExpr = Seq(Row(Seq(1, 2)), Row(Seq(4)))
    checkAnswer(df.select(slice(df("x"), lit(1), size($"x") - 1)), answerLengthExpr)
    checkAnswer(df.selectExpr("slice(x, 1, size(x) - 1)"), answerLengthExpr)
  }

  test("array_join function") {
    val df = Seq(
      (Seq[String]("a", "b"), ","),
      (Seq[String]("a", null, "b"), ","),
      (Seq.empty[String], ",")
    ).toDF("x", "delimiter")

    checkAnswer(
      df.select(array_join(df("x"), ";")),
      Seq(Row("a;b"), Row("a;b"), Row(""))
    )
    checkAnswer(
      df.select(array_join(df("x"), ";", "NULL")),
      Seq(Row("a;b"), Row("a;NULL;b"), Row(""))
    )
    checkAnswer(
      df.selectExpr("array_join(x, delimiter)"),
      Seq(Row("a,b"), Row("a,b"), Row("")))
    checkAnswer(
      df.selectExpr("array_join(x, delimiter, 'NULL')"),
      Seq(Row("a,b"), Row("a,NULL,b"), Row("")))

    val idf = Seq(Seq(1, 2, 3)).toDF("x")

    checkAnswer(
      idf.select(array_join(idf("x"), ", ")),
      Seq(Row("1, 2, 3"))
    )
    checkAnswer(
      idf.selectExpr("array_join(x, ', ')"),
      Seq(Row("1, 2, 3"))
    )
    intercept[AnalysisException] {
      idf.selectExpr("array_join(x, 1)")
    }
    intercept[AnalysisException] {
      idf.selectExpr("array_join(x, ', ', 1)")
    }
  }

  test("array_min function") {
    val df = Seq(
      Seq[Option[Int]](Some(1), Some(3), Some(2)),
      Seq.empty[Option[Int]],
      Seq[Option[Int]](None),
      Seq[Option[Int]](None, Some(1), Some(-100))
    ).toDF("a")

    val answer = Seq(Row(1), Row(null), Row(null), Row(-100))

    checkAnswer(df.select(array_min(df("a"))), answer)
    checkAnswer(df.selectExpr("array_min(a)"), answer)
  }

  test("array_max function") {
    val df = Seq(
      Seq[Option[Int]](Some(1), Some(3), Some(2)),
      Seq.empty[Option[Int]],
      Seq[Option[Int]](None),
      Seq[Option[Int]](None, Some(1), Some(-100))
    ).toDF("a")

    val answer = Seq(Row(3), Row(null), Row(null), Row(1))

    checkAnswer(df.select(array_max(df("a"))), answer)
    checkAnswer(df.selectExpr("array_max(a)"), answer)
  }

  test("sequence") {
    checkAnswer(Seq((-2, 2)).toDF().select(sequence($"_1", $"_2")),
      Seq(Row(Array(-2, -1, 0, 1, 2))))
    checkAnswer(Seq((7, 2, -2)).toDF().select(sequence($"_1", $"_2", $"_3")),
      Seq(Row(Array(7, 5, 3))))

    checkAnswer(
      spark.sql("select sequence(" +
        "   cast('2018-01-01 00:00:00' as timestamp)" +
        ",  cast('2018-01-02 00:00:00' as timestamp)" +
        ",  interval 12 hours)"),
      Seq(Row(Array(
        Timestamp.valueOf("2018-01-01 00:00:00"),
        Timestamp.valueOf("2018-01-01 12:00:00"),
        Timestamp.valueOf("2018-01-02 00:00:00")))))

    withDefaultTimeZone(UTC) {
      checkAnswer(
        spark.sql("select sequence(" +
          "   cast('2018-01-01' as date)" +
          ",  cast('2018-03-01' as date)" +
          ",  interval 1 month)"),
        Seq(Row(Array(
          Date.valueOf("2018-01-01"),
          Date.valueOf("2018-02-01"),
          Date.valueOf("2018-03-01")))))
    }

    // test type coercion
    checkAnswer(
      Seq((1.toByte, 3L, 1)).toDF().select(sequence($"_1", $"_2", $"_3")),
      Seq(Row(Array(1L, 2L, 3L))))

    checkAnswer(
      spark.sql("select sequence(" +
        "   cast('2018-01-01' as date)" +
        ",  cast('2018-01-02 00:00:00' as timestamp)" +
        ",  interval 12 hours)"),
      Seq(Row(Array(
        Timestamp.valueOf("2018-01-01 00:00:00"),
        Timestamp.valueOf("2018-01-01 12:00:00"),
        Timestamp.valueOf("2018-01-02 00:00:00")))))

    // test invalid data types
    intercept[AnalysisException] {
      Seq((true, false)).toDF().selectExpr("sequence(_1, _2)")
    }
    intercept[AnalysisException] {
      Seq((true, false, 42)).toDF().selectExpr("sequence(_1, _2, _3)")
    }
    intercept[AnalysisException] {
      Seq((1, 2, 0.5)).toDF().selectExpr("sequence(_1, _2, _3)")
    }
  }

  test("reverse function - string") {
    val oneRowDF = Seq(("Spark", 3215)).toDF("s", "i")
    def testString(): Unit = {
      checkAnswer(oneRowDF.select(reverse($"s")), Seq(Row("krapS")))
      checkAnswer(oneRowDF.selectExpr("reverse(s)"), Seq(Row("krapS")))
      checkAnswer(oneRowDF.select(reverse($"i")), Seq(Row("5123")))
      checkAnswer(oneRowDF.selectExpr("reverse(i)"), Seq(Row("5123")))
      checkAnswer(oneRowDF.selectExpr("reverse(null)"), Seq(Row(null)))
    }

    // Test with local relation, the Project will be evaluated without codegen
    testString()
    // Test with cached relation, the Project will be evaluated with codegen
    oneRowDF.cache()
    testString()
  }

  test("reverse function - array for primitive type not containing null") {
    val idfNotContainsNull = Seq(
      Seq(1, 9, 8, 7),
      Seq(5, 8, 9, 7, 2),
      Seq.empty,
      null
    ).toDF("i")

    def testArrayOfPrimitiveTypeNotContainsNull(): Unit = {
      checkAnswer(
        idfNotContainsNull.select(reverse($"i")),
        Seq(Row(Seq(7, 8, 9, 1)), Row(Seq(2, 7, 9, 8, 5)), Row(Seq.empty), Row(null))
      )
      checkAnswer(
        idfNotContainsNull.selectExpr("reverse(i)"),
        Seq(Row(Seq(7, 8, 9, 1)), Row(Seq(2, 7, 9, 8, 5)), Row(Seq.empty), Row(null))
      )
    }

    // Test with local relation, the Project will be evaluated without codegen
    testArrayOfPrimitiveTypeNotContainsNull()
    // Test with cached relation, the Project will be evaluated with codegen
    idfNotContainsNull.cache()
    testArrayOfPrimitiveTypeNotContainsNull()
  }

  test("reverse function - array for primitive type containing null") {
    val idfContainsNull = Seq[Seq[Integer]](
      Seq(1, 9, 8, null, 7),
      Seq(null, 5, 8, 9, 7, 2),
      Seq.empty,
      null
    ).toDF("i")

    def testArrayOfPrimitiveTypeContainsNull(): Unit = {
      checkAnswer(
        idfContainsNull.select(reverse($"i")),
        Seq(Row(Seq(7, null, 8, 9, 1)), Row(Seq(2, 7, 9, 8, 5, null)), Row(Seq.empty), Row(null))
      )
      checkAnswer(
        idfContainsNull.selectExpr("reverse(i)"),
        Seq(Row(Seq(7, null, 8, 9, 1)), Row(Seq(2, 7, 9, 8, 5, null)), Row(Seq.empty), Row(null))
      )
    }

    // Test with local relation, the Project will be evaluated without codegen
    testArrayOfPrimitiveTypeContainsNull()
    // Test with cached relation, the Project will be evaluated with codegen
    idfContainsNull.cache()
    testArrayOfPrimitiveTypeContainsNull()
  }

  test("reverse function - array for non-primitive type") {
    val sdf = Seq(
      Seq("c", "a", "b"),
      Seq("b", null, "c", null),
      Seq.empty,
      null
    ).toDF("s")

    def testArrayOfNonPrimitiveType(): Unit = {
      checkAnswer(
        sdf.select(reverse($"s")),
        Seq(Row(Seq("b", "a", "c")), Row(Seq(null, "c", null, "b")), Row(Seq.empty), Row(null))
      )
      checkAnswer(
        sdf.selectExpr("reverse(s)"),
        Seq(Row(Seq("b", "a", "c")), Row(Seq(null, "c", null, "b")), Row(Seq.empty), Row(null))
      )
      checkAnswer(
        sdf.selectExpr("reverse(array(array(1, 2), array(3, 4)))"),
        Seq.fill(sdf.count().toInt)(Row(Seq(Seq(3, 4), Seq(1, 2))))
      )
    }

    // Test with local relation, the Project will be evaluated without codegen
    testArrayOfNonPrimitiveType()
    // Test with cached relation, the Project will be evaluated with codegen
    sdf.cache()
    testArrayOfNonPrimitiveType()
  }

  test("reverse function - data type mismatch") {
    val ex1 = intercept[AnalysisException] {
      sql("select reverse(struct(1, 'a'))")
    }
    assert(ex1.getMessage.contains("data type mismatch"))

    val ex2 = intercept[AnalysisException] {
      sql("select reverse(map(1, 'a'))")
    }
    assert(ex2.getMessage.contains("data type mismatch"))
  }

  test("array position function") {
    val df = Seq(
      (Seq[Int](1, 2), "x", 1),
      (Seq[Int](), "x", 1)
    ).toDF("a", "b", "c")

    checkAnswer(
      df.select(array_position(df("a"), 1)),
      Seq(Row(1L), Row(0L))
    )
    checkAnswer(
      df.selectExpr("array_position(a, 1)"),
      Seq(Row(1L), Row(0L))
    )
    checkAnswer(
      df.selectExpr("array_position(a, c)"),
      Seq(Row(1L), Row(0L))
    )
    checkAnswer(
      df.select(array_position(df("a"), df("c"))),
      Seq(Row(1L), Row(0L))
    )
    checkAnswer(
      df.select(array_position(df("a"), null)),
      Seq(Row(null), Row(null))
    )
    checkAnswer(
      df.selectExpr("array_position(a, null)"),
      Seq(Row(null), Row(null))
    )

    checkAnswer(
      OneRowRelation().selectExpr("array_position(array(1), 1.23D)"),
      Seq(Row(0L))
    )

    checkAnswer(
      OneRowRelation().selectExpr("array_position(array(1), 1.0D)"),
      Seq(Row(1L))
    )

    checkAnswer(
      OneRowRelation().selectExpr("array_position(array(1.D), 1)"),
      Seq(Row(1L))
    )

    checkAnswer(
      OneRowRelation().selectExpr("array_position(array(1.23D), 1)"),
      Seq(Row(0L))
    )

    checkAnswer(
      OneRowRelation().selectExpr("array_position(array(array(1)), array(1.0D))"),
      Seq(Row(1L))
    )

    checkAnswer(
      OneRowRelation().selectExpr("array_position(array(array(1)), array(1.23D))"),
      Seq(Row(0L))
    )

    checkAnswer(
      OneRowRelation().selectExpr("array_position(array(array(1), null)[0], 1)"),
      Seq(Row(1L))
    )
    checkAnswer(
      OneRowRelation().selectExpr("array_position(array(1, null), array(1, null)[0])"),
      Seq(Row(1L))
    )

    val e1 = intercept[AnalysisException] {
      Seq(("a string element", "a")).toDF().selectExpr("array_position(_1, _2)")
    }
    val errorMsg1 =
      s"""
         |Input to function array_position should have been array followed by a
         |value with same element type, but it's [string, string].
       """.stripMargin.replace("\n", " ").trim()
    assert(e1.message.contains(errorMsg1))

    val e2 = intercept[AnalysisException] {
      OneRowRelation().selectExpr("array_position(array(1), '1')")
    }
    val errorMsg2 =
      s"""
         |Input to function array_position should have been array followed by a
         |value with same element type, but it's [array<int>, string].
       """.stripMargin.replace("\n", " ").trim()
    assert(e2.message.contains(errorMsg2))
  }

  test("element_at function") {
    val df = Seq(
      (Seq[String]("1", "2", "3"), 1),
      (Seq[String](null, ""), -1),
      (Seq[String](), 2)
    ).toDF("a", "b")

    intercept[Exception] {
      checkAnswer(
        df.select(element_at(df("a"), 0)),
        Seq(Row(null), Row(null), Row(null))
      )
    }.getMessage.contains("SQL array indices start at 1")
    intercept[Exception] {
      checkAnswer(
        df.select(element_at(df("a"), 1.1)),
        Seq(Row(null), Row(null), Row(null))
      )
    }
    if (!conf.ansiEnabled) {
      checkAnswer(
        df.select(element_at(df("a"), 4)),
        Seq(Row(null), Row(null), Row(null))
      )
      checkAnswer(
        df.select(element_at(df("a"), df("b"))),
        Seq(Row("1"), Row(""), Row(null))
      )
      checkAnswer(
        df.selectExpr("element_at(a, b)"),
        Seq(Row("1"), Row(""), Row(null))
      )

      checkAnswer(
        df.select(element_at(df("a"), 1)),
        Seq(Row("1"), Row(null), Row(null))
      )
      checkAnswer(
        df.select(element_at(df("a"), -1)),
        Seq(Row("3"), Row(""), Row(null))
      )

      checkAnswer(
        df.selectExpr("element_at(a, 4)"),
        Seq(Row(null), Row(null), Row(null))
      )

      checkAnswer(
        df.selectExpr("element_at(a, 1)"),
        Seq(Row("1"), Row(null), Row(null))
      )
      checkAnswer(
        df.selectExpr("element_at(a, -1)"),
        Seq(Row("3"), Row(""), Row(null))
      )
    }

    val e1 = intercept[AnalysisException] {
      Seq(("a string element", 1)).toDF().selectExpr("element_at(_1, _2)")
    }
    val errorMsg1 =
      s"""
         |The first argument to function element_at should have been array or map type, but
         |its string type.
       """.stripMargin.replace("\n", " ").trim()
    assert(e1.message.contains(errorMsg1))

    checkAnswer(
      OneRowRelation().selectExpr("element_at(array(2, 1), 2S)"),
      Seq(Row(1))
    )

    checkAnswer(
      OneRowRelation().selectExpr("element_at(array('a', 'b'), 1Y)"),
      Seq(Row("a"))
    )

    checkAnswer(
      OneRowRelation().selectExpr("element_at(array(1, 2, 3), 3)"),
      Seq(Row(3))
    )

    val e2 = intercept[AnalysisException] {
      OneRowRelation().selectExpr("element_at(array('a', 'b'), 1L)")
    }
    val errorMsg2 =
      s"""
         |Input to function element_at should have been array followed by a int, but it's
         |[array<string>, bigint].
       """.stripMargin.replace("\n", " ").trim()
    assert(e2.message.contains(errorMsg2))

    checkAnswer(
      OneRowRelation().selectExpr("element_at(map(1, 'a', 2, 'b'), 2Y)"),
      Seq(Row("b"))
    )

    checkAnswer(
      OneRowRelation().selectExpr("element_at(map(1, 'a', 2, 'b'), 1S)"),
      Seq(Row("a"))
    )

    checkAnswer(
      OneRowRelation().selectExpr("element_at(map(1, 'a', 2, 'b'), 2)"),
      Seq(Row("b"))
    )

    checkAnswer(
      OneRowRelation().selectExpr("element_at(map(1, 'a', 2, 'b'), 2L)"),
      Seq(Row("b"))
    )

    checkAnswer(
      OneRowRelation().selectExpr("element_at(map(1, 'a', 2, 'b'), 1.0D)"),
      Seq(Row("a"))
    )

    if (!conf.ansiEnabled) {
      checkAnswer(
        OneRowRelation().selectExpr("element_at(map(1, 'a', 2, 'b'), 1.23D)"),
        Seq(Row(null))
      )
    }

    val e3 = intercept[AnalysisException] {
      OneRowRelation().selectExpr("element_at(map(1, 'a', 2, 'b'), '1')")
    }
    val errorMsg3 =
      s"""
         |Input to function element_at should have been map followed by a value of same
         |key type, but it's [map<int,string>, string].
       """.stripMargin.replace("\n", " ").trim()
    assert(e3.message.contains(errorMsg3))
  }

  test("array_union functions") {
    val df1 = Seq((Array(1, 2, 3), Array(4, 2))).toDF("a", "b")
    val ans1 = Row(Seq(1, 2, 3, 4))
    checkAnswer(df1.select(array_union($"a", $"b")), ans1)
    checkAnswer(df1.selectExpr("array_union(a, b)"), ans1)

    val df2 = Seq((Array[Integer](1, 2, null, 4, 5), Array(-5, 4, -3, 2, -1))).toDF("a", "b")
    val ans2 = Row(Seq(1, 2, null, 4, 5, -5, -3, -1))
    checkAnswer(df2.select(array_union($"a", $"b")), ans2)
    checkAnswer(df2.selectExpr("array_union(a, b)"), ans2)

    val df3 = Seq((Array(1L, 2L, 3L), Array(4L, 2L))).toDF("a", "b")
    val ans3 = Row(Seq(1L, 2L, 3L, 4L))
    checkAnswer(df3.select(array_union($"a", $"b")), ans3)
    checkAnswer(df3.selectExpr("array_union(a, b)"), ans3)

    val df4 = Seq((Array[java.lang.Long](1L, 2L, null, 4L, 5L), Array(-5L, 4L, -3L, 2L, -1L)))
      .toDF("a", "b")
    val ans4 = Row(Seq(1L, 2L, null, 4L, 5L, -5L, -3L, -1L))
    checkAnswer(df4.select(array_union($"a", $"b")), ans4)
    checkAnswer(df4.selectExpr("array_union(a, b)"), ans4)

    val df5 = Seq((Array("b", "a", "c"), Array("b", null, "a", "g"))).toDF("a", "b")
    val ans5 = Row(Seq("b", "a", "c", null, "g"))
    checkAnswer(df5.select(array_union($"a", $"b")), ans5)
    checkAnswer(df5.selectExpr("array_union(a, b)"), ans5)

    val df6 = Seq((null, Array("a"))).toDF("a", "b")
    assert(intercept[AnalysisException] {
      df6.select(array_union($"a", $"b"))
    }.getMessage.contains("data type mismatch"))
    assert(intercept[AnalysisException] {
      df6.selectExpr("array_union(a, b)")
    }.getMessage.contains("data type mismatch"))

    val df7 = Seq((null, null)).toDF("a", "b")
    assert(intercept[AnalysisException] {
      df7.select(array_union($"a", $"b"))
    }.getMessage.contains("data type mismatch"))
    assert(intercept[AnalysisException] {
      df7.selectExpr("array_union(a, b)")
    }.getMessage.contains("data type mismatch"))

    val df8 = Seq((Array(Array(1)), Array("a"))).toDF("a", "b")
    assert(intercept[AnalysisException] {
      df8.select(array_union($"a", $"b"))
    }.getMessage.contains("data type mismatch"))
    assert(intercept[AnalysisException] {
      df8.selectExpr("array_union(a, b)")
    }.getMessage.contains("data type mismatch"))
  }

  test("concat function - arrays") {
    val nseqi : Seq[Int] = null
    val nseqs : Seq[String] = null
    val df = Seq(
      (Seq(1), Seq(2, 3), Seq(5L, 6L), nseqi, Seq("a", "b", "c"), Seq("d", "e"), Seq("f"), nseqs),
      (Seq(1, 0), Seq.empty[Int], Seq(2L), nseqi, Seq("a"), Seq.empty[String], Seq(null), nseqs)
    ).toDF("i1", "i2", "i3", "in", "s1", "s2", "s3", "sn")

    // Simple test cases
    def simpleTest(): Unit = {
      if (!conf.ansiEnabled) {
        checkAnswer(
          df.select(concat($"i1", $"s1")),
          Seq(Row(Seq("1", "a", "b", "c")), Row(Seq("1", "0", "a")))
        )
      }
      checkAnswer(
        df.select(concat($"i1", $"i2", $"i3")),
        Seq(Row(Seq(1, 2, 3, 5, 6)), Row(Seq(1, 0, 2)))
      )
      checkAnswer(
        df.selectExpr("concat(array(1, null), i2, i3)"),
        Seq(Row(Seq(1, null, 2, 3, 5, 6)), Row(Seq(1, null, 2)))
      )
      checkAnswer(
        df.select(concat($"s1", $"s2", $"s3")),
        Seq(Row(Seq("a", "b", "c", "d", "e", "f")), Row(Seq("a", null)))
      )
      checkAnswer(
        df.selectExpr("concat(s1, s2, s3)"),
        Seq(Row(Seq("a", "b", "c", "d", "e", "f")), Row(Seq("a", null)))
      )
    }

    // Test with local relation, the Project will be evaluated without codegen
    simpleTest()
    // Test with cached relation, the Project will be evaluated with codegen
    df.cache()
    simpleTest()

    // Null test cases
    def nullTest(): Unit = {
      checkAnswer(
        df.select(concat($"i1", $"in")),
        Seq(Row(null), Row(null))
      )
      checkAnswer(
        df.select(concat($"in", $"i1")),
        Seq(Row(null), Row(null))
      )
      checkAnswer(
        df.select(concat($"s1", $"sn")),
        Seq(Row(null), Row(null))
      )
      checkAnswer(
        df.select(concat($"sn", $"s1")),
        Seq(Row(null), Row(null))
      )
    }

    // Test with local relation, the Project will be evaluated without codegen
    df.unpersist(blocking = true)
    nullTest()
    // Test with cached relation, the Project will be evaluated with codegen
    df.cache()
    nullTest()

    // Type error test cases
    intercept[AnalysisException] {
      df.selectExpr("concat(i1, i2, null)")
    }

    intercept[AnalysisException] {
      df.selectExpr("concat(i1, array(i1, i2))")
    }

    val e = intercept[AnalysisException] {
      df.selectExpr("concat(map(1, 2), map(3, 4))")
    }
    assert(e.getMessage.contains("string, binary or array"))
  }

  test("SPARK-31227: Non-nullable null type should not coerce to nullable type in concat") {
    val actual = spark.range(1).selectExpr("concat(array(), array(1)) as arr")
    val expected = spark.range(1).selectExpr("array(1) as arr")
    checkAnswer(actual, expected)
    assert(actual.schema === expected.schema)
  }

  test("flatten function") {
    // Test cases with a primitive type
    val intDF = Seq(
      (Seq(Seq(1, 2, 3), Seq(4, 5), Seq(6))),
      (Seq(Seq(1, 2))),
      (Seq(Seq(1), Seq.empty)),
      (Seq(Seq.empty, Seq(1))),
      (Seq(Seq.empty, Seq.empty)),
      (Seq(Seq(1), null)),
      (Seq(null, Seq(1))),
      (Seq(null, null))
    ).toDF("i")

    val intDFResult = Seq(
      Row(Seq(1, 2, 3, 4, 5, 6)),
      Row(Seq(1, 2)),
      Row(Seq(1)),
      Row(Seq(1)),
      Row(Seq.empty),
      Row(null),
      Row(null),
      Row(null))

    def testInt(): Unit = {
      checkAnswer(intDF.select(flatten($"i")), intDFResult)
      checkAnswer(intDF.selectExpr("flatten(i)"), intDFResult)
    }

    // Test with local relation, the Project will be evaluated without codegen
    testInt()
    // Test with cached relation, the Project will be evaluated with codegen
    intDF.cache()
    testInt()

    // Test cases with non-primitive types
    val strDF = Seq(
      (Seq(Seq("a", "b"), Seq("c"), Seq("d", "e", "f"))),
      (Seq(Seq("a", "b"))),
      (Seq(Seq("a", null), Seq(null, "b"), Seq(null, null))),
      (Seq(Seq("a"), Seq.empty)),
      (Seq(Seq.empty, Seq("a"))),
      (Seq(Seq.empty, Seq.empty)),
      (Seq(Seq("a"), null)),
      (Seq(null, Seq("a"))),
      (Seq(null, null))
    ).toDF("s")

    val strDFResult = Seq(
      Row(Seq("a", "b", "c", "d", "e", "f")),
      Row(Seq("a", "b")),
      Row(Seq("a", null, null, "b", null, null)),
      Row(Seq("a")),
      Row(Seq("a")),
      Row(Seq.empty),
      Row(null),
      Row(null),
      Row(null))

    def testString(): Unit = {
      checkAnswer(strDF.select(flatten($"s")), strDFResult)
      checkAnswer(strDF.selectExpr("flatten(s)"), strDFResult)
    }

    // Test with local relation, the Project will be evaluated without codegen
    testString()
    // Test with cached relation, the Project will be evaluated with codegen
    strDF.cache()
    testString()

    val arrDF = Seq((1, "a", Seq(1, 2, 3))).toDF("i", "s", "arr")

    def testArray(): Unit = {
      checkAnswer(
        arrDF.selectExpr("flatten(array(arr, array(null, 5), array(6, null)))"),
        Seq(Row(Seq(1, 2, 3, null, 5, 6, null))))
      checkAnswer(
        arrDF.selectExpr("flatten(array(array(arr, arr), array(arr)))"),
        Seq(Row(Seq(Seq(1, 2, 3), Seq(1, 2, 3), Seq(1, 2, 3)))))
    }

    // Test with local relation, the Project will be evaluated without codegen
    testArray()
    // Test with cached relation, the Project will be evaluated with codegen
    arrDF.cache()
    testArray()

    // Error test cases
    val oneRowDF = Seq((1, "a", Seq(1, 2, 3))).toDF("i", "s", "arr")
    intercept[AnalysisException] {
      oneRowDF.select(flatten($"arr"))
    }
    intercept[AnalysisException] {
      oneRowDF.select(flatten($"i"))
    }
    intercept[AnalysisException] {
      oneRowDF.select(flatten($"s"))
    }
    intercept[AnalysisException] {
      oneRowDF.selectExpr("flatten(null)")
    }
  }

  test("array_repeat function") {
    val strDF = Seq(
      ("hi", 2),
      (null, 2)
    ).toDF("a", "b")

    val strDFTwiceResult = Seq(
      Row(Seq("hi", "hi")),
      Row(Seq(null, null))
    )

    def testString(): Unit = {
      checkAnswer(strDF.select(array_repeat($"a", 2)), strDFTwiceResult)
      checkAnswer(strDF.select(array_repeat($"a", $"b")), strDFTwiceResult)
      checkAnswer(strDF.selectExpr("array_repeat(a, 2)"), strDFTwiceResult)
      checkAnswer(strDF.selectExpr("array_repeat(a, b)"), strDFTwiceResult)
    }

    // Test with local relation, the Project will be evaluated without codegen
    testString()
    // Test with cached relation, the Project will be evaluated with codegen
    strDF.cache()
    testString()

    val intDF = {
      val schema = StructType(Seq(
        StructField("a", IntegerType),
        StructField("b", IntegerType)))
      val data = Seq(
        Row(3, 2),
        Row(null, 2)
      )
      spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
    }

    val intDFTwiceResult = Seq(
      Row(Seq(3, 3)),
      Row(Seq(null, null))
    )

    def testInt(): Unit = {
      checkAnswer(intDF.select(array_repeat($"a", 2)), intDFTwiceResult)
      checkAnswer(intDF.select(array_repeat($"a", $"b")), intDFTwiceResult)
      checkAnswer(intDF.selectExpr("array_repeat(a, 2)"), intDFTwiceResult)
      checkAnswer(intDF.selectExpr("array_repeat(a, b)"), intDFTwiceResult)
    }

    // Test with local relation, the Project will be evaluated without codegen
    testInt()
    // Test with cached relation, the Project will be evaluated with codegen
    intDF.cache()
    testInt()

    val nullCountDF = {
      val schema = StructType(Seq(
        StructField("a", StringType),
        StructField("b", IntegerType)))
      val data = Seq(
        Row("hi", null),
        Row(null, null)
      )
      spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
    }

    def testNull(): Unit = {
      checkAnswer(
        nullCountDF.select(array_repeat($"a", $"b")),
        Seq(Row(null), Row(null))
      )
    }

    // Test with local relation, the Project will be evaluated without codegen
    testNull()
    // Test with cached relation, the Project will be evaluated with codegen
    nullCountDF.cache()
    testNull()

    // Error test cases
    val invalidTypeDF = Seq(("hi", "1")).toDF("a", "b")

    intercept[AnalysisException] {
      invalidTypeDF.select(array_repeat($"a", $"b"))
    }
    intercept[AnalysisException] {
      invalidTypeDF.select(array_repeat($"a", lit("1")))
    }
    intercept[AnalysisException] {
      invalidTypeDF.selectExpr("array_repeat(a, 1.0)")
    }

  }

  test("array remove") {
    val df = Seq(
      (Array[Int](2, 1, 2, 3), Array("a", "b", "c", "a"), Array("", ""), 2),
      (Array.empty[Int], Array.empty[String], Array.empty[String], 2),
      (null, null, null, 2)
    ).toDF("a", "b", "c", "d")
    checkAnswer(
      df.select(array_remove($"a", 2), array_remove($"b", "a"), array_remove($"c", "")),
      Seq(
        Row(Seq(1, 3), Seq("b", "c"), Seq.empty[String]),
        Row(Seq.empty[Int], Seq.empty[String], Seq.empty[String]),
        Row(null, null, null))
    )

    checkAnswer(
      df.select(array_remove($"a", $"d")),
      Seq(
        Row(Seq(1, 3)),
        Row(Seq.empty[Int]),
        Row(null))
    )

    checkAnswer(
      df.selectExpr("array_remove(a, d)"),
      Seq(
        Row(Seq(1, 3)),
        Row(Seq.empty[Int]),
        Row(null))
    )

    checkAnswer(
      OneRowRelation().selectExpr("array_remove(array(1, 2), 1.23D)"),
      Seq(
        Row(Seq(1.0, 2.0))
      )
    )

    checkAnswer(
      OneRowRelation().selectExpr("array_remove(array(1, 2), 1.0D)"),
      Seq(
        Row(Seq(2.0))
      )
    )

    checkAnswer(
      OneRowRelation().selectExpr("array_remove(array(1.0D, 2.0D), 2)"),
      Seq(
        Row(Seq(1.0))
      )
    )

    checkAnswer(
      OneRowRelation().selectExpr("array_remove(array(1.1D, 1.2D), 1)"),
      Seq(
        Row(Seq(1.1, 1.2))
      )
    )

    checkAnswer(
      df.selectExpr("array_remove(a, 2)", "array_remove(b, \"a\")",
        "array_remove(c, \"\")"),
      Seq(
        Row(Seq(1, 3), Seq("b", "c"), Seq.empty[String]),
        Row(Seq.empty[Int], Seq.empty[String], Seq.empty[String]),
        Row(null, null, null))
    )

    val e1 = intercept[AnalysisException] {
      Seq(("a string element", "a")).toDF().selectExpr("array_remove(_1, _2)")
    }
    val errorMsg1 =
      s"""
         |Input to function array_remove should have been array followed by a
         |value with same element type, but it's [string, string].
       """.stripMargin.replace("\n", " ").trim()
    assert(e1.message.contains(errorMsg1))

    val e2 = intercept[AnalysisException] {
      OneRowRelation().selectExpr("array_remove(array(1, 2), '1')")
    }

    val errorMsg2 =
      s"""
         |Input to function array_remove should have been array followed by a
         |value with same element type, but it's [array<int>, string].
       """.stripMargin.replace("\n", " ").trim()
    assert(e2.message.contains(errorMsg2))
  }

  test("array_distinct functions") {
    val df = Seq(
      (Array[Int](2, 1, 3, 4, 3, 5), Array("b", "c", "a", "c", "b", "", "")),
      (Array.empty[Int], Array.empty[String]),
      (null, null)
    ).toDF("a", "b")
    checkAnswer(
      df.select(array_distinct($"a"), array_distinct($"b")),
      Seq(
        Row(Seq(2, 1, 3, 4, 5), Seq("b", "c", "a", "")),
        Row(Seq.empty[Int], Seq.empty[String]),
        Row(null, null))
    )
    checkAnswer(
      df.selectExpr("array_distinct(a)", "array_distinct(b)"),
      Seq(
        Row(Seq(2, 1, 3, 4, 5), Seq("b", "c", "a", "")),
        Row(Seq.empty[Int], Seq.empty[String]),
        Row(null, null))
    )
  }

  // Shuffle expressions should produce same results at retries in the same DataFrame.
  private def checkShuffleResult(df: DataFrame): Unit = {
    checkAnswer(df, df.collect())
  }

  test("shuffle function - array for primitive type not containing null") {
    val idfNotContainsNull = Seq(
      Seq(1, 9, 8, 7),
      Seq(5, 8, 9, 7, 2),
      Seq.empty,
      null
    ).toDF("i")

    def testArrayOfPrimitiveTypeNotContainsNull(): Unit = {
      checkShuffleResult(idfNotContainsNull.select(shuffle($"i")))
      checkShuffleResult(idfNotContainsNull.selectExpr("shuffle(i)"))
    }

    // Test with local relation, the Project will be evaluated without codegen
    testArrayOfPrimitiveTypeNotContainsNull()
    // Test with cached relation, the Project will be evaluated with codegen
    idfNotContainsNull.cache()
    testArrayOfPrimitiveTypeNotContainsNull()
  }

  test("shuffle function - array for primitive type containing null") {
    val idfContainsNull = Seq[Seq[Integer]](
      Seq(1, 9, 8, null, 7),
      Seq(null, 5, 8, 9, 7, 2),
      Seq.empty,
      null
    ).toDF("i")

    def testArrayOfPrimitiveTypeContainsNull(): Unit = {
      checkShuffleResult(idfContainsNull.select(shuffle($"i")))
      checkShuffleResult(idfContainsNull.selectExpr("shuffle(i)"))
    }

    // Test with local relation, the Project will be evaluated without codegen
    testArrayOfPrimitiveTypeContainsNull()
    // Test with cached relation, the Project will be evaluated with codegen
    idfContainsNull.cache()
    testArrayOfPrimitiveTypeContainsNull()
  }

  test("shuffle function - array for non-primitive type") {
    val sdf = Seq(
      Seq("c", "a", "b"),
      Seq("b", null, "c", null),
      Seq.empty,
      null
    ).toDF("s")

    def testNonPrimitiveType(): Unit = {
      checkShuffleResult(sdf.select(shuffle($"s")))
      checkShuffleResult(sdf.selectExpr("shuffle(s)"))
    }

    // Test with local relation, the Project will be evaluated without codegen
    testNonPrimitiveType()
    // Test with cached relation, the Project will be evaluated with codegen
    sdf.cache()
    testNonPrimitiveType()
  }

  test("array_except functions") {
    val df1 = Seq((Array(1, 2, 4), Array(4, 2))).toDF("a", "b")
    val ans1 = Row(Seq(1))
    checkAnswer(df1.select(array_except($"a", $"b")), ans1)
    checkAnswer(df1.selectExpr("array_except(a, b)"), ans1)

    val df2 = Seq((Array[Integer](1, 2, null, 4, 5), Array[Integer](-5, 4, null, 2, -1)))
      .toDF("a", "b")
    val ans2 = Row(Seq(1, 5))
    checkAnswer(df2.select(array_except($"a", $"b")), ans2)
    checkAnswer(df2.selectExpr("array_except(a, b)"), ans2)

    val df3 = Seq((Array(1L, 2L, 4L), Array(4L, 2L))).toDF("a", "b")
    val ans3 = Row(Seq(1L))
    checkAnswer(df3.select(array_except($"a", $"b")), ans3)
    checkAnswer(df3.selectExpr("array_except(a, b)"), ans3)

    val df4 = Seq(
      (Array[java.lang.Long](1L, 2L, null, 4L, 5L), Array[java.lang.Long](-5L, 4L, null, 2L, -1L)))
      .toDF("a", "b")
    val ans4 = Row(Seq(1L, 5L))
    checkAnswer(df4.select(array_except($"a", $"b")), ans4)
    checkAnswer(df4.selectExpr("array_except(a, b)"), ans4)

    val df5 = Seq((Array("c", null, "a", "f"), Array("b", null, "a", "g"))).toDF("a", "b")
    val ans5 = Row(Seq("c", "f"))
    checkAnswer(df5.select(array_except($"a", $"b")), ans5)
    checkAnswer(df5.selectExpr("array_except(a, b)"), ans5)

    val df6 = Seq((null, null)).toDF("a", "b")
    intercept[AnalysisException] {
      df6.select(array_except($"a", $"b"))
    }
    intercept[AnalysisException] {
      df6.selectExpr("array_except(a, b)")
    }
    val df7 = Seq((Array(1), Array("a"))).toDF("a", "b")
    intercept[AnalysisException] {
      df7.select(array_except($"a", $"b"))
    }
    intercept[AnalysisException] {
      df7.selectExpr("array_except(a, b)")
    }
    val df8 = Seq((Array("a"), null)).toDF("a", "b")
    intercept[AnalysisException] {
      df8.select(array_except($"a", $"b"))
    }
    intercept[AnalysisException] {
      df8.selectExpr("array_except(a, b)")
    }
    val df9 = Seq((null, Array("a"))).toDF("a", "b")
    intercept[AnalysisException] {
      df9.select(array_except($"a", $"b"))
    }
    intercept[AnalysisException] {
      df9.selectExpr("array_except(a, b)")
    }

    val df10 = Seq(
      (Array[Integer](1, 2), Array[Integer](2)),
      (Array[Integer](1, 2), Array[Integer](1, null)),
      (Array[Integer](1, null, 3), Array[Integer](1, 2)),
      (Array[Integer](1, null), Array[Integer](2, null))
    ).toDF("a", "b")
    val result10 = df10.select(array_except($"a", $"b"))
    val expectedType10 = ArrayType(IntegerType, containsNull = true)
    assert(result10.first.schema(0).dataType === expectedType10)
  }

  test("array_intersect functions") {
    val df1 = Seq((Array(1, 2, 4), Array(4, 2))).toDF("a", "b")
    val ans1 = Row(Seq(2, 4))
    checkAnswer(df1.select(array_intersect($"a", $"b")), ans1)
    checkAnswer(df1.selectExpr("array_intersect(a, b)"), ans1)

    val df2 = Seq((Array[Integer](1, 2, null, 4, 5), Array[Integer](-5, 4, null, 2, -1)))
      .toDF("a", "b")
    val ans2 = Row(Seq(2, null, 4))
    checkAnswer(df2.select(array_intersect($"a", $"b")), ans2)
    checkAnswer(df2.selectExpr("array_intersect(a, b)"), ans2)

    val df3 = Seq((Array(1L, 2L, 4L), Array(4L, 2L))).toDF("a", "b")
    val ans3 = Row(Seq(2L, 4L))
    checkAnswer(df3.select(array_intersect($"a", $"b")), ans3)
    checkAnswer(df3.selectExpr("array_intersect(a, b)"), ans3)

    val df4 = Seq(
      (Array[java.lang.Long](1L, 2L, null, 4L, 5L), Array[java.lang.Long](-5L, 4L, null, 2L, -1L)))
      .toDF("a", "b")
    val ans4 = Row(Seq(2L, null, 4L))
    checkAnswer(df4.select(array_intersect($"a", $"b")), ans4)
    checkAnswer(df4.selectExpr("array_intersect(a, b)"), ans4)

    val df5 = Seq((Array("c", null, "a", "f"), Array("b", "a", null, "g"))).toDF("a", "b")
    val ans5 = Row(Seq(null, "a"))
    checkAnswer(df5.select(array_intersect($"a", $"b")), ans5)
    checkAnswer(df5.selectExpr("array_intersect(a, b)"), ans5)

    val df6 = Seq((null, null)).toDF("a", "b")
    assert(intercept[AnalysisException] {
      df6.select(array_intersect($"a", $"b"))
    }.getMessage.contains("data type mismatch"))
    assert(intercept[AnalysisException] {
      df6.selectExpr("array_intersect(a, b)")
    }.getMessage.contains("data type mismatch"))

    val df7 = Seq((Array(1), Array("a"))).toDF("a", "b")
    assert(intercept[AnalysisException] {
      df7.select(array_intersect($"a", $"b"))
    }.getMessage.contains("data type mismatch"))
    assert(intercept[AnalysisException] {
      df7.selectExpr("array_intersect(a, b)")
    }.getMessage.contains("data type mismatch"))

    val df8 = Seq((null, Array("a"))).toDF("a", "b")
    assert(intercept[AnalysisException] {
      df8.select(array_intersect($"a", $"b"))
    }.getMessage.contains("data type mismatch"))
    assert(intercept[AnalysisException] {
      df8.selectExpr("array_intersect(a, b)")
    }.getMessage.contains("data type mismatch"))
  }

  test("transform function - array for primitive type not containing null") {
    val df = Seq(
      Seq(1, 9, 8, 7),
      Seq(5, 8, 9, 7, 2),
      Seq.empty,
      null
    ).toDF("i")

    def testArrayOfPrimitiveTypeNotContainsNull(): Unit = {
      checkAnswer(df.selectExpr("transform(i, x -> x + 1)"),
        Seq(
          Row(Seq(2, 10, 9, 8)),
          Row(Seq(6, 9, 10, 8, 3)),
          Row(Seq.empty),
          Row(null)))
      checkAnswer(df.selectExpr("transform(i, (x, i) -> x + i)"),
        Seq(
          Row(Seq(1, 10, 10, 10)),
          Row(Seq(5, 9, 11, 10, 6)),
          Row(Seq.empty),
          Row(null)))
      checkAnswer(df.select(transform(col("i"), x => x + 1)),
        Seq(
          Row(Seq(2, 10, 9, 8)),
          Row(Seq(6, 9, 10, 8, 3)),
          Row(Seq.empty),
          Row(null)))
      checkAnswer(df.select(transform(col("i"), (x, i) => x + i)),
        Seq(
          Row(Seq(1, 10, 10, 10)),
          Row(Seq(5, 9, 11, 10, 6)),
          Row(Seq.empty),
          Row(null)))
    }

    // Test with local relation, the Project will be evaluated without codegen
    testArrayOfPrimitiveTypeNotContainsNull()
    // Test with cached relation, the Project will be evaluated with codegen
    df.cache()
    testArrayOfPrimitiveTypeNotContainsNull()
  }

  test("transform function - array for primitive type containing null") {
    val df = Seq[Seq[Integer]](
      Seq(1, 9, 8, null, 7),
      Seq(5, null, 8, 9, 7, 2),
      Seq.empty,
      null
    ).toDF("i")

    def testArrayOfPrimitiveTypeContainsNull(): Unit = {
      checkAnswer(df.selectExpr("transform(i, x -> x + 1)"),
        Seq(
          Row(Seq(2, 10, 9, null, 8)),
          Row(Seq(6, null, 9, 10, 8, 3)),
          Row(Seq.empty),
          Row(null)))
      checkAnswer(df.selectExpr("transform(i, (x, i) -> x + i)"),
        Seq(
          Row(Seq(1, 10, 10, null, 11)),
          Row(Seq(5, null, 10, 12, 11, 7)),
          Row(Seq.empty),
          Row(null)))
      checkAnswer(df.select(transform(col("i"), x => x + 1)),
        Seq(
          Row(Seq(2, 10, 9, null, 8)),
          Row(Seq(6, null, 9, 10, 8, 3)),
          Row(Seq.empty),
          Row(null)))
      checkAnswer(df.select(transform(col("i"), (x, i) => x + i)),
        Seq(
          Row(Seq(1, 10, 10, null, 11)),
          Row(Seq(5, null, 10, 12, 11, 7)),
          Row(Seq.empty),
          Row(null)))
    }

    // Test with local relation, the Project will be evaluated without codegen
    testArrayOfPrimitiveTypeContainsNull()
    // Test with cached relation, the Project will be evaluated with codegen
    df.cache()
    testArrayOfPrimitiveTypeContainsNull()
  }

  test("transform function - array for non-primitive type") {
    val df = Seq(
      Seq("c", "a", "b"),
      Seq("b", null, "c", null),
      Seq.empty,
      null
    ).toDF("s")

    def testNonPrimitiveType(): Unit = {
      checkAnswer(df.selectExpr("transform(s, x -> concat(x, x))"),
        Seq(
          Row(Seq("cc", "aa", "bb")),
          Row(Seq("bb", null, "cc", null)),
          Row(Seq.empty),
          Row(null)))
      checkAnswer(df.selectExpr("transform(s, (x, i) -> concat(x, i))"),
        Seq(
          Row(Seq("c0", "a1", "b2")),
          Row(Seq("b0", null, "c2", null)),
          Row(Seq.empty),
          Row(null)))
      checkAnswer(df.select(transform(col("s"), x => concat(x, x))),
        Seq(
          Row(Seq("cc", "aa", "bb")),
          Row(Seq("bb", null, "cc", null)),
          Row(Seq.empty),
          Row(null)))
      checkAnswer(df.select(transform(col("s"), (x, i) => concat(x, i))),
        Seq(
          Row(Seq("c0", "a1", "b2")),
          Row(Seq("b0", null, "c2", null)),
          Row(Seq.empty),
          Row(null)))
    }

    // Test with local relation, the Project will be evaluated without codegen
    testNonPrimitiveType()
    // Test with cached relation, the Project will be evaluated with codegen
    df.cache()
    testNonPrimitiveType()
  }

  test("transform function - special cases") {
    val df = Seq(
      Seq("c", "a", "b"),
      Seq("b", null, "c", null),
      Seq.empty,
      null
    ).toDF("arg")

    def testSpecialCases(): Unit = {
      checkAnswer(df.selectExpr("transform(arg, arg -> arg)"),
        Seq(
          Row(Seq("c", "a", "b")),
          Row(Seq("b", null, "c", null)),
          Row(Seq.empty),
          Row(null)))
      checkAnswer(df.selectExpr("transform(arg, arg)"),
        Seq(
          Row(Seq(Seq("c", "a", "b"), Seq("c", "a", "b"), Seq("c", "a", "b"))),
          Row(Seq(
            Seq("b", null, "c", null),
            Seq("b", null, "c", null),
            Seq("b", null, "c", null),
            Seq("b", null, "c", null))),
          Row(Seq.empty),
          Row(null)))
      checkAnswer(df.selectExpr("transform(arg, x -> concat(arg, array(x)))"),
        Seq(
          Row(Seq(Seq("c", "a", "b", "c"), Seq("c", "a", "b", "a"), Seq("c", "a", "b", "b"))),
          Row(Seq(
            Seq("b", null, "c", null, "b"),
            Seq("b", null, "c", null, null),
            Seq("b", null, "c", null, "c"),
            Seq("b", null, "c", null, null))),
          Row(Seq.empty),
          Row(null)))
      checkAnswer(df.select(transform(col("arg"), arg => arg)),
        Seq(
          Row(Seq("c", "a", "b")),
          Row(Seq("b", null, "c", null)),
          Row(Seq.empty),
          Row(null)))
      checkAnswer(df.select(transform(col("arg"), _ => col("arg"))),
        Seq(
          Row(Seq(Seq("c", "a", "b"), Seq("c", "a", "b"), Seq("c", "a", "b"))),
          Row(Seq(
            Seq("b", null, "c", null),
            Seq("b", null, "c", null),
            Seq("b", null, "c", null),
            Seq("b", null, "c", null))),
          Row(Seq.empty),
          Row(null)))
      checkAnswer(df.select(transform(col("arg"), x => concat(col("arg"), array(x)))),
        Seq(
          Row(Seq(Seq("c", "a", "b", "c"), Seq("c", "a", "b", "a"), Seq("c", "a", "b", "b"))),
          Row(Seq(
            Seq("b", null, "c", null, "b"),
            Seq("b", null, "c", null, null),
            Seq("b", null, "c", null, "c"),
            Seq("b", null, "c", null, null))),
          Row(Seq.empty),
          Row(null)))
    }

    // Test with local relation, the Project will be evaluated without codegen
    testSpecialCases()
    // Test with cached relation, the Project will be evaluated with codegen
    df.cache()
    testSpecialCases()
  }

  test("transform function - invalid") {
    val df = Seq(
      (Seq("c", "a", "b"), 1),
      (Seq("b", null, "c", null), 2),
      (Seq.empty, 3),
      (null, 4)
    ).toDF("s", "i")

    val ex1 = intercept[AnalysisException] {
      df.selectExpr("transform(s, (x, y, z) -> x + y + z)")
    }
    assert(ex1.getMessage.contains("The number of lambda function arguments '3' does not match"))

    val ex2 = intercept[AnalysisException] {
      df.selectExpr("transform(i, x -> x)")
    }
    assert(ex2.getMessage.contains("data type mismatch: argument 1 requires array type"))

    val ex3 = intercept[AnalysisException] {
      df.selectExpr("transform(a, x -> x)")
    }
    assert(ex3.getErrorClass == "MISSING_COLUMN")
    assert(ex3.messageParameters.head == "a")
  }

  test("map_filter") {
    val dfInts = Seq(
      Map(1 -> 10, 2 -> 20, 3 -> 30),
      Map(1 -> -1, 2 -> -2, 3 -> -3),
      Map(1 -> 10, 2 -> 5, 3 -> -3)).toDF("m")

    checkAnswer(dfInts.selectExpr(
      "map_filter(m, (k, v) -> k * 10 = v)", "map_filter(m, (k, v) -> k = -v)"),
      Seq(
        Row(Map(1 -> 10, 2 -> 20, 3 -> 30), Map()),
        Row(Map(), Map(1 -> -1, 2 -> -2, 3 -> -3)),
        Row(Map(1 -> 10), Map(3 -> -3))))

    checkAnswer(dfInts.select(
      map_filter(col("m"), (k, v) => k * 10 === v),
      map_filter(col("m"), (k, v) => k === (v * -1))),
      Seq(
        Row(Map(1 -> 10, 2 -> 20, 3 -> 30), Map()),
        Row(Map(), Map(1 -> -1, 2 -> -2, 3 -> -3)),
        Row(Map(1 -> 10), Map(3 -> -3))))

    val dfComplex = Seq(
      Map(1 -> Seq(Some(1)), 2 -> Seq(Some(1), Some(2)), 3 -> Seq(Some(1), Some(2), Some(3))),
      Map(1 -> null, 2 -> Seq(Some(-2), Some(-2)), 3 -> Seq[Option[Int]](None))).toDF("m")

    checkAnswer(dfComplex.selectExpr(
      "map_filter(m, (k, v) -> k = v[0])", "map_filter(m, (k, v) -> k = size(v))"),
      Seq(
        Row(Map(1 -> Seq(1)), Map(1 -> Seq(1), 2 -> Seq(1, 2), 3 -> Seq(1, 2, 3))),
        Row(Map(), Map(2 -> Seq(-2, -2)))))

    checkAnswer(dfComplex.select(
      map_filter(col("m"), (k, v) => k === element_at(v, 1)),
      map_filter(col("m"), (k, v) => k === size(v))),
      Seq(
        Row(Map(1 -> Seq(1)), Map(1 -> Seq(1), 2 -> Seq(1, 2), 3 -> Seq(1, 2, 3))),
        Row(Map(), Map(2 -> Seq(-2, -2)))))

    // Invalid use cases
    val df = Seq(
      (Map(1 -> "a"), 1),
      (Map.empty[Int, String], 2),
      (null, 3)
    ).toDF("s", "i")

    val ex1 = intercept[AnalysisException] {
      df.selectExpr("map_filter(s, (x, y, z) -> x + y + z)")
    }
    assert(ex1.getMessage.contains("The number of lambda function arguments '3' does not match"))

    val ex2 = intercept[AnalysisException] {
      df.selectExpr("map_filter(s, x -> x)")
    }
    assert(ex2.getMessage.contains("The number of lambda function arguments '1' does not match"))

    val ex3 = intercept[AnalysisException] {
      df.selectExpr("map_filter(i, (k, v) -> k > v)")
    }
    assert(ex3.getMessage.contains("data type mismatch: argument 1 requires map type"))

    val ex3a = intercept[AnalysisException] {
      df.select(map_filter(col("i"), (k, v) => k > v))
    }
    assert(ex3a.getMessage.contains("data type mismatch: argument 1 requires map type"))

    val ex4 = intercept[AnalysisException] {
      df.selectExpr("map_filter(a, (k, v) -> k > v)")
    }
    assert(ex4.getErrorClass == "MISSING_COLUMN")
    assert(ex4.messageParameters.head == "a")
  }

  test("filter function - array for primitive type not containing null") {
    val df = Seq(
      Seq(1, 9, 8, 7),
      Seq(5, 8, 9, 7, 2),
      Seq.empty,
      null
    ).toDF("i")

    def testArrayOfPrimitiveTypeNotContainsNull(): Unit = {
      checkAnswer(df.selectExpr("filter(i, x -> x % 2 == 0)"),
        Seq(
          Row(Seq(8)),
          Row(Seq(8, 2)),
          Row(Seq.empty),
          Row(null)))
      checkAnswer(df.select(filter(col("i"), _ % 2 === 0)),
        Seq(
          Row(Seq(8)),
          Row(Seq(8, 2)),
          Row(Seq.empty),
          Row(null)))
    }

    // Test with local relation, the Project will be evaluated without codegen
    testArrayOfPrimitiveTypeNotContainsNull()
    // Test with cached relation, the Project will be evaluated with codegen
    df.cache()
    testArrayOfPrimitiveTypeNotContainsNull()
  }

  test("filter function - array for primitive type containing null") {
    val df = Seq[Seq[Integer]](
      Seq(1, 9, 8, null, 7),
      Seq(5, null, 8, 9, 7, 2),
      Seq.empty,
      null
    ).toDF("i")

    def testArrayOfPrimitiveTypeContainsNull(): Unit = {
      checkAnswer(df.selectExpr("filter(i, x -> x % 2 == 0)"),
        Seq(
          Row(Seq(8)),
          Row(Seq(8, 2)),
          Row(Seq.empty),
          Row(null)))
      checkAnswer(df.select(filter(col("i"), _ % 2 === 0)),
        Seq(
          Row(Seq(8)),
          Row(Seq(8, 2)),
          Row(Seq.empty),
          Row(null)))
    }

    // Test with local relation, the Project will be evaluated without codegen
    testArrayOfPrimitiveTypeContainsNull()
    // Test with cached relation, the Project will be evaluated with codegen
    df.cache()
    testArrayOfPrimitiveTypeContainsNull()
  }

  test("filter function - array for non-primitive type") {
    val df = Seq(
      Seq("c", "a", "b"),
      Seq("b", null, "c", null),
      Seq.empty,
      null
    ).toDF("s")

    def testNonPrimitiveType(): Unit = {
      checkAnswer(df.selectExpr("filter(s, x -> x is not null)"),
        Seq(
          Row(Seq("c", "a", "b")),
          Row(Seq("b", "c")),
          Row(Seq.empty),
          Row(null)))
      checkAnswer(df.select(filter(col("s"), x => x.isNotNull)),
        Seq(
          Row(Seq("c", "a", "b")),
          Row(Seq("b", "c")),
          Row(Seq.empty),
          Row(null)))
    }

    // Test with local relation, the Project will be evaluated without codegen
    testNonPrimitiveType()
    // Test with cached relation, the Project will be evaluated with codegen
    df.cache()
    testNonPrimitiveType()
  }

  test("filter function - index argument") {
    val df = Seq(
      Seq("c", "a", "b"),
      Seq("b", null, "c", null),
      Seq.empty,
      null
    ).toDF("s")

    def testIndexArgument(): Unit = {
      checkAnswer(df.selectExpr("filter(s, (x, i) -> i % 2 == 0)"),
        Seq(
          Row(Seq("c", "b")),
          Row(Seq("b", "c")),
          Row(Seq.empty),
          Row(null)))
      checkAnswer(df.select(filter(col("s"), (x, i) => i % 2 === 0)),
        Seq(
          Row(Seq("c", "b")),
          Row(Seq("b", "c")),
          Row(Seq.empty),
          Row(null)))
    }

    // Test with local relation, the Project will be evaluated without codegen
    testIndexArgument()
    // Test with cached relation, the Project will be evaluated with codegen
    df.cache()
    testIndexArgument()
  }

  test("filter function - invalid") {
    val df = Seq(
      (Seq("c", "a", "b"), 1),
      (Seq("b", null, "c", null), 2),
      (Seq.empty, 3),
      (null, 4)
    ).toDF("s", "i")

    val ex1 = intercept[AnalysisException] {
      df.selectExpr("filter(s, (x, y, z) -> x + y)")
    }
    assert(ex1.getMessage.contains("The number of lambda function arguments '3' does not match"))

    val ex2 = intercept[AnalysisException] {
      df.selectExpr("filter(i, x -> x)")
    }
    assert(ex2.getMessage.contains("data type mismatch: argument 1 requires array type"))

    val ex2a = intercept[AnalysisException] {
      df.select(filter(col("i"), x => x))
    }
    assert(ex2a.getMessage.contains("data type mismatch: argument 1 requires array type"))

    val ex3 = intercept[AnalysisException] {
      df.selectExpr("filter(s, x -> x)")
    }
    assert(ex3.getMessage.contains("data type mismatch: argument 2 requires boolean type"))

    val ex3a = intercept[AnalysisException] {
      df.select(filter(col("s"), x => x))
    }
    assert(ex3a.getMessage.contains("data type mismatch: argument 2 requires boolean type"))

    val ex4 = intercept[AnalysisException] {
      df.selectExpr("filter(a, x -> x)")
    }
    assert(ex4.getErrorClass == "MISSING_COLUMN")
    assert(ex4.messageParameters.head == "a")
  }

  test("exists function - array for primitive type not containing null") {
    val df = Seq(
      Seq(1, 9, 8, 7),
      Seq(5, 9, 7),
      Seq.empty,
      null
    ).toDF("i")

    def testArrayOfPrimitiveTypeNotContainsNull(): Unit = {
      checkAnswer(df.selectExpr("exists(i, x -> x % 2 == 0)"),
        Seq(
          Row(true),
          Row(false),
          Row(false),
          Row(null)))
      checkAnswer(df.select(exists(col("i"), _ % 2 === 0)),
        Seq(
          Row(true),
          Row(false),
          Row(false),
          Row(null)))
    }

    // Test with local relation, the Project will be evaluated without codegen
    testArrayOfPrimitiveTypeNotContainsNull()
    // Test with cached relation, the Project will be evaluated with codegen
    df.cache()
    testArrayOfPrimitiveTypeNotContainsNull()
  }

  test("exists function - array for primitive type containing null") {
    val df = Seq[Seq[Integer]](
      Seq(1, 9, 8, null, 7),
      Seq(1, 3, 5),
      Seq(5, null, null, 9, 7, null),
      Seq.empty,
      null
    ).toDF("i")

    def testArrayOfPrimitiveTypeContainsNull(): Unit = {
      checkAnswer(df.selectExpr("exists(i, x -> x % 2 == 0)"),
        Seq(
          Row(true),
          Row(false),
          Row(null),
          Row(false),
          Row(null)))
      checkAnswer(df.select(exists(col("i"), _ % 2 === 0)),
        Seq(
          Row(true),
          Row(false),
          Row(null),
          Row(false),
          Row(null)))
    }

    // Test with local relation, the Project will be evaluated without codegen
    testArrayOfPrimitiveTypeContainsNull()
    // Test with cached relation, the Project will be evaluated with codegen
    df.cache()
    testArrayOfPrimitiveTypeContainsNull()
  }

  test("exists function - array for non-primitive type") {
    val df = Seq(
      Seq("c", "a", "b"),
      Seq("b", null, "c", null),
      Seq.empty,
      null
    ).toDF("s")

    def testNonPrimitiveType(): Unit = {
      checkAnswer(df.selectExpr("exists(s, x -> x is null)"),
        Seq(
          Row(false),
          Row(true),
          Row(false),
          Row(null)))
      checkAnswer(df.select(exists(col("s"), x => x.isNull)),
        Seq(
          Row(false),
          Row(true),
          Row(false),
          Row(null)))
    }

    // Test with local relation, the Project will be evaluated without codegen
    testNonPrimitiveType()
    // Test with cached relation, the Project will be evaluated with codegen
    df.cache()
    testNonPrimitiveType()
  }

  test("exists function - invalid") {
    val df = Seq(
      (Seq("c", "a", "b"), 1),
      (Seq("b", null, "c", null), 2),
      (Seq.empty, 3),
      (null, 4)
    ).toDF("s", "i")

    val ex1 = intercept[AnalysisException] {
      df.selectExpr("exists(s, (x, y) -> x + y)")
    }
    assert(ex1.getMessage.contains("The number of lambda function arguments '2' does not match"))

    val ex2 = intercept[AnalysisException] {
      df.selectExpr("exists(i, x -> x)")
    }
    assert(ex2.getMessage.contains("data type mismatch: argument 1 requires array type"))

    val ex2a = intercept[AnalysisException] {
      df.select(exists(col("i"), x => x))
    }
    assert(ex2.getMessage.contains("data type mismatch: argument 1 requires array type"))

    val ex3 = intercept[AnalysisException] {
      df.selectExpr("exists(s, x -> x)")
    }
    assert(ex3.getMessage.contains("data type mismatch: argument 2 requires boolean type"))

    val ex3a = intercept[AnalysisException] {
      df.select(exists(df("s"), x => x))
    }
    assert(ex3a.getMessage.contains("data type mismatch: argument 2 requires boolean type"))

    val ex4 = intercept[AnalysisException] {
      df.selectExpr("exists(a, x -> x)")
    }
    assert(ex4.getErrorClass == "MISSING_COLUMN")
    assert(ex4.messageParameters.head == "a")
  }

  test("forall function - array for primitive type not containing null") {
    val df = Seq(
      Seq(1, 9, 8, 7),
      Seq(2, 4, 6),
      Seq.empty,
      null
    ).toDF("i")

    def testArrayOfPrimitiveTypeNotContainsNull(): Unit = {
      checkAnswer(df.selectExpr("forall(i, x -> x % 2 == 0)"),
        Seq(
          Row(false),
          Row(true),
          Row(true),
          Row(null)))
      checkAnswer(df.select(forall(col("i"), x => x % 2 === 0)),
        Seq(
          Row(false),
          Row(true),
          Row(true),
          Row(null)))
    }

    // Test with local relation, the Project will be evaluated without codegen
    testArrayOfPrimitiveTypeNotContainsNull()
    // Test with cached relation, the Project will be evaluated with codegen
    df.cache()
    testArrayOfPrimitiveTypeNotContainsNull()
  }

  test("forall function - array for primitive type containing null") {
    val df = Seq[Seq[Integer]](
      Seq(1, 9, 8, null, 7),
      Seq(2, null, null, 4, 6, null),
      Seq(2, 4, 6, 8),
      Seq.empty,
      null
    ).toDF("i")

    def testArrayOfPrimitiveTypeContainsNull(): Unit = {
      checkAnswer(df.selectExpr("forall(i, x -> x % 2 == 0 or x is null)"),
        Seq(
          Row(false),
          Row(true),
          Row(true),
          Row(true),
          Row(null)))
      checkAnswer(df.select(forall(col("i"), x => (x % 2 === 0) || x.isNull)),
        Seq(
          Row(false),
          Row(true),
          Row(true),
          Row(true),
          Row(null)))
      checkAnswer(df.selectExpr("forall(i, x -> x % 2 == 0)"),
        Seq(
          Row(false),
          Row(null),
          Row(true),
          Row(true),
          Row(null)))
      checkAnswer(df.select(forall(col("i"), x => x % 2 === 0)),
        Seq(
          Row(false),
          Row(null),
          Row(true),
          Row(true),
          Row(null)))
    }

    // Test with local relation, the Project will be evaluated without codegen
    testArrayOfPrimitiveTypeContainsNull()
    // Test with cached relation, the Project will be evaluated with codegen
    df.cache()
    testArrayOfPrimitiveTypeContainsNull()
  }

  test("forall function - array for non-primitive type") {
    val df = Seq(
      Seq("c", "a", "b"),
      Seq[String](null, null, null, null),
      Seq.empty,
      null
    ).toDF("s")

    def testNonPrimitiveType(): Unit = {
      checkAnswer(df.selectExpr("forall(s, x -> x is null)"),
        Seq(
          Row(false),
          Row(true),
          Row(true),
          Row(null)))
      checkAnswer(df.select(forall(col("s"), _.isNull)),
        Seq(
          Row(false),
          Row(true),
          Row(true),
          Row(null)))
    }

    // Test with local relation, the Project will be evaluated without codegen
    testNonPrimitiveType()
    // Test with cached relation, the Project will be evaluated with codegen
    df.cache()
    testNonPrimitiveType()
  }

  test("forall function - invalid") {
    val df = Seq(
      (Seq("c", "a", "b"), 1),
      (Seq("b", null, "c", null), 2),
      (Seq.empty, 3),
      (null, 4)
    ).toDF("s", "i")

    val ex1 = intercept[AnalysisException] {
      df.selectExpr("forall(s, (x, y) -> x + y)")
    }
    assert(ex1.getMessage.contains("The number of lambda function arguments '2' does not match"))

    val ex2 = intercept[AnalysisException] {
      df.selectExpr("forall(i, x -> x)")
    }
    assert(ex2.getMessage.contains("data type mismatch: argument 1 requires array type"))

    val ex2a = intercept[AnalysisException] {
      df.select(forall(col("i"), x => x))
    }
    assert(ex2a.getMessage.contains("data type mismatch: argument 1 requires array type"))

    val ex3 = intercept[AnalysisException] {
      df.selectExpr("forall(s, x -> x)")
    }
    assert(ex3.getMessage.contains("data type mismatch: argument 2 requires boolean type"))

    val ex3a = intercept[AnalysisException] {
      df.select(forall(col("s"), x => x))
    }
    assert(ex3a.getMessage.contains("data type mismatch: argument 2 requires boolean type"))

    val ex4 = intercept[AnalysisException] {
      df.selectExpr("forall(a, x -> x)")
    }
    assert(ex4.getErrorClass == "MISSING_COLUMN")
    assert(ex4.messageParameters.head == "a")

    val ex4a = intercept[AnalysisException] {
      df.select(forall(col("a"), x => x))
    }
    assert(ex4a.getErrorClass == "MISSING_COLUMN")
    assert(ex4a.messageParameters.head == "a")
  }

  test("aggregate function - array for primitive type not containing null") {
    val df = Seq(
      Seq(1, 9, 8, 7),
      Seq(5, 8, 9, 7, 2),
      Seq.empty,
      null
    ).toDF("i")

    def testArrayOfPrimitiveTypeNotContainsNull(): Unit = {
      checkAnswer(df.selectExpr("aggregate(i, 0, (acc, x) -> acc + x)"),
        Seq(
          Row(25),
          Row(31),
          Row(0),
          Row(null)))
      checkAnswer(df.selectExpr("aggregate(i, 0, (acc, x) -> acc + x, acc -> acc * 10)"),
        Seq(
          Row(250),
          Row(310),
          Row(0),
          Row(null)))
      checkAnswer(df.select(aggregate(col("i"), lit(0), (acc, x) => acc + x)),
        Seq(
          Row(25),
          Row(31),
          Row(0),
          Row(null)))
      checkAnswer(df.select(aggregate(col("i"), lit(0), (acc, x) => acc + x, _ * 10)),
        Seq(
          Row(250),
          Row(310),
          Row(0),
          Row(null)))
    }

    // Test with local relation, the Project will be evaluated without codegen
    testArrayOfPrimitiveTypeNotContainsNull()
    // Test with cached relation, the Project will be evaluated with codegen
    df.cache()
    testArrayOfPrimitiveTypeNotContainsNull()
  }

  test("aggregate function - array for primitive type containing null") {
    val df = Seq[Seq[Integer]](
      Seq(1, 9, 8, 7),
      Seq(5, null, 8, 9, 7, 2),
      Seq.empty,
      null
    ).toDF("i")

    def testArrayOfPrimitiveTypeContainsNull(): Unit = {
      checkAnswer(df.selectExpr("aggregate(i, 0, (acc, x) -> acc + x)"),
        Seq(
          Row(25),
          Row(null),
          Row(0),
          Row(null)))
      checkAnswer(
        df.selectExpr("aggregate(i, 0, (acc, x) -> acc + x, acc -> coalesce(acc, 0) * 10)"),
        Seq(
          Row(250),
          Row(0),
          Row(0),
          Row(null)))
      checkAnswer(df.select(aggregate(col("i"), lit(0), (acc, x) => acc + x)),
        Seq(
          Row(25),
          Row(null),
          Row(0),
          Row(null)))
      checkAnswer(
        df.select(
          aggregate(col("i"), lit(0), (acc, x) => acc + x, acc => coalesce(acc, lit(0)) * 10)),
        Seq(
          Row(250),
          Row(0),
          Row(0),
          Row(null)))
    }

    // Test with local relation, the Project will be evaluated without codegen
    testArrayOfPrimitiveTypeContainsNull()
    // Test with cached relation, the Project will be evaluated with codegen
    df.cache()
    testArrayOfPrimitiveTypeContainsNull()
  }

  test("aggregate function - array for non-primitive type") {
    val df = Seq(
      (Seq("c", "a", "b"), "a"),
      (Seq("b", null, "c", null), "b"),
      (Seq.empty, "c"),
      (null, "d")
    ).toDF("ss", "s")

    def testNonPrimitiveType(): Unit = {
      checkAnswer(df.selectExpr("aggregate(ss, s, (acc, x) -> concat(acc, x))"),
        Seq(
          Row("acab"),
          Row(null),
          Row("c"),
          Row(null)))
      checkAnswer(
        df.selectExpr("aggregate(ss, s, (acc, x) -> concat(acc, x), acc -> coalesce(acc , ''))"),
        Seq(
          Row("acab"),
          Row(""),
          Row("c"),
          Row(null)))
      checkAnswer(df.select(aggregate(col("ss"), col("s"), (acc, x) => concat(acc, x))),
        Seq(
          Row("acab"),
          Row(null),
          Row("c"),
          Row(null)))
      checkAnswer(
        df.select(
          aggregate(col("ss"), col("s"), (acc, x) => concat(acc, x),
            acc => coalesce(acc, lit("")))),
        Seq(
          Row("acab"),
          Row(""),
          Row("c"),
          Row(null)))
    }

    // Test with local relation, the Project will be evaluated without codegen
    testNonPrimitiveType()
    // Test with cached relation, the Project will be evaluated with codegen
    df.cache()
    testNonPrimitiveType()
  }

  test("aggregate function - invalid") {
    val df = Seq(
      (Seq("c", "a", "b"), 1),
      (Seq("b", null, "c", null), 2),
      (Seq.empty, 3),
      (null, 4)
    ).toDF("s", "i")

    val ex1 = intercept[AnalysisException] {
      df.selectExpr("aggregate(s, '', x -> x)")
    }
    assert(ex1.getMessage.contains("The number of lambda function arguments '1' does not match"))

    val ex2 = intercept[AnalysisException] {
      df.selectExpr("aggregate(s, '', (acc, x) -> x, (acc, x) -> x)")
    }
    assert(ex2.getMessage.contains("The number of lambda function arguments '2' does not match"))

    val ex3 = intercept[AnalysisException] {
      df.selectExpr("aggregate(i, 0, (acc, x) -> x)")
    }
    assert(ex3.getMessage.contains("data type mismatch: argument 1 requires array type"))

    val ex3a = intercept[AnalysisException] {
      df.select(aggregate(col("i"), lit(0), (acc, x) => x))
    }
    assert(ex3a.getMessage.contains("data type mismatch: argument 1 requires array type"))

    val ex4 = intercept[AnalysisException] {
      df.selectExpr("aggregate(s, 0, (acc, x) -> x)")
    }
    assert(ex4.getMessage.contains("data type mismatch: argument 3 requires int type"))

    val ex4a = intercept[AnalysisException] {
      df.select(aggregate(col("s"), lit(0), (acc, x) => x))
    }
    assert(ex4a.getMessage.contains("data type mismatch: argument 3 requires int type"))

    val ex5 = intercept[AnalysisException] {
      df.selectExpr("aggregate(a, 0, (acc, x) -> x)")
    }
    assert(ex5.getErrorClass == "MISSING_COLUMN")
    assert(ex5.messageParameters.head == "a")
  }

  test("map_zip_with function - map of primitive types") {
    val df = Seq(
      (Map(8 -> 6L, 3 -> 5L, 6 -> 2L), Map[Integer, Integer]((6, 4), (8, 2), (3, 2))),
      (Map(10 -> 6L, 8 -> 3L), Map[Integer, Integer]((8, 4), (4, null))),
      (Map.empty[Int, Long], Map[Integer, Integer]((5, 1))),
      (Map(5 -> 1L), null)
    ).toDF("m1", "m2")

    checkAnswer(df.selectExpr("map_zip_with(m1, m2, (k, v1, v2) -> k == v1 + v2)"),
      Seq(
        Row(Map(8 -> true, 3 -> false, 6 -> true)),
        Row(Map(10 -> null, 8 -> false, 4 -> null)),
        Row(Map(5 -> null)),
        Row(null)))

    checkAnswer(df.select(map_zip_with(df("m1"), df("m2"), (k, v1, v2) => k === v1 + v2)),
      Seq(
        Row(Map(8 -> true, 3 -> false, 6 -> true)),
        Row(Map(10 -> null, 8 -> false, 4 -> null)),
        Row(Map(5 -> null)),
        Row(null)))
  }

  test("map_zip_with function - map of non-primitive types") {
    val df = Seq(
      (Map("z" -> "a", "y" -> "b", "x" -> "c"), Map("x" -> "a", "z" -> "c")),
      (Map("b" -> "a", "c" -> "d"), Map("c" -> "a", "b" -> null, "d" -> "k")),
      (Map("a" -> "d"), Map.empty[String, String]),
      (Map("a" -> "d"), null)
    ).toDF("m1", "m2")

    checkAnswer(df.selectExpr("map_zip_with(m1, m2, (k, v1, v2) -> (v1, v2))"),
      Seq(
        Row(Map("z" -> Row("a", "c"), "y" -> Row("b", null), "x" -> Row("c", "a"))),
        Row(Map("b" -> Row("a", null), "c" -> Row("d", "a"), "d" -> Row(null, "k"))),
        Row(Map("a" -> Row("d", null))),
        Row(null)))

    checkAnswer(df.select(map_zip_with(col("m1"), col("m2"), (k, v1, v2) => struct(v1, v2))),
      Seq(
        Row(Map("z" -> Row("a", "c"), "y" -> Row("b", null), "x" -> Row("c", "a"))),
        Row(Map("b" -> Row("a", null), "c" -> Row("d", "a"), "d" -> Row(null, "k"))),
        Row(Map("a" -> Row("d", null))),
        Row(null)))
  }

  test("map_zip_with function - invalid") {
    val df = Seq(
      (Map(1 -> 2), Map(1 -> "a"), Map("a" -> "b"), Map(Map(1 -> 2) -> 2), 1)
    ).toDF("mii", "mis", "mss", "mmi", "i")

    val ex1 = intercept[AnalysisException] {
      df.selectExpr("map_zip_with(mii, mis, (x, y) -> x + y)")
    }
    assert(ex1.getMessage.contains("The number of lambda function arguments '2' does not match"))

    val ex2 = intercept[AnalysisException] {
      df.selectExpr("map_zip_with(mis, mmi, (x, y, z) -> concat(x, y, z))")
    }
    assert(ex2.getMessage.contains("The input to function map_zip_with should have " +
      "been two maps with compatible key types"))

    val ex2a = intercept[AnalysisException] {
      df.select(map_zip_with(df("mis"), col("mmi"), (x, y, z) => concat(x, y, z)))
    }
    assert(ex2a.getMessage.contains("The input to function map_zip_with should have " +
      "been two maps with compatible key types"))

    val ex3 = intercept[AnalysisException] {
      df.selectExpr("map_zip_with(i, mis, (x, y, z) -> concat(x, y, z))")
    }
    assert(ex3.getMessage.contains("type mismatch: argument 1 requires map type"))

    val ex3a = intercept[AnalysisException] {
      df.select(map_zip_with(col("i"), col("mis"), (x, y, z) => concat(x, y, z)))
    }
    assert(ex3a.getMessage.contains("type mismatch: argument 1 requires map type"))

    val ex4 = intercept[AnalysisException] {
      df.selectExpr("map_zip_with(mis, i, (x, y, z) -> concat(x, y, z))")
    }
    assert(ex4.getMessage.contains("type mismatch: argument 2 requires map type"))

    val ex4a = intercept[AnalysisException] {
      df.select(map_zip_with(col("mis"), col("i"), (x, y, z) => concat(x, y, z)))
    }
    assert(ex4a.getMessage.contains("type mismatch: argument 2 requires map type"))

    val ex5 = intercept[AnalysisException] {
      df.selectExpr("map_zip_with(mmi, mmi, (x, y, z) -> x)")
    }
    assert(ex5.getMessage.contains("function map_zip_with does not support ordering on type map"))
  }

  test("transform keys function - primitive data types") {
    val dfExample1 = Seq(
      Map[Int, Int](1 -> 1, 9 -> 9, 8 -> 8, 7 -> 7)
    ).toDF("i")

    val dfExample2 = Seq(
      Map[Int, Double](1 -> 1.0, 2 -> 1.40, 3 -> 1.70)
    ).toDF("j")

    val dfExample3 = Seq(
      Map[Int, Boolean](25 -> true, 26 -> false)
    ).toDF("x")

    val dfExample4 = Seq(
      Map[Array[Int], Boolean](Array(1, 2) -> false)
    ).toDF("y")


    def testMapOfPrimitiveTypesCombination(): Unit = {
      checkAnswer(dfExample1.selectExpr("transform_keys(i, (k, v) -> k + v)"),
        Seq(Row(Map(2 -> 1, 18 -> 9, 16 -> 8, 14 -> 7))))

      checkAnswer(dfExample1.select(transform_keys(col("i"), (k, v) => k + v)),
        Seq(Row(Map(2 -> 1, 18 -> 9, 16 -> 8, 14 -> 7))))

      checkAnswer(dfExample2.selectExpr("transform_keys(j, " +
        "(k, v) -> map_from_arrays(ARRAY(1, 2, 3), ARRAY('one', 'two', 'three'))[k])"),
        Seq(Row(Map("one" -> 1.0, "two" -> 1.4, "three" -> 1.7))))

      checkAnswer(dfExample2.select(
          transform_keys(
            col("j"),
            (k, v) => element_at(
              map_from_arrays(
                array(lit(1), lit(2), lit(3)),
                array(lit("one"), lit("two"), lit("three"))
              ),
              k
            )
          )
        ),
        Seq(Row(Map("one" -> 1.0, "two" -> 1.4, "three" -> 1.7))))

      checkAnswer(dfExample2.selectExpr("transform_keys(j, (k, v) -> CAST(v * 2 AS BIGINT) + k)"),
        Seq(Row(Map(3 -> 1.0, 4 -> 1.4, 6 -> 1.7))))

      checkAnswer(dfExample2.select(transform_keys(col("j"),
        (k, v) => (v * 2).cast("bigint") + k)),
        Seq(Row(Map(3 -> 1.0, 4 -> 1.4, 6 -> 1.7))))

      checkAnswer(dfExample2.selectExpr("transform_keys(j, (k, v) -> k + v)"),
        Seq(Row(Map(2.0 -> 1.0, 3.4 -> 1.4, 4.7 -> 1.7))))

      checkAnswer(dfExample2.select(transform_keys(col("j"), (k, v) => k + v)),
        Seq(Row(Map(2.0 -> 1.0, 3.4 -> 1.4, 4.7 -> 1.7))))

      intercept[SparkException] {
        dfExample3.selectExpr("transform_keys(x, (k, v) ->  k % 2 = 0 OR v)").collect()
      }
      intercept[SparkException] {
        dfExample3.select(transform_keys(col("x"), (k, v) => k % 2 === 0 || v)).collect()
      }
      withSQLConf(SQLConf.MAP_KEY_DEDUP_POLICY.key -> SQLConf.MapKeyDedupPolicy.LAST_WIN.toString) {
        checkAnswer(dfExample3.selectExpr("transform_keys(x, (k, v) ->  k % 2 = 0 OR v)"),
          Seq(Row(Map(true -> true, true -> false))))

        checkAnswer(dfExample3.select(transform_keys(col("x"), (k, v) => k % 2 === 0 || v)),
          Seq(Row(Map(true -> true, true -> false))))
      }

      checkAnswer(dfExample3.selectExpr("transform_keys(x, (k, v) -> if(v, 2 * k, 3 * k))"),
        Seq(Row(Map(50 -> true, 78 -> false))))

      checkAnswer(dfExample3.select(transform_keys(col("x"),
        (k, v) => when(v, k * 2).otherwise(k * 3))),
        Seq(Row(Map(50 -> true, 78 -> false))))

      checkAnswer(dfExample4.selectExpr("transform_keys(y, (k, v) -> array_contains(k, 3) AND v)"),
        Seq(Row(Map(false -> false))))

      checkAnswer(dfExample4.select(transform_keys(col("y"),
        (k, v) => array_contains(k, lit(3)) && v)),
        Seq(Row(Map(false -> false))))
    }

    // Test with local relation, the Project will be evaluated without codegen
    testMapOfPrimitiveTypesCombination()
    dfExample1.cache()
    dfExample2.cache()
    dfExample3.cache()
    dfExample4.cache()
    // Test with cached relation, the Project will be evaluated with codegen
    testMapOfPrimitiveTypesCombination()
  }

  test("transform keys function - Invalid lambda functions and exceptions") {
    val dfExample1 = Seq(
      Map[String, String]("a" -> null)
    ).toDF("i")

    val dfExample2 = Seq(
      Seq(1, 2, 3, 4)
    ).toDF("j")

    val ex1 = intercept[AnalysisException] {
      dfExample1.selectExpr("transform_keys(i, k -> k)")
    }
    assert(ex1.getMessage.contains("The number of lambda function arguments '1' does not match"))

    val ex2 = intercept[AnalysisException] {
      dfExample1.selectExpr("transform_keys(i, (k, v, x) -> k + 1)")
    }
    assert(ex2.getMessage.contains(
      "The number of lambda function arguments '3' does not match"))

    val ex3 = intercept[Exception] {
      dfExample1.selectExpr("transform_keys(i, (k, v) -> v)").show()
    }
    assert(ex3.getMessage.contains("Cannot use null as map key"))

    val ex3a = intercept[Exception] {
      dfExample1.select(transform_keys(col("i"), (k, v) => v)).show()
    }
    assert(ex3a.getMessage.contains("Cannot use null as map key"))

    val ex4 = intercept[AnalysisException] {
      dfExample2.selectExpr("transform_keys(j, (k, v) -> k + 1)")
    }
    assert(ex4.getMessage.contains(
      "data type mismatch: argument 1 requires map type"))
  }

  test("transform values function - test primitive data types") {
    val dfExample1 = Seq(
      Map[Int, Int](1 -> 1, 9 -> 9, 8 -> 8, 7 -> 7)
    ).toDF("i")

    val dfExample2 = Seq(
      Map[Boolean, String](false -> "abc", true -> "def")
    ).toDF("x")

    val dfExample3 = Seq(
      Map[String, Int]("a" -> 1, "b" -> 2, "c" -> 3)
    ).toDF("y")

    val dfExample4 = Seq(
      Map[Int, Double](1 -> 1.0, 2 -> 1.40, 3 -> 1.70)
    ).toDF("z")

    val dfExample5 = Seq(
      Map[Int, Array[Int]](1 -> Array(1, 2))
    ).toDF("c")

    def testMapOfPrimitiveTypesCombination(): Unit = {
      checkAnswer(dfExample1.selectExpr("transform_values(i, (k, v) -> k + v)"),
        Seq(Row(Map(1 -> 2, 9 -> 18, 8 -> 16, 7 -> 14))))

      checkAnswer(dfExample2.selectExpr(
        "transform_values(x, (k, v) -> if(k, v, CAST(k AS String)))"),
        Seq(Row(Map(false -> "false", true -> "def"))))

      checkAnswer(dfExample2.selectExpr("transform_values(x, (k, v) -> NOT k AND v = 'abc')"),
        Seq(Row(Map(false -> true, true -> false))))

      checkAnswer(dfExample3.selectExpr("transform_values(y, (k, v) -> v * v)"),
        Seq(Row(Map("a" -> 1, "b" -> 4, "c" -> 9))))

      checkAnswer(dfExample3.selectExpr(
        "transform_values(y, (k, v) -> k || ':' || CAST(v as String))"),
        Seq(Row(Map("a" -> "a:1", "b" -> "b:2", "c" -> "c:3"))))

      checkAnswer(
        dfExample3.selectExpr("transform_values(y, (k, v) -> concat(k, cast(v as String)))"),
        Seq(Row(Map("a" -> "a1", "b" -> "b2", "c" -> "c3"))))

      checkAnswer(
        dfExample4.selectExpr(
          "transform_values(" +
            "z,(k, v) -> map_from_arrays(ARRAY(1, 2, 3), " +
            "ARRAY('one', 'two', 'three'))[k] || '_' || CAST(v AS String))"),
        Seq(Row(Map(1 -> "one_1.0", 2 -> "two_1.4", 3 ->"three_1.7"))))

      checkAnswer(
        dfExample4.selectExpr("transform_values(z, (k, v) -> k-v)"),
        Seq(Row(Map(1 -> 0.0, 2 -> 0.6000000000000001, 3 -> 1.3))))

      checkAnswer(
        dfExample5.selectExpr("transform_values(c, (k, v) -> k + cardinality(v))"),
        Seq(Row(Map(1 -> 3))))

      checkAnswer(dfExample1.select(transform_values(col("i"), (k, v) => k + v)),
        Seq(Row(Map(1 -> 2, 9 -> 18, 8 -> 16, 7 -> 14))))

      checkAnswer(dfExample2.select(
        transform_values(col("x"), (k, v) => when(k, v).otherwise(k.cast("string")))),
        Seq(Row(Map(false -> "false", true -> "def"))))

      checkAnswer(dfExample2.select(transform_values(col("x"),
        (k, v) => (!k) && v === "abc")),
        Seq(Row(Map(false -> true, true -> false))))

      checkAnswer(dfExample3.select(transform_values(col("y"), (k, v) => v * v)),
        Seq(Row(Map("a" -> 1, "b" -> 4, "c" -> 9))))

      checkAnswer(dfExample3.select(
        transform_values(col("y"), (k, v) => concat(k, lit(":"), v.cast("string")))),
        Seq(Row(Map("a" -> "a:1", "b" -> "b:2", "c" -> "c:3"))))

      checkAnswer(
        dfExample3.select(transform_values(col("y"), (k, v) => concat(k, v.cast("string")))),
        Seq(Row(Map("a" -> "a1", "b" -> "b2", "c" -> "c3"))))

      val testMap = map_from_arrays(
        array(lit(1), lit(2), lit(3)),
        array(lit("one"), lit("two"), lit("three"))
      )

      checkAnswer(
        dfExample4.select(transform_values(col("z"),
          (k, v) => concat(element_at(testMap, k), lit("_"), v.cast("string")))),
        Seq(Row(Map(1 -> "one_1.0", 2 -> "two_1.4", 3 ->"three_1.7"))))

      checkAnswer(
        dfExample4.select(transform_values(col("z"), (k, v) => k - v)),
        Seq(Row(Map(1 -> 0.0, 2 -> 0.6000000000000001, 3 -> 1.3))))

      checkAnswer(
        dfExample5.select(transform_values(col("c"), (k, v) => k + size(v))),
        Seq(Row(Map(1 -> 3))))
    }

    // Test with local relation, the Project will be evaluated without codegen
    testMapOfPrimitiveTypesCombination()
    dfExample1.cache()
    dfExample2.cache()
    dfExample3.cache()
    dfExample4.cache()
    dfExample5.cache()
    // Test with cached relation, the Project will be evaluated with codegen
    testMapOfPrimitiveTypesCombination()
  }

  test("transform values function - test empty") {
    val dfExample1 = Seq(
      Map.empty[Integer, Integer]
    ).toDF("i")

    val dfExample2 = Seq(
      Map.empty[BigInt, String]
    ).toDF("j")

    def testEmpty(): Unit = {
      checkAnswer(dfExample1.selectExpr("transform_values(i, (k, v) -> NULL)"),
        Seq(Row(Map.empty[Integer, Integer])))

      checkAnswer(dfExample1.selectExpr("transform_values(i, (k, v) -> k)"),
        Seq(Row(Map.empty[Integer, Integer])))

      checkAnswer(dfExample1.selectExpr("transform_values(i, (k, v) -> v)"),
        Seq(Row(Map.empty[Integer, Integer])))

      checkAnswer(dfExample1.selectExpr("transform_values(i, (k, v) -> 0)"),
        Seq(Row(Map.empty[Integer, Integer])))

      checkAnswer(dfExample1.selectExpr("transform_values(i, (k, v) -> 'value')"),
        Seq(Row(Map.empty[Integer, String])))

      checkAnswer(dfExample1.selectExpr("transform_values(i, (k, v) -> true)"),
        Seq(Row(Map.empty[Integer, Boolean])))

      checkAnswer(dfExample2.selectExpr("transform_values(j, (k, v) -> k + cast(v as BIGINT))"),
        Seq(Row(Map.empty[BigInt, BigInt])))

      checkAnswer(dfExample1.select(transform_values(col("i"),
        (k, v) => lit(null).cast("int"))),
        Seq(Row(Map.empty[Integer, Integer])))

      checkAnswer(dfExample1.select(transform_values(col("i"), (k, v) => k)),
        Seq(Row(Map.empty[Integer, Integer])))

      checkAnswer(dfExample1.select(transform_values(col("i"), (k, v) => v)),
        Seq(Row(Map.empty[Integer, Integer])))

      checkAnswer(dfExample1.select(transform_values(col("i"), (k, v) => lit(0))),
        Seq(Row(Map.empty[Integer, Integer])))

      checkAnswer(dfExample1.select(transform_values(col("i"), (k, v) => lit("value"))),
        Seq(Row(Map.empty[Integer, String])))

      checkAnswer(dfExample1.select(transform_values(col("i"), (k, v) => lit(true))),
        Seq(Row(Map.empty[Integer, Boolean])))

      checkAnswer(dfExample1.select(transform_values(col("i"), (k, v) => v.cast("bigint"))),
        Seq(Row(Map.empty[BigInt, BigInt])))
    }

    testEmpty()
    dfExample1.cache()
    dfExample2.cache()
    testEmpty()
  }

  test("transform values function - test null values") {
    val dfExample1 = Seq(
      Map[Int, Integer](1 -> 1, 2 -> 2, 3 -> 3, 4 -> 4)
    ).toDF("a")

    val dfExample2 = Seq(
      Map[Int, String](1 -> "a", 2 -> "b", 3 -> null)
    ).toDF("b")

    def testNullValue(): Unit = {
      checkAnswer(dfExample1.selectExpr("transform_values(a, (k, v) -> null)"),
        Seq(Row(Map[Int, Integer](1 -> null, 2 -> null, 3 -> null, 4 -> null))))

      checkAnswer(dfExample2.selectExpr(
        "transform_values(b, (k, v) -> IF(v IS NULL, k + 1, k + 2))"),
        Seq(Row(Map(1 -> 3, 2 -> 4, 3 -> 4))))

      checkAnswer(dfExample1.select(transform_values(col("a"),
        (k, v) => lit(null).cast("int"))),
        Seq(Row(Map[Int, Integer](1 -> null, 2 -> null, 3 -> null, 4 -> null))))

      checkAnswer(dfExample2.select(
        transform_values(col("b"), (k, v) => when(v.isNull, k + 1).otherwise(k + 2))
        ),
        Seq(Row(Map(1 -> 3, 2 -> 4, 3 -> 4))))
    }

    testNullValue()
    dfExample1.cache()
    dfExample2.cache()
    testNullValue()
  }

  test("transform values function - test invalid functions") {
    val dfExample1 = Seq(
      Map[Int, Int](1 -> 1, 9 -> 9, 8 -> 8, 7 -> 7)
    ).toDF("i")

    val dfExample2 = Seq(
      Map[String, String]("a" -> "b")
    ).toDF("j")

    val dfExample3 = Seq(
      Seq(1, 2, 3, 4)
    ).toDF("x")

    def testInvalidLambdaFunctions(): Unit = {

      val ex1 = intercept[AnalysisException] {
        dfExample1.selectExpr("transform_values(i, k -> k)")
      }
      assert(ex1.getMessage.contains("The number of lambda function arguments '1' does not match"))

      val ex2 = intercept[AnalysisException] {
        dfExample2.selectExpr("transform_values(j, (k, v, x) -> k + 1)")
      }
      assert(ex2.getMessage.contains("The number of lambda function arguments '3' does not match"))

      val ex3 = intercept[AnalysisException] {
        dfExample3.selectExpr("transform_values(x, (k, v) -> k + 1)")
      }
      assert(ex3.getMessage.contains(
        "data type mismatch: argument 1 requires map type"))

      val ex3a = intercept[AnalysisException] {
        dfExample3.select(transform_values(col("x"), (k, v) => k + 1))
      }
      assert(ex3a.getMessage.contains(
        "data type mismatch: argument 1 requires map type"))
    }

    testInvalidLambdaFunctions()
    dfExample1.cache()
    dfExample2.cache()
    dfExample3.cache()
    testInvalidLambdaFunctions()
  }

  test("arrays zip_with function - for primitive types") {
    val df1 = Seq[(Seq[Integer], Seq[Integer])](
      (Seq(9001, 9002, 9003), Seq(4, 5, 6)),
      (Seq(1, 2), Seq(3, 4)),
      (Seq.empty, Seq.empty),
      (null, null)
    ).toDF("val1", "val2")
    val df2 = Seq[(Seq[Integer], Seq[Long])](
      (Seq(1, null, 3), Seq(1L, 2L)),
      (Seq(1, 2, 3), Seq(4L, 11L))
    ).toDF("val1", "val2")
    val expectedValue1 = Seq(
      Row(Seq(9005, 9007, 9009)),
      Row(Seq(4, 6)),
      Row(Seq.empty),
      Row(null))
    checkAnswer(df1.selectExpr("zip_with(val1, val2, (x, y) -> x + y)"), expectedValue1)
    checkAnswer(df1.select(zip_with(df1("val1"), df1("val2"), (x, y) => x + y)), expectedValue1)
    val expectedValue2 = Seq(
      Row(Seq(Row(1L, 1), Row(2L, null), Row(null, 3))),
      Row(Seq(Row(4L, 1), Row(11L, 2), Row(null, 3))))
    checkAnswer(df2.selectExpr("zip_with(val1, val2, (x, y) -> (y, x))"), expectedValue2)
    checkAnswer(
      df2.select(zip_with(df2("val1"), df2("val2"), (x, y) => struct(y, x))),
      expectedValue2
    )
  }

  test("arrays zip_with function - for non-primitive types") {
    val df = Seq(
      (Seq("a"), Seq("x", "y", "z")),
      (Seq("a", null), Seq("x", "y")),
      (Seq.empty[String], Seq.empty[String]),
      (Seq("a", "b", "c"), null)
    ).toDF("val1", "val2")
    val expectedValue1 = Seq(
      Row(Seq(Row("x", "a"), Row("y", null), Row("z", null))),
      Row(Seq(Row("x", "a"), Row("y", null))),
      Row(Seq.empty),
      Row(null))
    checkAnswer(
      df.selectExpr("zip_with(val1, val2, (x, y) -> (y, x))"),
      expectedValue1
    )
    checkAnswer(
      df.select(zip_with(col("val1"), col("val2"), (x, y) => struct(y, x))),
      expectedValue1
    )
  }

  test("arrays zip_with function - invalid") {
    val df = Seq(
      (Seq("c", "a", "b"), Seq("x", "y", "z"), 1),
      (Seq("b", null, "c", null), Seq("x"), 2),
      (Seq.empty, Seq("x", "z"), 3),
      (null, Seq("x", "z"), 4)
    ).toDF("a1", "a2", "i")
    val ex1 = intercept[AnalysisException] {
      df.selectExpr("zip_with(a1, a2, x -> x)")
    }
    assert(ex1.getMessage.contains("The number of lambda function arguments '1' does not match"))
    val ex2 = intercept[AnalysisException] {
      df.selectExpr("zip_with(a1, a2, (acc, x) -> x, (acc, x) -> x)")
    }
    assert(ex2.getMessage.contains("Invalid number of arguments for function zip_with"))
    val ex3 = intercept[AnalysisException] {
      df.selectExpr("zip_with(i, a2, (acc, x) -> x)")
    }
    assert(ex3.getMessage.contains("data type mismatch: argument 1 requires array type"))
    val ex3a = intercept[AnalysisException] {
      df.select(zip_with(df("i"), df("a2"), (acc, x) => x))
    }
    assert(ex3a.getMessage.contains("data type mismatch: argument 1 requires array type"))
    val ex4 = intercept[AnalysisException] {
      df.selectExpr("zip_with(a1, a, (acc, x) -> x)")
    }
    assert(ex4.getErrorClass == "MISSING_COLUMN")
    assert(ex4.messageParameters.head == "a")
  }

  private def assertValuesDoNotChangeAfterCoalesceOrUnion(v: Column): Unit = {
    import DataFrameFunctionsSuite.CodegenFallbackExpr
    for ((codegenFallback, wholeStage) <- Seq((true, false), (false, false), (false, true))) {
      val c = if (codegenFallback) {
        Column(CodegenFallbackExpr(v.expr))
      } else {
        v
      }
      withSQLConf(
        (SQLConf.CODEGEN_FALLBACK.key, codegenFallback.toString),
        (SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key, wholeStage.toString)) {
        val df = spark.range(0, 4, 1, 4).withColumn("c", c)
        val rows = df.collect()
        val rowsAfterCoalesce = df.coalesce(2).collect()
        assert(rows === rowsAfterCoalesce, "Values changed after coalesce when " +
          s"codegenFallback=$codegenFallback and wholeStage=$wholeStage.")

        val df1 = spark.range(0, 2, 1, 2).withColumn("c", c)
        val rows1 = df1.collect()
        val df2 = spark.range(2, 4, 1, 2).withColumn("c", c)
        val rows2 = df2.collect()
        val rowsAfterUnion = df1.union(df2).collect()
        assert(rowsAfterUnion === rows1 ++ rows2, "Values changed after union when " +
          s"codegenFallback=$codegenFallback and wholeStage=$wholeStage.")
      }
    }
  }

  test("SPARK-14393: values generated by non-deterministic functions shouldn't change after " +
    "coalesce or union") {
    Seq(
      monotonically_increasing_id(), spark_partition_id(),
      rand(Random.nextLong()), randn(Random.nextLong())
    ).foreach(assertValuesDoNotChangeAfterCoalesceOrUnion(_))
  }

  test("SPARK-21281 fails if functions have no argument") {
    val df = Seq(1).toDF("a")

    val funcsMustHaveAtLeastOneArg =
      ("coalesce", (df: DataFrame) => df.select(coalesce())) ::
      ("coalesce", (df: DataFrame) => df.selectExpr("coalesce()")) ::
      ("hash", (df: DataFrame) => df.select(hash())) ::
      ("hash", (df: DataFrame) => df.selectExpr("hash()")) ::
      ("xxhash64", (df: DataFrame) => df.select(xxhash64())) ::
      ("xxhash64", (df: DataFrame) => df.selectExpr("xxhash64()")) :: Nil
    funcsMustHaveAtLeastOneArg.foreach { case (name, func) =>
      val errMsg = intercept[AnalysisException] { func(df) }.getMessage
      assert(errMsg.contains(s"input to function $name requires at least one argument"))
    }

    val funcsMustHaveAtLeastTwoArgs =
      ("greatest", (df: DataFrame) => df.select(greatest())) ::
      ("greatest", (df: DataFrame) => df.selectExpr("greatest()")) ::
      ("least", (df: DataFrame) => df.select(least())) ::
      ("least", (df: DataFrame) => df.selectExpr("least()")) :: Nil
    funcsMustHaveAtLeastTwoArgs.foreach { case (name, func) =>
      val errMsg = intercept[AnalysisException] { func(df) }.getMessage
      assert(errMsg.contains(s"input to function $name requires at least two arguments"))
    }
  }

  test("SPARK-24734: Fix containsNull of Concat for array type") {
    val df = Seq((Seq(1), Seq[Integer](null), Seq("a", "b"))).toDF("k1", "k2", "v")
    val ex = intercept[Exception] {
      df.select(map_from_arrays(concat($"k1", $"k2"), $"v")).show()
    }
    assert(ex.getMessage.contains("Cannot use null as map key"))
  }

  test("SPARK-26370: Fix resolution of higher-order function for the same identifier") {
    val df = Seq(
      (Seq(1, 9, 8, 7), 1, 2),
      (Seq(5, 9, 7), 2, 2),
      (Seq.empty, 3, 2),
      (null, 4, 2)
    ).toDF("i", "x", "d")

    checkAnswer(df.selectExpr("x", "exists(i, x -> x % d == 0)"),
      Seq(
        Row(1, true),
        Row(2, false),
        Row(3, false),
        Row(4, null)))
    checkAnswer(df.filter("exists(i, x -> x % d == 0)"),
      Seq(Row(Seq(1, 9, 8, 7), 1, 2)))
    checkAnswer(df.select("x").filter("exists(i, x -> x % d == 0)"),
      Seq(Row(1)))
  }

  test("SPARK-29462: Empty array of NullType for array function with no arguments") {
    Seq((true, StringType), (false, NullType)).foreach {
      case (arrayDefaultToString, expectedType) =>
        withSQLConf(SQLConf.LEGACY_CREATE_EMPTY_COLLECTION_USING_STRING_TYPE.key ->
          arrayDefaultToString.toString) {
          val schema = spark.range(1).select(array()).schema
          assert(schema.nonEmpty && schema.head.dataType.isInstanceOf[ArrayType])
          val actualType = schema.head.dataType.asInstanceOf[ArrayType].elementType
          assert(actualType === expectedType)
        }
    }
  }

  test("SPARK-30790: Empty map with NullType as key/value type for map function with no argument") {
    Seq((true, StringType), (false, NullType)).foreach {
      case (mapDefaultToString, expectedType) =>
        withSQLConf(SQLConf.LEGACY_CREATE_EMPTY_COLLECTION_USING_STRING_TYPE.key ->
          mapDefaultToString.toString) {
          val schema = spark.range(1).select(map()).schema
          assert(schema.nonEmpty && schema.head.dataType.isInstanceOf[MapType])
          val actualKeyType = schema.head.dataType.asInstanceOf[MapType].keyType
          val actualValueType = schema.head.dataType.asInstanceOf[MapType].valueType
          assert(actualKeyType === expectedType)
          assert(actualValueType === expectedType)
        }
    }
  }

  test("SPARK-26071: convert map to array and use as map key") {
    val df = Seq(Map(1 -> "a")).toDF("m")
    intercept[AnalysisException](df.select(map($"m", lit(1))))
    checkAnswer(
      df.select(map(map_entries($"m"), lit(1))),
      Row(Map(Seq(Row(1, "a")) -> 1)))
  }

  test("SPARK-34794: lambda variable name issues in nested functions") {
    val df1 = Seq((Seq(1, 2), Seq("a", "b"))).toDF("numbers", "letters")

    checkAnswer(df1.select(flatten(transform($"numbers", (number: Column) =>
      transform($"letters", (letter: Column) =>
        struct(number, letter))))),
      Seq(Row(Seq(Row(1, "a"), Row(1, "b"), Row(2, "a"), Row(2, "b"))))
    )
    checkAnswer(df1.select(flatten(transform($"numbers", (number: Column, i: Column) =>
      transform($"letters", (letter: Column, j: Column) =>
        struct(number + j, concat(letter, i)))))),
      Seq(Row(Seq(Row(1, "a0"), Row(2, "b0"), Row(2, "a1"), Row(3, "b1"))))
    )

    val df2 = Seq((Map("a" -> 1, "b" -> 2), Map("a" -> 2, "b" -> 3))).toDF("m1", "m2")

    checkAnswer(df2.select(map_zip_with($"m1", $"m2", (k1: Column, ov1: Column, ov2: Column) =>
      map_zip_with($"m1", $"m2", (k2: Column, iv1: Column, iv2: Column) =>
        ov1 + iv1 + ov2 + iv2))),
      Seq(Row(Map("a" -> Map("a" -> 6, "b" -> 8), "b" -> Map("a" -> 8, "b" -> 10))))
    )
  }
}

object DataFrameFunctionsSuite {
  case class CodegenFallbackExpr(child: Expression) extends UnaryExpression with CodegenFallback {
    override def nullable: Boolean = child.nullable
    override def dataType: DataType = child.dataType
    override lazy val resolved = true
    override def eval(input: InternalRow): Any = child.eval(input)
    override protected def withNewChildInternal(newChild: Expression): CodegenFallbackExpr =
      copy(child = newChild)
  }
}
