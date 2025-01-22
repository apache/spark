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
import java.sql.{Date, Timestamp}
import java.time.LocalDateTime

import org.apache.spark.SparkThrowable
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.variant.VariantExpressionEvalUtils
import org.apache.spark.sql.catalyst.util.DateTimeConstants._
import org.apache.spark.sql.catalyst.util.DateTimeUtils._
import org.apache.spark.sql.execution.datasources.parquet.{ParquetTest, SparkShreddingUtils}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._
import org.apache.spark.types.variant._
import org.apache.spark.unsafe.types.{UTF8String, VariantVal}

class VariantShreddingSuite extends QueryTest with SharedSparkSession with ParquetTest {
  def parseJson(s: String): VariantVal = {
    val v = VariantBuilder.parseJson(s, false)
    new VariantVal(v.getValue, v.getMetadata)
  }

  // Make a variant value binary by parsing a JSON string.
  def value(s: String): Array[Byte] = VariantBuilder.parseJson(s, false).getValue

  // Make a variant metadata binary that includes a set of keys.
  def metadata(keys: Seq[String]): Array[Byte] = {
    val builder = new VariantBuilder(false)
    keys.foreach(builder.addKey)
    builder.result().getMetadata
  }

  // Build a shredded variant value binary. Its IDs refer to the metadata built from `metadataKeys`,
  // which can include more keys than the JSON string contains.
  def shreddedValue(s: String, metadataKeys: Seq[String]): Array[Byte] = {
    val builder = new VariantBuilder(false)
    metadataKeys.foreach(builder.addKey)
    builder.appendVariant(VariantBuilder.parseJson(s, false))
    builder.result().getValue
  }

  // Given an expected schema of a Variant value, return a write schema with a single column `v`
  // with the corresponding shredding schema.
  def writeSchema(schema: DataType): StructType =
    StructType(Array(StructField("v", SparkShreddingUtils.variantShreddingSchema(schema))))

  def withPushConfigs(pushConfigs: Seq[Boolean] = Seq(true, false))(fn: => Unit): Unit = {
    for (push <- pushConfigs) {
      withSQLConf(SQLConf.PUSH_VARIANT_INTO_SCAN.key -> push.toString) {
        fn
      }
    }
  }

  def isPushEnabled: Boolean = SQLConf.get.getConf(SQLConf.PUSH_VARIANT_INTO_SCAN)

  def testWithTempPath(name: String)(block: File => Unit): Unit = test(name) {
    withPushConfigs() {
      withTempPath { path =>
        block(path)
      }
    }
  }

  def writeRows(path: File, schema: StructType, rows: Row*): Unit =
    spark.createDataFrame(spark.sparkContext.parallelize(rows.map(Row(_)), numSlices = 1), schema)
      .write.mode("overwrite").parquet(path.getAbsolutePath)

  def writeRows(path: File, schema: String, rows: Row*): Unit =
    writeRows(path, StructType.fromDDL(schema), rows: _*)

  def read(path: File): DataFrame =
    spark.read.schema("v variant").parquet(path.getAbsolutePath)

  def checkExpr(path: File, expr: String, expected: Any*): Unit = withAllParquetReaders {
    checkAnswer(read(path).selectExpr(expr), expected.map(Row(_)))
  }

  def checkException(path: File, expr: String, msg: String): Unit = withAllParquetReaders {
    val ex = intercept[Exception with SparkThrowable] {
      read(path).selectExpr(expr).collect()
    }
    // When reading with the parquet-mr reader, the expected message can be nested in
    // `ex.getCause.getCause`.
    assert(ex.getMessage.contains(msg) || ex.getCause.getMessage.contains(msg)
      || ex.getCause.getCause.getMessage.contains(msg))
  }

  testWithTempPath("scalar types rebuild") { path =>
    val scalarTypes = Array(
      BooleanType, ByteType, ShortType, IntegerType, LongType, FloatType, DoubleType,
      TimestampType, TimestampNTZType, DateType,
      StringType, BinaryType,
      DecimalType(9, 3), DecimalType(18, 6), DecimalType(22, 9))
    val schema = StructType(scalarTypes.zipWithIndex.map { case (t, i) =>
      StructField(i.toString, t)
    })

    val values = Seq[Any](
      true, 1.toByte, 2.toShort, 3, 4L, 5.5F, 6.6,
      new Timestamp(7), LocalDateTime.of(1, 1, 1, 0, 0, 8, 0), new Date(9),
      "str10", Array[Byte](11),
      Decimal("12.12"), Decimal("13.13"), Decimal("14.14")).map(Row(null, _))
    val row = Row(metadata(scalarTypes.indices.map(_.toString)), null, Row.fromSeq(values))

    writeRows(path, writeSchema(schema), row)
    for (tz <- Seq("Etc/UTC", "America/Los_Angeles")) {
      withSQLConf(SQLConf.SESSION_LOCAL_TIMEZONE.key -> tz) {
        val timestamp = if (tz == "Etc/UTC") {
          "1970-01-01 00:00:00.007+00:00"
        } else {
          "1969-12-31 16:00:00.007-08:00"
        }
        checkExpr(path, "to_json(v)",
          """{"0":true,"1":1,"10":"str10","11":"Cw==","12":12.12,"13":13.13,"14":14.14,""" +
            s""""2":2,"3":3,"4":4,"5":5.5,"6":6.6,"7":"$timestamp",""" +
            """"8":"0001-01-01 00:00:08","9":"1969-12-31"}""")
        checkExpr(path, "variant_get(v, '$.0', 'int')", 1)
        checkExpr(path, "variant_get(v, '$.2', 'boolean')", true)
        checkExpr(path, "variant_get(v, '$.6', 'float')", 6.6F)
        checkExpr(path, "variant_get(v, '$.11', 'string')", new String(Array[Byte](11)))
        checkExpr(path, "variant_get(v, '$.14', 'decimal(9, 1)')", BigDecimal("14.1"))
      }
    }
  }

  testWithTempPath("object rebuild") { path =>
    writeRows(path, writeSchema(StructType.fromDDL("b int, d int")),
      Row(metadata(Seq("b", "d")), null, Row(Row(null, 1), Row(null, null))),
      Row(metadata(Seq("b", "d")), null, Row(Row(null, 1), Row(value("null"), null))),
      Row(metadata(Seq("a", "b", "c", "d")),
        shreddedValue("""{"a": 1, "c": 3}""", Seq("a", "b", "c", "d")),
        Row(Row(null, 2), Row(value("4"), null))),
      Row(metadata(Nil), value("null"), null),
      null)
    checkExpr(path, "to_json(v)", """{"b":1}""", """{"b":1,"d":null}""",
      """{"a":1,"b":2,"c":3,"d":4}""", "null", null)
    checkExpr(path, "variant_get(v, '$.b', 'string')", "1", "1", "2", null, null)
    checkExpr(path, "variant_get(v, '$.d', 'string')", null, null, "4", null, null)
  }

  testWithTempPath("array rebuild") { path =>
    writeRows(path, writeSchema(ArrayType(IntegerType)),
      Row(metadata(Nil), null, Array(Row(null, 1), Row(null, 2), Row(value("3"), null))),
      Row(metadata(Seq("a", "b")), null, Array(
        Row(shreddedValue("""{"a": 1}""", Seq("a", "b")), null),
        Row(shreddedValue("""{"b": 2}""", Seq("a", "b")), null))),
      Row(metadata(Seq("a", "b")), value("""{"a": 1, "b": 2}"""), null))
    checkExpr(path, "to_json(v)", """[1,2,3]""", """[{"a":1},{"b":2}]""", """{"a":1,"b":2}""")
    checkExpr(path, "variant_get(v, '$[2]', 'int')", 3, null, null)
    checkExpr(path, "variant_get(v, '$[1].b', 'int')", null, 2, null)
    checkExpr(path, "variant_get(v, '$.a', 'long')", null, null, 1L)
  }

  testWithTempPath("malformed input") { path =>
    // Top-level variant must not be missing.
    writeRows(path, writeSchema(IntegerType), Row(metadata(Nil), null, null))
    checkException(path, "v", "MALFORMED_VARIANT")

    // Array-element variant must not be missing.
    writeRows(path, writeSchema(ArrayType(IntegerType)),
      Row(metadata(Nil), null, Array(Row(null, null))))
    checkException(path, "v", "MALFORMED_VARIANT")
    checkException(path, "variant_get(v, '$[0]')", "MALFORMED_VARIANT")

    // Shredded field must not be null.
    // Construct the schema manually, because SparkShreddingUtils.variantShreddingSchema will make
    // `a` non-nullable, which would prevent us from writing the file.
    val schema = StructType(Seq(StructField("v", StructType(Seq(
      StructField("metadata", BinaryType),
      StructField("value", BinaryType),
      StructField("typed_value", StructType(Seq(
        StructField("a", StructType(Seq(
          StructField("value", BinaryType),
          StructField("typed_value", BinaryType))))))))))))
    writeRows(path, schema, Row(metadata(Seq("a")), null, Row(null)))
    checkException(path, "v", "MALFORMED_VARIANT")
    checkException(path, "variant_get(v, '$.a')", "MALFORMED_VARIANT")

    // `value` must not contain any shredded field.
    writeRows(path, writeSchema(StructType.fromDDL("a int")),
      Row(metadata(Seq("a")), value("""{"a": 1}"""), Row(Row(null, null))))
    checkException(path, "v", "MALFORMED_VARIANT")
    checkException(path, "cast(v as map<string, int>)", "MALFORMED_VARIANT")
    if (isPushEnabled) {
      checkExpr(path, "cast(v as struct<a int>)", Row(null))
      checkExpr(path, "variant_get(v, '$.a', 'int')", null)
    } else {
      checkException(path, "cast(v as struct<a int>)", "MALFORMED_VARIANT")
      checkException(path, "variant_get(v, '$.a', 'int')", "MALFORMED_VARIANT")
    }

    // Scalar reader reads from `typed_value` if both `value` and `typed_value` are not null.
    // Cast from `value` succeeds, cast from `typed_value` fails.
    writeRows(path, "v struct<metadata binary, value binary, typed_value string>",
      Row(metadata(Nil), value("1"), "invalid"))
    checkException(path, "cast(v as int)", "INVALID_VARIANT_CAST")
    checkExpr(path, "try_cast(v as int)", null)

    // Cast from `value` fails, cast from `typed_value` succeeds.
    writeRows(path, "v struct<metadata binary, value binary, typed_value string>",
      Row(metadata(Nil), value("\"invalid\""), "1"))
    checkExpr(path, "cast(v as int)", 1)
    checkExpr(path, "try_cast(v as int)", 1)
  }

  testWithTempPath("extract from shredded object") { path =>
    val keys1 = Seq("a", "b", "c", "d")
    val keys2 = Seq("a", "b", "c", "e", "f")
    writeRows(path, "v struct<metadata binary, value binary, typed_value struct<" +
      "a struct<value binary, typed_value int>, b struct<value binary>," +
      "c struct<typed_value decimal(20, 10)>>>",
      // {"a":1,"b":"2","c":3.3,"d":4.4}, d is in the left over value.
      Row(metadata(keys1), shreddedValue("""{"d": 4.4}""", keys1),
        Row(Row(null, 1), Row(value("\"2\"")), Row(Decimal("3.3")))),
      // {"a":5.4,"b":-6,"e":{"f":[true]}}, e is in the left over value.
      Row(metadata(keys2), shreddedValue("""{"e": {"f": [true]}}""", keys2),
        Row(Row(value("5.4"), null), Row(value("-6")), Row(null))),
      // [{"a":1}], the unshredded array at the top-level is put into `value` as a whole.
      Row(metadata(Seq("a")), value("""[{"a": 1}]"""), null))

    checkAnswer(read(path).selectExpr("variant_get(v, '$.a', 'int')",
      "variant_get(v, '$.b', 'long')", "variant_get(v, '$.c', 'double')",
      "variant_get(v, '$.d', 'decimal(9, 4)')"),
      Seq(Row(1, 2L, 3.3, BigDecimal("4.4")), Row(5, -6L, null, null), Row(null, null, null, null)))
    checkExpr(path, "variant_get(v, '$.e.f[0]', 'boolean')", null, true, null)
    checkExpr(path, "variant_get(v, '$[0].a', 'boolean')", null, null, true)
    checkExpr(path, "try_cast(v as struct<a float, e variant>)",
      Row(1.0F, null), Row(5.4F, parseJson("""{"f": [true]}""")), null)

    // String "2" cannot be cast into boolean.
    checkException(path, "variant_get(v, '$.b', 'boolean')", "INVALID_VARIANT_CAST")
    // Decimal cannot be cast into date.
    checkException(path, "variant_get(v, '$.c', 'date')", "INVALID_VARIANT_CAST")
    // The value of `c` doesn't fit into `decimal(1, 1)`.
    checkException(path, "variant_get(v, '$.c', 'decimal(1, 1)')", "INVALID_VARIANT_CAST")
    checkExpr(path, "try_variant_get(v, '$.b', 'boolean')", null, true, null)
    // Scalar cannot be cast into struct.
    checkException(path, "variant_get(v, '$.a', 'struct<a int>')", "INVALID_VARIANT_CAST")
    checkExpr(path, "try_variant_get(v, '$.a', 'struct<a int>')", null, null, null)

    checkExpr(path, "try_cast(v as map<string, double>)",
      Map("a" -> 1.0, "b" -> 2.0, "c" -> 3.3, "d" -> 4.4),
      Map("a" -> 5.4, "b" -> -6.0, "e" -> null), null)
    checkExpr(path, "try_cast(v as array<string>)", null, null, Seq("""{"a":1}"""))

    val strings = Seq("""{"a":1,"b":"2","c":3.3,"d":4.4}""",
      """{"a":5.4,"b":-6,"e":{"f":[true]}}""", """[{"a":1}]""")
    checkExpr(path, "cast(v as string)", strings: _*)
    checkExpr(path, "v",
      VariantExpressionEvalUtils.castToVariant(
        InternalRow(1, UTF8String.fromString("2"), Decimal("3.3000000000"), Decimal("4.4")),
        StructType.fromDDL("a int, b string, c decimal(20, 10), d decimal(2, 1)")
      ),
      parseJson(strings(1)),
      parseJson(strings(2))
    )
  }

  testWithTempPath("extract from shredded array") { path =>
    val keys = Seq("a", "b")
    writeRows(path, "v struct<metadata binary, value binary, typed_value array<" +
      "struct<value binary, typed_value struct<a struct<value binary, typed_value string>>>>>",
      // [{"a":"2000-01-01"},{"a":"1000-01-01","b":[7]}], b is in the left over value.
      Row(metadata(keys), null, Array(
        Row(null, Row(Row(null, "2000-01-01"))),
        Row(shreddedValue("""{"b": [7]}""", keys), Row(Row(null, "1000-01-01"))))),
      // [null,{"a":null},{"a":"null"},{}]
      Row(metadata(keys), null, Array(
        Row(value("null"), null),
        Row(null, Row(Row(value("null"), null))),
        Row(null, Row(Row(null, "null"))),
        Row(null, Row(Row(null, null))))))

    val date1 = Date.valueOf("2000-01-01")
    val date2 = Date.valueOf("1000-01-01")
    checkExpr(path, "variant_get(v, '$[0].a', 'date')", date1, null)
    // try_cast succeeds.
    checkExpr(path, "try_variant_get(v, '$[1].a', 'date')", date2, null)
    // The first array returns null because of out-of-bound index.
    // The second array returns "null".
    checkExpr(path, "variant_get(v, '$[2].a', 'string')", null, "null")
    // Return null because of invalid cast.
    checkExpr(path, "try_variant_get(v, '$[1].a', 'int')", null, null)

    checkExpr(path, "variant_get(v, '$[0].b[0]', 'int')", null, null)
    checkExpr(path, "variant_get(v, '$[1].b[0]', 'int')", 7, null)
    // Validate timestamp-related casts uses the session time zone correctly.
    Seq("Etc/UTC", "America/Los_Angeles").foreach { tz =>
      withSQLConf(SQLConf.SESSION_LOCAL_TIMEZONE.key -> tz) {
        val expected = sql("select timestamp'1000-01-01', timestamp_ntz'1000-01-01'").head()
        checkAnswer(read(path).selectExpr("variant_get(v, '$[1].a', 'timestamp')",
          "variant_get(v, '$[1].a', 'timestamp_ntz')"), Seq(expected, Row(null, null)))
      }
    }
    checkException(path, "variant_get(v, '$[0]', 'int')", "INVALID_VARIANT_CAST")
    // An out-of-bound array access produces null. It never causes an invalid cast.
    checkExpr(path, "variant_get(v, '$[4]', 'int')", null, null)

    checkExpr(path, "cast(v as array<struct<a string, b array<int>>>)",
      Seq(Row("2000-01-01", null), Row("1000-01-01", Seq(7))),
      Seq(null, Row(null, null), Row("null", null), Row(null, null)))
    checkExpr(path, "cast(v as array<map<string, string>>)",
      Seq(Map("a" -> "2000-01-01"), Map("a" -> "1000-01-01", "b" -> "[7]")),
      Seq(null, Map("a" -> null), Map("a" -> "null"), Map()))
    checkExpr(path, "try_cast(v as array<map<string, date>>)",
      Seq(Map("a" -> date1), Map("a" -> date2, "b" -> null)),
      Seq(null, Map("a" -> null), Map("a" -> null), Map()))

    val strings = Seq("""[{"a":"2000-01-01"},{"a":"1000-01-01","b":[7]}]""",
      """[null,{"a":null},{"a":"null"},{}]""")
    checkExpr(path, "cast(v as string)", strings: _*)
    checkExpr(path, "v", strings.map(parseJson): _*)
  }

  testWithTempPath("missing fields") { path =>
    writeRows(path, "v struct<metadata binary, typed_value struct<" +
      "a struct<value binary, typed_value int>, b struct<typed_value int>>>",
      Row(metadata(Nil), Row(Row(null, null), Row(null))),
      Row(metadata(Nil), Row(Row(value("null"), null), Row(null))),
      Row(metadata(Nil), Row(Row(null, 1), Row(null))),
      Row(metadata(Nil), Row(Row(null, null), Row(2))),
      Row(metadata(Nil), Row(Row(value("null"), null), Row(2))),
      Row(metadata(Nil), Row(Row(null, 3), Row(4))))

    val strings = Seq("{}", """{"a":null}""", """{"a":1}""", """{"b":2}""", """{"a":null,"b":2}""",
      """{"a":3,"b":4}""")
    checkExpr(path, "cast(v as string)", strings: _*)
    checkExpr(path, "v", strings.map(parseJson): _*)

    checkExpr(path, "variant_get(v, '$.a', 'string')", null, null, "1", null, null, "3")
    checkExpr(path, "variant_get(v, '$.a')", null, parseJson("null"), parseJson("1"), null,
      parseJson("null"), parseJson("3"))
  }

  testWithTempPath("custom casts") { path =>
    writeRows(path, writeSchema(LongType),
      Row(metadata(Nil), null, Long.MaxValue / MICROS_PER_SECOND + 1),
      Row(metadata(Nil), null, Long.MaxValue / MICROS_PER_SECOND))

    // long -> timestamp
    checkException(path, "cast(v as timestamp)", "INVALID_VARIANT_CAST")
    checkExpr(path, "try_cast(v as timestamp)",
      null, toJavaTimestamp(Long.MaxValue / MICROS_PER_SECOND * MICROS_PER_SECOND))

    writeRows(path, writeSchema(DecimalType(38, 19)),
      Row(metadata(Nil), null, Decimal("1E18")),
      Row(metadata(Nil), null, Decimal("100")),
      Row(metadata(Nil), null, Decimal("10")),
      Row(metadata(Nil), null, Decimal("1")),
      Row(metadata(Nil), null, Decimal("0")),
      Row(metadata(Nil), null, Decimal("0.1")),
      Row(metadata(Nil), null, Decimal("0.01")),
      Row(metadata(Nil), null, Decimal("1E-18")))

    checkException(path, "cast(v as timestamp)", "INVALID_VARIANT_CAST")
    // decimal -> timestamp
    checkExpr(path, "try_cast(v as timestamp)",
      (null +: Seq(100000000, 10000000, 1000000, 0, 100000, 10000, 0).map(toJavaTimestamp(_))): _*)
    // decimal -> string
    checkExpr(path, "cast(v as string)",
      "1000000000000000000", "100", "10", "1", "0", "0.1", "0.01", "0.000000000000000001")
  }
}
