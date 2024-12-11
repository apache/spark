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
import org.apache.spark.sql.execution.datasources.parquet.{ParquetTest, SparkShreddingUtils}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._
import org.apache.spark.types.variant._

class VariantShreddingSuite extends QueryTest with SharedSparkSession with ParquetTest {
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

  def testWithTempPath(name: String)(block: File => Unit): Unit = test(name) {
    withTempPath { path =>
      block(path)
    }
  }

  def writeRows(path: File, schema: StructType, rows: Row*): Unit =
    spark.createDataFrame(spark.sparkContext.parallelize(rows.map(Row(_)), numSlices = 1), schema)
      .write.mode("overwrite").parquet(path.getAbsolutePath)

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
    writeRows(path, schema,
      Row(metadata(Seq("a")), null, Row(null)))
    checkException(path, "v", "MALFORMED_VARIANT")
    // `value` must not contain any shredded field.
    writeRows(path, writeSchema(StructType.fromDDL("a int")),
      Row(metadata(Seq("a")), value("""{"a": 1}"""), Row(Row(null, null))))
    checkException(path, "v", "MALFORMED_VARIANT")
  }
}
