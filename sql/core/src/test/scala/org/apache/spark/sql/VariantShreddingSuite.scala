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

import org.apache.spark.sql.execution.datasources.parquet.ParquetTest
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

  // Make a variant value binary by parsing a JSON string on a pre-defined metadata key set.
  def shreddedValue(s: String, metadataKeys: Seq[String]): Array[Byte] = {
    val builder = new VariantBuilder(false)
    metadataKeys.foreach(builder.addKey)
    builder.appendVariant(VariantBuilder.parseJson(s, false))
    builder.result().getValue
  }

  def checkRead(path: File, expected: Seq[String]): Unit = {
    withAllParquetReaders {
      val df = spark.read.schema("v variant").parquet(path.getAbsolutePath)
        .selectExpr("to_json(v)")
      checkAnswer(df, expected.map(Row(_)))
    }
  }

  test("scalar types rebuild") {
    val scalarTypes = Array(
      BooleanType, ByteType, ShortType, IntegerType, LongType, FloatType, DoubleType,
      TimestampType, TimestampNTZType, DateType,
      StringType, BinaryType,
      DecimalType(9, 3), DecimalType(18, 6), DecimalType(22, 9))
    val obj = StructType(scalarTypes.zipWithIndex.map { case (t, i) =>
      StructField(i.toString, StructType(Array(StructField("typed_value", t))))
    })
    val v = StructType(Array(
      StructField("typed_value", obj),
      StructField("value", BinaryType),
      StructField("metadata", BinaryType)
    ))

    val values = Seq[Any](
      true, 1.toByte, 2.toShort, 3, 4L, 5.5F, 6.6,
      new Timestamp(7), LocalDateTime.of(1, 1, 1, 0, 0, 8, 0), new Date(9),
      "str10", Array[Byte](11),
      Decimal("12.12"), Decimal("13.13"), Decimal("14.14"))
    val row = Row(Row.fromSeq(values.map(Row(_))), null,
      metadata(scalarTypes.indices.map(_.toString)))

    withTempPath { path =>
      spark.createDataFrame(spark.sparkContext.parallelize(Seq(Row(row))),
          StructType(Array(StructField("v", v))))
        .write.parquet(path.getAbsolutePath)
      for (tz <- Seq("Etc/UTC", "America/Los_Angeles")) {
        withSQLConf(SQLConf.SESSION_LOCAL_TIMEZONE.key -> tz) {
          val timestamp = if (tz == "Etc/UTC") {
            "1970-01-01 00:00:00.007+00:00"
          } else {
            "1969-12-31 16:00:00.007-08:00"
          }
          checkRead(path, Seq(
            """{"0":true,"1":1,"10":"str10","11":"Cw==","12":12.12,"13":13.13,"14":14.14,""" +
              s""""2":2,"3":3,"4":4,"5":5.5,"6":6.6,"7":"$timestamp",""" +
              """"8":"0001-01-01 00:00:08","9":"1969-12-31"}"""))
        }
      }
    }
  }

  test("object rebuild") {
    val schema = StructType.fromDDL("v struct<metadata binary, value binary, " +
      "typed_value struct<b struct<typed_value int, value binary>, " +
      "d struct<typed_value int, value binary>>>")
    withTempPath { path =>
      spark.createDataFrame(spark.sparkContext.parallelize(Seq(
          Row(metadata(Seq("b", "d")), null, Row(Row(1, null), Row(null, null))),
          Row(metadata(Seq("b", "d")), null, Row(Row(1, null), Row(null, value("null")))),
          Row(metadata(Seq("a", "b", "c", "d")),
            shreddedValue("""{"a": 1, "c": 3}""", Seq("a", "b", "c", "d")),
            Row(Row(2, null), Row(null, value("4")))),
          Row(metadata(Nil), value("null"), null),
          null).map(Row(_))), schema)
        .write.parquet(path.getAbsolutePath)
      checkRead(path, Seq(
        """{"b":1}""",
        """{"b":1,"d":null}""",
        """{"a":1,"b":2,"c":3,"d":4}""",
        "null",
        null))
    }
  }

  test("array rebuild") {
    val schema = StructType.fromDDL("v struct<metadata binary, value binary, " +
      "typed_value array<struct<typed_value int, value binary>>>")
    withTempPath { path =>
      spark.createDataFrame(spark.sparkContext.parallelize(Seq(
          Row(metadata(Nil), null, Array(Row(1, null), Row(2, null), Row(null, value("3")))),
          Row(metadata(Seq("a", "b")), null, Array(
            Row(null, shreddedValue("""{"a": 1}""", Seq("a", "b"))),
            Row(null, shreddedValue("""{"b": 2}""", Seq("a", "b"))))),
          Row(metadata(Seq("a", "b")), value("""{"a": 1, "b": 2}"""), null)
        ).map(Row(_))), schema)
        .write.parquet(path.getAbsolutePath)
      checkRead(path, Seq("""[1,2,3]""", """[{"a":1},{"b":2}]""", """{"a":1,"b":2}"""))
    }
  }
}
