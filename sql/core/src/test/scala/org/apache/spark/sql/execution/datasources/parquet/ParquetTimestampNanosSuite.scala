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

package org.apache.spark.sql.execution.datasources.parquet

import java.io.File

import org.apache.hadoop.fs.Path
import org.apache.parquet.example.data.simple.SimpleGroupFactory
import org.apache.parquet.hadoop.ParquetFileWriter.Mode
import org.apache.parquet.hadoop.example.ExampleParquetWriter
import org.apache.parquet.schema.{LogicalTypeAnnotation, MessageType, Types}
import org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64

import org.apache.spark.SparkException
import org.apache.spark.sql.{AnalysisException, QueryTest, Row}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._

class ParquetTimestampNanosSuite extends QueryTest with ParquetTest with SharedSparkSession {

  private def withNanosEnabled(f: => Unit): Unit = {
    withSQLConf(
      SQLConf.TYPES_FRAMEWORK_ENABLED.key -> "true",
      SQLConf.TIMESTAMP_NANOS_TYPES_ENABLED.key -> "true")(f)
  }

  private def hasCause(t: Throwable, clazz: Class[_]): Boolean = {
    var cur = t
    while (cur != null) {
      if (clazz.isInstance(cur)) return true
      cur = cur.getCause
    }
    false
  }

  private def writeForeignNanosParquet(
      file: File,
      isAdjustedToUTC: Boolean,
      values: Seq[Option[Long]]): Unit = {
    val schema: MessageType = Types.buildMessage()
      .optional(INT64)
      .as(LogicalTypeAnnotation.timestampType(isAdjustedToUTC, TimeUnit.NANOS))
      .named("ts")
      .named("spark_schema")
    val conf = spark.sessionState.newHadoopConf()
    val writer = ExampleParquetWriter.builder(new Path(file.toURI))
      .withType(schema)
      .withConf(conf)
      .withWriteMode(Mode.OVERWRITE)
      .build()
    try {
      val factory = new SimpleGroupFactory(schema)
      values.foreach { v =>
        val group = factory.newGroup()
        v.foreach(x => group.add("ts", x))
        writer.write(group)
      }
    } finally {
      writer.close()
    }
  }

  test("SPARK-57102: Spark write/read round-trips nanos value and precision") {
    withNanosEnabled {
      Seq("true", "false").foreach { vectorized =>
        withSQLConf(SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key -> vectorized) {
          Seq(7, 8, 9).foreach { p =>
            withTempPath { dir =>
              val frac = "123456789".take(p)
              val df = spark.sql(
                s"""SELECT * FROM VALUES
                   |  (TIMESTAMP_NTZ '2020-01-01 12:34:56.$frac',
                   |   TIMESTAMP_LTZ '2020-01-01 12:34:56.$frac'),
                   |  (TIMESTAMP_NTZ '1969-12-31 23:59:59.$frac',
                   |   TIMESTAMP_LTZ '1969-12-31 23:59:59.$frac'),
                   |  (CAST(NULL AS TIMESTAMP_NTZ($p)), CAST(NULL AS TIMESTAMP_LTZ($p)))
                   |  AS t(ntz, ltz)""".stripMargin)
              assert(df.schema("ntz").dataType === TimestampNTZNanosType(p))
              assert(df.schema("ltz").dataType === TimestampLTZNanosType(p))

              df.write.parquet(dir.getCanonicalPath)
              val read = spark.read.parquet(dir.getCanonicalPath)

              assert(read.schema("ntz").dataType === TimestampNTZNanosType(p))
              assert(read.schema("ltz").dataType === TimestampLTZNanosType(p))
              checkAnswer(read, df.collect().toSeq)
            }
          }
        }
      }
    }
  }

  test("SPARK-57102: read a foreign TIMESTAMP(NANOS) file as nanosecond timestamp types") {
    withNanosEnabled {
      withSQLConf(
        SQLConf.SESSION_LOCAL_TIMEZONE.key -> "UTC",
        SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key -> "false") {
        val values = Seq(Some(123L), Some(1000000123L), Some(-1L), None)

        withTempPath { dir =>
          val file = new File(dir, "ntz.parquet")
          writeForeignNanosParquet(file, isAdjustedToUTC = false, values)
          val read = spark.read.parquet(file.getCanonicalPath)
          assert(read.schema("ts").dataType === TimestampNTZNanosType(9))
          checkAnswer(read, spark.sql(
            """SELECT * FROM VALUES
              |  (TIMESTAMP_NTZ '1970-01-01 00:00:00.000000123'),
              |  (TIMESTAMP_NTZ '1970-01-01 00:00:01.000000123'),
              |  (TIMESTAMP_NTZ '1969-12-31 23:59:59.999999999'),
              |  (CAST(NULL AS TIMESTAMP_NTZ(9)))
              |  AS t(ts)""".stripMargin).collect().toSeq)
        }

        withTempPath { dir =>
          val file = new File(dir, "ltz.parquet")
          writeForeignNanosParquet(file, isAdjustedToUTC = true, values)
          val read = spark.read.parquet(file.getCanonicalPath)
          assert(read.schema("ts").dataType === TimestampLTZNanosType(9))
          checkAnswer(read, spark.sql(
            """SELECT * FROM VALUES
              |  (TIMESTAMP_LTZ '1970-01-01 00:00:00.000000123'),
              |  (TIMESTAMP_LTZ '1970-01-01 00:00:01.000000123'),
              |  (TIMESTAMP_LTZ '1969-12-31 23:59:59.999999999'),
              |  (CAST(NULL AS TIMESTAMP_LTZ(9)))
              |  AS t(ts)""".stripMargin).collect().toSeq)
        }
      }
    }
  }

  test("SPARK-57102: legacy nanosAsLong reads a foreign TIMESTAMP(NANOS) file as LongType") {
    withTempPath { dir =>
      val file = new File(dir, "foreign.parquet")
      writeForeignNanosParquet(file, isAdjustedToUTC = false, Seq(Some(123L), Some(-1L)))
      withSQLConf(
        SQLConf.TIMESTAMP_NANOS_TYPES_ENABLED.key -> "true",
        SQLConf.LEGACY_PARQUET_NANOS_AS_LONG.key -> "true",
        SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key -> "false") {
        val read = spark.read.parquet(file.getCanonicalPath)
        assert(read.schema("ts").dataType === LongType)
        checkAnswer(read, Seq(Row(123L), Row(-1L)))
      }
    }
  }

  test("SPARK-57102: reading a foreign TIMESTAMP(NANOS) file fails when the feature is disabled") {
    withTempPath { dir =>
      val file = new File(dir, "foreign.parquet")
      writeForeignNanosParquet(file, isAdjustedToUTC = false, Seq(Some(123L)))
      withSQLConf(
        SQLConf.TIMESTAMP_NANOS_TYPES_ENABLED.key -> "false",
        SQLConf.LEGACY_PARQUET_NANOS_AS_LONG.key -> "false") {
        val e = intercept[AnalysisException] {
          spark.read.parquet(file.getCanonicalPath).schema
        }
        assert(e.getMessage.contains("NANOS") || e.getMessage.contains("Parquet"))
      }
    }
  }

  test("SPARK-57102: writing a timestamp outside the INT64 epoch-nanos range fails loudly") {
    withNanosEnabled {
      withTempPath { dir =>
        val df = spark.sql("SELECT TIMESTAMP_NTZ '9999-12-31 23:59:59.999999999' AS ntz")
        val e = intercept[SparkException] {
          df.write.parquet(dir.getCanonicalPath)
        }
        assert(
          hasCause(e, classOf[ArithmeticException]),
          s"Expected an arithmetic overflow, but got: ${e.getMessage}")
      }
    }
  }

  test("SPARK-57102: nanos timestamps round-trip inside a nested (array) column") {
    withNanosEnabled {
      withSQLConf(SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key -> "false") {
        withTempPath { dir =>
          val df = spark.sql(
            "SELECT array(TIMESTAMP_NTZ '2020-01-01 00:00:00.123456789', " +
            "TIMESTAMP_NTZ '1969-12-31 23:59:59.000000001') AS arr")
          df.write.parquet(dir.getCanonicalPath)
          val read = spark.read.parquet(dir.getCanonicalPath)
          assert(read.schema("arr").dataType.asInstanceOf[ArrayType].elementType ===
            TimestampNTZNanosType(9))
          checkAnswer(read, df.collect().toSeq)
        }
      }
    }
  }

  test("SPARK-57102: nanos timestamps round-trip via the V2 file source") {
    withNanosEnabled {
      withSQLConf(
        SQLConf.USE_V1_SOURCE_LIST.key -> "",
        SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key -> "false") {
        withTempPath { dir =>
          val df = spark.sql(
            "SELECT TIMESTAMP_NTZ '2020-01-01 12:34:56.123456789' AS ntz, " +
            "TIMESTAMP_LTZ '2020-01-01 12:34:56.123456789' AS ltz")
          df.write.parquet(dir.getCanonicalPath)
          val read = spark.read.parquet(dir.getCanonicalPath)
          assert(read.schema("ntz").dataType === TimestampNTZNanosType(9))
          assert(read.schema("ltz").dataType === TimestampLTZNanosType(9))
          checkAnswer(read, df.collect().toSeq)
        }
      }
    }
  }
}
