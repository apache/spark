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

import org.apache.spark.{SparkArithmeticException, SparkException}
import org.apache.spark.sql.{AnalysisException, QueryTest, Row}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._

class ParquetTimestampNanosSuite extends QueryTest with ParquetTest with SharedSparkSession {

  private def withNanosEnabled(f: => Unit): Unit = {
    withSQLConf(
      SQLConf.TIMESTAMP_NANOS_TYPES_ENABLED.key -> "true")(f)
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
      withSQLConf(SQLConf.SESSION_LOCAL_TIMEZONE.key -> "UTC") {
        withAllParquetReaders {
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
  }

  test("SPARK-57102: explicit lower-precision read schema truncates sub-precision nanos") {
    withNanosEnabled {
      withSQLConf(SQLConf.SESSION_LOCAL_TIMEZONE.key -> "UTC") {
        withAllParquetReaders {
          // Foreign file with full 9-digit precision: .123456789 after the epoch, and -1ns (which
          // floors to epochMicros = -1, nanosWithinMicro = 999).
          val values = Seq(Some(123456789L), Some(-1L))

          withTempPath { dir =>
            val file = new File(dir, "ntz.parquet")
            writeForeignNanosParquet(file, isAdjustedToUTC = false, values)
            // Reading with an explicit TIMESTAMP_NTZ(7) schema must floor the sub-microsecond
            // digits to precision 7, matching DateTimeUtils.localDateTimeToTimestampNanos.
            val read = spark.read.schema("ts TIMESTAMP_NTZ(7)").parquet(file.getCanonicalPath)
            assert(read.schema("ts").dataType === TimestampNTZNanosType(7))
            checkAnswer(read, spark.sql(
              """SELECT * FROM VALUES
                |  (TIMESTAMP_NTZ '1970-01-01 00:00:00.123456700'),
                |  (TIMESTAMP_NTZ '1969-12-31 23:59:59.999999900')
                |  AS t(ts)""".stripMargin).collect().toSeq)
          }

          withTempPath { dir =>
            val file = new File(dir, "ltz.parquet")
            writeForeignNanosParquet(file, isAdjustedToUTC = true, values)
            // precision 8 drops only the last digit.
            val read = spark.read.schema("ts TIMESTAMP_LTZ(8)").parquet(file.getCanonicalPath)
            assert(read.schema("ts").dataType === TimestampLTZNanosType(8))
            checkAnswer(read, spark.sql(
              """SELECT * FROM VALUES
                |  (TIMESTAMP_LTZ '1970-01-01 00:00:00.123456780'),
                |  (TIMESTAMP_LTZ '1969-12-31 23:59:59.999999990')
                |  AS t(ts)""".stripMargin).collect().toSeq)
          }
        }
      }
    }
  }

  test("SPARK-57102: requesting a nanos type over a non-NANOS Parquet column fails clearly") {
    withNanosEnabled {
      withSQLConf(SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key -> "false") {
        withTempPath { dir =>
          // Write a microsecond column: the Parquet annotation is TIMESTAMP(MICROS), not NANOS.
          spark.sql("SELECT TIMESTAMP_NTZ '2020-01-01 12:34:56.123456' AS ts")
            .write.parquet(dir.getCanonicalPath)
          // Forcing a nanosecond read schema leaves no matching converter case (the guard requires
          // a NANOS annotation), so it falls through to the generic PARQUET_CONVERSION_FAILURE
          // error - the same path every other type uses, not a confusing one.
          val e = intercept[SparkException] {
            spark.read.schema("ts TIMESTAMP_NTZ(7)").parquet(dir.getCanonicalPath).collect()
          }
          var cause: Throwable = e
          while (cause != null && (cause.getMessage == null ||
              !cause.getMessage.contains("PARQUET_CONVERSION_FAILURE"))) {
            cause = cause.getCause
          }
          assert(cause != null,
            s"Expected a PARQUET_CONVERSION_FAILURE error, but got: ${e.getMessage}")
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
    Seq(true, false).foreach { isAdjustedToUTC =>
      withTempPath { dir =>
        val file = new File(dir, "foreign.parquet")
        writeForeignNanosParquet(file, isAdjustedToUTC, Seq(Some(123L)))
        withSQLConf(
          SQLConf.TIMESTAMP_NANOS_TYPES_ENABLED.key -> "false",
          SQLConf.LEGACY_PARQUET_NANOS_AS_LONG.key -> "false") {
          checkError(
            exception = intercept[AnalysisException] {
              spark.read.parquet(file.getCanonicalPath).schema
            },
            condition = "PARQUET_TYPE_ILLEGAL",
            parameters = Map("parquetType" -> s"INT64 (TIMESTAMP(NANOS,$isAdjustedToUTC))"))
        }
      }
    }
  }

  test("SPARK-57102: writing a timestamp outside the INT64 epoch-nanos range fails loudly") {
    withNanosEnabled {
      withSQLConf(SQLConf.SESSION_LOCAL_TIMEZONE.key -> "UTC") {
        Seq("TIMESTAMP_NTZ", "TIMESTAMP_LTZ").foreach { typeName =>
          withTempPath { dir =>
            val df = spark.sql(s"SELECT $typeName '9999-12-31 23:59:59.999999999' AS ts")
            val e = intercept[SparkException] {
              df.write.parquet(dir.getCanonicalPath)
            }
            var cause: Throwable = e
            while (cause != null && !cause.isInstanceOf[SparkArithmeticException]) {
              cause = cause.getCause
            }
            assert(
              cause != null,
              s"Expected a DATETIME_OVERFLOW error for $typeName, but got: ${e.getMessage}")
            // NTZ renders without a zone; LTZ renders as a UTC instant with a trailing `Z`.
            val renderedValue =
              if (typeName == "TIMESTAMP_NTZ") "9999-12-31T23:59:59.999999999"
              else "9999-12-31T23:59:59.999999999Z"
            checkError(
              exception = cause.asInstanceOf[SparkArithmeticException],
              condition = "DATETIME_OVERFLOW",
              parameters = Map("operation" ->
                (s"write the timestamp value $renderedValue as Parquet INT64 " +
                  "epoch-nanoseconds (supported range: 1677-09-21T00:12:43.145224192Z to " +
                  "2262-04-11T23:47:16.854775807Z)")))
          }
        }
      }
    }
  }

  test("SPARK-57102: datetime rebase configs do not affect TIMESTAMP(NANOS) reads") {
    withNanosEnabled {
      withSQLConf(SQLConf.SESSION_LOCAL_TIMEZONE.key -> "UTC") {
        withAllParquetReaders {
          withTempPath { dir =>
            val file = new File(dir, "ltz.parquet")
            // 1800-01-01 00:00:00 UTC predates the last Julian-to-Gregorian switch instant of the
            // rebase logic, so applying a timestamp rebase (or the EXCEPTION-mode guard) on this
            // value would change the result or fail the read.
            writeForeignNanosParquet(
              file, isAdjustedToUTC = true, Seq(Some(-5364662400000000000L)))
            Seq("EXCEPTION", "CORRECTED", "LEGACY").foreach { mode =>
              withSQLConf(SQLConf.PARQUET_REBASE_MODE_IN_READ.key -> mode) {
                checkAnswer(
                  spark.read.parquet(file.getCanonicalPath),
                  spark.sql(
                    "SELECT TIMESTAMP_LTZ '1800-01-01 00:00:00.000000000'").collect().toSeq)
              }
            }
          }
        }
      }
    }
  }

  test("SPARK-57102: nanos timestamps round-trip inside a nested (array) column") {
    withNanosEnabled {
      withAllParquetReaders {
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
      withSQLConf(SQLConf.USE_V1_SOURCE_LIST.key -> "") {
        withAllParquetReaders {
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

  test("SPARK-57828: vectorized reader produces identical results to row-based for nanos") {
    withNanosEnabled {
      withSQLConf(SQLConf.SESSION_LOCAL_TIMEZONE.key -> "UTC") {
        // Write foreign parquet files with TIMESTAMP(NANOS) values covering edge cases:
        // positive, negative (pre-epoch), values with precision truncation, and nulls.
        val values = Seq(
          Some(123456789L),       // 1970-01-01 00:00:00.123456789
          Some(-1L),              // 1969-12-31 23:59:59.999999999 (pre-epoch, floor semantics)
          Some(1000000000L),      // 1970-01-01 00:00:01.000000000 (exact second)
          Some(-1000000001L),     // 1969-12-31 23:59:58.999999999
          None                    // null
        )

        Seq(7, 8, 9).foreach { p =>
          Seq(true, false).foreach { isAdjustedToUTC =>
            val typeName = if (isAdjustedToUTC) "TIMESTAMP_LTZ" else "TIMESTAMP_NTZ"
            withClue(s"$typeName($p)") {
              withTempPath { dir =>
                val file = new File(dir, s"ts_nanos_${typeName}_p$p.parquet")
                writeForeignNanosParquet(file, isAdjustedToUTC, values)

                val schema = s"ts $typeName($p)"

                // Row-based read
                val rowBased = withSQLConf(
                  SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key -> "false") {
                  spark.read.schema(schema).parquet(file.getCanonicalPath).collect()
                }

                // Vectorized read
                val vectorized = withSQLConf(
                  SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key -> "true") {
                  spark.read.schema(schema).parquet(file.getCanonicalPath).collect()
                }

                assert(vectorized.toSeq === rowBased.toSeq,
                  s"Vectorized and row-based results differ for $typeName($p)")
              }
            }
          }
        }
      }
    }
  }
}
