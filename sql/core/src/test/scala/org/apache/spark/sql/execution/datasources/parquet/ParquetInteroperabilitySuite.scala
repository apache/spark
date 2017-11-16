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
import java.util.TimeZone

import org.apache.commons.io.FileUtils

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSQLContext

class ParquetInteroperabilitySuite extends ParquetCompatibilityTest with SharedSQLContext {
  test("parquet files with different physical schemas but share the same logical schema") {
    import ParquetCompatibilityTest._

    // This test case writes two Parquet files, both representing the following Catalyst schema
    //
    //   StructType(
    //     StructField(
    //       "f",
    //       ArrayType(IntegerType, containsNull = false),
    //       nullable = false))
    //
    // The first Parquet file comes with parquet-avro style 2-level LIST-annotated group, while the
    // other one comes with parquet-protobuf style 1-level unannotated primitive field.
    withTempDir { dir =>
      val avroStylePath = new File(dir, "avro-style").getCanonicalPath
      val protobufStylePath = new File(dir, "protobuf-style").getCanonicalPath

      val avroStyleSchema =
        """message avro_style {
          |  required group f (LIST) {
          |    repeated int32 array;
          |  }
          |}
        """.stripMargin

      writeDirect(avroStylePath, avroStyleSchema, { rc =>
        rc.message {
          rc.field("f", 0) {
            rc.group {
              rc.field("array", 0) {
                rc.addInteger(0)
                rc.addInteger(1)
              }
            }
          }
        }
      })

      logParquetSchema(avroStylePath)

      val protobufStyleSchema =
        """message protobuf_style {
          |  repeated int32 f;
          |}
        """.stripMargin

      writeDirect(protobufStylePath, protobufStyleSchema, { rc =>
        rc.message {
          rc.field("f", 0) {
            rc.addInteger(2)
            rc.addInteger(3)
          }
        }
      })

      logParquetSchema(protobufStylePath)

      checkAnswer(
        spark.read.parquet(dir.getCanonicalPath),
        Seq(
          Row(Seq(0, 1)),
          Row(Seq(2, 3))))
    }
  }

  val ImpalaFile = "test-data/impala_timestamp.parq"
  test("parquet timestamp conversion") {
    // Make a table with one parquet file written by impala, and one parquet file written by spark.
    // We should only adjust the timestamps in the impala file, and only if the conf is set

    // here's the timestamps in the impala file, as they were saved by impala
    val impalaFileData =
      Seq(
        "2001-01-01 01:01:01",
        "2002-02-02 02:02:02",
        "2003-03-03 03:03:03"
      ).map { s => java.sql.Timestamp.valueOf(s) }
    val impalaFile = Thread.currentThread().getContextClassLoader.getResource(ImpalaFile)
      .toURI.getPath
    withTempPath { tableDir =>
      val ts = Seq(
        "2004-04-04 04:04:04",
        "2005-05-05 05:05:05",
        "2006-06-06 06:06:06"
      ).map { s => java.sql.Timestamp.valueOf(s) }
      val s = spark
      import s.implicits._
      // match the column names of the file from impala
      val df = spark.createDataset(ts).toDF().repartition(1).withColumnRenamed("value", "ts")
      val schema = df.schema
      df.write.parquet(tableDir.getAbsolutePath)
      FileUtils.copyFile(new File(impalaFile), new File(tableDir, "part-00001.parq"))

      Seq(false, true).foreach { applyConversion =>
        Seq(false, true).foreach { vectorized =>
          withSQLConf(
              (SQLConf.PARQUET_SKIP_TIMESTAMP_CONVERSION.key, applyConversion.toString()),
              (SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key, vectorized.toString())
          ) {
            val read = spark.read.parquet(tableDir.getAbsolutePath).collect()
            assert(read.size === 6)
            // if we apply the conversion, we'll get the "right" values, as saved by impala in the
            // original file.  Otherwise, they're off by the local timezone offset, set to
            // America/Los_Angeles in tests
            val impalaExpectations = if (applyConversion) {
              impalaFileData
            } else {
              impalaFileData.map { ts =>
                DateTimeUtils.toJavaTimestamp(DateTimeUtils.convertTz(
                  DateTimeUtils.fromJavaTimestamp(ts),
                  TimeZone.getTimeZone("UTC"),
                  TimeZone.getDefault()))
              }
            }
            val fullExpectations = (ts ++ impalaExpectations).map {
              _.toString()
            }.sorted.toArray
            val actual = read.map {
              _.getTimestamp(0).toString()
            }.sorted
            withClue(s"applyConversion = $applyConversion; vectorized = $vectorized") {
              assert(fullExpectations === actual)

              // TODO run query with a filter, make sure pushdown is OK
      // https://github.com/apache/spark/pull/16781/files#diff-1e55698cc579cbae676f827a89c2dc2eR449
            }
          }
        }
      }
    }
  }
}
