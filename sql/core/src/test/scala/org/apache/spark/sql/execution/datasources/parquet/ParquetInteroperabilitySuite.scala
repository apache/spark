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

import org.apache.commons.io.FileUtils
import org.apache.hadoop.fs.{FileSystem, Path, PathFilter}
import org.apache.parquet.format.converter.ParquetMetadataConverter.NO_FILTER
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName

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

  test("parquet timestamp conversion") {
    // Make a table with one parquet file written by impala, and one parquet file written by spark.
    // We should only adjust the timestamps in the impala file, and only if the conf is set
    val impalaFile = "test-data/impala_timestamp.parq"

    // here are the timestamps in the impala file, as they were saved by impala
    val impalaFileData =
      Seq(
        "2001-01-01 01:01:01",
        "2002-02-02 02:02:02",
        "2003-03-03 03:03:03"
      ).map(java.sql.Timestamp.valueOf)
    val impalaPath = Thread.currentThread().getContextClassLoader.getResource(impalaFile)
      .toURI.getPath
    withTempPath { tableDir =>
      val ts = Seq(
        "2004-04-04 04:04:04",
        "2005-05-05 05:05:05",
        "2006-06-06 06:06:06"
      ).map { s => java.sql.Timestamp.valueOf(s) }
      import testImplicits._
      // match the column names of the file from impala
      withSQLConf(SQLConf.PARQUET_OUTPUT_TIMESTAMP_TYPE.key ->
        SQLConf.ParquetOutputTimestampType.INT96.toString) {
        val df = spark.createDataset(ts).toDF().repartition(1)
          .withColumnRenamed("value", "ts")
        df.write.parquet(tableDir.getAbsolutePath)
      }
      FileUtils.copyFile(new File(impalaPath), new File(tableDir, "part-00001.parq"))

      Seq(false, true).foreach { int96TimestampConversion =>
        Seq(false, true).foreach { vectorized =>
          withSQLConf(
              (SQLConf.PARQUET_OUTPUT_TIMESTAMP_TYPE.key,
                SQLConf.ParquetOutputTimestampType.INT96.toString),
              (SQLConf.PARQUET_INT96_TIMESTAMP_CONVERSION.key, int96TimestampConversion.toString()),
              (SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key, vectorized.toString())
          ) {
            val readBack = spark.read.parquet(tableDir.getAbsolutePath).collect()
            assert(readBack.size === 6)
            // if we apply the conversion, we'll get the "right" values, as saved by impala in the
            // original file.  Otherwise, they're off by the local timezone offset, set to
            // America/Los_Angeles in tests
            val impalaExpectations = if (int96TimestampConversion) {
              impalaFileData
            } else {
              impalaFileData.map { ts =>
                DateTimeUtils.toJavaTimestamp(DateTimeUtils.convertTz(
                  DateTimeUtils.fromJavaTimestamp(ts),
                  DateTimeUtils.TimeZoneUTC,
                  DateTimeUtils.getTimeZone(conf.sessionLocalTimeZone)))
              }
            }
            val fullExpectations = (ts ++ impalaExpectations).map(_.toString).sorted.toArray
            val actual = readBack.map(_.getTimestamp(0).toString).sorted
            withClue(
              s"int96TimestampConversion = $int96TimestampConversion; vectorized = $vectorized") {
              assert(fullExpectations === actual)

              // Now test that the behavior is still correct even with a filter which could get
              // pushed down into parquet.  We don't need extra handling for pushed down
              // predicates because (a) in ParquetFilters, we ignore TimestampType and (b) parquet
              // does not read statistics from int96 fields, as they are unsigned.  See
              // scalastyle:off line.size.limit
              // https://github.com/apache/parquet-mr/blob/2fd62ee4d524c270764e9b91dca72e5cf1a005b7/parquet-hadoop/src/main/java/org/apache/parquet/format/converter/ParquetMetadataConverter.java#L419
              // https://github.com/apache/parquet-mr/blob/2fd62ee4d524c270764e9b91dca72e5cf1a005b7/parquet-hadoop/src/main/java/org/apache/parquet/format/converter/ParquetMetadataConverter.java#L348
              // scalastyle:on line.size.limit
              //
              // Just to be defensive in case anything ever changes in parquet, this test checks
              // the assumption on column stats, and also the end-to-end behavior.

              val hadoopConf = spark.sessionState.newHadoopConf()
              val fs = FileSystem.get(hadoopConf)
              val parts = fs.listStatus(new Path(tableDir.getAbsolutePath), new PathFilter {
                override def accept(path: Path): Boolean = !path.getName.startsWith("_")
              })
              // grab the meta data from the parquet file.  The next section of asserts just make
              // sure the test is configured correctly.
              assert(parts.size == 2)
              parts.foreach { part =>
                val oneFooter =
                  ParquetFileReader.readFooter(hadoopConf, part.getPath, NO_FILTER)
                assert(oneFooter.getFileMetaData.getSchema.getColumns.size === 1)
                val typeName = oneFooter
                  .getFileMetaData.getSchema.getColumns.get(0).getPrimitiveType.getPrimitiveTypeName
                assert(typeName === PrimitiveTypeName.INT96)
                val oneBlockMeta = oneFooter.getBlocks().get(0)
                val oneBlockColumnMeta = oneBlockMeta.getColumns().get(0)
                // This is the important assert.  Column stats are written, but they are ignored
                // when the data is read back as mentioned above, b/c int96 is unsigned.  This
                // assert makes sure this holds even if we change parquet versions (if eg. there
                // were ever statistics even on unsigned columns).
                assert(!oneBlockColumnMeta.getStatistics.hasNonNullValue)
              }

              // These queries should return the entire dataset with the conversion applied,
              // but if the predicates were applied to the raw values in parquet, they would
              // incorrectly filter data out.
              val query = spark.read.parquet(tableDir.getAbsolutePath)
                .where("ts > '2001-01-01 01:00:00'")
              val countWithFilter = query.count()
              val exp = if (int96TimestampConversion) 6 else 5
              assert(countWithFilter === exp, query)
            }
          }
        }
      }
    }
  }
}
