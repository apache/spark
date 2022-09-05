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

import java.time.{Duration, Period}

import org.apache.hadoop.fs.{FileSystem, Path}

import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.execution.datasources.CommonFileDataSourceSuite
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._

abstract class ParquetFileFormatSuite
  extends QueryTest
  with ParquetTest
  with SharedSparkSession
  with CommonFileDataSourceSuite {

  override protected def dataSourceFormat = "parquet"

  test("read parquet footers in parallel") {
    def testReadFooters(ignoreCorruptFiles: Boolean): Unit = {
      withTempDir { dir =>
        val fs = FileSystem.get(spark.sessionState.newHadoopConf())
        val basePath = dir.getCanonicalPath

        val path1 = new Path(basePath, "first")
        val path2 = new Path(basePath, "second")
        val path3 = new Path(basePath, "third")

        spark.range(1).toDF("a").coalesce(1).write.parquet(path1.toString)
        spark.range(1, 2).toDF("a").coalesce(1).write.parquet(path2.toString)
        spark.range(2, 3).toDF("a").coalesce(1).write.json(path3.toString)

        val fileStatuses =
          Seq(fs.listStatus(path1), fs.listStatus(path2), fs.listStatus(path3)).flatten

        val footers = ParquetFileFormat.readParquetFootersInParallel(
          spark.sessionState.newHadoopConf(), fileStatuses, ignoreCorruptFiles)

        assert(footers.size == 2)
      }
    }

    testReadFooters(true)
    val exception = intercept[SparkException] {
      testReadFooters(false)
    }.getCause
    assert(exception.getMessage().contains("Could not read footer for file"))
  }

  test("SPARK-36825, SPARK-36854: year-month/day-time intervals written and read as INT32/INT64") {
    Seq(false, true).foreach { offHeapEnabled =>
      withSQLConf(SQLConf.COLUMN_VECTOR_OFFHEAP_ENABLED.key -> offHeapEnabled.toString) {
        Seq(
          YearMonthIntervalType() -> ((i: Int) => Period.of(i, i, 0)),
          DayTimeIntervalType() -> ((i: Int) => Duration.ofDays(i).plusSeconds(i))
        ).foreach { case (it, f) =>
          val data = (1 to 10).map(i => Row(i, f(i)))
          val schema = StructType(Array(StructField("d", IntegerType, false),
            StructField("i", it, false)))
          withTempPath { file =>
            val df = spark.createDataFrame(sparkContext.parallelize(data), schema)
            df.write.parquet(file.getCanonicalPath)
            withAllParquetReaders {
              val df2 = spark.read.parquet(file.getCanonicalPath)
              checkAnswer(df2, df.collect().toSeq)
            }
          }
        }
      }
    }
  }

  test("support batch reads for schema") {
    val testUDT = new TestUDT.MyDenseVectorUDT
    Seq(true, false).foreach { enabled =>
      withSQLConf(SQLConf.PARQUET_VECTORIZED_READER_NESTED_COLUMN_ENABLED.key -> enabled.toString) {
        Seq(
          Seq(StructField("f1", IntegerType), StructField("f2", BooleanType)) -> true,
          Seq(StructField("f1", IntegerType), StructField("f2", ArrayType(IntegerType))) -> enabled,
          Seq(StructField("f1", BooleanType), StructField("f2", testUDT)) -> enabled
        ).foreach { case (schema, expected) =>
          assert(ParquetUtils.isBatchReadSupportedForSchema(conf, StructType(schema)) == expected)
        }
      }
    }
  }

  test("support batch reads for data type") {
    val testUDT = new TestUDT.MyDenseVectorUDT
    Seq(true, false).foreach { enabled =>
      withSQLConf(SQLConf.PARQUET_VECTORIZED_READER_NESTED_COLUMN_ENABLED.key -> enabled.toString) {
        Seq(
          IntegerType -> true,
          BooleanType -> true,
          ArrayType(TimestampType) -> enabled,
          StructType(Seq(StructField("f1", DecimalType.SYSTEM_DEFAULT),
            StructField("f2", StringType))) -> enabled,
          MapType(keyType = LongType, valueType = DateType) -> enabled,
          testUDT -> enabled,
          ArrayType(testUDT) -> enabled,
          StructType(Seq(StructField("f1", ByteType), StructField("f2", testUDT))) -> enabled,
          MapType(keyType = testUDT, valueType = BinaryType) -> enabled
        ).foreach { case (dt, expected) =>
          assert(ParquetUtils.isBatchReadSupported(conf, dt) == expected)
        }
      }
    }
  }
}

class ParquetFileFormatV1Suite extends ParquetFileFormatSuite {
  override protected def sparkConf: SparkConf =
    super
      .sparkConf
      .set(SQLConf.USE_V1_SOURCE_LIST, "parquet")
}

class ParquetFileFormatV2Suite extends ParquetFileFormatSuite {
  override protected def sparkConf: SparkConf =
    super
      .sparkConf
      .set(SQLConf.USE_V1_SOURCE_LIST, "")
}
