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

package org.apache.spark.sql.hive

import java.io.File

import org.apache.spark.sql.{Row, SaveMode}
import org.apache.spark.sql.catalyst.catalog.HiveTableRelation
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.util.Utils

/**
 * A suite of tests for the Parquet support through the data sources API.
 */
class HiveParquetSourceSuite extends ParquetPartitioningTest {
  import testImplicits._
  import spark._

  override def beforeAll(): Unit = {
    super.beforeAll()
    dropTables("partitioned_parquet",
      "partitioned_parquet_with_key",
      "partitioned_parquet_with_complextypes",
      "partitioned_parquet_with_key_and_complextypes",
      "normal_parquet")

    sql(
      s"""
        |CREATE TEMPORARY VIEW partitioned_parquet
        |USING org.apache.spark.sql.parquet
        |OPTIONS (
        |  path '${partitionedTableDir.toURI}'
        |)
      """.stripMargin)

    sql(
      s"""
        |CREATE TEMPORARY VIEW partitioned_parquet_with_key
        |USING org.apache.spark.sql.parquet
        |OPTIONS (
        |  path '${partitionedTableDirWithKey.toURI}'
        |)
      """.stripMargin)

    sql(
      s"""
        |CREATE TEMPORARY VIEW normal_parquet
        |USING org.apache.spark.sql.parquet
        |OPTIONS (
        |  path '${new File(partitionedTableDir, "p=1").toURI}'
        |)
      """.stripMargin)

    sql(
      s"""
        |CREATE TEMPORARY VIEW partitioned_parquet_with_key_and_complextypes
        |USING org.apache.spark.sql.parquet
        |OPTIONS (
        |  path '${partitionedTableDirWithKeyAndComplexTypes.toURI}'
        |)
      """.stripMargin)

    sql(
      s"""
        |CREATE TEMPORARY VIEW partitioned_parquet_with_complextypes
        |USING org.apache.spark.sql.parquet
        |OPTIONS (
        |  path '${partitionedTableDirWithComplexTypes.toURI}'
        |)
      """.stripMargin)
  }

  test("SPARK-6016 make sure to use the latest footers") {
    val tableName = "spark_6016_fix"
    withTable(tableName) {
      // Create a DataFrame with two partitions. So, the created table will have two parquet files.
      val df1 = (1 to 10).map(Tuple1(_)).toDF("a").coalesce(2)
      df1.write.mode(SaveMode.Overwrite).format("parquet").saveAsTable(tableName)
      checkAnswer(
        sql(s"select * from $tableName"),
        (1 to 10).map(i => Row(i))
      )

      // Create a DataFrame with four partitions. So the created table will have four parquet files.
      val df2 = (1 to 10).map(Tuple1(_)).toDF("b").coalesce(4)
      df2.write.mode(SaveMode.Overwrite).format("parquet").saveAsTable(tableName)
      // For the bug of SPARK-6016, we are caching two outdated footers for df1. Then,
      // since the new table has four parquet files, we are trying to read new footers from two
      // files and then merge metadata in footers of these four
      // (two outdated ones and two latest one), which will cause an error.
      checkAnswer(
        sql(s"select * from $tableName"),
        (1 to 10).map(i => Row(i))
      )
    }
  }

  test("SPARK-8811: compatibility with array of struct in Hive") {
    withTempPath { dir =>
      withTable("array_of_struct") {
        val conf = Seq(
          HiveUtils.CONVERT_METASTORE_PARQUET.key -> "false",
          SQLConf.PARQUET_BINARY_AS_STRING.key -> "true",
          SQLConf.PARQUET_WRITE_LEGACY_FORMAT.key -> "false")

        withSQLConf(conf: _*) {
          sql(
            s"""CREATE TABLE array_of_struct
               |STORED AS PARQUET LOCATION '${dir.toURI}'
               |AS SELECT
               |  '1st' AS a,
               |  '2nd' AS b,
               |  ARRAY(NAMED_STRUCT('a', 'val_a', 'b', 'val_b')) AS c
             """.stripMargin)

          checkAnswer(
            spark.read.parquet(dir.getCanonicalPath),
            Row("1st", "2nd", Seq(Row("val_a", "val_b"))))
        }
      }
    }
  }

  test("Verify the PARQUET conversion parameter: CONVERT_METASTORE_PARQUET") {
    withTempView("single") {
      val singleRowDF = Seq((0, "foo")).toDF("key", "value")
      singleRowDF.createOrReplaceTempView("single")

      Seq("true", "false").foreach { parquetConversion =>
        withSQLConf(HiveUtils.CONVERT_METASTORE_PARQUET.key -> parquetConversion) {
          val tableName = "test_parquet_ctas"
          withTable(tableName) {
            sql(
              s"""
                 |CREATE TABLE $tableName STORED AS PARQUET
                 |AS SELECT tmp.key, tmp.value FROM single tmp
               """.stripMargin)

            val df = spark.sql(s"SELECT * FROM $tableName WHERE key=0")
            checkAnswer(df, singleRowDF)

            val queryExecution = df.queryExecution
            if (parquetConversion == "true") {
              queryExecution.analyzed.collectFirst {
                case _: LogicalRelation =>
              }.getOrElse {
                fail(s"Expecting the query plan to convert parquet to data sources, " +
                  s"but got:\n$queryExecution")
              }
            } else {
              queryExecution.analyzed.collectFirst {
                case _: HiveTableRelation =>
              }.getOrElse {
                fail(s"Expecting no conversion from parquet to data sources, " +
                  s"but got:\n$queryExecution")
              }
            }
          }
        }
      }
    }
  }

  test("values in arrays and maps stored in parquet are always nullable") {
    val df = createDataFrame(Tuple2(Map(2 -> 3), Seq(4, 5, 6)) :: Nil).toDF("m", "a")
    val mapType1 = MapType(IntegerType, IntegerType, valueContainsNull = false)
    val arrayType1 = ArrayType(IntegerType, containsNull = false)
    val expectedSchema1 =
      StructType(
        StructField("m", mapType1, nullable = true) ::
          StructField("a", arrayType1, nullable = true) :: Nil)
    assert(df.schema === expectedSchema1)

    withTable("alwaysNullable") {
      df.write.format("parquet").saveAsTable("alwaysNullable")

      val mapType2 = MapType(IntegerType, IntegerType, valueContainsNull = true)
      val arrayType2 = ArrayType(IntegerType, containsNull = true)
      val expectedSchema2 =
        StructType(
          StructField("m", mapType2, nullable = true) ::
            StructField("a", arrayType2, nullable = true) :: Nil)

      assert(table("alwaysNullable").schema === expectedSchema2)

      checkAnswer(
        sql("SELECT m, a FROM alwaysNullable"),
        Row(Map(2 -> 3), Seq(4, 5, 6)))
    }
  }

  test("Aggregation attribute names can't contain special chars \" ,;{}()\\n\\t=\"") {
    withTempDir { tempDir =>
      val filePath = new File(tempDir, "testParquet").getCanonicalPath
      val filePath2 = new File(tempDir, "testParquet2").getCanonicalPath

      val df = Seq(1, 2, 3).map(i => (i, i.toString)).toDF("int", "str")
      val df2 = df.as('x).join(df.as('y), $"x.str" === $"y.str").groupBy("y.str").max("y.int")
      intercept[Throwable](df2.write.parquet(filePath))

      val df3 = df2.toDF("str", "max_int")
      df3.write.parquet(filePath2)
      val df4 = read.parquet(filePath2)
      checkAnswer(df4, Row("1", 1) :: Row("2", 2) :: Row("3", 3) :: Nil)
      assert(df4.columns === Array("str", "max_int"))
    }
  }
}
