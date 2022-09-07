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

package org.apache.spark.sql.execution.datasources

import java.io.File

import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag

import org.apache.spark.sql.{DataFrame, Row, SaveMode}
import org.apache.spark.sql.functions.struct
import org.apache.spark.sql.test.SQLTestUtils
import org.apache.spark.sql.types.StructType

/**
 * A helper trait that provides convenient facilities for file-based data source testing.
 * Specifically, it is used for Parquet and Orc testing. It can be used to write tests
 * that are shared between Parquet and Orc.
 */
private[sql] trait FileBasedDataSourceTest extends SQLTestUtils {

  // Defines the data source name to run the test.
  protected val dataSourceName: String
  // The SQL config key for enabling vectorized reader.
  protected val vectorizedReaderEnabledKey: String
  // The SQL config key for enabling vectorized reader for nested types.
  protected val vectorizedReaderNestedEnabledKey: String

  /**
   * Reads data source file from given `path` as `DataFrame` and passes it to given function.
   *
   * @param path           The path to file
   * @param testVectorized Whether to read the file with vectorized reader.
   * @param f              The given function that takes a `DataFrame` as input.
   */
  protected def readFile(path: String, testVectorized: Boolean = true)
      (f: DataFrame => Unit): Unit = {
    withSQLConf(vectorizedReaderEnabledKey -> "false") {
      f(spark.read.format(dataSourceName).load(path.toString))
    }
    if (testVectorized) {
      Seq(true, false).foreach { enableNested =>
        withSQLConf(vectorizedReaderEnabledKey -> "true",
            vectorizedReaderNestedEnabledKey -> enableNested.toString) {
          f(spark.read.format(dataSourceName).load(path))
        }
      }
    }
  }

  /**
   * Writes `data` to a data source file, which is then passed to `f` and will be deleted after `f`
   * returns.
   */
  protected def withDataSourceFile[T <: Product : ClassTag : TypeTag]
      (data: Seq[T])
      (f: String => Unit): Unit = {
    withTempPath { file =>
      spark.createDataFrame(data).write.format(dataSourceName).save(file.getCanonicalPath)
      f(file.getCanonicalPath)
    }
  }

  /**
   * Writes `data` to a data source file and reads it back as a [[DataFrame]],
   * which is then passed to `f`. The file will be deleted after `f` returns.
   */
  protected def withDataSourceDataFrame[T <: Product : ClassTag : TypeTag]
      (data: Seq[T], testVectorized: Boolean = true)
      (f: DataFrame => Unit): Unit = {
    withDataSourceFile(data)(path => readFile(path.toString, testVectorized)(f))
  }

  /**
   * Writes `data` to a data source file, reads it back as a [[DataFrame]] and registers it as a
   * temporary table named `tableName`, then call `f`. The temporary table together with the
   * data file will be dropped/deleted after `f` returns.
   */
  protected def withDataSourceTable[T <: Product : ClassTag : TypeTag]
      (data: Seq[T], tableName: String, testVectorized: Boolean = true)
      (f: => Unit): Unit = {
    withDataSourceDataFrame(data, testVectorized) { df =>
      df.createOrReplaceTempView(tableName)
      withTempView(tableName)(f)
    }
  }

  protected def makeDataSourceFile[T <: Product : ClassTag : TypeTag](
      data: Seq[T], path: File): Unit = {
    spark.createDataFrame(data).write.mode(SaveMode.Overwrite).format(dataSourceName)
      .save(path.getCanonicalPath)
  }

  protected def makeDataSourceFile[T <: Product : ClassTag : TypeTag](
      df: DataFrame, path: File): Unit = {
    df.write.mode(SaveMode.Overwrite).format(dataSourceName).save(path.getCanonicalPath)
  }

  /**
   * Takes single level `inputDF` dataframe to generate multi-level nested
   * dataframes as new test data. It tests both non-nested and nested dataframes
   * which are written and read back with specified datasource.
   */
  protected def withNestedDataFrame(inputDF: DataFrame): Seq[(DataFrame, String, Any => Any)] = {
    assert(inputDF.schema.fields.length == 1)
    assert(!inputDF.schema.fields.head.dataType.isInstanceOf[StructType])
    val df = inputDF.toDF("temp")
    Seq(
      (
        df.withColumnRenamed("temp", "a"),
        "a", // zero nesting
        (x: Any) => x),
      (
        df.withColumn("a", struct(df("temp") as "b")).drop("temp"),
        "a.b", // one level nesting
        (x: Any) => Row(x)),
      (
        df.withColumn("a", struct(struct(df("temp") as "c") as "b")).drop("temp"),
        "a.b.c", // two level nesting
        (x: Any) => Row(Row(x))
      ),
      (
        df.withColumnRenamed("temp", "a.b"),
        "`a.b`", // zero nesting with column name containing `dots`
        (x: Any) => x
      ),
      (
        df.withColumn("a.b", struct(df("temp") as "c.d") ).drop("temp"),
        "`a.b`.`c.d`", // one level nesting with column names containing `dots`
        (x: Any) => Row(x)
      )
    )
  }
}
