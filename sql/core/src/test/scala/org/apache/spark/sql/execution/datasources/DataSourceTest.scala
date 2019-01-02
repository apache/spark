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

import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.test.SQLTestUtils

private[sql] trait DataSourceTest extends SQLTestUtils {

  protected val dataSourceName: String
  protected val vectorizedReaderEnabledKey: String

  /**
   * Reads data source file from given `path` as `DataFrame` and pass it to given function.
   *
   * @param path           The path to file
   * @param testVectorized Whether to read the file with vectorized reader. If the data source
   *                       doesn't support vectorized reader, this is no op.
   * @param f              The given function that takes a `DataFrame` as input.
   */
  protected def readFile(path: String, testVectorized: Boolean = true)
      (f: DataFrame => Unit): Unit = {
    (true :: false :: Nil).foreach { vectorized =>
      if (!vectorized || testVectorized) {
        withSQLConf(vectorizedReaderEnabledKey -> vectorized.toString) {
          f(spark.read.format(dataSourceName).load(path.toString))
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
}
