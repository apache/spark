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

package org.apache.spark.sql.parquet

import java.io.File

import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag
import scala.util.Try

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.catalyst.util
import org.apache.spark.util.Utils

/**
 * A helper trait that provides convenient facilities for Parquet testing.
 *
 * NOTE: Considering classes `Tuple1` ... `Tuple22` all extend `Product`, it would be more
 * convenient to use tuples rather than special case classes when writing test cases/suites.
 * Especially, `Tuple1.apply` can be used to easily wrap a single type/value.
 */
trait ParquetTest {
  val sqlContext: SQLContext

  import sqlContext._

  protected def configuration = sparkContext.hadoopConfiguration

  /**
   * Sets all SQL configurations specified in `pairs`, calls `f`, and then restore all SQL
   * configurations.
   *
   * @todo Probably this method should be moved to a more general place
   */
  protected def withSQLConf(pairs: (String, String)*)(f: => Unit): Unit = {
    val (keys, values) = pairs.unzip
    val currentValues = keys.map(key => Try(getConf(key)).toOption)
    (keys, values).zipped.foreach(setConf)
    try f finally {
      keys.zip(currentValues).foreach {
        case (key, Some(value)) => setConf(key, value)
        case (key, None) => conf.unsetConf(key)
      }
    }
  }

  /**
   * Generates a temporary path without creating the actual file/directory, then pass it to `f`. If
   * a file/directory is created there by `f`, it will be delete after `f` returns.
   *
   * @todo Probably this method should be moved to a more general place
   */
  protected def withTempPath(f: File => Unit): Unit = {
    val file = util.getTempFilePath("parquetTest").getCanonicalFile
    try f(file) finally if (file.exists()) Utils.deleteRecursively(file)
  }

  /**
   * Creates a temporary directory, which is then passed to `f` and will be deleted after `f`
   * returns.
   *
   * @todo Probably this method should be moved to a more general place
   */
  protected def withTempDir(f: File => Unit): Unit = {
    val dir = Utils.createTempDir().getCanonicalFile
    try f(dir) finally Utils.deleteRecursively(dir)
  }

  /**
   * Writes `data` to a Parquet file, which is then passed to `f` and will be deleted after `f`
   * returns.
   */
  protected def withParquetFile[T <: Product: ClassTag: TypeTag]
      (data: Seq[T])
      (f: String => Unit): Unit = {
    import sqlContext.implicits._
    withTempPath { file =>
      sparkContext.parallelize(data).saveAsParquetFile(file.getCanonicalPath)
      f(file.getCanonicalPath)
    }
  }

  /**
   * Writes `data` to a Parquet file and reads it back as a [[DataFrame]],
   * which is then passed to `f`. The Parquet file will be deleted after `f` returns.
   */
  protected def withParquetRDD[T <: Product: ClassTag: TypeTag]
      (data: Seq[T])
      (f: DataFrame => Unit): Unit = {
    withParquetFile(data)(path => f(parquetFile(path)))
  }

  /**
   * Drops temporary table `tableName` after calling `f`.
   */
  protected def withTempTable(tableName: String)(f: => Unit): Unit = {
    try f finally dropTempTable(tableName)
  }

  /**
   * Writes `data` to a Parquet file, reads it back as a [[DataFrame]] and registers it as a
   * temporary table named `tableName`, then call `f`. The temporary table together with the
   * Parquet file will be dropped/deleted after `f` returns.
   */
  protected def withParquetTable[T <: Product: ClassTag: TypeTag]
      (data: Seq[T], tableName: String)
      (f: => Unit): Unit = {
    withParquetRDD(data) { rdd =>
      sqlContext.registerRDDAsTable(rdd, tableName)
      withTempTable(tableName)(f)
    }
  }
}
