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
package org.apache.spark.sql.test

import java.io.File
import java.util.UUID

import org.scalatest.Assertions.fail

import org.apache.spark.sql.{AnalysisException, DataFrame, SparkSession, SQLImplicits}
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.util.{SparkErrorUtils, SparkFileUtils}

trait SQLHelper {

  def spark: SparkSession

  // Shorthand for running a query using our SparkSession
  protected lazy val sql: String => DataFrame = spark.sql _

  /**
   * A helper object for importing SQL implicits.
   *
   * Note that the alternative of importing `spark.implicits._` is not possible here. This is
   * because we create the `SparkSession` immediately before the first test is run, but the
   * implicits import is needed in the constructor.
   */
  protected object testImplicits extends SQLImplicits {
    override protected def session: SparkSession = spark
  }

  /**
   * Sets all SQL configurations specified in `pairs`, calls `f`, and then restores all SQL
   * configurations.
   */
  protected def withSQLConf(pairs: (String, String)*)(f: => Unit): Unit = {
    val (keys, values) = pairs.unzip
    val currentValues = keys.map { key =>
      if (spark.conf.getOption(key).isDefined) {
        Some(spark.conf.get(key))
      } else {
        None
      }
    }
    keys.lazyZip(values).foreach { (k, v) =>
      if (spark.conf.isModifiable(k)) {
        spark.conf.set(k, v)
      } else {
        throw new AnalysisException(
          errorClass = "_LEGACY_ERROR_TEMP_3050",
          messageParameters = Map("k" -> k))
      }

    }
    try f
    finally {
      keys.zip(currentValues).foreach {
        case (key, Some(value)) => spark.conf.set(key, value)
        case (key, None) => spark.conf.unset(key)
      }
    }
  }

  /**
   * Creates a temporary database and switches current database to it before executing `f`. This
   * database is dropped after `f` returns.
   *
   * Note that this method doesn't switch current database before executing `f`.
   */
  protected def withTempDatabase(f: String => Unit): Unit = {
    val dbName = s"db_${UUID.randomUUID().toString.replace('-', '_')}"

    try {
      spark.sql(s"CREATE DATABASE $dbName")
    } catch {
      case cause: Throwable =>
        fail("Failed to create temporary database", cause)
    }

    try f(dbName)
    finally {
      if (spark.catalog.currentDatabase == dbName) {
        spark.sql(s"USE default")
      }
      spark.sql(s"DROP DATABASE $dbName CASCADE")
    }
  }

  /**
   * Generates a temporary path without creating the actual file/directory, then pass it to `f`.
   * If a file/directory is created there by `f`, it will be delete after `f` returns.
   */
  protected def withTempPath(f: File => Unit): Unit = {
    val path = SparkFileUtils.createTempDir()
    path.delete()
    try f(path)
    finally SparkFileUtils.deleteRecursively(path)
  }

  /**
   * Drops temporary view `viewNames` after calling `f`.
   */
  protected def withTempView(viewNames: String*)(f: => Unit): Unit = {
    SparkErrorUtils.tryWithSafeFinally(f) {
      viewNames.foreach { viewName =>
        try spark.catalog.dropTempView(viewName)
        catch {
          // If the test failed part way, we don't want to mask the failure by failing to remove
          // temp views that never got created.
          case _: NoSuchTableException =>
        }
      }
    }
  }

  /**
   * Drops table `tableName` after calling `f`.
   */
  protected def withTable(tableNames: String*)(f: => Unit): Unit = {
    SparkErrorUtils.tryWithSafeFinally(f) {
      tableNames.foreach { name =>
        spark.sql(s"DROP TABLE IF EXISTS $name").collect()
      }
    }
  }

  /**
   * Drops view `viewName` after calling `f`.
   */
  protected def withView(viewNames: String*)(f: => Unit): Unit = {
    SparkErrorUtils.tryWithSafeFinally(f)(viewNames.foreach { name =>
      spark.sql(s"DROP VIEW IF EXISTS $name")
    })
  }
}
