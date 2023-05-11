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
package org.apache.spark.sql

import java.io.File
import java.util.UUID

import org.scalatest.Assertions.fail

import org.apache.spark.sql.catalyst.catalog.SessionCatalog.DEFAULT_DATABASE
import org.apache.spark.util.Utils

trait SQLHelper {

  def spark: SparkSession

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
    (keys, values).zipped.foreach { (k, v) =>
      if (spark.conf.isModifiable(k)) {
        spark.conf.set(k, v)
      } else {
        throw new AnalysisException(s"Cannot modify the value of a static config: $k")
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
        spark.sql(s"USE $DEFAULT_DATABASE")
      }
      spark.sql(s"DROP DATABASE $dbName CASCADE")
    }
  }

  /**
   * Generates a temporary path without creating the actual file/directory, then pass it to `f`.
   * If a file/directory is created there by `f`, it will be delete after `f` returns.
   */
  protected def withTempPath(f: File => Unit): Unit = {
    val path = Utils.createTempDir()
    path.delete()
    try f(path)
    finally Utils.deleteRecursively(path)
  }

  /**
   * Drops table `tableName` after calling `f`.
   */
  protected def withTable(tableNames: String*)(f: => Unit): Unit = {
    Utils.tryWithSafeFinally(f) {
      tableNames.foreach { name =>
        spark.sql(s"DROP TABLE IF EXISTS $name").collect()
      }
    }
  }
}
