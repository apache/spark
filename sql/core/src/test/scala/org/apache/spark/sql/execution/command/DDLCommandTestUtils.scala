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

package org.apache.spark.sql.execution.command

import org.scalactic.source.Position
import org.scalatest.Tag

import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.execution.datasources.PartitioningUtils
import org.apache.spark.sql.test.SQLTestUtils

/**
 * The common settings and utility functions for all v1 and v2 test suites. When a function
 * is not applicable to all supported catalogs, it should be placed to a specific trait:
 *
 *   - V1 In-Memory catalog: `org.apache.spark.sql.execution.command.v1.CommandSuiteBase`
 *   - V1 Hive External catalog: `org.apache.spark.sql.hive.execution.command.CommandSuiteBase`
 *   - V2 In-Memory catalog: `org.apache.spark.sql.execution.command.v2.CommandSuiteBase`
 */
trait DDLCommandTestUtils extends SQLTestUtils {
  // The version of the catalog under testing such as "V1", "V2", "Hive V1".
  protected def version: String
  // Name of the command as SQL statement, for instance "SHOW PARTITIONS"
  protected def command: String
  // The catalog name which can be used in SQL statements under testing
  protected def catalog: String
  // The clause is used in creating tables for testing
  protected def defaultUsing: String

  // Overrides the `test` method, and adds a prefix to easily find identify the catalog to which
  // the failed test in logs belongs to.
  override def test(testName: String, testTags: Tag*)(testFun: => Any)
    (implicit pos: Position): Unit = {
    super.test(s"$command $version: " + testName, testTags: _*)(testFun)
  }

  protected def withNamespaceAndTable(ns: String, tableName: String, cat: String = catalog)
      (f: String => Unit): Unit = {
    val nsCat = s"$cat.$ns"
    withNamespace(nsCat) {
      sql(s"CREATE NAMESPACE $nsCat")
      val t = s"$nsCat.$tableName"
      withTable(t) {
        f(t)
      }
    }
  }

  // Checks that the table `t` contains only the `expected` partitions.
  protected def checkPartitions(t: String, expected: Map[String, String]*): Unit = {
    val partitions = sql(s"SHOW PARTITIONS $t")
      .collect()
      .toSet
      .map((row: Row) => row.getString(0))
      .map(PartitioningUtils.parsePathFragment)
    assert(partitions === expected.toSet)
  }

  protected def createWideTable(table: String): Unit = {
    sql(s"""
      |CREATE TABLE $table (
      |  price int, qty int,
      |  year int, month int, hour int, minute int, sec int, extra int)
      |$defaultUsing
      |PARTITIONED BY (year, month, hour, minute, sec, extra)
      |""".stripMargin)
    sql(s"""
      |INSERT INTO $table
      |PARTITION(year = 2016, month = 3, hour = 10, minute = 10, sec = 10, extra = 1) SELECT 3, 3
      |""".stripMargin)
    sql(s"""
      |ALTER TABLE $table
      |ADD PARTITION(year = 2016, month = 4, hour = 10, minute = 10, sec = 10, extra = 1)
      |""".stripMargin)
  }

  protected def checkLocation(t: String, spec: TablePartitionSpec, expected: String): Unit

  // Getting the total table size in the filesystem in bytes
  def getTableSize(tableName: String): Int = {
    val stats =
      sql(s"DESCRIBE TABLE EXTENDED $tableName")
        .where("col_name = 'Statistics'")
        .select("data_type")
    if (stats.isEmpty) {
      throw new IllegalArgumentException(s"The table $tableName does not have stats")
    }
    val tableSizeInStats = "^(\\d+) bytes.*$".r
    val size = stats.first().getString(0) match {
      case tableSizeInStats(s) => s.toInt
      case _ => throw new IllegalArgumentException("Not found table size in stats")
    }
    size
  }

  def partSpecToString(spec: Map[String, Any]): String = {
    spec.map {
      case (k, v: String) => s"$k = '$v'"
      case (k, v) => s"$k = $v"
    }.mkString("PARTITION (", ", ", ")")
  }

  def cacheRelation(name: String): Unit = {
    assert(!spark.catalog.isCached(name))
    sql(s"CACHE TABLE $name")
    assert(spark.catalog.isCached(name))
  }

  def checkCachedRelation(name: String, expected: Seq[Row]): Unit = {
    assert(spark.catalog.isCached(name))
    QueryTest.checkAnswer(sql(s"SELECT * FROM $name"), expected)
  }
}
