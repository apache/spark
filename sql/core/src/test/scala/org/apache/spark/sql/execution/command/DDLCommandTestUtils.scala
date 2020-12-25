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

import org.apache.spark.sql.Row
import org.apache.spark.sql.execution.datasources.PartitioningUtils
import org.apache.spark.sql.test.SQLTestUtils

trait DDLCommandTestUtils extends SQLTestUtils {
  // The version of the catalog under testing such as "V1", "V2", "Hive V1".
  protected def version: String
  // Name of the command as SQL statement, for instance "SHOW PARTITIONS"
  protected def command: String
  protected def catalog: String
  protected def defaultUsing: String

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
}
