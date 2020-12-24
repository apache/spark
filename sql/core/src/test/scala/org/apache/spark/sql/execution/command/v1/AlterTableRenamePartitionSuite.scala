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

package org.apache.spark.sql.execution.command.v1

import org.apache.spark.sql.{AnalysisException, Row}
import org.apache.spark.sql.catalyst.analysis.{NoSuchPartitionException, PartitionAlreadyExistsException}
import org.apache.spark.sql.execution.command
import org.apache.spark.sql.internal.SQLConf

trait AlterTableRenamePartitionSuiteBase extends command.AlterTableRenamePartitionSuiteBase {
  protected def createSinglePartTable(t: String): Unit = {
    sql(s"CREATE TABLE $t (id bigint, data string) $defaultUsing PARTITIONED BY (id)")
    sql(s"INSERT INTO $t PARTITION (id = 1) SELECT 'abc'")
  }

  test("rename without explicitly specifying database") {
    val t = "tbl"
    withTable(t) {
      createSinglePartTable(t)
      checkPartitions(t, Map("id" -> "1"))

      sql(s"ALTER TABLE $t PARTITION (id = 1) RENAME TO PARTITION (id = 2)")
      checkPartitions(t, Map("id" -> "2"))
      checkAnswer(sql(s"SELECT id, data FROM $t"), Row(2, "abc"))
    }
  }

  test("table to alter does not exist") {
    withNamespace(s"$catalog.ns") {
      sql(s"CREATE NAMESPACE $catalog.ns")
      val errMsg = intercept[AnalysisException] {
        sql(s"ALTER TABLE $catalog.ns.no_tbl PARTITION (id=1) RENAME TO PARTITION (id=2)")
      }.getMessage
      assert(errMsg.contains("Table not found"))
    }
  }

  test("partition to rename does not exist") {
    withNamespaceAndTable("ns", "tbl") { t =>
      createSinglePartTable(t)
      checkPartitions(t, Map("id" -> "1"))
      val errMsg = intercept[NoSuchPartitionException] {
        sql(s"ALTER TABLE $t PARTITION (id = 3) RENAME TO PARTITION (id = 2)")
      }.getMessage
      assert(errMsg.contains("Partition not found in table"))
    }
  }

  test("target partition exists") {
    withNamespaceAndTable("ns", "tbl") { t =>
      createSinglePartTable(t)
      sql(s"INSERT INTO $t PARTITION (id = 2) SELECT 'def'")
      checkPartitions(t, Map("id" -> "1"), Map("id" -> "2"))
      val errMsg = intercept[PartitionAlreadyExistsException] {
        sql(s"ALTER TABLE $t PARTITION (id = 1) RENAME TO PARTITION (id = 2)")
      }.getMessage
      assert(errMsg.contains("Partition already exists"))
    }
  }

  test("single part partition") {
    withNamespaceAndTable("ns", "tbl") { t =>
      createSinglePartTable(t)
      checkPartitions(t, Map("id" -> "1"))

      sql(s"ALTER TABLE $t PARTITION (id = 1) RENAME TO PARTITION (id = 2)")
      checkPartitions(t, Map("id" -> "2"))
      checkAnswer(sql(s"SELECT id, data FROM $t"), Row(2, "abc"))
    }
  }

  test("multi part partition") {
    withNamespaceAndTable("ns", "tbl") { t =>
      createWideTable(t)
      checkPartitions(t,
        Map(
          "year" -> "2016",
          "month" -> "3",
          "hour" -> "10",
          "minute" -> "10",
          "sec" -> "10",
          "extra" -> "1"),
        Map(
          "year" -> "2016",
          "month" -> "4",
          "hour" -> "10",
          "minute" -> "10",
          "sec" -> "10",
          "extra" -> "1"))

      sql(s"""
        |ALTER TABLE $t
        |PARTITION (
        |  year = 2016, month = 3, hour = 10, minute = 10, sec = 10, extra = 1
        |) RENAME TO PARTITION (
        |  year = 2016, month = 3, hour = 10, minute = 10, sec = 123, extra = 1
        |)""".stripMargin)
      checkPartitions(t,
        Map(
          "year" -> "2016",
          "month" -> "3",
          "hour" -> "10",
          "minute" -> "10",
          "sec" -> "123",
          "extra" -> "1"),
        Map(
          "year" -> "2016",
          "month" -> "4",
          "hour" -> "10",
          "minute" -> "10",
          "sec" -> "10",
          "extra" -> "1"))
      checkAnswer(sql(s"SELECT month, sec, price FROM $t"), Row(3, 123, 3))
    }
  }

  test("with location") {
    withNamespaceAndTable("ns", "tbl") { t =>
      createSinglePartTable(t)
      sql(s"ALTER TABLE $t ADD PARTITION (id = 2) LOCATION 'loc1'")
      sql(s"INSERT INTO $t PARTITION (id = 2) SELECT 'def'")
      checkPartitions(t, Map("id" -> "1"), Map("id" -> "2"))

      sql(s"ALTER TABLE $t PARTITION (id = 2) RENAME TO PARTITION (id = 3)")
      checkPartitions(t, Map("id" -> "1"), Map("id" -> "3"))
      checkAnswer(sql(s"SELECT id, data FROM $t"), Seq(Row(1, "abc"), Row(3, "def")))
    }
  }

  test("partition spec in RENAME PARTITION should be case insensitive") {
    withNamespaceAndTable("ns", "tbl") { t =>
      createSinglePartTable(t)
      checkPartitions(t, Map("id" -> "1"))

      withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
        val errMsg = intercept[AnalysisException] {
          sql(s"ALTER TABLE $t PARTITION (ID = 1) RENAME TO PARTITION (id = 2)")
        }.getMessage
        assert(errMsg.contains("ID is not a valid partition column"))
        checkPartitions(t, Map("id" -> "1"))
      }

      withSQLConf(SQLConf.CASE_SENSITIVE.key -> "false") {
        sql(s"ALTER TABLE $t PARTITION (ID = 1) RENAME TO PARTITION (id = 2)")
        checkPartitions(t, Map("id" -> "2"))
        checkAnswer(sql(s"SELECT id, data FROM $t"), Row(2, "abc"))
      }
    }
  }
}

class AlterTableRenamePartitionSuite
  extends AlterTableRenamePartitionSuiteBase
  with CommandSuiteBase
