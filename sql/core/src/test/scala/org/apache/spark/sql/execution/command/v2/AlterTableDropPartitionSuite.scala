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

package org.apache.spark.sql.execution.command.v2

import org.apache.spark.SparkConf
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.analysis.NoSuchPartitionsException
import org.apache.spark.sql.connector.InMemoryPartitionTableCatalog
import org.apache.spark.sql.execution.command
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

class AlterTableDropPartitionSuite
  extends command.AlterTableDropPartitionSuiteBase
  with SharedSparkSession {

  override def version: String = "V2"
  override def catalog: String = "test_catalog"
  override def defaultUsing: String = "USING _"

  override def sparkConf: SparkConf = super.sparkConf
    .set(s"spark.sql.catalog.$catalog", classOf[InMemoryPartitionTableCatalog].getName)

  protected def checkDropPartition(
      t: String,
      ifExists: String,
      specs: Map[String, Any]*): Unit = {
    checkPartitions(t, specs.map(_.mapValues(_.toString)): _*)
    val specStr = specs.map(
      _.map {
        case (k, v: String) => s"$k = '$v'"
        case (k, v) => s"$k = $v"
      }.mkString("PARTITION (", ", ", ")"))
      .mkString(", ")
    sql(s"ALTER TABLE $t DROP $ifExists $specStr")
    checkPartitions(t)
  }

  test("single partition") {
    withNsTable("ns", "tbl") { t =>
      sql(s"CREATE TABLE $t (id bigint, data string) $defaultUsing PARTITIONED BY (id)")
      Seq("", "IF EXISTS").foreach { ifExists =>
        sql(s"ALTER TABLE $t ADD PARTITION (id=1) LOCATION 'loc'")
        checkDropPartition(t, ifExists, Map("id" -> 1))
      }
    }
  }

  test("multiple partitions") {
    withNsTable("ns", "tbl") { t =>
      sql(s"CREATE TABLE $t (id bigint, data string) $defaultUsing PARTITIONED BY (id)")
      Seq("", "IF EXISTS").foreach { ifExists =>
        sql(s"""
          |ALTER TABLE $t ADD
          |PARTITION (id=1) LOCATION 'loc'
          |PARTITION (id=2) LOCATION 'loc1'""".stripMargin)
        checkDropPartition(t, ifExists, Map("id" -> 1), Map("id" -> 2))
      }
    }
  }

  test("multi-part partition") {
    withNsTable("ns", "tbl") { t =>
      sql(s"CREATE TABLE $t (id bigint, a int, b string) $defaultUsing PARTITIONED BY (a, b)")
      Seq("", "IF EXISTS").foreach { ifExists =>
        sql(s"ALTER TABLE $t ADD PARTITION (a = 2, b = 'abc')")
        checkDropPartition(t, ifExists, Map("a" -> 2, "b" -> "abc"))
      }
    }
  }

  test("partition not exists") {
    withNsTable("ns", "tbl") { t =>
      sql(s"CREATE TABLE $t (id bigint, data string) $defaultUsing PARTITIONED BY (id)")
      sql(s"ALTER TABLE $t ADD PARTITION (id=1) LOCATION 'loc'")

      val errMsg = intercept[NoSuchPartitionsException] {
        sql(s"ALTER TABLE $t DROP PARTITION (id=1), PARTITION (id=2)")
      }.getMessage
      assert(errMsg.contains("The following partitions not found in table"))

      checkPartitions(t, Map("id" -> "1"))
      sql(s"ALTER TABLE $t DROP IF EXISTS PARTITION (id=1), PARTITION (id=2)")
      checkPartitions(t)
    }
  }

  test("case sensitivity in resolving partition specs") {
    withNsTable("ns", "tbl") { t =>
      sql(s"CREATE TABLE $t (id bigint, data string) USING foo PARTITIONED BY (id)")
      withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
        val errMsg = intercept[AnalysisException] {
          sql(s"ALTER TABLE $t DROP PARTITION (ID=1)")
        }.getMessage
        assert(errMsg.contains(s"ID is not a valid partition column in table $t"))
      }

      withSQLConf(SQLConf.CASE_SENSITIVE.key -> "false") {
        Seq("", "IF EXISTS").foreach { ifExists =>
          sql(s"ALTER TABLE $t ADD PARTITION (ID=1) LOCATION 'loc1'")
          checkDropPartition(t, ifExists, Map("id" -> 1))
        }
      }
    }
  }
}
