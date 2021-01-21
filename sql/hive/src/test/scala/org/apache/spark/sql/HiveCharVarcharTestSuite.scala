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

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{CatalogColumnStat, CatalogStatistics}
import org.apache.spark.sql.execution.command.CharVarcharDDLTestBase
import org.apache.spark.sql.hive.test.TestHiveSingleton
import org.apache.spark.sql.internal.SQLConf

class HiveCharVarcharTestSuite extends CharVarcharTestSuite with TestHiveSingleton {

  // The default Hive serde doesn't support nested null values.
  override def format: String = "hive OPTIONS(fileFormat='parquet')"

  private var originalPartitionMode = ""

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    originalPartitionMode = spark.conf.get("hive.exec.dynamic.partition.mode", "")
    spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
  }

  override protected def afterAll(): Unit = {
    if (originalPartitionMode == "") {
      spark.conf.unset("hive.exec.dynamic.partition.mode")
    } else {
      spark.conf.set("hive.exec.dynamic.partition.mode", originalPartitionMode)
    }
    super.afterAll()
  }

  test("SPARK-33892: SHOW CREATE TABLE AS SERDE w/ char/varchar") {
    withTable("t") {
      sql(s"CREATE TABLE t(v VARCHAR(3), c CHAR(5)) USING $format")
      val rest = sql("SHOW CREATE TABLE t AS SERDE").head().getString(0)
      assert(rest.contains("VARCHAR(3)"))
      assert(rest.contains("CHAR(5)"))
    }
  }

  test("SPARK-34188: read side length check should not blocks CBO size estimating") {
    withTable("t") {
      sql(s"CREATE TABLE t(v VARCHAR(3), c CHAR(4)) USING $format")
      val stats = Some(CatalogStatistics(400L, Some(20L), Map(
        "v" -> CatalogColumnStat(Some(20L), Some("1"), Some("123"), Some(0), Some(4), Some(4), None,
          CatalogColumnStat.VERSION),
        "c" -> CatalogColumnStat(Some(19L), Some("0"), Some("1234"), Some(0), Some(4), Some(4),
          None, CatalogColumnStat.VERSION))
      ))
      spark.sessionState.catalog.alterTableStats(TableIdentifier("t"), stats)
      withSQLConf((SQLConf.CBO_ENABLED.key, "true"), (SQLConf.PLAN_STATS_ENABLED.key, "true")) {
        val newStat = spark.table("t").where("v = '124'").queryExecution.optimizedPlan.stats
        assert(newStat.rowCount.get === 1)
      }
    }
  }
}

class HiveCharVarcharDDLTestSuite extends CharVarcharDDLTestBase with TestHiveSingleton {

  // The default Hive serde doesn't support nested null values.
  override def format: String = "hive OPTIONS(fileFormat='parquet')"

  private var originalPartitionMode = ""

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    originalPartitionMode = spark.conf.get("hive.exec.dynamic.partition.mode", "")
    spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
  }

  override protected def afterAll(): Unit = {
    if (originalPartitionMode == "") {
      spark.conf.unset("hive.exec.dynamic.partition.mode")
    } else {
      spark.conf.set("hive.exec.dynamic.partition.mode", originalPartitionMode)
    }
    super.afterAll()
  }
}
