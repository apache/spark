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
import org.apache.spark.sql.{AnalysisException, Row}
import org.apache.spark.sql.connector.{InMemoryPartitionTableCatalog, InMemoryTableCatalog}
import org.apache.spark.sql.execution.command
import org.apache.spark.sql.test.SharedSparkSession

class ShowPartitionsSuite extends command.ShowPartitionsSuiteBase with SharedSparkSession {
  override def version: String = "V2"
  override def catalog: String = "test_catalog"
  override def defaultNamespace: Seq[String] = Nil
  override def defaultUsing: String = "USING _"

  override def sparkConf: SparkConf = super.sparkConf
    .set(s"spark.sql.catalog.$catalog", classOf[InMemoryPartitionTableCatalog].getName)
    .set(s"spark.sql.catalog.non_part_$catalog", classOf[InMemoryTableCatalog].getName)

  override protected def createDateTable(table: String): Unit = {
    sql(s"""
      |CREATE TABLE $table (price int, qty int, year int, month int)
      |$defaultUsing
      |partitioned by (year, month)""".stripMargin)
    sql(s"ALTER TABLE $table ADD PARTITION(year = 2015, month = 1)")
    sql(s"ALTER TABLE $table ADD PARTITION(year = 2015, month = 2)")
    sql(s"ALTER TABLE $table ADD PARTITION(year = 2016, month = 2)")
    sql(s"ALTER TABLE $table ADD PARTITION(year = 2016, month = 3)")
  }

  override protected def wrongPartitionColumnsError(columns: String*): String = {
    s"Partition key ${columns.mkString(",")} not exists"
  }

  test("a table does not support partitioning") {
    val table = s"non_part_$catalog.tab1"
    withTable(table) {
      sql(s"""
        |CREATE TABLE $table (price int, qty int, year int, month int)
        |$defaultUsing""".stripMargin)
      val errMsg = intercept[AnalysisException] {
        sql(s"SHOW PARTITIONS $table")
      }.getMessage
      assert(errMsg.contains(
        "SHOW PARTITIONS cannot run for a table which does not support partitioning"))
    }
  }

  test("filter by partitions") {
    val table = s"$catalog.dateTable"
    withTable(table) {
      createDateTable(table)
      runShowPartitionsSql(
        s"show partitions $table PARTITION(year=2015, month=1)",
        Row("year=2015/month=1") :: Nil)
    }
  }

  test("show everything more than 5 part keys") {
    val table = s"$catalog.wideTable"
    withTable(table) {
      sql(s"""
        |CREATE TABLE $table (
        |  price int, qty int,
        |  year int, month int, hour int, minute int, sec int, extra int)
        |$defaultUsing
        |PARTITIONED BY (year, month, hour, minute, sec, extra)""".stripMargin)
      sql(s"""
        |ALTER TABLE $table
        |ADD PARTITION(year = 2016, month = 3, hour = 10, minute = 10, sec = 10, extra = 1)
        """.stripMargin)
      sql(s"""
        |ALTER TABLE $table
        |ADD PARTITION(year = 2016, month = 4, hour = 10, minute = 10, sec = 10, extra = 1)
        """.stripMargin)
      runShowPartitionsSql(
        s"show partitions $table",
        Row("year=2016/month=3/hour=10/minute=10/sec=10/extra=1") ::
          Row("year=2016/month=4/hour=10/minute=10/sec=10/extra=1") :: Nil)
    }
  }
}
