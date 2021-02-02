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

import org.apache.spark.sql.{AnalysisException, QueryTest, Row}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SQLTestUtils
import org.apache.spark.sql.types.{StringType, StructType}

trait ShowPartitionsSuiteBase extends QueryTest with SQLTestUtils {
  protected def version: String
  protected def catalog: String
  protected def defaultUsing: String
  // Gets the schema of `SHOW PARTITIONS`
  private val showSchema: StructType = new StructType().add("partition", StringType, false)
  protected def runShowPartitionsSql(sqlText: String, expected: Seq[Row]): Unit = {
    val df = spark.sql(sqlText)
    assert(df.schema === showSchema)
    checkAnswer(df, expected)
  }

  override def test(testName: String, testTags: Tag*)(testFun: => Any)
      (implicit pos: Position): Unit = {
    super.test(s"SHOW PARTITIONS $version: " + testName, testTags: _*)(testFun)
  }

  protected def createDateTable(table: String): Unit = {
    sql(s"""
      |CREATE TABLE $table (price int, qty int, year int, month int)
      |$defaultUsing
      |partitioned by (year, month)""".stripMargin)
    sql(s"INSERT INTO $table PARTITION(year = 2015, month = 1) SELECT 1, 1")
    sql(s"INSERT INTO $table PARTITION(year = 2015, month = 2) SELECT 2, 2")
    sql(s"ALTER TABLE $table ADD PARTITION(year = 2016, month = 2)")
    sql(s"ALTER TABLE $table ADD PARTITION(year = 2016, month = 3)")
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

  test("show partitions of non-partitioned table") {
    withNamespace(s"$catalog.ns") {
      sql(s"CREATE NAMESPACE $catalog.ns")
      val table = s"$catalog.ns.not_partitioned_table"
      withTable(table) {
        sql(s"CREATE TABLE $table (col1 int) $defaultUsing")
        val errMsg = intercept[AnalysisException] {
          sql(s"SHOW PARTITIONS $table")
        }.getMessage
        assert(errMsg.contains("not allowed on a table that is not partitioned"))
      }
    }
  }

  test("non-partitioning columns") {
    withNamespace(s"$catalog.ns") {
      sql(s"CREATE NAMESPACE $catalog.ns")
      val table = s"$catalog.ns.dateTable"
      withTable(table) {
        createDateTable(table)
        val errMsg = intercept[AnalysisException] {
          sql(s"SHOW PARTITIONS $table PARTITION(abcd=2015, xyz=1)")
        }.getMessage
        assert(errMsg.contains("abcd is not a valid partition column"))
      }
    }
  }

  test("show everything") {
    withNamespace(s"$catalog.ns") {
      sql(s"CREATE NAMESPACE $catalog.ns")
      val table = s"$catalog.ns.dateTable"
      withTable(table) {
        createDateTable(table)
        runShowPartitionsSql(
          s"show partitions $table",
          Row("year=2015/month=1") ::
          Row("year=2015/month=2") ::
          Row("year=2016/month=2") ::
          Row("year=2016/month=3") :: Nil)
      }
    }
  }

  test("filter by partitions") {
    withNamespace(s"$catalog.ns") {
      sql(s"CREATE NAMESPACE $catalog.ns")
      val table = s"$catalog.ns.dateTable"
      withTable(table) {
        createDateTable(table)
        runShowPartitionsSql(
          s"show partitions $table PARTITION(year=2015)",
          Row("year=2015/month=1") ::
          Row("year=2015/month=2") :: Nil)
        runShowPartitionsSql(
          s"show partitions $table PARTITION(year=2015, month=1)",
          Row("year=2015/month=1") :: Nil)
        runShowPartitionsSql(
          s"show partitions $table PARTITION(month=2)",
          Row("year=2015/month=2") ::
          Row("year=2016/month=2") :: Nil)
      }
    }
  }

  test("show everything more than 5 part keys") {
    withNamespace(s"$catalog.ns") {
      sql(s"CREATE NAMESPACE $catalog.ns")
      val table = s"$catalog.ns.wideTable"
      withTable(table) {
        createWideTable(table)
        runShowPartitionsSql(
          s"show partitions $table",
          Row("year=2016/month=3/hour=10/minute=10/sec=10/extra=1") ::
          Row("year=2016/month=4/hour=10/minute=10/sec=10/extra=1") :: Nil)
      }
    }
  }

  test("SPARK-33667: case sensitivity of partition spec") {
    withNamespace(s"$catalog.ns") {
      sql(s"CREATE NAMESPACE $catalog.ns")
      val t = s"$catalog.ns.part_table"
      withTable(t) {
        sql(s"""
          |CREATE TABLE $t (price int, qty int, year int, month int)
          |$defaultUsing
          |PARTITIONED BY (year, month)""".stripMargin)
        sql(s"INSERT INTO $t PARTITION(year = 2015, month = 1) SELECT 1, 1")
        Seq(
          true -> "PARTITION(year = 2015, month = 1)",
          false -> "PARTITION(YEAR = 2015, Month = 1)"
        ).foreach { case (caseSensitive, partitionSpec) =>
          withSQLConf(SQLConf.CASE_SENSITIVE.key -> caseSensitive.toString) {
            runShowPartitionsSql(
              s"SHOW PARTITIONS $t $partitionSpec",
              Row("year=2015/month=1") :: Nil)
          }
        }
      }
    }
  }

  test("SPARK-33777: sorted output") {
    withNamespace(s"$catalog.ns") {
      sql(s"CREATE NAMESPACE $catalog.ns")
      val table = s"$catalog.ns.dateTable"
      withTable(table) {
        sql(s"""
          |CREATE TABLE $table (id int, part string)
          |$defaultUsing
          |PARTITIONED BY (part)""".stripMargin)
        sql(s"ALTER TABLE $table ADD PARTITION(part = 'b')")
        sql(s"ALTER TABLE $table ADD PARTITION(part = 'a')")
        val partitions = sql(s"show partitions $table")
        assert(partitions.first().getString(0) === "part=a")
      }
    }
  }
}
