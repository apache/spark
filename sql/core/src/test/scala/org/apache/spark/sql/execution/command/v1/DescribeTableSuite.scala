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

import java.util.Locale

import org.apache.spark.sql.{AnalysisException, QueryTest, Row}
import org.apache.spark.sql.connector.catalog.CatalogManager.SESSION_CATALOG_NAME
import org.apache.spark.sql.execution.command
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StringType

/**
 * This base suite contains unified tests for the `DESCRIBE TABLE` command that checks V1
 * table catalogs. The tests that cannot run for all V1 catalogs are located in more
 * specific test suites:
 *
 *   - V1 In-Memory catalog: `org.apache.spark.sql.execution.command.v1.DescribeTableSuite`
 *   - V1 Hive External catalog:
 *     `org.apache.spark.sql.hive.execution.command.DescribeTableSuite`
 */
trait DescribeTableSuiteBase extends command.DescribeTableSuiteBase
  with command.TestsV1AndV2Commands {

  def getProvider(): String = defaultUsing.stripPrefix("USING").trim.toLowerCase(Locale.ROOT)

  test("Describing of a non-existent partition") {
    withNamespaceAndTable("ns", "table") { tbl =>
      spark.sql(s"CREATE TABLE $tbl (id bigint, data string) $defaultUsing " +
        "PARTITIONED BY (id)")
      val e = intercept[AnalysisException] {
        sql(s"DESCRIBE TABLE $tbl PARTITION (id = 1)")
      }
      assert(e.message === "Partition not found in table 'table' database 'ns':\nid -> 1")
    }
  }

  test("describe a non-existent column") {
    withNamespaceAndTable("ns", "tbl") { tbl =>
      sql(s"""
        |CREATE TABLE $tbl
        |(key int COMMENT 'column_comment', col struct<x:int, y:string>)
        |$defaultUsing""".stripMargin)
      val errMsg = intercept[AnalysisException] {
        sql(s"DESC $tbl key1").collect()
      }.getMessage
      assert(errMsg === "Column key1 does not exist.")
    }
  }

  test("describe a column in case insensitivity") {
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "false") {
      withNamespaceAndTable("ns", "tbl") { tbl =>
        sql(s"CREATE TABLE $tbl (key int COMMENT 'comment1') $defaultUsing")
        QueryTest.checkAnswer(
          sql(s"DESC $tbl KEY"),
          Seq(Row("col_name", "KEY"), Row("data_type", "int"), Row("comment", "comment1")))
      }
    }

    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
      withNamespaceAndTable("ns", "tbl") { tbl =>
        sql(s"CREATE TABLE $tbl (key int COMMENT 'comment1') $defaultUsing")
        val errMsg = intercept[AnalysisException] {
          sql(s"DESC $tbl KEY").collect()
        }.getMessage
        assert(errMsg === "Column KEY does not exist.")
      }
    }
  }

  test("describe extended (formatted) a column") {
    withNamespaceAndTable("ns", "tbl") { tbl =>
      sql(s"""
        |CREATE TABLE $tbl
        |(key INT COMMENT 'column_comment', col STRING)
        |$defaultUsing""".stripMargin)
      sql(s"INSERT INTO $tbl SELECT 1, 'a'")
      sql(s"INSERT INTO $tbl SELECT 2, 'b'")
      sql(s"INSERT INTO $tbl SELECT 3, 'c'")
      sql(s"INSERT INTO $tbl SELECT null, 'd'")

      val descriptionDf = sql(s"DESCRIBE TABLE EXTENDED $tbl key")
      assert(descriptionDf.schema.map(field => (field.name, field.dataType)) === Seq(
        ("info_name", StringType),
        ("info_value", StringType)))
      QueryTest.checkAnswer(
        descriptionDf,
        Seq(
          Row("col_name", "key"),
          Row("data_type", "int"),
          Row("comment", "column_comment"),
          Row("min", "NULL"),
          Row("max", "NULL"),
          Row("num_nulls", "NULL"),
          Row("distinct_count", "NULL"),
          Row("avg_col_len", "NULL"),
          Row("max_col_len", "NULL"),
          Row("histogram", "NULL")))
      sql(s"ANALYZE TABLE $tbl COMPUTE STATISTICS FOR COLUMNS key")

      Seq("EXTENDED", "FORMATTED").foreach { extended =>
        val descriptionDf2 = sql(s"DESCRIBE TABLE $extended $tbl key")
        QueryTest.checkAnswer(
          descriptionDf2,
          Seq(
            Row("col_name", "key"),
            Row("data_type", "int"),
            Row("comment", "column_comment"),
            Row("min", "1"),
            Row("max", "3"),
            Row("num_nulls", "1"),
            Row("distinct_count", "3"),
            Row("avg_col_len", "4"),
            Row("max_col_len", "4"),
            Row("histogram", "NULL")))
      }
    }
  }

  test("describe a column with histogram statistics") {
    withSQLConf(
      SQLConf.HISTOGRAM_ENABLED.key -> "true",
      SQLConf.HISTOGRAM_NUM_BINS.key -> "2") {
      withNamespaceAndTable("ns", "tbl") { tbl =>
        sql(s"""
          |CREATE TABLE $tbl
          |(key INT COMMENT 'column_comment', col STRING)
          |$defaultUsing""".stripMargin)
        sql(s"INSERT INTO $tbl SELECT 1, 'a'")
        sql(s"INSERT INTO $tbl SELECT 2, 'b'")
        sql(s"INSERT INTO $tbl SELECT 3, 'c'")
        sql(s"INSERT INTO $tbl SELECT null, 'd'")
        sql(s"ANALYZE TABLE $tbl COMPUTE STATISTICS FOR COLUMNS key")

        val descriptionDf = sql(s"DESCRIBE TABLE EXTENDED $tbl key")
        QueryTest.checkAnswer(
          descriptionDf,
          Seq(
            Row("col_name", "key"),
            Row("data_type", "int"),
            Row("comment", "column_comment"),
            Row("min", "1"),
            Row("max", "3"),
            Row("num_nulls", "1"),
            Row("distinct_count", "3"),
            Row("avg_col_len", "4"),
            Row("max_col_len", "4"),
            Row("histogram", "height: 1.5, num_of_bins: 2"),
            Row("bin_0", "lower_bound: 1.0, upper_bound: 2.0, distinct_count: 2"),
            Row("bin_1", "lower_bound: 2.0, upper_bound: 3.0, distinct_count: 1")))
      }
    }
  }
}

/**
 * The class contains tests for the `DESCRIBE TABLE` command to check V1 In-Memory
 * table catalog.
 */
class DescribeTableSuite extends DescribeTableSuiteBase with CommandSuiteBase {
  override def commandVersion: String = super[DescribeTableSuiteBase].commandVersion

  test("DESCRIBE TABLE EXTENDED of a partitioned table") {
    withNamespaceAndTable("ns", "table") { tbl =>
      spark.sql(s"CREATE TABLE $tbl (id bigint, data string) $defaultUsing" +
        " PARTITIONED BY (id)" +
        " TBLPROPERTIES ('bar'='baz')" +
        " COMMENT 'this is a test table'" +
        " LOCATION 'file:/tmp/testcat/table_name'")
      val descriptionDf = spark.sql(s"DESCRIBE TABLE EXTENDED $tbl")
      assert(descriptionDf.schema.map(field => (field.name, field.dataType)) === Seq(
        ("col_name", StringType),
        ("data_type", StringType),
        ("comment", StringType)))
      QueryTest.checkAnswer(
        descriptionDf.filter("!(col_name in ('Created Time', 'Created By'))"),
        Seq(
          Row("data", "string", null),
          Row("id", "bigint", null),
          Row("# Partition Information", "", ""),
          Row("# col_name", "data_type", "comment"),
          Row("id", "bigint", null),
          Row("", "", ""),
          Row("# Detailed Table Information", "", ""),
          Row("Catalog", SESSION_CATALOG_NAME, ""),
          Row("Database", "ns", ""),
          Row("Table", "table", ""),
          Row("Last Access", "UNKNOWN", ""),
          Row("Type", "EXTERNAL", ""),
          Row("Provider", getProvider(), ""),
          Row("Comment", "this is a test table", ""),
          Row("Table Properties", "[bar=baz]", ""),
          Row("Location", "file:/tmp/testcat/table_name", ""),
          Row("Partition Provider", "Catalog", "")))
    }
  }
}
