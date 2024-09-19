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

import org.apache.spark.sql.{AnalysisException, QueryTest, Row}
import org.apache.spark.sql.connector.catalog.TableCatalog
import org.apache.spark.sql.execution.command
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StringType
import org.apache.spark.util.Utils

/**
 * The class contains tests for the `DESCRIBE TABLE` command to check V2 table catalogs.
 */
class DescribeTableSuite extends command.DescribeTableSuiteBase
  with CommandSuiteBase {

  test("Describing a partition is not supported") {
    withNamespaceAndTable("ns", "table") { tbl =>
      spark.sql(s"CREATE TABLE $tbl (id bigint, data string) $defaultUsing " +
        "PARTITIONED BY (id)")
      val e = intercept[AnalysisException] {
        sql(s"DESCRIBE TABLE $tbl PARTITION (id = 1)")
      }
      assert(e.message === "DESCRIBE does not support partition for v2 tables.")
    }
  }

  test("DESCRIBE TABLE of a partitioned table by nested columns") {
    withNamespaceAndTable("ns", "table") { tbl =>
      sql(s"CREATE TABLE $tbl (s struct<id:INT, a:BIGINT>, data string) " +
        s"$defaultUsing PARTITIONED BY (s.id, s.a)")
      val descriptionDf = sql(s"DESCRIBE TABLE $tbl")
      QueryTest.checkAnswer(
        descriptionDf.filter("col_name != 'Created Time'"),
        Seq(
          Row("data", "string", null),
          Row("s", "struct<id:int,a:bigint>", null),
          Row("# Partition Information", "", ""),
          Row("# col_name", "data_type", "comment"),
          Row("s.id", "int", null),
          Row("s.a", "bigint", null)))
    }
  }

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
        descriptionDf,
        Seq(
          Row("id", "bigint", null),
          Row("data", "string", null),
          Row("# Partition Information", "", ""),
          Row("# col_name", "data_type", "comment"),
          Row("id", "bigint", null),
          Row("", "", ""),
          Row("# Metadata Columns", "", ""),
          Row("index", "int", "Metadata column used to conflict with a data column"),
          Row("_partition", "string", "Partition key used to store the row"),
          Row("", "", ""),
          Row("# Detailed Table Information", "", ""),
          Row("Name", tbl, ""),
          Row("Type", "MANAGED", ""),
          Row("Comment", "this is a test table", ""),
          Row("Location", "file:/tmp/testcat/table_name", ""),
          Row("Provider", "_", ""),
          Row(TableCatalog.PROP_OWNER.capitalize, Utils.getCurrentUserName(), ""),
          Row("Table Properties", "[bar=baz]", "")))
    }
  }

  test("describe a non-existent column") {
    withNamespaceAndTable("ns", "tbl") { tbl =>
      sql(s"""
        |CREATE TABLE $tbl
        |(key int COMMENT 'column_comment', col struct<x:int, y:string>)
        |$defaultUsing""".stripMargin)
      val query = s"DESC $tbl key1"
      checkError(
        exception = intercept[AnalysisException] {
          sql(query).collect()
        },
        errorClass = "UNRESOLVED_COLUMN.WITH_SUGGESTION",
        sqlState = "42703",
        parameters = Map(
          "objectName" -> "`key1`",
          "proposal" -> "`test_catalog`.`ns`.`tbl`.`key`, `test_catalog`.`ns`.`tbl`.`col`"),
        context = ExpectedContext(
          fragment = query,
          start = 0,
          stop = query.length -1)
      )
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
        val query = s"DESC $tbl KEY"
        checkError(
          exception = intercept[AnalysisException] {
            sql(query).collect()
          },
          errorClass = "UNRESOLVED_COLUMN.WITH_SUGGESTION",
          sqlState = "42703",
          parameters = Map(
            "objectName" -> "`KEY`",
            "proposal" -> "`test_catalog`.`ns`.`tbl`.`key`"),
          context = ExpectedContext(
            fragment = query,
            start = 0,
            stop = query.length - 1))
      }
    }
  }

  test("describe extended (formatted) a column") {
    withNamespaceAndTable("ns", "tbl") { tbl =>
      sql(s"""
        |CREATE TABLE $tbl
        |(key INT COMMENT 'column_comment', col STRING)
        |$defaultUsing""".stripMargin)

      sql(s"INSERT INTO $tbl values (1, 'aaa'), (2, 'bbb'), (3, 'ccc'), (null, 'ddd')")
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
          Row("num_nulls", "1"),
          Row("distinct_count", "4"),
          Row("avg_col_len", "NULL"),
          Row("max_col_len", "NULL")))
    }
  }

  test("SPARK-46535: describe extended (formatted) a column without col stats") {
    withNamespaceAndTable("ns", "tbl") { tbl =>
      sql(
        s"""
           |CREATE TABLE $tbl
           |(key INT COMMENT 'column_comment', col STRING)
           |$defaultUsing""".stripMargin)

      val descriptionDf = sql(s"DESCRIBE TABLE EXTENDED $tbl key")
      assert(descriptionDf.schema.map(field => (field.name, field.dataType)) === Seq(
        ("info_name", StringType),
        ("info_value", StringType)))
      QueryTest.checkAnswer(
        descriptionDf,
        Seq(
          Row("col_name", "key"),
          Row("data_type", "int"),
          Row("comment", "column_comment")))
    }
  }
}
