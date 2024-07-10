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

import org.apache.spark.sql.{AnalysisException, QueryTest, Row}
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.catalyst.util.quoteIdentifier
import org.apache.spark.sql.types.{BooleanType, MetadataBuilder, StringType, StructType}

/**
 * This base suite contains unified tests for the `DESCRIBE TABLE` command that check V1 and V2
 * table catalogs. The tests that cannot run for all supported catalogs are located in more
 * specific test suites:
 *
 *   - V2 table catalog tests: `org.apache.spark.sql.execution.command.v2.DescribeTableSuite`
 *   - V1 table catalog tests:
 *     `org.apache.spark.sql.execution.command.v1.DescribeTableSuiteBase`
 *     - V1 In-Memory catalog: `org.apache.spark.sql.execution.command.v1.DescribeTableSuite`
 *     - V1 Hive External catalog:
 *        `org.apache.spark.sql.hive.execution.command.DescribeTableSuite`
 */
trait DescribeTableSuiteBase extends QueryTest with DDLCommandTestUtils {
  override val command = "DESCRIBE TABLE"

  test("DESCRIBE TABLE in a catalog when table does not exist") {
    withNamespaceAndTable("ns", "table") { tbl =>
      val parsed = CatalystSqlParser.parseMultipartIdentifier(s"${tbl}_non_existence")
        .map(part => quoteIdentifier(part)).mkString(".")
      val e = intercept[AnalysisException] {
        sql(s"DESCRIBE TABLE ${tbl}_non_existence")
      }
      checkErrorTableNotFound(e, parsed,
        ExpectedContext(s"${tbl}_non_existence", 15, 14 + s"${tbl}_non_existence".length))
    }
  }

  test("DESCRIBE TABLE of a non-partitioned table") {
    withNamespaceAndTable("ns", "table") { tbl =>
      spark.sql(s"CREATE TABLE $tbl (id bigint, data string) $defaultUsing")
      val descriptionDf = spark.sql(s"DESCRIBE TABLE $tbl")
      assert(descriptionDf.schema.map(field => (field.name, field.dataType)) ===
        Seq(
          ("col_name", StringType),
          ("data_type", StringType),
          ("comment", StringType)))
      QueryTest.checkAnswer(
        descriptionDf,
        Seq(
          Row("data", "string", null),
          Row("id", "bigint", null)))
    }
  }

  test("DESCRIBE TABLE of a partitioned table") {
    withNamespaceAndTable("ns", "table") { tbl =>
      spark.sql(s"CREATE TABLE $tbl (id bigint, data string) $defaultUsing PARTITIONED BY (id)")
      val descriptionDf = spark.sql(s"DESCRIBE TABLE $tbl")
      assert(descriptionDf.schema.map(field => (field.name, field.dataType)) === Seq(
        ("col_name", StringType),
        ("data_type", StringType),
        ("comment", StringType)))
      QueryTest.checkAnswer(
        descriptionDf.filter("col_name != 'Created Time'"),
        Seq(
          Row("data", "string", null),
          Row("id", "bigint", null),
          Row("# Partition Information", "", ""),
          Row("# col_name", "data_type", "comment"),
          Row("id", "bigint", null)))
    }
  }

  test("SPARK-34561: drop/add columns to a dataset of `DESCRIBE TABLE`") {
    withNamespaceAndTable("ns", "table") { tbl =>
      sql(s"CREATE TABLE $tbl (c0 INT) $defaultUsing")
      val description = sql(s"DESCRIBE TABLE $tbl")
      val noCommentDataset = description.drop("comment")
      val expectedSchema = new StructType()
        .add(
          name = "col_name",
          dataType = StringType,
          nullable = false,
          metadata = new MetadataBuilder().putString("comment", "name of the column").build())
        .add(
          name = "data_type",
          dataType = StringType,
          nullable = false,
          metadata = new MetadataBuilder().putString("comment", "data type of the column").build())
      assert(noCommentDataset.schema === expectedSchema)
      val isNullDataset = noCommentDataset
        .withColumn("is_null", noCommentDataset("col_name").isNull)
      assert(isNullDataset.schema === expectedSchema.add("is_null", BooleanType, false))
    }
  }

  test("SPARK-34576: drop/add columns to a dataset of `DESCRIBE COLUMN`") {
    withNamespaceAndTable("ns", "table") { tbl =>
      sql(s"CREATE TABLE $tbl (c0 INT) $defaultUsing")
      val description = sql(s"DESCRIBE TABLE $tbl c0")
      val noCommentDataset = description.drop("info_value")
      val expectedSchema = new StructType()
        .add(
          name = "info_name",
          dataType = StringType,
          nullable = false,
          metadata = new MetadataBuilder().putString("comment", "name of the column info").build())
      assert(noCommentDataset.schema === expectedSchema)
      val isNullDataset = noCommentDataset
        .withColumn("is_null", noCommentDataset("info_name").isNull)
      assert(isNullDataset.schema === expectedSchema.add("is_null", BooleanType, false))
    }
  }

  test("describe a column") {
    withNamespaceAndTable("ns", "tbl") { tbl =>
      sql(s"""
        |CREATE TABLE $tbl
        |(key int COMMENT 'column_comment', col struct<x:int, y:string>)
        |$defaultUsing""".stripMargin)
      val descriptionDf = sql(s"DESC $tbl key")
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

  test("describe a column with fully qualified name") {
    withNamespaceAndTable("ns", "tbl") { tbl =>
      sql(s"CREATE TABLE $tbl (key int COMMENT 'comment1') $defaultUsing")
      QueryTest.checkAnswer(
        sql(s"DESC $tbl $tbl.key"),
        Seq(Row("col_name", "key"), Row("data_type", "int"), Row("comment", "comment1")))
    }
  }

  test("describe complex columns") {
    withNamespaceAndTable("ns", "tbl") { tbl =>
      sql(s"CREATE TABLE $tbl (`a.b` int, col struct<x:int, y:string>) $defaultUsing")
      QueryTest.checkAnswer(
        sql(s"DESC $tbl `a.b`"),
        Seq(Row("col_name", "a.b"), Row("data_type", "int"), Row("comment", "NULL")))
      QueryTest.checkAnswer(
        sql(s"DESCRIBE $tbl col"),
        Seq(
          Row("col_name", "col"),
          Row("data_type", "struct<x:int,y:string>"),
          Row("comment", "NULL")))
    }
  }

  test("describe a nested column") {
    withNamespaceAndTable("ns", "tbl") { tbl =>
      sql(s"CREATE TABLE $tbl (`a.b` int, col struct<x:int, y:string>) $defaultUsing")
      val errMsg = intercept[AnalysisException] {
        sql(s"DESCRIBE TABLE $tbl col.x")
      }.getMessage
      assert(errMsg === "DESC TABLE COLUMN does not support nested column: col.x.")
    }
  }

  test("describe a clustered table") {
    withNamespaceAndTable("ns", "tbl") { tbl =>
      sql(s"CREATE TABLE $tbl (col1 STRING COMMENT 'this is comment', col2 struct<x:int, y:int>) " +
        s"$defaultUsing CLUSTER BY (col1, col2.x)")
      val descriptionDf = sql(s"DESC $tbl")
      assert(descriptionDf.schema.map(field => (field.name, field.dataType)) === Seq(
        ("col_name", StringType),
        ("data_type", StringType),
        ("comment", StringType)))
      QueryTest.checkAnswer(
        descriptionDf,
        Seq(
          Row("col1", "string", "this is comment"),
          Row("col2", "struct<x:int,y:int>", null),
          Row("# Clustering Information", "", ""),
          Row("# col_name", "data_type", "comment"),
          Row("col1", "string", "this is comment"),
          Row("col2.x", "int", null)))
    }
  }

  test("describe a clustered table - alter table cluster by") {
    withNamespaceAndTable("ns", "tbl") { tbl =>
      sql(s"CREATE TABLE $tbl (col1 STRING COMMENT 'this is comment', col2 struct<x:int, y:int>) " +
        s"$defaultUsing CLUSTER BY (col1, col2.x)")
      sql(s"ALTER TABLE $tbl CLUSTER BY (col2.y, col1)")
      val descriptionDf = sql(s"DESC $tbl")
      assert(descriptionDf.schema.map(field => (field.name, field.dataType)) === Seq(
        ("col_name", StringType),
        ("data_type", StringType),
        ("comment", StringType)))
      QueryTest.checkAnswer(
        descriptionDf,
        Seq(
          Row("col1", "string", "this is comment"),
          Row("col2", "struct<x:int,y:int>", null),
          Row("# Clustering Information", "", ""),
          Row("# col_name", "data_type", "comment"),
          Row("col2.y", "int", null),
          Row("col1", "string", "this is comment")))
    }
  }

  test("describe a clustered table - alter table cluster by none") {
    withNamespaceAndTable("ns", "tbl") { tbl =>
      sql(s"CREATE TABLE $tbl (col1 STRING COMMENT 'this is comment', col2 struct<x:int, y:int>) " +
        s"$defaultUsing CLUSTER BY (col1, col2.x)")
      sql(s"ALTER TABLE $tbl CLUSTER BY NONE")
      val descriptionDf = sql(s"DESC $tbl")
      assert(descriptionDf.schema.map(field => (field.name, field.dataType)) === Seq(
        ("col_name", StringType),
        ("data_type", StringType),
        ("comment", StringType)))
      QueryTest.checkAnswer(
        descriptionDf,
        Seq(
          Row("col1", "string", "this is comment"),
          Row("col2", "struct<x:int,y:int>", null),
          Row("# Clustering Information", "", ""),
          Row("# col_name", "data_type", "comment")))
    }
  }
}
