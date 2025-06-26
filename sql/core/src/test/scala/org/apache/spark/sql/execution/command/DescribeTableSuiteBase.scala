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
import org.apache.spark.sql.functions.{col, struct}
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
      sql(s"CREATE TABLE $tbl (col1 STRING, col2 struct<x:int, y:int>) " +
        s"$defaultUsing CLUSTER BY (col1, col2.x)")
      sql(s"ALTER TABLE $tbl ALTER COLUMN col1 COMMENT 'this is comment';")
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

  test("describe a clustered table - dataframe writer v1") {
    withNamespaceAndTable("ns", "tbl") { tbl =>
      val df = spark.range(10).select(
        col("id").cast("string").as("col1"),
        struct(col("id").cast("int").as("x"), col("id").cast("int").as("y")).as("col2"))
      df.write.mode("append").clusterBy("col1", "col2.x").saveAsTable(tbl)
      val descriptionDf = sql(s"DESC $tbl")

      descriptionDf.show(false)
      assert(descriptionDf.schema.map(field => (field.name, field.dataType)) === Seq(
        ("col_name", StringType),
        ("data_type", StringType),
        ("comment", StringType)))
      QueryTest.checkAnswer(
        descriptionDf,
        Seq(
          Row("col1", "string", null),
          Row("col2", "struct<x:int,y:int>", null),
          Row("# Clustering Information", "", ""),
          Row("# col_name", "data_type", "comment"),
          Row("col2.x", "int", null),
          Row("col1", "string", null)))
    }
  }

  test("describe a clustered table - dataframe writer v2") {
    withNamespaceAndTable("ns", "tbl") { tbl =>
      val df = spark.range(10).select(
        col("id").cast("string").as("col1"),
        struct(col("id").cast("int").as("x"), col("id").cast("int").as("y")).as("col2"))
      df.writeTo(tbl).clusterBy("col1", "col2.x").create()
      val descriptionDf = sql(s"DESC $tbl")

      descriptionDf.show(false)
      assert(descriptionDf.schema.map(field => (field.name, field.dataType)) === Seq(
        ("col_name", StringType),
        ("data_type", StringType),
        ("comment", StringType)))
      QueryTest.checkAnswer(
        descriptionDf,
        Seq(
          Row("col1", "string", null),
          Row("col2", "struct<x:int,y:int>", null),
          Row("# Clustering Information", "", ""),
          Row("# col_name", "data_type", "comment"),
          Row("col2.x", "int", null),
          Row("col1", "string", null)))
    }
  }

  Seq(true, false).foreach { hasCollations =>
    test(s"DESCRIBE TABLE EXTENDED with collation specified = $hasCollations") {

      withNamespaceAndTable("ns", "tbl") { tbl =>
        val getCollationDescription = () => sql(s"DESCRIBE TABLE EXTENDED $tbl")
          .where("col_name = 'Collation'")

        val defaultCollation = if (hasCollations) "DEFAULT COLLATION uNiCoDe" else ""

        sql(s"CREATE TABLE $tbl (id string) $defaultUsing $defaultCollation")
        val descriptionDf = getCollationDescription()

        if (hasCollations) {
          checkAnswer(descriptionDf, Seq(Row("Collation", "UNICODE", "")))
        } else {
          assert(descriptionDf.isEmpty)
        }

        sql(s"ALTER TABLE $tbl DEFAULT COLLATION UniCode_cI_rTrIm")
        val newDescription = getCollationDescription()
        checkAnswer(newDescription, Seq(Row("Collation", "UNICODE_CI_RTRIM", "")))
      }
    }
  }
}

/** Represents JSON output of DESCRIBE TABLE AS JSON */
case class DescribeTableJson(
    table_name: Option[String] = None,
    catalog_name: Option[String] = None,
    namespace: Option[List[String]] = Some(Nil),
    schema_name: Option[String] = None,
    columns: Option[List[TableColumn]] = Some(Nil),
    created_time: Option[String] = None,
    last_access: Option[String] = None,
    created_by: Option[String] = None,
    `type`: Option[String] = None,
    collation: Option[String] = None,
    provider: Option[String] = None,
    bucket_columns: Option[List[String]] = Some(Nil),
    sort_columns: Option[List[String]] = Some(Nil),
    comment: Option[String] = None,
    table_properties: Option[Map[String, String]] = None,
    location: Option[String] = None,
    serde_library: Option[String] = None,
    storage_properties: Option[Map[String, String]] = None,
    partition_provider: Option[String] = None,
    partition_columns: Option[List[String]] = Some(Nil),
    partition_values: Option[Map[String, String]] = None,
    clustering_columns: Option[List[String]] = None,
    statistics: Option[Map[String, Any]] = None,
    view_text: Option[String] = None,
    view_original_text: Option[String] = None,
    view_schema_mode: Option[String] = None,
    view_catalog_and_namespace: Option[String] = None,
    view_query_output_columns: Option[List[String]] = None,
    view_creation_spark_configuration: Option[Map[String, String]] = None
)

/** Used for columns field of DescribeTableJson */
case class TableColumn(
    name: String,
    `type`: Type,
    element_nullable: Boolean = true,
    comment: Option[String] = None,
    default: Option[String] = None
)

case class Type(
    name: String,
    collation: Option[String] = None,
    fields: Option[List[Field]] = None,
    `type`: Option[Type] = None,
    element_type: Option[Type] = None,
    key_type: Option[Type] = None,
    value_type: Option[Type] = None,
    comment: Option[String] = None,
    default: Option[String] = None,
    element_nullable: Option[Boolean] = Some(true),
    value_nullable: Option[Boolean] = Some(true),
    nullable: Option[Boolean] = Some(true)
)

case class Field(
    name: String,
    `type`: Type,
    element_nullable: Boolean = true,
    comment: Option[String] = None,
    default: Option[String] = None
)
