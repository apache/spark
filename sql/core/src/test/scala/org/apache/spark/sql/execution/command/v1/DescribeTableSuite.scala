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

import org.json4s._
import org.json4s.jackson.JsonMethods.parse

import org.apache.spark.SPARK_VERSION
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
  implicit val formats: org.json4s.DefaultFormats.type = org.json4s.DefaultFormats

  def getProvider(): String = defaultUsing.stripPrefix("USING").trim.toLowerCase(Locale.ROOT)

  test("Describing of a non-existent partition") {
    withNamespaceAndTable("ns", "table") { tbl =>
      spark.sql(s"CREATE TABLE $tbl (id bigint, data string) $defaultUsing " +
        "PARTITIONED BY (id)")
      val e = intercept[AnalysisException] {
        sql(s"DESCRIBE TABLE $tbl PARTITION (id = 1)")
      }
      checkError(e,
        condition = "PARTITIONS_NOT_FOUND",
        parameters = Map("partitionList" -> "PARTITION (`id` = 1)",
          "tableName" -> "`ns`.`table`"))
    }
  }

  test("describe a non-existent column") {
    withNamespaceAndTable("ns", "tbl") { tbl =>
      sql(s"""
        |CREATE TABLE $tbl
        |(key int COMMENT 'column_comment', col struct<x:int, y:string>)
        |$defaultUsing""".stripMargin)
      checkError(
        exception = intercept[AnalysisException] {
          sql(s"DESC $tbl key1").collect()
        },
        condition = "COLUMN_NOT_FOUND",
        parameters = Map(
          "colName" -> "`key1`",
          "caseSensitiveConfig" -> "\"spark.sql.caseSensitive\""
        )
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
        checkError(
          exception = intercept[AnalysisException] {
            sql(s"DESC $tbl KEY").collect()
          },
          condition = "COLUMN_NOT_FOUND",
          parameters = Map(
            "colName" -> "`KEY`",
            "caseSensitiveConfig" -> "\"spark.sql.caseSensitive\""
          )
        )
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

  test("describe a column with a default value") {
    withTable("t") {
      sql(s"create table t(a int default 42) $defaultUsing")
      val descriptionDf = sql("describe table extended t a")
      QueryTest.checkAnswer(
        descriptionDf,
        Seq(
          Row("col_name", "a"),
          Row("data_type", "int"),
          Row("comment", "NULL"),
          Row("default", "42"),
          Row("min", "NULL"),
          Row("max", "NULL"),
          Row("num_nulls", "NULL"),
          Row("distinct_count", "NULL"),
          Row("max_col_len", "NULL"),
          Row("avg_col_len", "NULL"),
          Row("histogram", "NULL")))
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
        " DEFAULT COLLATION unicode" +
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
          Row("Collation", "UNICODE", ""),
          Row("Table Properties", "[bar=baz]", ""),
          Row("Location", "file:/tmp/testcat/table_name", ""),
          Row("Partition Provider", "Catalog", "")))
    }
  }

  test("DESCRIBE TABLE EXTENDED of a table with a default column value") {
    withTable("t") {
      spark.sql(s"CREATE TABLE t (id bigint default 42) $defaultUsing")
      val descriptionDf = spark.sql(s"DESCRIBE TABLE EXTENDED t")
      assert(descriptionDf.schema.map { field =>
        (field.name, field.dataType)
      } === Seq(
        ("col_name", StringType),
        ("data_type", StringType),
        ("comment", StringType)))
      QueryTest.checkAnswer(
        descriptionDf.filter(
          "!(col_name in ('Created Time', 'Created By', 'Database', 'Location', " +
            "'Provider', 'Type'))"),
        Seq(
          Row("id", "bigint", null),
          Row("", "", ""),
          Row("# Detailed Table Information", "", ""),
          Row("Catalog", SESSION_CATALOG_NAME, ""),
          Row("Table", "t", ""),
          Row("Last Access", "UNKNOWN", ""),
          Row("", "", ""),
          Row("# Column Default Values", "", ""),
          Row("id", "bigint", "42")
        ))
    }
  }

  test("DESCRIBE AS JSON throws when not EXTENDED") {
    withNamespaceAndTable("ns", "table") { t =>
      val tableCreationStr =
        s"""
           |CREATE TABLE $t (
           |  employee_id INT,
           |  employee_name STRING,
           |  department STRING,
           |  hire_date DATE
           |) USING parquet
           |OPTIONS ('compression' = 'snappy', 'max_records' = '1000')
           |PARTITIONED BY (department, hire_date)
           |CLUSTERED BY (employee_id) SORTED BY (employee_name ASC) INTO 4 BUCKETS
           |COMMENT 'Employee data table for testing partitions and buckets'
           |TBLPROPERTIES ('version' = '1.0')
           |""".stripMargin
      spark.sql(tableCreationStr)

      val error = intercept[AnalysisException] {
        spark.sql(s"DESCRIBE $t AS JSON")
      }

      checkError(
        exception = error,
        condition = "DESCRIBE_JSON_NOT_EXTENDED",
        parameters = Map("tableName" -> "table"))
    }
  }

  test("DESCRIBE AS JSON partitions, clusters, buckets") {
    withNamespaceAndTable("ns", "table") { t =>
      val tableCreationStr =
        s"""
           |CREATE TABLE $t (
           |  employee_id INT,
           |  employee_name STRING,
           |  department STRING,
           |  hire_date DATE
           |) USING parquet
           |OPTIONS ('compression' = 'snappy', 'max_records' = '1000')
           |PARTITIONED BY (department, hire_date)
           |CLUSTERED BY (employee_id) SORTED BY (employee_name ASC) INTO 4 BUCKETS
           |COMMENT 'Employee data table for testing partitions and buckets'
           |TBLPROPERTIES ('version' = '1.0')
           |""".stripMargin
      spark.sql(tableCreationStr)
      val descriptionDf = spark.sql(s"DESCRIBE EXTENDED $t AS JSON")
      val firstRow = descriptionDf.select("json_metadata").head()
      val jsonValue = firstRow.getString(0)
      val parsedOutput = parse(jsonValue).extract[DescribeTableJson]

      val expectedOutput = DescribeTableJson(
        table_name = Some("table"),
        catalog_name = Some(SESSION_CATALOG_NAME),
        namespace = Some(List("ns")),
        schema_name = Some("ns"),
        columns = Some(List(
          TableColumn("employee_id", Type("integer"), true),
          TableColumn("employee_name", Type("string"), true),
          TableColumn("department", Type("string"), true),
          TableColumn("hire_date", Type("date"), true)
        )),
        owner = Some(""),
        created_time = Some(""),
        last_access = Some("UNKNOWN"),
        created_by = Some(s"Spark $SPARK_VERSION"),
        `type` = Some("MANAGED"),
        provider = Some("parquet"),
        bucket_columns = Some(List("employee_id")),
        sort_columns = Some(List("employee_name")),
        comment = Some("Employee data table for testing partitions and buckets"),
        table_properties = Some(Map(
          "version" -> "1.0"
        )),
        location = Some(""),
        serde_library = Some("org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"),
        inputformat = Some("org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"),
        outputformat = Some("org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"),
        storage_properties = Some(Map(
          "compression" -> "snappy",
          "max_records" -> "1000"
        )),
        partition_provider = Some("Catalog"),
        partition_columns = Some(List("department", "hire_date"))
      )

      if (getProvider() == "hive") {
        assert(expectedOutput == parsedOutput.copy(owner = Some(""),
          created_time = Some(""),
          location = Some("")))
      } else {
        assert(expectedOutput.copy(inputformat = None, outputformat = None, serde_library = None)
          == parsedOutput.copy(owner = Some(""), created_time = Some(""), location = Some("")))
      }
    }
  }

  test("DESCRIBE AS JSON partition spec") {
    withNamespaceAndTable("ns", "table") { t =>
      val tableCreationStr =
        s"""
           |CREATE TABLE $t (
           |  id INT,
           |  name STRING,
           |  region STRING,
           |  category STRING
           |) USING parquet
           |PARTITIONED BY (region, category)
           |COMMENT 'test partition spec'
           |TBLPROPERTIES ('t' = 'test')
           |""".stripMargin
      spark.sql(tableCreationStr)
      spark.sql(s"ALTER TABLE $t ADD PARTITION (region='USA', category='tech')")

      val descriptionDf =
        spark.sql(s"DESCRIBE FORMATTED $t PARTITION (region='USA', category='tech') AS JSON")
      val firstRow = descriptionDf.select("json_metadata").head()
      val jsonValue = firstRow.getString(0)
      val parsedOutput = parse(jsonValue).extract[DescribeTableJson]

      val expectedOutput = DescribeTableJson(
        table_name = Some("table"),
        catalog_name = Some("spark_catalog"),
        namespace = Some(List("ns")),
        schema_name = Some("ns"),
        columns = Some(List(
          TableColumn("id", Type("integer"), true),
          TableColumn("name", Type("string"), true),
          TableColumn("region", Type("string"), true),
          TableColumn("category", Type("string"), true)
        )),
        last_access = Some("UNKNOWN"),
        created_by = Some(s"Spark $SPARK_VERSION"),
        `type` = Some("MANAGED"),
        provider = Some("parquet"),
        bucket_columns = Some(Nil),
        sort_columns = Some(Nil),
        comment = Some("test partition spec"),
        table_properties = Some(Map(
          "t" -> "test"
        )),
        serde_library = Some("org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"),
        inputformat = Some("org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"),
        outputformat = Some("org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"),
        storage_properties = Some(Map(
          "serialization.format" -> "1"
        )),
        partition_provider = Some("Catalog"),
        partition_columns = Some(List("region", "category")),
        partition_values = Some(Map("region" -> "USA", "category" -> "tech"))
      )

      val filteredParsedStorageProperties =
        parsedOutput.storage_properties.map(_.filterNot { case (key, _) => key == "path" })

      if (getProvider() == "hive") {
        assert(expectedOutput ==
          parsedOutput.copy(location = None, created_time = None, owner = None,
            storage_properties = filteredParsedStorageProperties))
      } else {
        assert(expectedOutput.copy(
          inputformat = None, outputformat = None, serde_library = None, storage_properties = None)
          == parsedOutput.copy(location = None, created_time = None, owner = None,
            storage_properties = filteredParsedStorageProperties))
      }
    }
  }

  test("DESCRIBE AS JSON default values") {
    withNamespaceAndTable("ns", "table") { t =>
      val tableCreationStr =
        s"""
           |CREATE TABLE $t (
           |  id INT DEFAULT 1,
           |  name STRING DEFAULT 'unknown',
           |  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
           |  is_active BOOLEAN DEFAULT true
           |)
           |USING parquet COMMENT 'table_comment'
           |""".stripMargin
      spark.sql(tableCreationStr)

      val descriptionDf = spark.sql(s"DESC EXTENDED $t AS JSON")
      val firstRow = descriptionDf.select("json_metadata").head()
      val jsonValue = firstRow.getString(0)
      val parsedOutput = parse(jsonValue).extract[DescribeTableJson]

      val expectedOutput = DescribeTableJson(
        table_name = Some("table"),
        catalog_name = Some("spark_catalog"),
        namespace = Some(List("ns")),
        schema_name = Some("ns"),
        columns = Some(List(
          TableColumn("id", Type("integer"), default = Some("1")),
          TableColumn("name", Type("string"), default = Some("'unknown'")),
          TableColumn("created_at", Type("timestamp_ltz"), default = Some("CURRENT_TIMESTAMP")),
          TableColumn("is_active", Type("boolean"), default = Some("true"))
        )),
        last_access = Some("UNKNOWN"),
        created_by = Some(s"Spark $SPARK_VERSION"),
        `type` = Some("MANAGED"),
        storage_properties = None,
        provider = Some("parquet"),
        bucket_columns = Some(Nil),
        sort_columns = Some(Nil),
        comment = Some("table_comment"),
        serde_library = Some("org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"),
        inputformat = Some("org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"),
        outputformat = Some("org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"),
        table_properties = None
      )
      if (getProvider() == "hive") {
        assert(
          expectedOutput ==
            parsedOutput.copy(location = None, created_time = None, owner = None)
        )
      } else {
        assert(
          expectedOutput.copy(inputformat = None, outputformat = None, serde_library = None) ==
            parsedOutput.copy(location = None, created_time = None, owner = None)
        )
      }
    }
  }

  test("DESCRIBE AS JSON temp view") {
    withNamespaceAndTable("ns", "table") { t =>
      withTempView("temp_view") {
        val tableCreationStr =
          s"""
             |CREATE TABLE $t (id INT, name STRING, created_at TIMESTAMP)
             |  USING parquet
             |  OPTIONS ('compression' 'snappy')
             |  CLUSTERED BY (id, name) SORTED BY (created_at) INTO 4 BUCKETS
             |  COMMENT 'test temp view'
             |  TBLPROPERTIES ('parquet.encryption' = 'true')
             |""".stripMargin
        spark.sql(tableCreationStr)
        spark.sql(s"CREATE TEMPORARY VIEW temp_view AS SELECT * FROM $t")
        val descriptionDf = spark.sql(s"DESCRIBE EXTENDED temp_view AS JSON")
        val firstRow = descriptionDf.select("json_metadata").head()
        val jsonValue = firstRow.getString(0)
        val parsedOutput = parse(jsonValue).extract[DescribeTableJson]

        val expectedOutput = DescribeTableJson(
          columns = Some(List(
            TableColumn("id", Type("integer")),
            TableColumn("name", Type("string")),
            TableColumn("created_at", Type("timestamp_ltz"))
          ))
        )

        assert(expectedOutput == parsedOutput)
      }
    }
  }

  test("DESCRIBE AS JSON persistent view") {
    withNamespaceAndTable("ns", "table") { t =>
      withView("view") {
        val tableCreationStr =
          s"""
             |CREATE TABLE $t (id INT, name STRING, created_at TIMESTAMP)
             |  USING parquet
             |  OPTIONS ('compression' 'snappy')
             |  CLUSTERED BY (id, name) SORTED BY (created_at) INTO 4 BUCKETS
             |  COMMENT 'test temp view'
             |  TBLPROPERTIES ('parquet.encryption' = 'true')
             |""".stripMargin
        spark.sql(tableCreationStr)
        spark.sql(s"CREATE VIEW view AS SELECT * FROM $t")
        val descriptionDf = spark.sql(s"DESCRIBE EXTENDED view AS JSON")
        val firstRow = descriptionDf.select("json_metadata").head()
        val jsonValue = firstRow.getString(0)
        val parsedOutput = parse(jsonValue).extract[DescribeTableJson]

        val expectedOutput = DescribeTableJson(
          table_name = Some("view"),
          catalog_name = Some("spark_catalog"),
          namespace = Some(List("default")),
          schema_name = Some("default"),
          columns = Some(List(
            TableColumn("id", Type("integer")),
            TableColumn("name", Type("string")),
            TableColumn("created_at", Type("timestamp_ltz"))
          )),
          serde_library = Some("org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"),
          inputformat = Some("org.apache.hadoop.mapred.SequenceFileInputFormat"),
          outputformat = Some("org.apache.hadoop.hive.ql.io.HiveSequenceFileOutputFormat"),
          storage_properties = Some(Map("serialization.format" -> "1")),
          last_access = Some("UNKNOWN"),
          created_by = Some(s"Spark $SPARK_VERSION"),
          `type` = Some("VIEW"),
          view_text = Some("SELECT * FROM spark_catalog.ns.table"),
          view_original_text = Some("SELECT * FROM spark_catalog.ns.table"),
          view_schema_mode = Some("COMPENSATION"),
          view_catalog_and_namespace = Some("spark_catalog.default"),
          view_query_output_columns = Some(List("id", "name", "created_at"))
        )

        if (getProvider() == "hive") {
          assert(expectedOutput ==
            parsedOutput.copy(table_properties = None, created_time = None, owner = None))
        } else {
          assert(expectedOutput.copy(inputformat = None,
            outputformat = None, serde_library = None, storage_properties = None)
            == parsedOutput.copy(table_properties = None, created_time = None, owner = None))
        }
      }
    }
  }

  test("DESCRIBE AS JSON for column throws Analysis Exception") {
    withNamespaceAndTable("ns", "table") { t =>
      val tableCreationStr =
        s"""
           |CREATE TABLE ns.table(
           |        cust_id INT,
           |        state VARCHAR(20),
           |        name STRING COMMENT "Short name"
           |    )
           |    USING parquet
           |    PARTITIONED BY (state)
           |""".stripMargin
      spark.sql(tableCreationStr)
      spark.sql("INSERT INTO ns.table PARTITION (state = \"CA\") VALUES (100, \"Jane\")")
      val error = intercept[AnalysisException] {
        spark.sql("DESCRIBE FORMATTED ns.table ns.table.name AS JSON")
      }

      checkError(
        exception = error,
        condition = "UNSUPPORTED_FEATURE.DESC_TABLE_COLUMN_JSON")
    }
  }

  test("DESCRIBE AS JSON complex types") {
    withNamespaceAndTable("ns", "table") { t =>
      val tableCreationStr =
        s"""
           |CREATE TABLE $t (
           |  id STRING,
           |  logs VARIANT,
           |  nested_struct STRUCT<
           |    name: STRING,
           |    age: INT,
           |    contact: STRUCT<
           |      email: STRING,
           |      phone_numbers: ARRAY<STRING>,
           |      addresses: ARRAY<STRUCT<
           |        street: STRING,
           |        city: STRING,
           |        zip: INT
           |      >>
           |    >
           |  >,
           |  preferences MAP<STRING, ARRAY<STRING>>
           |) USING parquet
           |  OPTIONS (option1 'value1', option2 'value2')
           |  PARTITIONED BY (id)
           |  COMMENT 'A table with nested complex types'
           |  TBLPROPERTIES ('property1' = 'value1', 'password' = 'password')
        """.stripMargin
      spark.sql(tableCreationStr)
      val descriptionDf = spark.sql(s"DESCRIBE EXTENDED $t AS JSON")
      val firstRow = descriptionDf.select("json_metadata").head()
      val jsonValue = firstRow.getString(0)
      val parsedOutput = parse(jsonValue).extract[DescribeTableJson]

      val expectedOutput = DescribeTableJson(
        table_name = Some("table"),
        catalog_name = Some("spark_catalog"),
        namespace = Some(List("ns")),
        schema_name = Some("ns"),
        columns = Some(List(
          TableColumn(
            name = "logs",
            `type` = Type("variant"),
            default = None
          ),
          TableColumn(
            name = "nested_struct",
            `type` = Type(
              name = "struct",
              fields = Some(List(
                Field(
                  name = "name",
                  `type` = Type("string")
                ),
                Field(
                  name = "age",
                  `type` = Type("integer")
                ),
                Field(
                  name = "contact",
                  `type` = Type(
                    name = "struct",
                    fields = Some(List(
                      Field(
                        name = "email",
                        `type` = Type("string")
                      ),
                      Field(
                        name = "phone_numbers",
                        `type` = Type(
                          name = "array",
                          element_type = Some(Type("string")),
                          element_nullable = Some(true)
                        )
                      ),
                      Field(
                        name = "addresses",
                        `type` = Type(
                          name = "array",
                          element_type = Some(Type(
                            name = "struct",
                            fields = Some(List(
                              Field(
                                name = "street",
                                `type` = Type("string")
                              ),
                              Field(
                                name = "city",
                                `type` = Type("string")
                              ),
                              Field(
                                name = "zip",
                                `type` = Type("integer")
                              )
                            ))
                          )),
                          element_nullable = Some(true)
                        )
                      )
                    ))
                  )
                )
              ))
            ),
            default = None
          ),
          TableColumn(
            name = "preferences",
            `type` = Type(
              name = "map",
              key_type = Some(Type("string")),
              value_type = Some(Type(
                name = "array",
                element_type = Some(Type("string")),
                element_nullable = Some(true)
              )),
              value_nullable = Some(true)
            ),
            default = None
          ),
          TableColumn(
            name = "id",
            `type` = Type("string"),
            default = None
          )
        )),
        serde_library = Some("org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"),
        inputformat = Some("org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"),
        outputformat = Some("org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"),
        storage_properties = Some(Map(
          "option1" -> "value1",
          "option2" -> "value2"
        )),
        last_access = Some("UNKNOWN"),
        created_by = Some(s"Spark $SPARK_VERSION"),
        `type` = Some("MANAGED"),
        provider = Some("parquet"),
        comment = Some("A table with nested complex types"),
        table_properties = Some(Map(
          "password" -> "*********(redacted)",
          "property1" -> "value1"
        )),
        partition_provider = Some("Catalog"),
        partition_columns = Some(List("id"))
      )

      if (getProvider() == "hive") {
        assert(expectedOutput ==
          parsedOutput.copy(location = None, created_time = None, owner = None))
      } else {
        assert(expectedOutput.copy(inputformat = None, outputformat = None, serde_library = None)
          == parsedOutput.copy(location = None, created_time = None, owner = None))
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
    owner: Option[String] = None,
    created_time: Option[String] = None,
    last_access: Option[String] = None,
    created_by: Option[String] = None,
    `type`: Option[String] = None,
    provider: Option[String] = None,
    bucket_columns: Option[List[String]] = Some(Nil),
    sort_columns: Option[List[String]] = Some(Nil),
    comment: Option[String] = None,
    table_properties: Option[Map[String, String]] = None,
    location: Option[String] = None,
    serde_library: Option[String] = None,
    inputformat: Option[String] = None,
    outputformat: Option[String] = None,
    storage_properties: Option[Map[String, String]] = None,
    partition_provider: Option[String] = None,
    partition_columns: Option[List[String]] = Some(Nil),
    partition_values: Option[Map[String, String]] = None,
    view_text: Option[String] = None,
    view_original_text: Option[String] = None,
    view_schema_mode: Option[String] = None,
    view_catalog_and_namespace: Option[String] = None,
    view_query_output_columns: Option[List[String]] = None
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
