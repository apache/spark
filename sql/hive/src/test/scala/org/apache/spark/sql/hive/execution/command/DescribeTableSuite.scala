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

package org.apache.spark.sql.hive.execution.command

import org.json4s._
import org.json4s.jackson.JsonMethods._

import org.apache.spark.sql.{AnalysisException, QueryTest, Row}
import org.apache.spark.sql.connector.catalog.CatalogManager.SESSION_CATALOG_NAME
import org.apache.spark.sql.connector.catalog.TableCatalog
import org.apache.spark.sql.execution.command.v1
import org.apache.spark.sql.types.StringType
import org.apache.spark.util.Utils

/**
 * The class contains tests for the `DESCRIBE TABLE` command to check V1 Hive external
 * table catalog.
 */
class DescribeTableSuite extends v1.DescribeTableSuiteBase with CommandSuiteBase {
  override def commandVersion: String = super[DescribeTableSuiteBase].commandVersion

  implicit val formats: org.json4s.DefaultFormats.type = org.json4s.DefaultFormats

  test("Table Ownership") {
    withNamespaceAndTable("ns", "tbl") { t =>
      sql(s"CREATE TABLE $t (c int) $defaultUsing")
      checkHiveClientCalls(expected = 6) {
        checkAnswer(
          sql(s"DESCRIBE TABLE EXTENDED $t")
            .where("col_name='Owner'")
            .select("col_name", "data_type"),
          Row("Owner", Utils.getCurrentUserName()))
      }
    }
  }


  test("DESCRIBE TABLE EXTENDED of a partitioned table") {
    withNamespaceAndTable("ns", "table") { tbl =>
      spark.sql(s"CREATE TABLE $tbl (id bigint, data string) $defaultUsing" +
        " PARTITIONED BY (id)" +
        " COMMENT 'this is a test table'" +
        " LOCATION 'file:/tmp/testcat/table_name'")
      val descriptionDf = spark.sql(s"DESCRIBE TABLE EXTENDED $tbl")
      assert(descriptionDf.schema.map(field => (field.name, field.dataType)) === Seq(
        ("col_name", StringType),
        ("data_type", StringType),
        ("comment", StringType)))
      QueryTest.checkAnswer(
        // Filter out 'Table Properties' to don't check `transient_lastDdlTime`
        descriptionDf.filter("!(col_name in ('Created Time', 'Table Properties', 'Created By'))"),
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
          Row(TableCatalog.PROP_OWNER.capitalize, Utils.getCurrentUserName(), ""),
          Row("Last Access", "UNKNOWN", ""),
          Row("Type", "EXTERNAL", ""),
          Row("Provider", getProvider(), ""),
          Row("Comment", "this is a test table", ""),
          Row("Location", "file:/tmp/testcat/table_name", ""),
          Row("Serde Library", "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe", ""),
          Row("InputFormat", "org.apache.hadoop.mapred.TextInputFormat", ""),
          Row("OutputFormat", "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat", ""),
          Row("Storage Properties", "[serialization.format=1]", ""),
          Row("Partition Provider", "Catalog", "")))
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
        condition = "DESCRIBE_JSON_NOT_EXTENDED")
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
          TableColumn("employee_id", Type("integer")),
          TableColumn("employee_name", Type("string")),
          TableColumn("department", Type("string")),
          TableColumn("hire_date", Type("date"))
        )),
        owner = Some(""),
        created_time = Some(""),
        last_access = Some("UNKNOWN"),
        created_by = Some("Spark 4.0.0-SNAPSHOT"),
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

      assert(expectedOutput == parsedOutput.copy(owner = Some(""),
        created_time = Some(""),
        location = Some("")))
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
          TableColumn("id", Type("integer")),
          TableColumn("name", Type("string")),
          TableColumn("region", Type("string")),
          TableColumn("category", Type("string"))
        )),
        last_access = Some("UNKNOWN"),
        created_by = Some("Spark 4.0.0-SNAPSHOT"),
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

      assert(expectedOutput ==
        parsedOutput.copy(location = None, created_time = None, owner = None,
          storage_properties = filteredParsedStorageProperties))
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
          TableColumn("id", Type("integer"), default_value = Some("1")),
          TableColumn("name", Type("string"), default_value = Some("'unknown'")),
          TableColumn("created_at", Type("timestamp_ltz"),
            default_value = Some("CURRENT_TIMESTAMP")),
          TableColumn("is_active", Type("boolean"), default_value = Some("true"))
        )),
        last_access = Some("UNKNOWN"),
        created_by = Some("Spark 4.0.0-SNAPSHOT"),
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
      assert(
        expectedOutput ==
          parsedOutput.copy(location = None, created_time = None, owner = None)
      )
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
            name = "nested_struct",
            `type` = Type(
              `type` = "struct",
              fields = Some(List(
                Field(
                  name = "name",
                  `type` = Type("string"),
                  nullable = Some(true)
                ),
                Field(
                  name = "age",
                  `type` = Type("integer"),
                  nullable = Some(true)
                ),
                Field(
                  name = "contact",
                  `type` = Type(
                    `type` = "struct",
                    fields = Some(List(
                      Field(
                        name = "email",
                        `type` = Type("string"),
                        nullable = Some(true)
                      ),
                      Field(
                        name = "phone_numbers",
                        `type` = Type(
                          `type` = "array",
                          elementType = Some(Type("string")),
                          containsNull = Some(true)
                        ),
                        nullable = Some(true)
                      ),
                      Field(
                        name = "addresses",
                        `type` = Type(
                          `type` = "array",
                          elementType = Some(Type(
                            `type` = "struct",
                            fields = Some(List(
                              Field(
                                name = "street",
                                `type` = Type("string"),
                                nullable = Some(true)
                              ),
                              Field(
                                name = "city",
                                `type` = Type("string"),
                                nullable = Some(true)
                              ),
                              Field(
                                name = "zip",
                                `type` = Type("integer"),
                                nullable = Some(true)
                              )
                            ))
                          )),
                          containsNull = Some(true)
                        ),
                        nullable = Some(true)
                      )
                    ))
                  ),
                  nullable = Some(true)
                )
              ))
            ),
            default_value = None
          ),
          TableColumn(
            name = "preferences",
            `type` = Type(
              `type` = "map",
              keyType = Some(Type("string")),
              valueType = Some(Type(
                `type` = "array",
                elementType = Some(Type("string")),
                containsNull = Some(true)
              )),
              valueContainsNull = Some(true)
            ),
            default_value = None
          ),
          TableColumn(
            name = "id",
            `type` = Type("string"),
            default_value = None
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
        created_by = Some("Spark 4.0.0-SNAPSHOT"),
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

      assert(expectedOutput ==
        parsedOutput.copy(location = None, created_time = None, owner = None))
    }
  }
}

/** Represents JSON output of DESCRIBE TABLE AS JSON  */
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
    partition_values: Option[Map[String, String]] = None
)

/** Used for columns field of DescribeTableJson */
case class TableColumn(
    name: String,
    `type`: Type,
    default_value: Option[String] = None
)

case class Type(
    `type`: String,
    fields: Option[List[Field]] = None,
    elementType: Option[Type] = None,
    keyType: Option[Type] = None,
    valueType: Option[Type] = None,
    nullable: Option[Boolean] = None,
    containsNull: Option[Boolean] = None,
    valueContainsNull: Option[Boolean] = None
)

case class Field(
    name: String,
    `type`: Type,
    nullable: Option[Boolean] = None
)
