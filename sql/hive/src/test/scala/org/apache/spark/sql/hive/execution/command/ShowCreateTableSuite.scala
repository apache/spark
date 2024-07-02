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

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, CatalogUtils}
import org.apache.spark.sql.catalyst.util.escapeSingleQuotedString
import org.apache.spark.sql.execution.command.v1
import org.apache.spark.sql.internal.HiveSerDe

/**
 * The class contains tests for the `SHOW CREATE TABLE` command to check V1 Hive external
 * table catalog.
 */
class ShowCreateTableSuite extends v1.ShowCreateTableSuiteBase with CommandSuiteBase {
  override def commandVersion: String = super[ShowCreateTableSuiteBase].commandVersion
  def nsTable: String = s"$ns.$table"

  override def getShowCreateDDL(table: String, serde: Boolean = false): Array[String] = {
    super.getShowCreateDDL(table, serde).filter(!_.startsWith("'transient_lastDdlTime'"))
  }

  test("simple hive table") {
    withNamespaceAndTable(ns, table) { t =>
      sql(
        s"""CREATE TABLE $t (
           |  c1 INT COMMENT 'bla',
           |  c2 STRING
           |)
           |USING HIVE
           |TBLPROPERTIES (
           |  'prop1' = 'value1',
           |  'prop2' = 'value2'
           |)
         """.stripMargin
      )
      val expected = s"CREATE TABLE $nsTable ( c1 INT COMMENT 'bla', c2 STRING)" +
        " ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'" +
        " WITH SERDEPROPERTIES ( 'serialization.format' = '1')" +
        " STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat'" +
        " OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'" +
        " TBLPROPERTIES ( 'prop1' = 'value1', 'prop2' = 'value2',"
      assert(getShowCreateDDL(t, true).mkString(" ") == expected)
    }
  }

  test("simple external hive table") {
    withTempDir { dir =>
      withNamespaceAndTable(ns, table) { t =>
        sql(
          s"""CREATE TABLE $t (
             |  c1 INT COMMENT 'bla',
             |  c2 STRING
             |)
             |USING HIVE
             |LOCATION '${dir.toURI}'
             |TBLPROPERTIES (
             |  'prop1' = 'value1',
             |  'prop2' = 'value2'
             |)
           """.stripMargin
        )
        val expected = s"CREATE EXTERNAL TABLE $nsTable ( c1 INT COMMENT 'bla', c2 STRING)" +
          s" ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'" +
          s" WITH SERDEPROPERTIES ( 'serialization.format' = '1')" +
          s" STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat'" +
          s" OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'" +
          s" LOCATION" +
          s" '${escapeSingleQuotedString(CatalogUtils.URIToString(dir.toURI)).dropRight(1)}'" +
          s" TBLPROPERTIES ( 'prop1' = 'value1', 'prop2' = 'value2',"
        assert(getShowCreateDDL(t, true).mkString(" ") == expected)
      }
    }
  }

  test("partitioned hive table") {
    withNamespaceAndTable(ns, table) { t =>
      sql(
        s"""CREATE TABLE $t (
           |  c1 INT COMMENT 'bla',
           |  c2 STRING
           |)
           |USING HIVE
           |COMMENT 'bla'
           |PARTITIONED BY (
           |  p1 BIGINT COMMENT 'bla',
           |  p2 STRING
           |)
         """.stripMargin
      )
      val expected = s"CREATE TABLE $nsTable ( c1 INT COMMENT 'bla', c2 STRING)" +
        " COMMENT 'bla' PARTITIONED BY (p1 BIGINT COMMENT 'bla', p2 STRING)" +
        " ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'" +
        " WITH SERDEPROPERTIES ( 'serialization.format' = '1')" +
        " STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat'" +
        " OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'" +
        " TBLPROPERTIES ("
      assert(getShowCreateDDL(t, true).mkString(" ") == expected)
    }
  }

  test("hive table with explicit storage info") {
    withNamespaceAndTable(ns, table) { t =>
      sql(
        s"""CREATE TABLE $t (
           |  c1 INT COMMENT 'bla',
           |  c2 STRING
           |)
           |ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
           |COLLECTION ITEMS TERMINATED BY '@'
           |MAP KEYS TERMINATED BY '#'
           |NULL DEFINED AS 'NaN'
         """.stripMargin
      )
      val expected = s"CREATE TABLE $nsTable ( c1 INT COMMENT 'bla', c2 STRING)" +
        " ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'" +
        " WITH SERDEPROPERTIES (" +
        " 'colelction.delim' = '@'," +
        " 'field.delim' = ','," +
        " 'mapkey.delim' = '#'," +
        " 'serialization.format' = ',')" +
        " STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat'" +
        " OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'" +
        " TBLPROPERTIES ("
      assert(getShowCreateDDL(t, true).mkString(" ") == expected)
    }
  }

  test("hive table with STORED AS clause") {
    withNamespaceAndTable(ns, table) { t =>
      sql(
        s"""CREATE TABLE $t (
           |  c1 INT COMMENT 'bla',
           |  c2 STRING
           |)
           |STORED AS PARQUET
         """.stripMargin
      )
      val expected = s"CREATE TABLE $nsTable ( c1 INT COMMENT 'bla', c2 STRING)" +
        " ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'" +
        " WITH SERDEPROPERTIES ( 'serialization.format' = '1')" +
        " STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'" +
        " OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'" +
        " TBLPROPERTIES ("
      assert(getShowCreateDDL(t, true).mkString(" ") == expected)
    }
  }

  test("hive table with serde info") {
    withNamespaceAndTable(ns, table) { t =>
      sql(
        s"""CREATE TABLE $t (
           |  c1 INT COMMENT 'bla',
           |  c2 STRING
           |)
           |ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
           |WITH SERDEPROPERTIES (
           |  'mapkey.delim' = ',',
           |  'field.delim' = ','
           |)
           |STORED AS
           |  INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
           |  OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
         """.stripMargin
      )
      val expected = s"CREATE TABLE $nsTable ( c1 INT COMMENT 'bla', c2 STRING)" +
        " ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'" +
        " WITH SERDEPROPERTIES (" +
        " 'field.delim' = ','," +
        " 'mapkey.delim' = ','," +
        " 'serialization.format' = '1')" +
        " STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'" +
        " OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'" +
        " TBLPROPERTIES ("
      assert(getShowCreateDDL(t, true).mkString(" ") == expected)
    }
  }

  test("hive bucketing is supported") {
    withNamespaceAndTable(ns, table) { t =>
      sql(
        s"""CREATE TABLE $t (a INT, b STRING)
           |STORED AS TEXTFILE
           |CLUSTERED BY (a)
           |SORTED BY (b)
           |INTO 2 BUCKETS
         """.stripMargin
      )
      val expected = s"CREATE TABLE $nsTable ( a INT, b STRING)" +
        " CLUSTERED BY (a) SORTED BY (b ASC) INTO 2 BUCKETS" +
        " ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'" +
        " WITH SERDEPROPERTIES ( 'serialization.format' = '1')" +
        " STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat'" +
        " OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'" +
        " TBLPROPERTIES ("
      assert(getShowCreateDDL(t, true).mkString(" ") == expected)
    }
  }

  private def checkCreateSparkTableAsHive(tableName: String): Unit = {
    val table = TableIdentifier(tableName, Some("default"))
    val db = table.database.get
    val hiveTable = spark.sharedState.externalCatalog.getTable(db, table.table)
    val sparkDDL = sql(s"SHOW CREATE TABLE ${table.quotedString}").head().getString(0)
    // Drops original Hive table.
    sql(s"DROP TABLE ${table.quotedString}")

    try {
      // Creates Spark datasource table using generated Spark DDL.
      sql(sparkDDL)
      val sparkTable = spark.sharedState.externalCatalog.getTable(db, table.table)
      checkHiveCatalogTables(hiveTable, sparkTable)
    } finally {
      sql(s"DROP TABLE IF EXISTS ${table.table}")
    }
  }

  private def checkHiveCatalogTables(hiveTable: CatalogTable, sparkTable: CatalogTable): Unit = {
    def normalize(table: CatalogTable): CatalogTable = {
      val nondeterministicProps = Set(
        "CreateTime",
        "transient_lastDdlTime",
        "grantTime",
        "lastUpdateTime",
        "last_modified_by",
        "last_modified_time",
        "Owner:",
        // The following are hive specific schema parameters which we do not need to match exactly.
        "totalNumberFiles",
        "maxFileSize",
        "minFileSize"
      )

      table.copy(
        createTime = 0L,
        lastAccessTime = 0L,
        properties = table.properties.filter { case (k, _) => !nondeterministicProps.contains(k) },
        stats = None,
        ignoredProperties = Map.empty,
        storage = table.storage.copy(properties = Map.empty),
        provider = None,
        tracksPartitionsInCatalog = false
      )
    }

    def fillSerdeFromProvider(table: CatalogTable): CatalogTable = {
      table.provider.flatMap(HiveSerDe.sourceToSerDe(_)).map { hiveSerde =>
        val newStorage = table.storage.copy(
          inputFormat = hiveSerde.inputFormat,
          outputFormat = hiveSerde.outputFormat,
          serde = hiveSerde.serde
        )
        table.copy(storage = newStorage)
      }.getOrElse(table)
    }

    assert(normalize(fillSerdeFromProvider(sparkTable)) == normalize(hiveTable))
  }

  test("simple hive table in Spark DDL") {
    withTable("t1") {
      sql(
        s"""
           |CREATE TABLE t1 (
           |  c1 STRING COMMENT 'bla',
           |  c2 STRING
           |)
           |TBLPROPERTIES (
           |  'prop1' = 'value1',
           |  'prop2' = 'value2'
           |)
           |STORED AS orc
         """.stripMargin
      )

      checkCreateSparkTableAsHive("t1")
    }
  }

  test("simple external hive table in Spark DDL") {
    withTempDir { dir =>
      withTable("t1") {
        sql(
          s"""
             |CREATE TABLE t1 (
             |  c1 STRING COMMENT 'bla',
             |  c2 STRING
             |)
             |LOCATION '${dir.toURI}'
             |TBLPROPERTIES (
             |  'prop1' = 'value1',
             |  'prop2' = 'value2'
             |)
             |STORED AS orc
           """.stripMargin
        )

        checkCreateSparkTableAsHive("t1")
      }
    }
  }

  test("hive table with STORED AS clause in Spark DDL") {
    withTable("t1") {
      sql(
        s"""
           |CREATE TABLE t1 (
           |  c1 INT COMMENT 'bla',
           |  c2 STRING
           |)
           |STORED AS PARQUET
         """.stripMargin
      )

      checkCreateSparkTableAsHive("t1")
    }
  }

  test("hive table with nested fields with STORED AS clause in Spark DDL") {
    withTable("t1") {
      sql(
        s"""
           |CREATE TABLE t1 (
           |  c1 INT COMMENT 'bla',
           |  c2 STRING,
           |  c3 STRUCT <s1: INT, s2: STRING>
           |)
           |STORED AS PARQUET
         """.stripMargin
      )

      checkCreateSparkTableAsHive("t1")
    }
  }

  test("hive table with unsupported fileformat in Spark DDL") {
    withTable("t1") {
      sql(
        s"""
           |CREATE TABLE t1 (
           |  c1 INT COMMENT 'bla',
           |  c2 STRING
           |)
           |STORED AS RCFILE
         """.stripMargin
      )

      checkError(
        exception = intercept[AnalysisException] {
          checkCreateSparkTableAsHive("t1")
        },
        errorClass = "UNSUPPORTED_SHOW_CREATE_TABLE.WITH_UNSUPPORTED_SERDE_CONFIGURATION",
        sqlState = "0A000",
        parameters = Map(
          "table" -> "t1",
          "configs" -> (" SERDE: org.apache.hadoop.hive.serde2.columnar.LazyBinaryColumnarSerDe " +
            "INPUTFORMAT: org.apache.hadoop.hive.ql.io.RCFileInputFormat " +
            "OUTPUTFORMAT: org.apache.hadoop.hive.ql.io.RCFileOutputFormat"))
      )
    }
  }

  test("hive table with serde info in Spark DDL") {
    withTable("t1") {
      sql(
        s"""
           |CREATE TABLE t1 (
           |  c1 INT COMMENT 'bla',
           |  c2 STRING
           |)
           |ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
           |STORED AS
           |  INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
           |  OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
         """.stripMargin
      )

      checkCreateSparkTableAsHive("t1")
    }
  }

  test("partitioned, bucketed hive table in Spark DDL") {
    withTable("t1") {
      sql(
        s"""
           |CREATE TABLE t1 (
           |  emp_id INT COMMENT 'employee id', emp_name STRING,
           |  emp_dob STRING COMMENT 'employee date of birth', emp_sex STRING COMMENT 'M/F'
           |)
           |COMMENT 'employee table'
           |PARTITIONED BY (
           |  emp_country STRING COMMENT '2-char code', emp_state STRING COMMENT '2-char code'
           |)
           |CLUSTERED BY (emp_sex) SORTED BY (emp_id ASC) INTO 10 BUCKETS
           |STORED AS ORC
         """.stripMargin
      )

      checkCreateSparkTableAsHive("t1")
    }
  }

  test("show create table for transactional hive table") {
    withTable("t1") {
      sql(
        s"""
           |CREATE TABLE t1 (
           |  c1 STRING COMMENT 'bla',
           |  c2 STRING
           |)
           |TBLPROPERTIES (
           |  'transactional' = 'true',
           |  'prop1' = 'value1',
           |  'prop2' = 'value2'
           |)
           |CLUSTERED BY (c1) INTO 10 BUCKETS
           |STORED AS ORC
         """.stripMargin
      )

      checkError(
        exception = intercept[AnalysisException] {
          sql("SHOW CREATE TABLE t1")
        },
        errorClass = "UNSUPPORTED_SHOW_CREATE_TABLE.ON_TRANSACTIONAL_HIVE_TABLE",
        sqlState = "0A000",
        parameters = Map("table" -> "`spark_catalog`.`default`.`t1`")
      )
    }
  }
}
