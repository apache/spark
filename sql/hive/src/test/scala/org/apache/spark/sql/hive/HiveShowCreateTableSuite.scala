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

package org.apache.spark.sql.hive

import org.apache.spark.sql.{AnalysisException, ShowCreateTableSuite}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, CatalogTable}
import org.apache.spark.sql.hive.test.TestHiveSingleton

class HiveShowCreateTableSuite extends ShowCreateTableSuite with TestHiveSingleton {

  test("simple hive table") {
    withTable("t1") {
      sql(
        s"""CREATE TABLE t1 (
           |  c1 INT COMMENT 'bla',
           |  c2 STRING
           |)
           |TBLPROPERTIES (
           |  'prop1' = 'value1',
           |  'prop2' = 'value2'
           |)
         """.stripMargin
      )

      checkCreateTable("t1")
    }
  }

  test("simple external hive table") {
    withTempDir { dir =>
      withTable("t1") {
        sql(
          s"""CREATE TABLE t1 (
             |  c1 INT COMMENT 'bla',
             |  c2 STRING
             |)
             |LOCATION '${dir.toURI}'
             |TBLPROPERTIES (
             |  'prop1' = 'value1',
             |  'prop2' = 'value2'
             |)
           """.stripMargin
        )

        checkCreateTable("t1")
      }
    }
  }

  test("partitioned hive table") {
    withTable("t1") {
      sql(
        s"""CREATE TABLE t1 (
           |  c1 INT COMMENT 'bla',
           |  c2 STRING
           |)
           |COMMENT 'bla'
           |PARTITIONED BY (
           |  p1 BIGINT COMMENT 'bla',
           |  p2 STRING
           |)
         """.stripMargin
      )

      checkCreateTable("t1")
    }
  }

  test("hive table with explicit storage info") {
    withTable("t1") {
      sql(
        s"""CREATE TABLE t1 (
           |  c1 INT COMMENT 'bla',
           |  c2 STRING
           |)
           |ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
           |COLLECTION ITEMS TERMINATED BY '@'
           |MAP KEYS TERMINATED BY '#'
           |NULL DEFINED AS 'NaN'
         """.stripMargin
      )

      checkCreateTable("t1")
    }
  }

  test("hive table with STORED AS clause") {
    withTable("t1") {
      sql(
        s"""CREATE TABLE t1 (
           |  c1 INT COMMENT 'bla',
           |  c2 STRING
           |)
           |STORED AS PARQUET
         """.stripMargin
      )

      checkCreateTable("t1")
    }
  }

  test("hive table with serde info") {
    withTable("t1") {
      sql(
        s"""CREATE TABLE t1 (
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

      checkCreateTable("t1")
    }
  }

  test("hive bucketing is supported") {
    withTable("t1") {
      sql(
        s"""CREATE TABLE t1 (a INT, b STRING)
           |CLUSTERED BY (a)
           |SORTED BY (b)
           |INTO 2 BUCKETS
         """.stripMargin
      )
      checkCreateTable("t1")
    }
  }

  test("hive partitioned view is not supported") {
    withTable("t1") {
      withView("v1") {
        sql(
          s"""
             |CREATE TABLE t1 (c1 INT, c2 STRING)
             |PARTITIONED BY (
             |  p1 BIGINT COMMENT 'bla',
             |  p2 STRING )
           """.stripMargin)

        createRawHiveTable(
          s"""
             |CREATE VIEW v1
             |PARTITIONED ON (p1, p2)
             |AS SELECT * from t1
           """.stripMargin
        )

        val cause = intercept[AnalysisException] {
          sql("SHOW CREATE TABLE v1")
        }

        assert(cause.getMessage.contains(" - partitioned view"))

        val causeForSpark = intercept[AnalysisException] {
          sql("SHOW CREATE TABLE v1 AS SPARK")
        }

        assert(causeForSpark.getMessage.contains(" - partitioned view"))
      }
    }
  }

  test("SPARK-24911: keep quotes for nested fields in hive") {
    withTable("t1") {
      val createTable = "CREATE TABLE `t1`(`a` STRUCT<`b`: STRING>) USING hive"
      sql(createTable)
      val shownDDL = getShowDDL("SHOW CREATE TABLE t1")
      assert(shownDDL == createTable.dropRight(" USING hive".length))

      checkCreateTable("t1")
    }
  }

  private def createRawHiveTable(ddl: String): Unit = {
    hiveContext.sharedState.externalCatalog.unwrapped.asInstanceOf[HiveExternalCatalog]
      .client.runSqlHive(ddl)
  }

  private def checkCreateSparkTable(tableName: String): Unit = {
    val table = TableIdentifier(tableName, Some("default"))
    val db = table.database.get
    val hiveTable = spark.sharedState.externalCatalog.getTable(db, table.table)
    val shownSparkDDL = sql(s"SHOW CREATE TABLE ${table.quotedString} AS SPARK").head().getString(0)
    // Drops original Hive table.
    sql(s"DROP TABLE ${table.quotedString}")

    try {
      sql(shownSparkDDL)
      val actual = spark.sharedState.externalCatalog.getTable(db, table.table)
      val shownDDL = sql(s"SHOW CREATE TABLE ${table.quotedString}").head().getString(0)

      // Drops created Spark table using `SHOW CREATE TABLE AS SPARK`.
      sql(s"DROP TABLE ${table.quotedString}")

      sql(shownDDL)
      val expected = spark.sharedState.externalCatalog.getTable(db, table.table)

      checkCatalogTables(expected, actual)
      checkHiveCatalogTables(hiveTable, actual)
    } finally {
      sql(s"DROP TABLE IF EXISTS ${table.table}")
    }
  }

  private def checkHiveCatalogTables(expected: CatalogTable, actual: CatalogTable): Unit = {
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
        properties = table.properties.filterKeys(!nondeterministicProps.contains(_)),
        stats = None,
        ignoredProperties = Map.empty,
        storage = CatalogStorageFormat.empty,
        provider = None,
        tracksPartitionsInCatalog = false
      )
    }
    assert(normalize(actual) == normalize(expected))
  }

  test("simple hive table as spark") {
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
         """.stripMargin
      )

      checkCreateSparkTable("t1")
    }
  }

  test("show create table as spark can't work on data source table") {
    withTable("t1") {
      sql(
        s"""
           |CREATE TABLE t1 (
           |  c1 STRING COMMENT 'bla',
           |  c2 STRING
           |)
           |USING orc
         """.stripMargin
      )

      val cause = intercept[AnalysisException] {
        checkCreateSparkTable("t1")
      }

      assert(cause.getMessage.contains("Use `SHOW CREATE TABLE` instead"))
    }
  }

  test("simple external hive table as spark") {
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
           """.stripMargin
        )

        checkCreateSparkTable("t1")
      }
    }
  }

  test("hive table with STORED AS clause as spark") {
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

      checkCreateSparkTable("t1")
    }
  }

  test("hive table with unsupported fileformat as spark") {
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

      val cause = intercept[AnalysisException] {
        checkCreateSparkTable("t1")
      }

      assert(cause.getMessage.contains("unsupported serde configuration"))
    }
  }

  test("hive table with serde info as spark") {
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

      checkCreateSparkTable("t1")
    }
  }

  test("hive view is not supported as spark") {
    withTable("t1") {
      withView("v1") {
        sql("CREATE TABLE t1 (c1 STRING, c2 STRING)")

        createRawHiveTable(
          s"""
             |CREATE VIEW v1
             |AS SELECT * from t1
           """.stripMargin
        )

        val cause = intercept[AnalysisException] {
          sql("SHOW CREATE TABLE v1 AS SPARK")
        }

        assert(cause.getMessage.contains("view isn't supported"))
      }
    }
  }

  test("partitioned, bucketed hive table as spark") {
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

      checkCreateSparkTable("t1")
    }
  }

  test("transactional hive table as spark") {
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
         """.stripMargin
      )


      val cause = intercept[AnalysisException] {
        sql("SHOW CREATE TABLE t1 AS SPARK")
      }

      assert(cause.getMessage.contains(
        "SHOW CRETE TABLE AS SPARK doesn't support transactional Hive table"))
    }
  }
}
