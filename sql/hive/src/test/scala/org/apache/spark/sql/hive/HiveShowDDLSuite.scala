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

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, CatalogTableType}
import org.apache.spark.sql.hive.execution.HiveNativeCommand
import org.apache.spark.sql.hive.test.TestHiveSingleton
import org.apache.spark.sql.test.SQLTestUtils
import org.apache.spark.sql.types._
import org.apache.spark.util.Utils

class HiveShowDDLSuite extends QueryTest with SQLTestUtils with TestHiveSingleton {
  import hiveContext.implicits._

  var jsonFilePath: String = _
  override def beforeAll(): Unit = {
    super.beforeAll()
    jsonFilePath = Utils.getSparkClassLoader.getResource("sample.json").getFile
  }
  // there are 3 types of create table to test
  // 1. HIVE Syntax: create table t1 (c1 int) partitionedby (c2 int) row format... tblproperties..
  // 2. Spark sql syntx: crate table t1 (c1 int) using .. options (... )
  // 3. saving table from datasource: df.write.format("parquet").saveAsTable("t1")

  /**
   * Hive syntax DDL
   */
  test("Hive syntax DDL: no row format") {
    withTempDir { tmpDir =>
      withTable("t1") {
        sql(
          s"""
            |create table t1(c1 int, c2 string)
            |stored as parquet
            |location '${tmpDir}'
          """.stripMargin)
        assert(compareCatalog(
          TableIdentifier("t1"),
          sql("show create table t1").collect()(0).toSeq(0).toString))
      }
    }
  }

  test("Hive syntax DDL - partitioned table with column and table comments") {
    withTempDir { tmpDir =>
      withTable("t1") {
        sql(
          s"""
             |create table t1(c1 bigint, c2 string)
             |PARTITIONED BY (c3 int COMMENT 'partition column', c4 string)
             |row format delimited fields terminated by ','
             |stored as parquet
             |location '${tmpDir}'
             |TBLPROPERTIES ('my.property.one'='true', 'my.property.two'='1',
             |'my.property.three'='2', 'my.property.four'='false')
          """.stripMargin)

        assert(compareCatalog(
          TableIdentifier("t1"),
          sql("show create table t1").collect()(0).toSeq(0).toString))
      }
    }
  }

  test("Hive syntax DDL - external, row format and tblproperties") {
    withTempDir { tmpDir =>
      withTable("t1") {
        sql(
          s"""
            |create external table t1(c1 bigint, c2 string)
            |row format delimited fields terminated by ','
            |stored as parquet
            |location '${tmpDir}'
            |TBLPROPERTIES ('my.property.one'='true', 'my.property.two'='1',
            |'my.property.three'='2', 'my.property.four'='false')
          """.stripMargin)

        assert(compareCatalog(
          TableIdentifier("t1"),
          sql("show create table t1").collect()(0).toSeq(0).toString))
      }
    }
  }

  test("Hive syntax DDL -  more row format definition") {
    withTempDir { tmpDir =>
      withTable("t1") {
        sql(
          s"""
            |create external table t1(c1 int COMMENT 'first column', c2 string)
            |COMMENT 'some table'
            |PARTITIONED BY (c3 int COMMENT 'partition column', c4 string)
            |row format delimited fields terminated by ','
            |COLLECTION ITEMS TERMINATED BY ','
            |MAP KEYS TERMINATED BY ','
            |NULL DEFINED AS 'NaN'
            |stored as parquet
            |location '${tmpDir}'
            |TBLPROPERTIES ('my.property.one'='true', 'my.property.two'='1',
            |'my.property.three'='2', 'my.property.four'='false')
          """.stripMargin)

        assert(compareCatalog(
          TableIdentifier("t1"),
          sql("show create table t1").collect()(0).toSeq(0).toString))
      }
    }
  }

  test("Hive syntax DDL - hive view") {
    withTempDir { tmpDir =>
      withTable("t1") {
        withView("v1") {
          sql(
            s"""
              |create table t1(c1 int, c2 string)
              |row format delimited fields terminated by ','
              |stored as parquet
              |location '${tmpDir}'
            """.stripMargin)
          sql(
            """
              |create view v1 as select * from t1
            """.stripMargin)
          assert(compareCatalog(
            TableIdentifier("v1"),
            sql("show create table v1").collect()(0).toSeq(0).toString))
        }
      }
    }
  }

  test("Hive syntax DDL - SERDE") {
    withTempDir { tmpDir =>
      withTable("t1") {
        sql(
          s"""
             |create table t1(c1 int, c2 string)
             |ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
             |WITH SERDEPROPERTIES ('mapkey.delim'=',', 'field.delim'=',')
             |STORED AS
             |INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
             |OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
             |location '${tmpDir}'
           """.stripMargin)

        assert(compareCatalog(
          TableIdentifier("t1"),
          sql("show create table t1").collect()(0).toSeq(0).toString))
      }
    }
  }

  // hive native DDL that is not supported by Spark SQL DDL
  test("Hive syntax DDL -- CLUSTERED") {
    withTable("t1") {
      HiveNativeCommand(
        s"""
          |create table t1 (c1 int, c2 string)
          |clustered by (c1) sorted by (c2 desc) into 5 buckets
        """.stripMargin).run(sqlContext)
      val ddl = sql("show create table t1").collect()
      assert(ddl(0).toSeq(0).toString.contains("WARN"))
      assert(ddl(1).toSeq(0).toString
        .contains("CLUSTERED BY ( `c1` ) \nSORTED BY ( `c2` DESC ) \nINTO 5 BUCKETS"))
    }
  }

  test("Hive syntax DDL -- STORED BY") {
    withTable("tmp_showcrt1") {
      HiveNativeCommand(
        s"""
           |CREATE EXTERNAL TABLE tmp_showcrt1 (key string, value boolean)
           |ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe'
           |STORED BY 'org.apache.hadoop.hive.ql.metadata.DefaultStorageHandler'
           |WITH SERDEPROPERTIES ('field.delim'=',', 'serialization.format'=',')
        """.stripMargin).run(sqlContext)
      val ddl = sql("show create table tmp_showcrt1").collect()
      assert(ddl(0).toSeq(0).toString.contains("WARN"))
      assert(ddl(1).toSeq(0).toString
        .contains("STORED BY 'org.apache.hadoop.hive.ql.metadata.DefaultStorageHandler'"))
    }
  }

  test("Hive syntax DDL -- SKEWED BY") {
    withTable("stored_as_dirs_multiple") {
      HiveNativeCommand(
      s"""
         |CREATE TABLE  stored_as_dirs_multiple (col1 STRING, col2 int, col3 STRING)
         |SKEWED BY (col1, col2) ON (('s1',1), ('s3',3), ('s13',13), ('s78',78))
         |stored as DIRECTORIES
       """.stripMargin).run(sqlContext)
      val ddl = sql("show create table stored_as_dirs_multiple").collect()
      assert(ddl(0).toSeq(0).toString.contains("WARN"))
      assert(ddl(1).toSeq(0).toString.contains("SKEWED BY ( `col1`, `col2` )"))
    }
  }

  /**
   * Datasource table syntax DDL
   */
  test("Datasource Table DDL syntax - persistent JSON table with a user specified schema") {
    withTable("jsonTable") {
      sql(
        s"""
           |CREATE TABLE jsonTable (
           |a string,
           |b String,
           |`c_!@(3)` int,
           |`<d>` Struct<`d!`:array<int>, `=`:array<struct<Dd2: boolean>>>)
           |USING org.apache.spark.sql.json.DefaultSource
           |OPTIONS (
           |  path '$jsonFilePath')
         """.stripMargin)
      assert(compareCatalog(
        TableIdentifier("jsonTable"),
        sql("show create table jsonTable").collect()(0).toSeq(0).toString))
    }
  }

  test("Datasource Table DDL syntax - persistent JSON with a subset of user-specified fields") {
    withTable("jsonTable") {
      // This works because JSON objects are self-describing and JSONRelation can get needed
      // field values based on field names.
      sql(
        s"""
           |CREATE TABLE jsonTable (`<d>` Struct<`=`:array<struct<Dd2: boolean>>>, b String)
           |USING org.apache.spark.sql.json.DefaultSource
           |OPTIONS (path '$jsonFilePath')
         """.stripMargin)
      assert(compareCatalog(
        TableIdentifier("jsonTable"),
        sql("show create table jsonTable").collect()(0).toSeq(0).toString))
    }
  }

  test("Datasource Table DDL syntax - USING and OPTIONS - no user-specified schema") {
    withTable("jsonTable") {
      sql(
        s"""
           |CREATE TABLE jsonTable
           |USING org.apache.spark.sql.json.DefaultSource
           |OPTIONS (
           |  path '$jsonFilePath',
           |  key.key1 'value1',
           |  'key.key2' 'value2'
           |)
         """.stripMargin)
      assert(compareCatalog(
        TableIdentifier("jsonTable"),
        sql("show create table jsonTable").collect()(0).toSeq(0).toString))
    }
  }

  test("Datasource Table DDL syntax - USING and NO options") {
    withTable("parquetTable") {
      sql(
        s"""
           |CREATE TABLE parquetTable (c1 int, c2 string, c3 long)
           |USING parquet
         """.stripMargin)
      assert(compareCatalog(
        TableIdentifier("parquetTable"),
        sql("show create table parquetTable").collect()(0).toSeq(0).toString))
    }
  }

  /**
   * Datasource saved to table
   */
  test("Save datasource to table - dataframe with a select ") {
    withTable("t_datasource") {
      val df = sql("select 1, 'abc'")
      df.write.saveAsTable("t_datasource")
      assert(compareCatalog(
        TableIdentifier("t_datasource"),
        sql("show create table t_datasource").collect()(0).toSeq(0).toString))
    }
  }

  test("Save datasource to table - dataframe from json file") {
    val jsonFilePath = Utils.getSparkClassLoader.getResource("sample.json").getFile
    withTable("t_datasource") {
      val df = sqlContext.read.json(jsonFilePath)
      df.write.format("json").saveAsTable("t_datasource")
      assert(compareCatalog(
        TableIdentifier("t_datasource"),
        sql("show create table t_datasource").collect()(0).toSeq(0).toString))
    }
  }

  test("Save datasource to table -- dataframe with user-specified schema") {
    withTable("ttt3") {
      val df = (1 to 3).map(i => (i, s"val_$i", i * 2)).toDF("a", "b", "c")
      df.write
        .format("parquet")
        .mode(SaveMode.Overwrite)
        .saveAsTable("ttt3")
      assert(compareCatalog(
        TableIdentifier("ttt3"),
        sql("show create table ttt3").collect()(0).toSeq(0).toString))
    }
  }

  test("Save datasource to table -- partitioned, bucket and sort") {
    // does not have a place to keep the partitioning columns.
    val df = (1 to 3).map(i => (i, s"val_$i", i * 2)).toDF("a", "b", "c")
    withTable("ttt3", "ttt5") {
      df.write
        .partitionBy("a", "b")
        .bucketBy(5, "c")
        .sortBy("c")
        .format("parquet")
        .mode(SaveMode.Overwrite)
        .saveAsTable("ttt3")
      val generatedDDL =
        sql("show create table ttt3").collect()(0).toSeq(0).toString.replace("<DataFrame>", "df")
      assert(generatedDDL.contains("partitionBy(\"a\", \"b\")"))
      assert(generatedDDL.contains("bucketBy(5, \"c\")"))
      assert(generatedDDL.contains("sortBy(\"c\")"))
      assert(generatedDDL.contains("format(\"parquet\")"))
    }
  }

  /**
   * In order to verify whether the generated DDL from a table is correct, we can
   * compare the CatalogTable generated from the existing table to the CatalogTable
   * generated from the table created with the the generated DDL
   * @param expectedTable
   * @param actualDDL
   * @return true or false
   */
  private def compareCatalog(expectedTable: TableIdentifier, actualDDL: String): Boolean = {
    val actualTable = expectedTable.table + "_actual"
    var actual: CatalogTable = null
    val expected: CatalogTable =
      sqlContext.sessionState.catalog.getTableMetadata(expectedTable)
    withTempDir { tmpDir =>
      if (actualDDL.contains("CREATE VIEW")) {
        withView(actualTable) {
          sql(actualDDL.replace(expectedTable.table.toLowerCase(), actualTable))
          actual = sqlContext.sessionState.catalog.getTableMetadata(TableIdentifier(actualTable))
        }
      } else {
        withTable(actualTable) {
          var revisedActualDDL: String = null
          if (expected.tableType == CatalogTableType.EXTERNAL_TABLE) {
            revisedActualDDL = actualDDL.replace(expectedTable.table.toLowerCase(), actualTable)
          } else {
            revisedActualDDL = actualDDL
              .replace(expectedTable.table.toLowerCase(), actualTable)
              .replaceAll("path.*,", s"path '${tmpDir}',")
          }
          sql(revisedActualDDL)
          actual = sqlContext.sessionState.catalog.getTableMetadata(TableIdentifier(actualTable))
        }
      }
    }

    if (expected.properties.get("spark.sql.sources.provider").isDefined) {
      // datasource table: The generated DDL will be like:
      // CREATE EXTERNAL TABLE <tableName> (<column list>) USING <datasource provider>
      // OPTIONS (<option list>)
      // So we only need to get the column schema from tblproperties,
      // and able OPTIONS from storage.serdeProperties to compare

      // datasource provider should be the same
      val sameProvider = expected.properties.get("spark.sql.sources.provider") ==
        actual.properties.get("spark.sql.sources.provider")

      // the actual table name appended "_actual" during the test
      val sameTableName = actual.qualifiedName.startsWith(expected.qualifiedName)
      val sameColumnSchema = schemaStringFromParts(expected) == schemaStringFromParts(actual)
      val sameOptions = expected.storage.serdeProperties forall { p =>
            if (p._1 == "path") {
              // for path, since we replace the directory with random value. ignore the value
              actual.storage.serdeProperties.get("path").isDefined
            } else {
              Some(p._2) == actual.storage.serdeProperties.get(p._1)
            }
          }
      sameProvider && sameTableName && sameColumnSchema && sameOptions
    } else {
      // hive table:
      // Generate hive table syntax is like:
      // CREATE <EXTERNAL> TABLE <tableName> (<column list> <PartitionedBy> <Buecket>
      // <ROW FORMAT..> <STORED AS ..> <LOCATION> <TBLPROPERTIES>
      val sameTableType = expected.tableType == actual.tableType
      val sameTableName = actual.qualifiedName.startsWith(expected.qualifiedName)
      val sameColmnSchema = expected.schema.size == actual.schema.size && {
          expected.schema.zip(actual.schema) forall { p =>
            p._1.name == p._2.name &&
              p._1.dataType == p._2.dataType &&
              p._1.nullable == p._2.nullable &&
              p._1.comment == p._2.comment
          }
        }
      val samePartitionBy = expected.partitionColumns.size == actual.partitionColumns.size && {
          expected.partitionColumns.zip(actual.partitionColumns) forall { p =>
            p._1.name == p._2.name &&
              p._1.dataType == p._2.dataType &&
              p._1.nullable == p._2.nullable &&
              p._1.comment == p._2.comment
          }
        }

      // TODO current SparkSQl does not support bucketing yet
      // TODO current CatalogTable does not populate sortcolumns yet

      val sameStorageFormat = expected.storage.inputFormat == actual.storage.inputFormat &&
        actual.storage.outputFormat == actual.storage.outputFormat &&
            expected.storage.serde == actual.storage.serde &&
            expected.storage.locationUri == actual.storage.locationUri &&  {
              // storage serdeProperties
              expected.storage.serdeProperties forall { p =>
                Some(p._2) == actual.storage.serdeProperties.get(p._1)
              }
            }

      val sameTblProperties = {
        expected.properties forall { p =>
          if (p._1 != "transient_lastDdlTime" && p._1 != "SORTBUCKETCOLSPREFIX") {
            Some(p._2) == actual.properties.get(p._1)
          } else {
            // ignore transient_lastDdlTime and SORTBUCKETCOLSPREFIX because catalogTable does not
            // have a meaningful sortcolumns yet.
            true
          }
        }
      }
      sameTableType && sameTableName && sameColmnSchema &&
        samePartitionBy && sameStorageFormat && sameTblProperties
    }
  }

  private def schemaStringFromParts(ct: CatalogTable): Option[String] = {
    ct.properties.get("spark.sql.sources.schema.numParts").map { numParts =>
      val parts = (0 until numParts.toInt).map { index =>
        val part = ct.properties.get(s"spark.sql.sources.schema.part.$index").orNull
        if (part == null) {
          throw new AnalysisException(
            "Could not read schema from the metastore because it is corrupted " +
              s"(missing part $index of the schema, $numParts parts are expected).")
        }
        part
      }
      // Stick all parts back to a single schema string.
      parts.mkString
    }
  }
}
