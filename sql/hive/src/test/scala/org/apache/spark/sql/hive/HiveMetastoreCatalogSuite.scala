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

import java.io.File

import org.apache.spark.sql.{QueryTest, Row, SaveMode}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.CatalogTableType
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.catalyst.plans.logical.SubqueryAlias
import org.apache.spark.sql.hive.test.TestHiveSingleton
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.{ExamplePointUDT, SQLTestUtils}
import org.apache.spark.sql.types.{DecimalType, IntegerType, StringType, StructField, StructType}

class HiveMetastoreCatalogSuite extends TestHiveSingleton with SQLTestUtils {
  import spark.implicits._

  test("struct field should accept underscore in sub-column name") {
    val hiveTypeStr = "struct<a: int, b_1: string, c: string>"
    val dataType = CatalystSqlParser.parseDataType(hiveTypeStr)
    assert(dataType.isInstanceOf[StructType])
  }

  test("udt to metastore type conversion") {
    val udt = new ExamplePointUDT
    assertResult(udt.sqlType.catalogString) {
      udt.catalogString
    }
  }

  test("duplicated metastore relations") {
    val df = spark.sql("SELECT * FROM src")
    logInfo(df.queryExecution.toString)
    df.as('a).join(df.as('b), $"a.key" === $"b.key")
  }

  test("should not truncate struct type catalog string") {
    def field(n: Int): StructField = {
      StructField("col" + n, StringType)
    }
    val dataType = StructType((1 to 100).map(field))
    assert(CatalystSqlParser.parseDataType(dataType.catalogString) == dataType)
  }

  test("view relation") {
    withView("vw1") {
      spark.sql("create view vw1 as select 1 as id")
      val plan = spark.sql("select id from vw1").queryExecution.analyzed
      val aliases = plan.collect {
        case x @ SubqueryAlias("vw1", _, Some(TableIdentifier("vw1", Some("default")))) => x
      }
      assert(aliases.size == 1)
    }
  }
}

class DataSourceWithHiveMetastoreCatalogSuite
  extends QueryTest with SQLTestUtils with TestHiveSingleton {
  import hiveContext._
  import testImplicits._

  private val testDF = range(1, 3).select(
    ('id + 0.1) cast DecimalType(10, 3) as 'd1,
    'id cast StringType as 'd2
  ).coalesce(1)

  Seq(
    "parquet" -> (
      "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
      "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
      "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    ),

    "orc" -> (
      "org.apache.hadoop.hive.ql.io.orc.OrcInputFormat",
      "org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat",
      "org.apache.hadoop.hive.ql.io.orc.OrcSerde"
    )
  ).foreach { case (provider, (inputFormat, outputFormat, serde)) =>
    test(s"Persist non-partitioned $provider relation into metastore as managed table") {
      withTable("t") {
        withSQLConf(SQLConf.PARQUET_WRITE_LEGACY_FORMAT.key -> "true") {
          testDF
            .write
            .mode(SaveMode.Overwrite)
            .format(provider)
            .saveAsTable("t")
        }

        val hiveTable = sessionState.catalog.getTableMetadata(TableIdentifier("t", Some("default")))
        assert(hiveTable.storage.inputFormat === Some(inputFormat))
        assert(hiveTable.storage.outputFormat === Some(outputFormat))
        assert(hiveTable.storage.serde === Some(serde))

        assert(hiveTable.partitionColumnNames.isEmpty)
        assert(hiveTable.tableType === CatalogTableType.MANAGED)

        val columns = hiveTable.schema
        assert(columns.map(_.name) === Seq("d1", "d2"))
        assert(columns.map(_.dataType) === Seq(DecimalType(10, 3), StringType))

        checkAnswer(table("t"), testDF)
        assert(sessionState.metadataHive.runSqlHive("SELECT * FROM t") === Seq("1.1\t1", "2.1\t2"))
      }
    }

    test(s"Persist non-partitioned $provider relation into metastore as external table") {
      withTempPath { dir =>
        withTable("t") {
          val path = dir.getCanonicalFile

          withSQLConf(SQLConf.PARQUET_WRITE_LEGACY_FORMAT.key -> "true") {
            testDF
              .write
              .mode(SaveMode.Overwrite)
              .format(provider)
              .option("path", path.toString)
              .saveAsTable("t")
          }

          val hiveTable =
            sessionState.catalog.getTableMetadata(TableIdentifier("t", Some("default")))
          assert(hiveTable.storage.inputFormat === Some(inputFormat))
          assert(hiveTable.storage.outputFormat === Some(outputFormat))
          assert(hiveTable.storage.serde === Some(serde))

          assert(hiveTable.tableType === CatalogTableType.EXTERNAL)
          assert(hiveTable.storage.locationUri ===
            Some(path.toURI.toString.stripSuffix(File.separator)))

          val columns = hiveTable.schema
          assert(columns.map(_.name) === Seq("d1", "d2"))
          assert(columns.map(_.dataType) === Seq(DecimalType(10, 3), StringType))

          checkAnswer(table("t"), testDF)
          assert(sessionState.metadataHive.runSqlHive("SELECT * FROM t") ===
            Seq("1.1\t1", "2.1\t2"))
        }
      }
    }

    test(s"Persist non-partitioned $provider relation into metastore as managed table using CTAS") {
      withTempPath { dir =>
        withTable("t") {
          val path = dir.getCanonicalPath

          sql(
            s"""CREATE TABLE t USING $provider
               |OPTIONS (path '$path')
               |AS SELECT 1 AS d1, "val_1" AS d2
             """.stripMargin)

          val hiveTable =
            sessionState.catalog.getTableMetadata(TableIdentifier("t", Some("default")))
          assert(hiveTable.storage.inputFormat === Some(inputFormat))
          assert(hiveTable.storage.outputFormat === Some(outputFormat))
          assert(hiveTable.storage.serde === Some(serde))

          assert(hiveTable.partitionColumnNames.isEmpty)
          assert(hiveTable.tableType === CatalogTableType.EXTERNAL)

          val columns = hiveTable.schema
          assert(columns.map(_.name) === Seq("d1", "d2"))
          assert(columns.map(_.dataType) === Seq(IntegerType, StringType))

          checkAnswer(table("t"), Row(1, "val_1"))
          assert(sessionState.metadataHive.runSqlHive("SELECT * FROM t") === Seq("1\tval_1"))
        }
      }
    }
  }
}
