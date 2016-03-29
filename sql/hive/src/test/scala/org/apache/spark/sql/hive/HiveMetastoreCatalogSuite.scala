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

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.hive.client.{ExternalTable, ManagedTable}
import org.apache.spark.sql.hive.test.TestHiveSingleton
import org.apache.spark.sql.test.{ExamplePointUDT, SQLTestUtils}
import org.apache.spark.sql.types.{DecimalType, StringType, StructType}
import org.apache.spark.sql.{SQLConf, QueryTest, Row, SaveMode}

class HiveMetastoreCatalogSuite extends SparkFunSuite with TestHiveSingleton {
  import hiveContext.implicits._

  test("struct field should accept underscore in sub-column name") {
    val hiveTypeStr = "struct<a: int, b_1: string, c: string>"
    val dateType = HiveMetastoreTypes.toDataType(hiveTypeStr)
    assert(dateType.isInstanceOf[StructType])
  }

  test("udt to metastore type conversion") {
    val udt = new ExamplePointUDT
    assertResult(HiveMetastoreTypes.toMetastoreType(udt.sqlType)) {
      HiveMetastoreTypes.toMetastoreType(udt)
    }
  }

  test("duplicated metastore relations") {
    val df = hiveContext.sql("SELECT * FROM src")
    logInfo(df.queryExecution.toString)
    df.as('a).join(df.as('b), $"a.key" === $"b.key")
  }

  test("SPARK-13454: drop a table with a name starting with underscore") {
    hiveContext.range(10).write.saveAsTable("_spark13454")
    hiveContext.range(20).registerTempTable("_spark13454")
    // This will drop both metastore table and temp table.
    hiveContext.sql("drop table `_spark13454`")
    assert(hiveContext.tableNames().filter(name => name == "_spark13454").length === 0)

    hiveContext.range(10).write.saveAsTable("_spark13454")
    hiveContext.range(20).registerTempTable("_spark13454")
    hiveContext.sql("drop table default.`_spark13454`")
    // This will drop the metastore table but keep the temptable.
    assert(hiveContext.tableNames().filter(name => name == "_spark13454").length === 1)
    // Make sure it is the temp table.
    assert(hiveContext.table("_spark13454").count() === 20)
    hiveContext.sql("drop table if exists `_spark13454`")
    assert(hiveContext.tableNames().filter(name => name == "_spark13454").length === 0)

    hiveContext.range(10).write.saveAsTable("spark13454")
    hiveContext.sql("drop table spark13454")
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

        val hiveTable = catalog.client.getTable("default", "t")
        assert(hiveTable.inputFormat === Some(inputFormat))
        assert(hiveTable.outputFormat === Some(outputFormat))
        assert(hiveTable.serde === Some(serde))

        assert(!hiveTable.isPartitioned)
        assert(hiveTable.tableType === ManagedTable)

        val columns = hiveTable.schema
        assert(columns.map(_.name) === Seq("d1", "d2"))
        assert(columns.map(_.hiveType) === Seq("decimal(10,3)", "string"))

        checkAnswer(table("t"), testDF)
        assert(runSqlHive("SELECT * FROM t") === Seq("1.1\t1", "2.1\t2"))
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

          val hiveTable = catalog.client.getTable("default", "t")
          assert(hiveTable.inputFormat === Some(inputFormat))
          assert(hiveTable.outputFormat === Some(outputFormat))
          assert(hiveTable.serde === Some(serde))

          assert(hiveTable.tableType === ExternalTable)
          assert(hiveTable.location.get === path.toURI.toString.stripSuffix(File.separator))

          val columns = hiveTable.schema
          assert(columns.map(_.name) === Seq("d1", "d2"))
          assert(columns.map(_.hiveType) === Seq("decimal(10,3)", "string"))

          checkAnswer(table("t"), testDF)
          assert(runSqlHive("SELECT * FROM t") === Seq("1.1\t1", "2.1\t2"))
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

          val hiveTable = catalog.client.getTable("default", "t")
          assert(hiveTable.inputFormat === Some(inputFormat))
          assert(hiveTable.outputFormat === Some(outputFormat))
          assert(hiveTable.serde === Some(serde))

          assert(hiveTable.isPartitioned === false)
          assert(hiveTable.tableType === ExternalTable)
          assert(hiveTable.partitionColumns.length === 0)

          val columns = hiveTable.schema
          assert(columns.map(_.name) === Seq("d1", "d2"))
          assert(columns.map(_.hiveType) === Seq("int", "string"))

          checkAnswer(table("t"), Row(1, "val_1"))
          assert(runSqlHive("SELECT * FROM t") === Seq("1\tval_1"))
        }
      }
    }
  }
}
