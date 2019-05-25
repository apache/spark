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

import org.apache.spark.sql.{QueryTest, Row, SaveMode}
import org.apache.spark.sql.catalyst.{AliasIdentifier, TableIdentifier}
import org.apache.spark.sql.catalyst.catalog.CatalogTableType
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.catalyst.plans.logical.SubqueryAlias
import org.apache.spark.sql.hive.test.TestHiveSingleton
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.{ExamplePointUDT, SQLTestUtils}
import org.apache.spark.sql.types._

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
        case x @ SubqueryAlias(AliasIdentifier("vw1", Some("default")), _) => x
      }
      assert(aliases.size == 1)
    }
  }

  test("Validate catalog metadata for supported data types")  {
    withTable("t") {
      sql(
        """
          |CREATE TABLE t (
          |c1 boolean,
          |c2 tinyint,
          |c3 smallint,
          |c4 short,
          |c5 bigint,
          |c6 long,
          |c7 float,
          |c8 double,
          |c9 date,
          |c10 timestamp,
          |c11 string,
          |c12 char(10),
          |c13 varchar(10),
          |c14 binary,
          |c15 decimal,
          |c16 decimal(10),
          |c17 decimal(10,2),
          |c18 array<string>,
          |c19 array<int>,
          |c20 array<char(10)>,
          |c21 map<int,int>,
          |c22 map<int,char(10)>,
          |c23 struct<a:int,b:int>,
          |c24 struct<c:varchar(10),d:int>
          |)
        """.stripMargin)

      val schema = hiveClient.getTable("default", "t").schema
      val expectedSchema = new StructType()
        .add("c1", "boolean")
        .add("c2", "tinyint")
        .add("c3", "smallint")
        .add("c4", "short")
        .add("c5", "bigint")
        .add("c6", "long")
        .add("c7", "float")
        .add("c8", "double")
        .add("c9", "date")
        .add("c10", "timestamp")
        .add("c11", "string")
        .add("c12", "string", true,
          new MetadataBuilder().putString(HIVE_TYPE_STRING, "char(10)").build())
        .add("c13", "string", true,
          new MetadataBuilder().putString(HIVE_TYPE_STRING, "varchar(10)").build())
        .add("c14", "binary")
        .add("c15", "decimal")
        .add("c16", "decimal(10)")
        .add("c17", "decimal(10,2)")
        .add("c18", "array<string>")
        .add("c19", "array<int>")
        .add("c20", "array<string>", true,
          new MetadataBuilder().putString(HIVE_TYPE_STRING, "array<char(10)>").build())
        .add("c21", "map<int,int>")
        .add("c22", "map<int,string>", true,
          new MetadataBuilder().putString(HIVE_TYPE_STRING, "map<int,char(10)>").build())
        .add("c23", "struct<a:int,b:int>")
        .add("c24", "struct<c:string,d:int>", true,
          new MetadataBuilder().putString(HIVE_TYPE_STRING, "struct<c:varchar(10),d:int>").build())
      assert(schema == expectedSchema)
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

  override def beforeAll(): Unit = {
    super.beforeAll()
    sparkSession.sessionState.catalog.reset()
    sparkSession.metadataHive.reset()
  }

  Seq(
    "parquet" -> ((
      "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
      "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
      "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    )),

    "org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat" -> ((
      "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
      "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
      "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    )),

    "orc" -> ((
      "org.apache.hadoop.hive.ql.io.orc.OrcInputFormat",
      "org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat",
      "org.apache.hadoop.hive.ql.io.orc.OrcSerde"
    )),

    "org.apache.spark.sql.hive.orc" -> ((
      "org.apache.hadoop.hive.ql.io.orc.OrcInputFormat",
      "org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat",
      "org.apache.hadoop.hive.ql.io.orc.OrcSerde"
    )),

    "org.apache.spark.sql.execution.datasources.orc.OrcFileFormat" -> ((
      "org.apache.hadoop.hive.ql.io.orc.OrcInputFormat",
      "org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat",
      "org.apache.hadoop.hive.ql.io.orc.OrcSerde"
    ))
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
        if (HiveUtils.isHive23) {
          assert(sparkSession.metadataHive.runSqlHive("SELECT * FROM t") ===
            Seq("1.100\t1", "2.100\t2"))
        } else {
          assert(sparkSession.metadataHive.runSqlHive("SELECT * FROM t") ===
            Seq("1.1\t1", "2.1\t2"))
        }
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
          assert(hiveTable.storage.locationUri === Some(makeQualifiedPath(dir.getAbsolutePath)))

          val columns = hiveTable.schema
          assert(columns.map(_.name) === Seq("d1", "d2"))
          assert(columns.map(_.dataType) === Seq(DecimalType(10, 3), StringType))

          checkAnswer(table("t"), testDF)
          if (HiveUtils.isHive23) {
            assert(sparkSession.metadataHive.runSqlHive("SELECT * FROM t") ===
              Seq("1.100\t1", "2.100\t2"))
          } else {
            assert(sparkSession.metadataHive.runSqlHive("SELECT * FROM t") ===
              Seq("1.1\t1", "2.1\t2"))
          }
        }
      }
    }

    test(s"Persist non-partitioned $provider relation into metastore as managed table using CTAS") {
      withTempPath { dir =>
        withTable("t") {
          sql(
            s"""CREATE TABLE t USING $provider
               |OPTIONS (path '${dir.toURI}')
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
          assert(sparkSession.metadataHive.runSqlHive("SELECT * FROM t") === Seq("1\tval_1"))
        }
      }
    }

  }
}
