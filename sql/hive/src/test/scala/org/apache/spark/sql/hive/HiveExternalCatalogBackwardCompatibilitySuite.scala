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

import java.net.URI

import org.scalatest.BeforeAndAfterEach

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, CatalogTable, CatalogTableType}
import org.apache.spark.sql.hive.client.HiveClient
import org.apache.spark.sql.hive.test.TestHiveSingleton
import org.apache.spark.sql.test.SQLTestUtils
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.Utils


class HiveExternalCatalogBackwardCompatibilitySuite extends QueryTest
  with SQLTestUtils with TestHiveSingleton with BeforeAndAfterEach {

  // To test `HiveExternalCatalog`, we need to read/write the raw table meta from/to hive client.
  val hiveClient: HiveClient =
    spark.sharedState.externalCatalog.asInstanceOf[HiveExternalCatalog].client

  val tempDir = Utils.createTempDir().getCanonicalFile

  override def beforeEach(): Unit = {
    sql("CREATE DATABASE test_db")
    for ((tbl, _) <- rawTablesAndExpectations) {
      hiveClient.createTable(tbl, ignoreIfExists = false)
    }
  }

  override def afterEach(): Unit = {
    Utils.deleteRecursively(tempDir)
    hiveClient.dropDatabase("test_db", ignoreIfNotExists = false, cascade = true)
  }

  private def getTableMetadata(tableName: String): CatalogTable = {
    spark.sharedState.externalCatalog.getTable("test_db", tableName)
  }

  private def defaultTablePath(tableName: String): String = {
    spark.sessionState.catalog.defaultTablePath(TableIdentifier(tableName, Some("test_db")))
  }


  // Raw table metadata that are dumped from tables created by Spark 2.0. Note that, all spark
  // versions prior to 2.1 would generate almost same raw table metadata for a specific table.
  val simpleSchema = new StructType().add("i", "int")
  val partitionedSchema = new StructType().add("i", "int").add("j", "int")

  lazy val hiveTable = CatalogTable(
    identifier = TableIdentifier("tbl1", Some("test_db")),
    tableType = CatalogTableType.MANAGED,
    storage = CatalogStorageFormat.empty.copy(
      inputFormat = Some("org.apache.hadoop.mapred.TextInputFormat"),
      outputFormat = Some("org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat")),
    schema = simpleSchema)

  lazy val externalHiveTable = CatalogTable(
    identifier = TableIdentifier("tbl2", Some("test_db")),
    tableType = CatalogTableType.EXTERNAL,
    storage = CatalogStorageFormat.empty.copy(
      locationUri = Some(tempDir.getAbsolutePath),
      inputFormat = Some("org.apache.hadoop.mapred.TextInputFormat"),
      outputFormat = Some("org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat")),
    schema = simpleSchema)

  lazy val partitionedHiveTable = CatalogTable(
    identifier = TableIdentifier("tbl3", Some("test_db")),
    tableType = CatalogTableType.MANAGED,
    storage = CatalogStorageFormat.empty.copy(
      inputFormat = Some("org.apache.hadoop.mapred.TextInputFormat"),
      outputFormat = Some("org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat")),
    schema = partitionedSchema,
    partitionColumnNames = Seq("j"))


  val simpleSchemaJson =
    """
      |{
      | "type": "struct",
      | "fields": [{
      |             "name": "i",
      |             "type": "integer",
      |             "nullable": true,
      |             "metadata": {}
      |            }]
      |}
    """.stripMargin

  val partitionedSchemaJson =
    """
      |{
      | "type": "struct",
      | "fields": [{
      |             "name": "i",
      |             "type": "integer",
      |             "nullable": true,
      |             "metadata": {}
      |            },
      |            {
      |             "name": "j",
      |             "type": "integer",
      |             "nullable": true,
      |             "metadata": {}
      |            }]
      |}
    """.stripMargin

  lazy val dataSourceTable = CatalogTable(
    identifier = TableIdentifier("tbl4", Some("test_db")),
    tableType = CatalogTableType.MANAGED,
    storage = CatalogStorageFormat.empty.copy(properties = Map("path" -> defaultTablePath("tbl4"))),
    schema = new StructType(),
    properties = Map(
      "spark.sql.sources.provider" -> "json",
      "spark.sql.sources.schema.numParts" -> "1",
      "spark.sql.sources.schema.part.0" -> simpleSchemaJson))

  lazy val hiveCompatibleDataSourceTable = CatalogTable(
    identifier = TableIdentifier("tbl5", Some("test_db")),
    tableType = CatalogTableType.MANAGED,
    storage = CatalogStorageFormat.empty.copy(properties = Map("path" -> defaultTablePath("tbl5"))),
    schema = simpleSchema,
    properties = Map(
      "spark.sql.sources.provider" -> "parquet",
      "spark.sql.sources.schema.numParts" -> "1",
      "spark.sql.sources.schema.part.0" -> simpleSchemaJson))

  lazy val partitionedDataSourceTable = CatalogTable(
    identifier = TableIdentifier("tbl6", Some("test_db")),
    tableType = CatalogTableType.MANAGED,
    storage = CatalogStorageFormat.empty.copy(properties = Map("path" -> defaultTablePath("tbl6"))),
    schema = new StructType(),
    properties = Map(
      "spark.sql.sources.provider" -> "json",
      "spark.sql.sources.schema.numParts" -> "1",
      "spark.sql.sources.schema.part.0" -> partitionedSchemaJson,
      "spark.sql.sources.schema.numPartCols" -> "1",
      "spark.sql.sources.schema.partCol.0" -> "j"))

  lazy val externalDataSourceTable = CatalogTable(
    identifier = TableIdentifier("tbl7", Some("test_db")),
    tableType = CatalogTableType.EXTERNAL,
    storage = CatalogStorageFormat.empty.copy(
      locationUri = Some(defaultTablePath("tbl7") + "-__PLACEHOLDER__"),
      properties = Map("path" -> tempDir.getAbsolutePath)),
    schema = new StructType(),
    properties = Map(
      "spark.sql.sources.provider" -> "json",
      "spark.sql.sources.schema.numParts" -> "1",
      "spark.sql.sources.schema.part.0" -> simpleSchemaJson))

  lazy val hiveCompatibleExternalDataSourceTable = CatalogTable(
    identifier = TableIdentifier("tbl8", Some("test_db")),
    tableType = CatalogTableType.EXTERNAL,
    storage = CatalogStorageFormat.empty.copy(
      locationUri = Some(tempDir.getAbsolutePath),
      properties = Map("path" -> tempDir.getAbsolutePath)),
    schema = simpleSchema,
    properties = Map(
      "spark.sql.sources.provider" -> "parquet",
      "spark.sql.sources.schema.numParts" -> "1",
      "spark.sql.sources.schema.part.0" -> simpleSchemaJson))

  lazy val dataSourceTableWithoutSchema = CatalogTable(
    identifier = TableIdentifier("tbl9", Some("test_db")),
    tableType = CatalogTableType.EXTERNAL,
    storage = CatalogStorageFormat.empty.copy(
      locationUri = Some(defaultTablePath("tbl9") + "-__PLACEHOLDER__"),
      properties = Map("path" -> tempDir.getAbsolutePath)),
    schema = new StructType(),
    properties = Map("spark.sql.sources.provider" -> "json"))

  // A list of all raw tables we want to test, with their expected schema.
  lazy val rawTablesAndExpectations = Seq(
    hiveTable -> simpleSchema,
    externalHiveTable -> simpleSchema,
    partitionedHiveTable -> partitionedSchema,
    dataSourceTable -> simpleSchema,
    hiveCompatibleDataSourceTable -> simpleSchema,
    partitionedDataSourceTable -> partitionedSchema,
    externalDataSourceTable -> simpleSchema,
    hiveCompatibleExternalDataSourceTable -> simpleSchema,
    dataSourceTableWithoutSchema -> new StructType())

  test("make sure we can read table created by old version of Spark") {
    for ((tbl, expectedSchema) <- rawTablesAndExpectations) {
      val readBack = getTableMetadata(tbl.identifier.table)
      assert(readBack.schema.sameType(expectedSchema))

      if (tbl.tableType == CatalogTableType.EXTERNAL) {
        // trim the URI prefix
        val tableLocation = new URI(readBack.storage.locationUri.get).getPath
        assert(tableLocation == tempDir.getAbsolutePath)
      }
    }
  }

  test("make sure we can alter table location created by old version of Spark") {
    withTempDir { dir =>
      for ((tbl, _) <- rawTablesAndExpectations if tbl.tableType == CatalogTableType.EXTERNAL) {
        sql(s"ALTER TABLE ${tbl.identifier} SET LOCATION '${dir.getAbsolutePath}'")

        val readBack = getTableMetadata(tbl.identifier.table)

        // trim the URI prefix
        val actualTableLocation = new URI(readBack.storage.locationUri.get).getPath
        assert(actualTableLocation == dir.getAbsolutePath)
      }
    }
  }

  test("make sure we can rename table created by old version of Spark") {
    for ((tbl, expectedSchema) <- rawTablesAndExpectations) {
      val newName = tbl.identifier.table + "_renamed"
      sql(s"ALTER TABLE ${tbl.identifier} RENAME TO $newName")

      val readBack = getTableMetadata(newName)
      assert(readBack.schema.sameType(expectedSchema))

      // trim the URI prefix
      val actualTableLocation = new URI(readBack.storage.locationUri.get).getPath
      val expectedLocation = if (tbl.tableType == CatalogTableType.EXTERNAL) {
        tempDir.getAbsolutePath
      } else {
        // trim the URI prefix
        new URI(defaultTablePath(newName)).getPath
      }
      assert(actualTableLocation == expectedLocation)
    }
  }
}
