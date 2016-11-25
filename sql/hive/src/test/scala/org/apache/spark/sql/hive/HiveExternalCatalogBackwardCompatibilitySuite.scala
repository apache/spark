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

import org.apache.hadoop.fs.Path

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, CatalogTable, CatalogTableType}
import org.apache.spark.sql.hive.client.HiveClient
import org.apache.spark.sql.hive.test.TestHiveSingleton
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.Utils


class HiveExternalCatalogBackwardCompatibilitySuite extends QueryTest with TestHiveSingleton {

  // To test `HiveExternalCatalog`, we need to read/write the raw table meta from/to hive client.
  val hiveClient: HiveClient =
    spark.sharedState.externalCatalog.asInstanceOf[HiveExternalCatalog].client

  val tempDir = Utils.createTempDir().getCanonicalFile

  override def beforeAll(): Unit = {
    for ((tbl, _, _) <- rawTablesAndExpectations) {
      hiveClient.createTable(tbl, ignoreIfExists = false)
    }
  }

  override def afterAll(): Unit = {
    Utils.deleteRecursively(tempDir)
    for (i <- 1 to rawTablesAndExpectations.length) {
      hiveClient.dropTable("default", s"tbl$i", ignoreIfNotExists = true, purge = false)
    }
  }


  // Raw table metadata that are dumped from tables created by Spark 2.0
  val simpleSchema = new StructType().add("i", "int")
  val partitionedSchema = new StructType().add("i", "int").add("j", "int")

  val hiveTable = CatalogTable(
    identifier = TableIdentifier("tbl1", Some("default")),
    tableType = CatalogTableType.MANAGED,
    storage = CatalogStorageFormat.empty.copy(
      inputFormat = Some("org.apache.hadoop.mapred.TextInputFormat"),
      outputFormat = Some("org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat")),
    schema = simpleSchema)

  val externalHiveTable = CatalogTable(
    identifier = TableIdentifier("tbl2", Some("default")),
    tableType = CatalogTableType.EXTERNAL,
    storage = CatalogStorageFormat.empty.copy(
      locationUri = Some(tempDir.getCanonicalPath),
      inputFormat = Some("org.apache.hadoop.mapred.TextInputFormat"),
      outputFormat = Some("org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat")),
    schema = simpleSchema)

  val partitionedHiveTable = CatalogTable(
    identifier = TableIdentifier("tbl3", Some("default")),
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

  def defaultTablePath(tableName: String): String = {
    spark.sessionState.catalog.defaultTablePath(TableIdentifier(tableName))
  }

  val dataSourceTable = CatalogTable(
    identifier = TableIdentifier("tbl4", Some("default")),
    tableType = CatalogTableType.MANAGED,
    storage = CatalogStorageFormat.empty.copy(properties = Map("path" -> defaultTablePath("tbl4"))),
    schema = new StructType(),
    properties = Map(
      "spark.sql.sources.provider" -> "json",
      "spark.sql.sources.schema.numParts" -> "1",
      "spark.sql.sources.schema.part.0" -> simpleSchemaJson))

  val hiveCompatibleDataSourceTable = CatalogTable(
    identifier = TableIdentifier("tbl5", Some("default")),
    tableType = CatalogTableType.MANAGED,
    storage = CatalogStorageFormat.empty.copy(properties = Map("path" -> defaultTablePath("tbl5"))),
    schema = simpleSchema,
    properties = Map(
      "spark.sql.sources.provider" -> "parquet",
      "spark.sql.sources.schema.numParts" -> "1",
      "spark.sql.sources.schema.part.0" -> simpleSchemaJson))

  val partitionedDataSourceTable = CatalogTable(
    identifier = TableIdentifier("tbl6", Some("default")),
    tableType = CatalogTableType.MANAGED,
    storage = CatalogStorageFormat.empty.copy(properties = Map("path" -> defaultTablePath("tbl6"))),
    schema = new StructType(),
    properties = Map(
      "spark.sql.sources.provider" -> "json",
      "spark.sql.sources.schema.numParts" -> "1",
      "spark.sql.sources.schema.part.0" -> partitionedSchemaJson,
      "spark.sql.sources.schema.numPartCols" -> "1",
      "spark.sql.sources.schema.partCol.0" -> "j"))

  val externalDataSourceTable = CatalogTable(
    identifier = TableIdentifier("tbl7", Some("default")),
    tableType = CatalogTableType.EXTERNAL,
    storage = CatalogStorageFormat.empty.copy(
      locationUri = Some(new Path(defaultTablePath("tbl7"), "-__PLACEHOLDER__").toString),
      properties = Map("path" -> tempDir.getAbsolutePath)),
    schema = new StructType(),
    properties = Map(
      "spark.sql.sources.provider" -> "json",
      "spark.sql.sources.schema.numParts" -> "1",
      "spark.sql.sources.schema.part.0" -> simpleSchemaJson))

  val hiveCompatibleExternalDataSourceTable = CatalogTable(
    identifier = TableIdentifier("tbl8", Some("default")),
    tableType = CatalogTableType.EXTERNAL,
    storage = CatalogStorageFormat.empty.copy(
      locationUri = Some(tempDir.getAbsolutePath),
      properties = Map("path" -> tempDir.getAbsolutePath)),
    schema = simpleSchema,
    properties = Map(
      "spark.sql.sources.provider" -> "parquet",
      "spark.sql.sources.schema.numParts" -> "1",
      "spark.sql.sources.schema.part.0" -> simpleSchemaJson))

  val dataSourceTableWithoutSchema = CatalogTable(
    identifier = TableIdentifier("tbl9", Some("default")),
    tableType = CatalogTableType.EXTERNAL,
    storage = CatalogStorageFormat.empty.copy(
      locationUri = Some(new Path(defaultTablePath("tbl9"), "-__PLACEHOLDER__").toString),
      properties = Map("path" -> tempDir.getAbsolutePath)),
    schema = new StructType(),
    properties = Map("spark.sql.sources.provider" -> "json"))

  // A list of all raw tables we want to test, with their expected schema and table location.
  val rawTablesAndExpectations = Seq(
    (hiveTable, simpleSchema, None),
    (externalHiveTable, simpleSchema, Some(tempDir.getCanonicalPath)),
    (partitionedHiveTable, partitionedSchema, None),
    (dataSourceTable, simpleSchema, None),
    (hiveCompatibleDataSourceTable, simpleSchema, None),
    (partitionedDataSourceTable, partitionedSchema, None),
    (externalDataSourceTable, simpleSchema, Some(tempDir.getCanonicalPath)),
    (hiveCompatibleExternalDataSourceTable, simpleSchema, Some(tempDir.getCanonicalPath)),
    (dataSourceTableWithoutSchema, new StructType(), None))

  test("make sure we can read table created by old version of Spark") {
    for ((tbl, expectedSchema, expectedLocation) <- rawTablesAndExpectations) {
      val readBack = spark.sharedState.externalCatalog.getTable(
        tbl.identifier.database.get, tbl.identifier.table)

      assert(readBack.schema == expectedSchema)
      expectedLocation.foreach { loc =>
        // trim the URI prefix
        val tableLocation = new URI(readBack.storage.locationUri.get).getPath
        assert(tableLocation == loc)
      }
    }
  }
}
