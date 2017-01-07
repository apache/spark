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

package org.apache.spark.sql.hive.execution

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, CatalogTable, CatalogTableType}
import org.apache.spark.sql.hive.test.TestHiveSingleton
import org.apache.spark.sql.test.SQLTestUtils
import org.apache.spark.sql.types.StructType

/**
 * A suite for testing resolve the views defined by older versions of Spark(before 2.2)
 */
class SQLViewBackwardCompatibilitySuite extends QueryTest
  with SQLTestUtils with TestHiveSingleton {

  private val DATABASE_NAME = "test_db"

  override def beforeAll(): Unit = {
    sql(s"CREATE DATABASE $DATABASE_NAME")
    for ((tbl, _) <- rawTablesAndExpectations) {
      spark.sessionState.catalog.createTable(tbl, ignoreIfExists = false)
    }
  }

  override def afterAll(): Unit = {
    sql(s"DROP DATABASE IF EXISTS $DATABASE_NAME CASCADE")
  }

  private val simpleSchema = new StructType().add("id", "int").add("id1", "int")

  lazy val jsonTable = CatalogTable(
    identifier = TableIdentifier("json_table", Some(DATABASE_NAME)),
    tableType = CatalogTableType.MANAGED,
    provider = Some("json"),
    storage = CatalogStorageFormat.empty,
    schema = simpleSchema)

  lazy val parquetTable = CatalogTable(
    identifier = TableIdentifier("parquet_table", Some(DATABASE_NAME)),
    tableType = CatalogTableType.MANAGED,
    provider = Some("parquet"),
    storage = CatalogStorageFormat.empty,
    schema = simpleSchema,
    partitionColumnNames = Seq("id"))

  lazy val hiveParquetTable = CatalogTable(
    identifier = TableIdentifier("hive_parquet_table", Some(DATABASE_NAME)),
    tableType = CatalogTableType.MANAGED,
    provider = Some("hive"),
    storage = CatalogStorageFormat.empty.copy(
      inputFormat = Some("org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"),
      outputFormat = Some("org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"),
      serde = Some("org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe")),
    schema = simpleSchema)

  lazy val hiveOrcTable = CatalogTable(
    identifier = TableIdentifier("hive_orc_table", Some(DATABASE_NAME)),
    tableType = CatalogTableType.MANAGED,
    provider = Some("hive"),
    storage = CatalogStorageFormat.empty.copy(
      inputFormat = Some("org.apache.hadoop.hive.ql.io.orc.OrcInputFormat"),
      outputFormat = Some("org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat"),
      serde = Some("org.apache.hadoop.hive.ql.io.orc.OrcSerde")),
    schema = simpleSchema)

  // A list of all raw tables we want to test, with their expected schema.
  lazy val rawTablesAndExpectations = Seq(
    jsonTable -> simpleSchema,
    parquetTable -> simpleSchema,
    hiveParquetTable -> simpleSchema,
    hiveOrcTable -> simpleSchema)

  test("make sure we can resolve view created by old version of Spark") {
    for ((tbl, _) <- rawTablesAndExpectations) {
      withView(s"$DATABASE_NAME.view") {
        // The views defined by older versions of Spark(before 2.2) will have empty view default
        // database name, and all the relations referenced in the viewText will have database part
        // defined.
        val tableName = tbl.identifier.toString
        val view = CatalogTable(
          identifier = TableIdentifier("view", Some(DATABASE_NAME)),
          tableType = CatalogTableType.VIEW,
          storage = CatalogStorageFormat.empty,
          schema = tbl.schema,
          viewOriginalText = Some(s"SELECT * FROM ${tbl.identifier.table}"),
          viewText = Some("SELECT `gen_attr_0` AS `id`, `gen_attr_1` AS `id1` FROM (SELECT " +
            "`gen_attr_0`, `gen_attr_1` FROM (SELECT `id` AS `gen_attr_0`, `id1` AS " +
            s"`gen_attr_1` FROM $tableName) AS gen_subquery_0) AS ${tbl.identifier.table}")
        )
        hiveContext.sessionState.catalog.createTable(view, ignoreIfExists = false)
        // Check the output schema.
        assert(sql(s"SELECT * FROM $DATABASE_NAME.view").schema.sameType(view.schema))
      }
    }
  }
}
