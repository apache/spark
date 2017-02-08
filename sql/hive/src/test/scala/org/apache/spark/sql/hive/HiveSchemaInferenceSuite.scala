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
import java.util.concurrent.{Executors, TimeUnit}

import org.scalatest.BeforeAndAfterEach

import org.apache.spark.metrics.source.HiveCatalogMetrics
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.execution.datasources.FileStatusCache
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.hive.client.HiveClient
import org.apache.spark.sql.hive.test.TestHiveSingleton
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SQLTestUtils
import org.apache.spark.sql.types._

class HiveSchemaInferenceSuite
  extends QueryTest with TestHiveSingleton with SQLTestUtils with BeforeAndAfterEach {

  import HiveSchemaInferenceSuite._

  // Create a CatalogTable instance modeling an external Hive table in a metastore that isn't
  // controlled by Spark (i.e. has no Spark-specific table properties set).
  private def hiveExternalCatalogTable(
      tableName: String,
      location: String,
      schema: StructType,
      partitionColumns: Seq[String],
      properties: Map[String, String] = Map.empty): CatalogTable = {
    CatalogTable(
      identifier = TableIdentifier(table = tableName, database = Option("default")),
      tableType = CatalogTableType.EXTERNAL,
      storage = CatalogStorageFormat(
        locationUri = Option(location),
        inputFormat = Option("org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"),
        outputFormat = Option("org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"),
        serde = Option("org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"),
        compressed = false,
        properties = Map("serialization.format" -> "1")),
      schema = schema,
      provider = Option("hive"),
      partitionColumnNames = partitionColumns,
      properties = properties)
  }

  // Creates CatalogTablePartition instances for adding partitions of data to our test table.
  private def hiveCatalogPartition(location: String, index: Int): CatalogTablePartition
    = CatalogTablePartition(
      spec = Map("partcol1" -> index.toString, "partcol2" -> index.toString),
      storage = CatalogStorageFormat(
        locationUri = Option(s"${location}/partCol1=$index/partCol2=$index/"),
        inputFormat = Option("org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"),
        outputFormat = Option("org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"),
        serde = Option("org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"),
        compressed = false,
        properties = Map("serialization.format" -> "1")))

  // Creates a case-sensitive external Hive table for testing schema inference options. Table
  // will not have Spark-specific table properties set.
  private def setupCaseSensitiveTable(
      tableName: String,
      dir: File): Unit = {
    spark.range(NUM_RECORDS)
      .selectExpr("id as fieldOne", "id as partCol1", "id as partCol2")
      .write
      .partitionBy("partCol1", "partCol2")
      .mode("overwrite")
      .parquet(dir.getAbsolutePath)

    val lowercaseSchema = StructType(Seq(
      StructField("fieldone", LongType),
      StructField("partcol1", IntegerType),
      StructField("partcol2", IntegerType)))

    val client = spark.sharedState.externalCatalog.asInstanceOf[HiveExternalCatalog].client

    val catalogTable = hiveExternalCatalogTable(
      tableName,
      dir.getAbsolutePath,
      lowercaseSchema,
      Seq("partcol1", "partcol2"))
    client.createTable(catalogTable, true)

    val partitions = (0 until NUM_RECORDS).map(hiveCatalogPartition(dir.getAbsolutePath, _)).toSeq
    client.createPartitions("default", tableName, partitions, true)
  }

  // Create a test table used for a single unit test, with data stored in the specified directory.
  private def withTestTable(dir: File)(f: File => Unit): Unit = {
    setupCaseSensitiveTable(TEST_TABLE_NAME, dir)
    try f(dir) finally spark.sql(s"DROP TABLE IF EXISTS $TEST_TABLE_NAME")
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    FileStatusCache.resetForTesting()
  }

  override def afterEach(): Unit = {
    super.afterEach()
    FileStatusCache.resetForTesting()
  }

  test("Queries against case-sensitive tables with no schema in table properties should work " +
    "when schema inference is enabled") {
    withSQLConf("spark.sql.hive.schemaInferenceMode" -> "INFER_AND_SAVE") {
      withTempDir { dir =>
        withTestTable(dir) { dir =>
          val expectedSchema = StructType(Seq(
            StructField("fieldOne", LongType),
            // Partition columns remain case-insensitive
            StructField("partcol1", IntegerType),
            StructField("partcol2", IntegerType)))
          assert(spark.sql(FIELD_QUERY).count == NUM_RECORDS)
          assert(spark.sql(PARTITION_COLUMN_QUERY).count == NUM_RECORDS)
          // Test that the case-sensitive schema was storied as a table property after inference
          assert(spark.sql(SELECT_ALL_QUERY).schema == expectedSchema)
        }
      }
    }
  }

  test("Schema should be inferred but not stored when ...") {
    withSQLConf("spark.sql.hive.schemaInferenceMode" -> "INFER_ONLY") {
      withTempDir { dir =>
        withTestTable(dir) { dir =>
          val existingSchema = spark.sql(SELECT_ALL_QUERY).schema
          assert(spark.sql(FIELD_QUERY).count == NUM_RECORDS)
          assert(spark.sql(PARTITION_COLUMN_QUERY).count == NUM_RECORDS)
          assert(spark.sql(SELECT_ALL_QUERY).schema == existingSchema)
        }
      }
    }
  }
}

object HiveSchemaInferenceSuite {
  private val NUM_RECORDS = 10
  private val TEST_TABLE_NAME = "test_table"
  private val FIELD_QUERY = s"SELECT * FROM $TEST_TABLE_NAME WHERE fieldOne >= 0"
  private val PARTITION_COLUMN_QUERY = s"SELECT * FROM $TEST_TABLE_NAME WHERE partCol1 >= 0"
  private val SELECT_ALL_QUERY = s"SELECT * FROM $TEST_TABLE_NAME"
}
