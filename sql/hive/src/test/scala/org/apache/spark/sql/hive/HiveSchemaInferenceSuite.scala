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
import java.util.Locale

import scala.util.Random

import org.scalatest.BeforeAndAfterEach

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.execution.datasources.FileStatusCache
import org.apache.spark.sql.hive.test.TestHiveSingleton
import org.apache.spark.sql.internal.{HiveSerDe, SQLConf}
import org.apache.spark.sql.internal.SQLConf.HiveCaseSensitiveInferenceMode.{Value => InferenceMode, _}
import org.apache.spark.sql.test.SQLTestUtils
import org.apache.spark.sql.types._

class HiveSchemaInferenceSuite
  extends QueryTest with TestHiveSingleton with SQLTestUtils with BeforeAndAfterEach {

  import HiveSchemaInferenceSuite._
  import HiveExternalCatalog.DATASOURCE_SCHEMA_PREFIX

  override def beforeEach(): Unit = {
    super.beforeEach()
    FileStatusCache.resetForTesting()
  }

  override def afterEach(): Unit = {
    super.afterEach()
    spark.sessionState.catalog.invalidateAllCachedTables()
    FileStatusCache.resetForTesting()
  }

  private val externalCatalog =
    spark.sharedState.externalCatalog.unwrapped.asInstanceOf[HiveExternalCatalog]
  private val client = externalCatalog.client

  // Return a copy of the given schema with all field names converted to lower case.
  private def lowerCaseSchema(schema: StructType): StructType = {
    StructType(schema.map(f => f.copy(name = f.name.toLowerCase(Locale.ROOT))))
  }

  // Create a Hive external test table containing the given field and partition column names.
  // Returns a case-sensitive schema for the table.
  private def setupExternalTable(
      fileType: String,
      fields: Seq[String],
      partitionCols: Seq[String],
      dir: File): StructType = {
    // Treat all table fields as bigints...
    val structFields = fields.map { field =>
      StructField(
        name = field,
        dataType = LongType,
        nullable = true,
        metadata = Metadata.empty)
    }
    // and all partition columns as ints
    val partitionStructFields = partitionCols.map { field =>
      StructField(
        // Partition column case isn't preserved
        name = field.toLowerCase(Locale.ROOT),
        dataType = IntegerType,
        nullable = true,
        metadata = Metadata.empty)
    }
    val schema = StructType(structFields ++ partitionStructFields)

    // Write some test data (partitioned if specified)
    val writer = spark.range(NUM_RECORDS)
      .selectExpr((fields ++ partitionCols).map("id as " + _): _*)
      .write
      .partitionBy(partitionCols: _*)
      .mode("overwrite")
    fileType match {
      case ORC_FILE_TYPE =>
       writer.orc(dir.getAbsolutePath)
      case PARQUET_FILE_TYPE =>
       writer.parquet(dir.getAbsolutePath)
    }

    // Create Hive external table with lowercased schema
    val serde = HiveSerDe.serdeMap(fileType)
    client.createTable(
      CatalogTable(
        identifier = TableIdentifier(table = TEST_TABLE_NAME, database = Option(DATABASE)),
        tableType = CatalogTableType.EXTERNAL,
        storage = CatalogStorageFormat(
          locationUri = Option(dir.toURI),
          inputFormat = serde.inputFormat,
          outputFormat = serde.outputFormat,
          serde = serde.serde,
          compressed = false,
          properties = Map("serialization.format" -> "1")),
        schema = schema,
        provider = Option("hive"),
        partitionColumnNames = partitionCols.map(_.toLowerCase(Locale.ROOT)),
        properties = Map.empty),
      true)

    // Check that the table returned by HiveExternalCatalog has schemaPreservesCase set to false
    // and that the raw table returned by the Hive client doesn't have any Spark SQL properties
    // set (table needs to be obtained from client since HiveExternalCatalog filters these
    // properties out).
    assert(!externalCatalog.getTable(DATABASE, TEST_TABLE_NAME).schemaPreservesCase)
    val rawTable = client.getTable(DATABASE, TEST_TABLE_NAME)
    assert(!rawTable.properties.exists { case (k, _) => k.startsWith(DATASOURCE_SCHEMA_PREFIX) })

    // Add partition records (if specified)
    if (!partitionCols.isEmpty) {
      spark.catalog.recoverPartitions(TEST_TABLE_NAME)
    }

    schema
  }

  private def withTestTables(
    fileType: String)(f: (Seq[String], Seq[String], StructType) => Unit): Unit = {
    // Test both a partitioned and unpartitioned Hive table
    val tableFields = Seq(
      (Seq("fieldOne"), Seq("partCol1", "partCol2")),
      (Seq("fieldOne", "fieldTwo"), Seq.empty[String]))

    tableFields.foreach { case (fields, partCols) =>
      withTempDir { dir =>
        val schema = setupExternalTable(fileType, fields, partCols, dir)
        withTable(TEST_TABLE_NAME) { f(fields, partCols, schema) }
      }
    }
  }

  private def withFileTypes(f: (String) => Unit): Unit
    = Seq(ORC_FILE_TYPE, PARQUET_FILE_TYPE).foreach(f)

  private def withInferenceMode(mode: InferenceMode)(f: => Unit): Unit = {
    withSQLConf(
      HiveUtils.CONVERT_METASTORE_ORC.key -> "true",
      SQLConf.HIVE_CASE_SENSITIVE_INFERENCE.key -> mode.toString)(f)
  }

  private def testFieldQuery(fields: Seq[String]): Unit = {
    if (!fields.isEmpty) {
      val query = s"SELECT * FROM ${TEST_TABLE_NAME} WHERE ${Random.shuffle(fields).head} >= 0"
      assert(spark.sql(query).count() == NUM_RECORDS)
    }
  }

  private def testTableSchema(expectedSchema: StructType): Unit
    = assert(spark.table(TEST_TABLE_NAME).schema == expectedSchema)

  withFileTypes { fileType =>
    test(s"$fileType: schema should be inferred and saved when INFER_AND_SAVE is specified") {
      withInferenceMode(INFER_AND_SAVE) {
        withTestTables(fileType) { (fields, partCols, schema) =>
          testFieldQuery(fields)
          testFieldQuery(partCols)
          testTableSchema(schema)

          // Verify the catalog table now contains the updated schema and properties
          val catalogTable = externalCatalog.getTable(DATABASE, TEST_TABLE_NAME)
          assert(catalogTable.schemaPreservesCase)
          assert(catalogTable.schema == schema)
          assert(catalogTable.partitionColumnNames == partCols.map(_.toLowerCase(Locale.ROOT)))
        }
      }
    }
  }

  withFileTypes { fileType =>
    test(s"$fileType: schema should be inferred but not stored when INFER_ONLY is specified") {
      withInferenceMode(INFER_ONLY) {
        withTestTables(fileType) { (fields, partCols, schema) =>
          val originalTable = externalCatalog.getTable(DATABASE, TEST_TABLE_NAME)
          testFieldQuery(fields)
          testFieldQuery(partCols)
          testTableSchema(schema)
          // Catalog table shouldn't be altered
          assert(externalCatalog.getTable(DATABASE, TEST_TABLE_NAME) == originalTable)
        }
      }
    }
  }

  withFileTypes { fileType =>
    test(s"$fileType: schema should not be inferred when NEVER_INFER is specified") {
      withInferenceMode(NEVER_INFER) {
        withTestTables(fileType) { (fields, partCols, schema) =>
          val originalTable = externalCatalog.getTable(DATABASE, TEST_TABLE_NAME)
          // Only check the table schema as the test queries will break
          testTableSchema(lowerCaseSchema(schema))
          assert(externalCatalog.getTable(DATABASE, TEST_TABLE_NAME) == originalTable)
        }
      }
    }
  }

  test("mergeWithMetastoreSchema() should return expected results") {
    // Field type conflict resolution
    assertResult(
      StructType(Seq(
        StructField("lowerCase", StringType),
        StructField("UPPERCase", DoubleType, nullable = false)))) {

      HiveMetastoreCatalog.mergeWithMetastoreSchema(
        StructType(Seq(
          StructField("lowercase", StringType),
          StructField("uppercase", DoubleType, nullable = false))),

        StructType(Seq(
          StructField("lowerCase", BinaryType),
          StructField("UPPERCase", IntegerType, nullable = true))))
    }

    // MetaStore schema is subset of parquet schema
    assertResult(
      StructType(Seq(
        StructField("UPPERCase", DoubleType, nullable = false)))) {

      HiveMetastoreCatalog.mergeWithMetastoreSchema(
        StructType(Seq(
          StructField("uppercase", DoubleType, nullable = false))),

        StructType(Seq(
          StructField("lowerCase", BinaryType),
          StructField("UPPERCase", IntegerType, nullable = true))))
    }

    // Metastore schema contains additional non-nullable fields.
    assert(intercept[Throwable] {
      HiveMetastoreCatalog.mergeWithMetastoreSchema(
        StructType(Seq(
          StructField("uppercase", DoubleType, nullable = false),
          StructField("lowerCase", BinaryType, nullable = false))),

        StructType(Seq(
          StructField("UPPERCase", IntegerType, nullable = true))))
    }.getMessage.contains("Detected conflicting schemas"))

    // Conflicting non-nullable field names
    intercept[Throwable] {
      HiveMetastoreCatalog.mergeWithMetastoreSchema(
        StructType(Seq(StructField("lower", StringType, nullable = false))),
        StructType(Seq(StructField("lowerCase", BinaryType))))
    }

    // Parquet schema is subset of metaStore schema and has uppercase field name
    assertResult(
      StructType(Seq(
        StructField("UPPERCase", DoubleType, nullable = true),
        StructField("lowerCase", BinaryType, nullable = true)))) {

      HiveMetastoreCatalog.mergeWithMetastoreSchema(
        StructType(Seq(
          StructField("UPPERCase", DoubleType, nullable = true),
          StructField("lowerCase", BinaryType, nullable = true))),

        StructType(Seq(
          StructField("lowerCase", BinaryType, nullable = true))))
    }

    // Metastore schema contains additional nullable fields.
    assert(intercept[Throwable] {
      HiveMetastoreCatalog.mergeWithMetastoreSchema(
        StructType(Seq(
          StructField("UPPERCase", DoubleType, nullable = false),
          StructField("lowerCase", BinaryType, nullable = true))),

        StructType(Seq(
          StructField("lowerCase", BinaryType, nullable = true))))
    }.getMessage.contains("Detected conflicting schemas"))

    // Check that merging missing nullable fields works as expected.
    assertResult(
      StructType(Seq(
        StructField("firstField", StringType, nullable = true),
        StructField("secondField", StringType, nullable = true),
        StructField("thirdfield", StringType, nullable = true)))) {
      HiveMetastoreCatalog.mergeWithMetastoreSchema(
        StructType(Seq(
          StructField("firstfield", StringType, nullable = true),
          StructField("secondfield", StringType, nullable = true),
          StructField("thirdfield", StringType, nullable = true))),
        StructType(Seq(
          StructField("firstField", StringType, nullable = true),
          StructField("secondField", StringType, nullable = true))))
    }

    // Merge should fail if the Metastore contains any additional fields that are not
    // nullable.
    assert(intercept[Throwable] {
      HiveMetastoreCatalog.mergeWithMetastoreSchema(
        StructType(Seq(
          StructField("firstfield", StringType, nullable = true),
          StructField("secondfield", StringType, nullable = true),
          StructField("thirdfield", StringType, nullable = false))),
        StructType(Seq(
          StructField("firstField", StringType, nullable = true),
          StructField("secondField", StringType, nullable = true))))
    }.getMessage.contains("Detected conflicting schemas"))

    // Schema merge should maintain metastore order.
    assertResult(
      StructType(Seq(
        StructField("first_field", StringType, nullable = true),
        StructField("second_field", StringType, nullable = true),
        StructField("third_field", StringType, nullable = true),
        StructField("fourth_field", StringType, nullable = true),
        StructField("fifth_field", StringType, nullable = true)))) {
      HiveMetastoreCatalog.mergeWithMetastoreSchema(
        StructType(Seq(
          StructField("first_field", StringType, nullable = true),
          StructField("second_field", StringType, nullable = true),
          StructField("third_field", StringType, nullable = true),
          StructField("fourth_field", StringType, nullable = true),
          StructField("fifth_field", StringType, nullable = true))),
        StructType(Seq(
          StructField("fifth_field", StringType, nullable = true),
          StructField("third_field", StringType, nullable = true),
          StructField("second_field", StringType, nullable = true))))
    }
  }
}

object HiveSchemaInferenceSuite {
  private val NUM_RECORDS = 10
  private val DATABASE = "default"
  private val TEST_TABLE_NAME = "test_table"
  private val ORC_FILE_TYPE = "orc"
  private val PARQUET_FILE_TYPE = "parquet"
}
