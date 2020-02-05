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

import org.apache.hadoop.conf.Configuration

import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.execution.command.DDLUtils
import org.apache.spark.sql.types.StructType

/**
 * Test suite for the [[HiveExternalCatalog]].
 */
class HiveExternalCatalogSuite extends ExternalCatalogSuite {

  private val externalCatalog: HiveExternalCatalog = {
    val catalog = new HiveExternalCatalog(new SparkConf, new Configuration)
    catalog.client.reset()
    catalog
  }

  protected override val utils: CatalogTestUtils = new CatalogTestUtils {
    override val tableInputFormat: String = "org.apache.hadoop.mapred.SequenceFileInputFormat"
    override val tableOutputFormat: String = "org.apache.hadoop.mapred.SequenceFileOutputFormat"
    override def newEmptyCatalog(): ExternalCatalog = externalCatalog
    override val defaultProvider: String = "hive"
  }

  protected override def resetState(): Unit = {
    externalCatalog.client.reset()
  }

  import utils._

  test("SPARK-18647: do not put provider in table properties for Hive serde table") {
    val catalog = newBasicCatalog()
    val hiveTable = CatalogTable(
      identifier = TableIdentifier("hive_tbl", Some("db1")),
      tableType = CatalogTableType.MANAGED,
      storage = storageFormat,
      schema = new StructType().add("col1", "int").add("col2", "string"),
      provider = Some("hive"))
    catalog.createTable(hiveTable, ignoreIfExists = false)

    val rawTable = externalCatalog.client.getTable("db1", "hive_tbl")
    assert(!rawTable.properties.contains(HiveExternalCatalog.DATASOURCE_PROVIDER))
    assert(DDLUtils.isHiveTable(externalCatalog.getTable("db1", "hive_tbl")))
  }

  Seq("parquet", "hive").foreach { format =>
    test(s"Partition columns should be put at the end of table schema for the format $format") {
      val catalog = newBasicCatalog()
      val newSchema = new StructType()
        .add("col1", "int")
        .add("col2", "string")
        .add("partCol1", "int")
        .add("partCol2", "string")
      val table = CatalogTable(
        identifier = TableIdentifier("tbl", Some("db1")),
        tableType = CatalogTableType.MANAGED,
        storage = CatalogStorageFormat.empty,
        schema = new StructType()
          .add("col1", "int")
          .add("partCol1", "int")
          .add("partCol2", "string")
          .add("col2", "string"),
        provider = Some(format),
        partitionColumnNames = Seq("partCol1", "partCol2"))
      catalog.createTable(table, ignoreIfExists = false)

      val restoredTable = externalCatalog.getTable("db1", "tbl")
      assert(restoredTable.schema == newSchema)
    }
  }

  test("SPARK-22306: alter table schema should not erase the bucketing metadata at hive side") {
    val catalog = newBasicCatalog()
    externalCatalog.client.runSqlHive(
      """
        |CREATE TABLE db1.t(a string, b string)
        |CLUSTERED BY (a, b) SORTED BY (a, b) INTO 10 BUCKETS
        |STORED AS PARQUET
      """.stripMargin)

    val newSchema = new StructType().add("a", "string").add("b", "string").add("c", "string")
    catalog.alterTableDataSchema("db1", "t", newSchema)

    assert(catalog.getTable("db1", "t").schema == newSchema)
    val bucketString = externalCatalog.client.runSqlHive("DESC FORMATTED db1.t")
      .filter(_.contains("Num Buckets")).head
    assert(bucketString.contains("10"))
  }

  test("SPARK-30050: analyze/rename table should not erase the bucketing metadata at hive side") {
    val catalog = newBasicCatalog()
    externalCatalog.client.runSqlHive(
      """
        |CREATE TABLE db1.t(a string, b string)
        |CLUSTERED BY (a, b) SORTED BY (a, b) INTO 10 BUCKETS
        |STORED AS PARQUET
      """.stripMargin)

    val bucketString1 = externalCatalog.client.runSqlHive("DESC FORMATTED db1.t")
      .filter(_.contains("Num Buckets")).head
    assert(bucketString1.contains("10"))

    catalog.alterTableStats("db1", "t", None)

    val bucketString2 = externalCatalog.client.runSqlHive("DESC FORMATTED db1.t")
      .filter(_.contains("Num Buckets")).head
    assert(bucketString2.contains("10"))

    catalog.renameTable("db1", "t", "t2")

    val bucketString3 = externalCatalog.client.runSqlHive("DESC FORMATTED db1.t2")
      .filter(_.contains("Num Buckets")).head
    assert(bucketString3.contains("10"))
  }

  test("SPARK-23001: NullPointerException when running desc database") {
    val catalog = newBasicCatalog()
    catalog.createDatabase(newDb("dbWithNullDesc").copy(description = null), ignoreIfExists = false)
    assert(catalog.getDatabase("dbWithNullDesc").description == "")
  }

  test("SPARK-29498 CatalogTable to HiveTable should not change the table's ownership") {
    val catalog = newBasicCatalog()
    val owner = "SPARK-29498"
    val hiveTable = CatalogTable(
      identifier = TableIdentifier("spark_29498", Some("db1")),
      tableType = CatalogTableType.MANAGED,
      storage = storageFormat,
      owner = owner,
      schema = new StructType().add("i", "int"),
      provider = Some("hive"))

    catalog.createTable(hiveTable, ignoreIfExists = false)
    assert(catalog.getTable("db1", "spark_29498").owner === owner)
  }
}
