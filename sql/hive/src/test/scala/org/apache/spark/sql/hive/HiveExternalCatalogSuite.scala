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
import org.apache.spark.sql.catalyst.types.DataTypeUtils
import org.apache.spark.sql.execution.QueryExecutionException
import org.apache.spark.sql.execution.command.DDLUtils
import org.apache.spark.sql.types.{StringType, StructField, StructType}

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

  test("SPARK-30868 throw an exception if HiveClient#runSqlHive fails") {
    val client = externalCatalog.client
    // test add jars which doesn't exists
    val jarPath = "file:///tmp/not_exists.jar"
    assertThrows[QueryExecutionException](client.runSqlHive(s"ADD JAR $jarPath"))

    // test change to the database which doesn't exists
    assertThrows[QueryExecutionException](client.runSqlHive(
      s"use db_not_exists"))

    // test create hive table failed with unsupported into type
    assertThrows[QueryExecutionException](client.runSqlHive(
      s"CREATE TABLE t(n into)"))

    // test desc table failed with wrong `FORMATED` keyword
    assertThrows[QueryExecutionException](client.runSqlHive(
      s"DESC FORMATED t"))

    // test wrong insert query
    assertThrows[QueryExecutionException](client.runSqlHive(
      "INSERT overwrite directory \"fs://localhost/tmp\" select 1 as a"))
  }

  test("SPARK-31061: alterTable should be able to change table provider/hive") {
    val catalog = newBasicCatalog()
    Seq("parquet", "hive").foreach( provider => {
      val tableDDL = CatalogTable(
        identifier = TableIdentifier("parq_tbl", Some("db1")),
        tableType = CatalogTableType.MANAGED,
        storage = storageFormat,
        schema = new StructType().add("col1", "int"),
        provider = Some(provider))
      catalog.dropTable("db1", "parq_tbl", true, true)
      catalog.createTable(tableDDL, ignoreIfExists = false)

      val rawTable = externalCatalog.getTable("db1", "parq_tbl")
      assert(rawTable.provider === Some(provider))

      val fooTable = rawTable.copy(provider = Some("foo"))
      catalog.alterTable(fooTable)
      val alteredTable = externalCatalog.getTable("db1", "parq_tbl")
      assert(alteredTable.provider === Some("foo"))
    })
  }

  test("write collated strings as regular strings in hive - but read them back as collated") {
    val catalog = newBasicCatalog()
    val tableName = "collation_tbl"
    val columnName = "col1"

    val collationsSchema = StructType(Seq(
      StructField(columnName, StringType("UNICODE"))
    ))
    val noCollationsSchema = StructType(Seq(
      StructField(columnName, StringType)
    ))

    val tableDDL = CatalogTable(
      identifier = TableIdentifier(tableName, Some("db1")),
      tableType = CatalogTableType.MANAGED,
      storage = storageFormat,
      schema = collationsSchema,
      provider = Some("hive"))

    catalog.createTable(tableDDL, ignoreIfExists = false)

    val rawTable = externalCatalog.getRawTable("db1", tableName)
    assert(DataTypeUtils.sameType(rawTable.schema, noCollationsSchema))

    val readBackTable = externalCatalog.getTable("db1", tableName)
    assert(DataTypeUtils.sameType(readBackTable.schema, collationsSchema))

    // perform alter table
    val newSchema = StructType(Seq(
      StructField("col1", StringType("UTF8_LCASE"))
    ))
    catalog.alterTableDataSchema("db1", tableName, newSchema)

    val alteredRawTable = externalCatalog.getRawTable("db1", tableName)
    assert(DataTypeUtils.sameType(alteredRawTable.schema, noCollationsSchema))

    val alteredTable = externalCatalog.getTable("db1", tableName)
    assert(DataTypeUtils.sameType(alteredTable.schema, newSchema))
  }
}
