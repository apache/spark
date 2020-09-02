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

import scala.language.existentials

import org.apache.hadoop.conf.Configuration
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.launcher.SparkLauncher
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.hive.{HiveExternalCatalog, HiveUtils}
import org.apache.spark.sql.hive.test.TestHiveSingleton
import org.apache.spark.sql.internal.StaticSQLConf._
import org.apache.spark.sql.types._
import org.apache.spark.tags.{ExtendedHiveTest, SlowHiveTest}
import org.apache.spark.util.Utils

/**
 * A separate set of DDL tests that uses Hive 2.1 libraries, which behave a little differently
 * from the built-in ones.
 */
@SlowHiveTest
@ExtendedHiveTest
class Hive_2_1_DDLSuite extends SparkFunSuite with TestHiveSingleton with BeforeAndAfterEach
  with BeforeAndAfterAll {

  // Create a custom HiveExternalCatalog instance with the desired configuration. We cannot
  // use SparkSession here since there's already an active on managed by the TestHive object.
  private var catalog = {
    val warehouse = Utils.createTempDir()
    val metastore = Utils.createTempDir()
    metastore.delete()
    val sparkConf = new SparkConf()
      .set(SparkLauncher.SPARK_MASTER, "local")
      .set(WAREHOUSE_PATH.key, warehouse.toURI().toString())
      .set(CATALOG_IMPLEMENTATION.key, "hive")
      .set(HiveUtils.HIVE_METASTORE_VERSION.key, "2.1")
      .set(HiveUtils.HIVE_METASTORE_JARS.key, "maven")

    val hadoopConf = new Configuration()
    hadoopConf.set("hive.metastore.warehouse.dir", warehouse.toURI().toString())
    hadoopConf.set("javax.jdo.option.ConnectionURL",
      s"jdbc:derby:;databaseName=${metastore.getAbsolutePath()};create=true")
    // These options are needed since the defaults in Hive 2.1 cause exceptions with an
    // empty metastore db.
    hadoopConf.set("datanucleus.schema.autoCreateAll", "true")
    hadoopConf.set("hive.metastore.schema.verification", "false")

    new HiveExternalCatalog(sparkConf, hadoopConf)
  }

  override def afterEach: Unit = {
    catalog.listTables("default").foreach { t =>
      catalog.dropTable("default", t, true, false)
    }
    spark.sessionState.catalog.reset()
  }

  override def afterAll(): Unit = {
    try {
      catalog = null
    } finally {
      super.afterAll()
    }
  }

  test("SPARK-21617: ALTER TABLE for non-compatible DataSource tables") {
    testAlterTable(
      "t1",
      "CREATE TABLE t1 (c1 int) USING json",
      StructType(Array(StructField("c1", IntegerType), StructField("c2", IntegerType))),
      hiveCompatible = false)
  }

  test("SPARK-21617: ALTER TABLE for Hive-compatible DataSource tables") {
    testAlterTable(
      "t1",
      "CREATE TABLE t1 (c1 int) USING parquet",
      StructType(Array(StructField("c1", IntegerType), StructField("c2", IntegerType))))
  }

  test("SPARK-21617: ALTER TABLE for Hive tables") {
    testAlterTable(
      "t1",
      "CREATE TABLE t1 (c1 int) STORED AS parquet",
      StructType(Array(StructField("c1", IntegerType), StructField("c2", IntegerType))))
  }

  test("SPARK-21617: ALTER TABLE with incompatible schema on Hive-compatible table") {
    val exception = intercept[AnalysisException] {
      testAlterTable(
        "t1",
        "CREATE TABLE t1 (c1 string) USING parquet",
        StructType(Array(StructField("c2", IntegerType))))
    }
    assert(exception.getMessage().contains("types incompatible with the existing columns"))
  }

  private def testAlterTable(
      tableName: String,
      createTableStmt: String,
      updatedSchema: StructType,
      hiveCompatible: Boolean = true): Unit = {
    spark.sql(createTableStmt)
    val oldTable = spark.sessionState.catalog.externalCatalog.getTable("default", tableName)
    catalog.createTable(oldTable, true)
    catalog.alterTableDataSchema("default", tableName, updatedSchema)

    val updatedTable = catalog.getTable("default", tableName)
    assert(updatedTable.schema.fieldNames === updatedSchema.fieldNames)
  }

}
