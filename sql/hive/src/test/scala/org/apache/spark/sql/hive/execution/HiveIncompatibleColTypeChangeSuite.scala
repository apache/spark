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

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.hive.test.TestHiveSingleton
import org.apache.spark.sql.types._

/**
 * A separate set of Hive DDL tests when setting
 * `hive.metastore.disallow.incompatible.col.type.changes=true`
 */
class HiveIncompatibleColTypeChangeSuite extends SparkFunSuite with TestHiveSingleton {

  private val catalog = spark.sessionState.catalog.externalCatalog

  override def beforeAll(): Unit = {
    super.beforeAll()
    hiveClient.runSqlHive(
      "SET hive.metastore.disallow.incompatible.col.type.changes=true")
  }

  override def afterEach(): Unit = {
    catalog.listTables("default").foreach { t =>
      catalog.dropTable("default", t, true, false)
    }
    spark.sessionState.catalog.reset()
  }

  override def afterAll(): Unit = {
    try {
      hiveClient.runSqlHive(
        "SET hive.metastore.disallow.incompatible.col.type.changes=false")
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
    checkError(
      exception = intercept[AnalysisException] {
        testAlterTable(
          "t1",
          "CREATE TABLE t1 (c1 string) USING parquet",
          StructType(Array(StructField("c2", IntegerType))))
      },
      condition = "_LEGACY_ERROR_TEMP_3065",
      parameters = Map(
        "clazz" -> "org.apache.hadoop.hive.ql.metadata.HiveException",
        "msg" -> ("Unable to alter table. " +
          "The following columns have types incompatible with the existing columns " +
          "in their respective positions :\ncol"))
    )
  }

  private def testAlterTable(
      tableName: String,
      createTableStmt: String,
      updatedSchema: StructType,
      hiveCompatible: Boolean = true): Unit = {
    spark.sql(createTableStmt)
    val oldTable = catalog.getTable("default", tableName)
    catalog.createTable(oldTable, true)
    catalog.alterTableSchema("default", tableName, updatedSchema)

    val updatedTable = catalog.getTable("default", tableName)
    assert(updatedTable.schema.fieldNames === updatedSchema.fieldNames)
  }

}
