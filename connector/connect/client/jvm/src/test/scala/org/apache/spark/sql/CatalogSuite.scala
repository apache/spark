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

package org.apache.spark.sql

import io.grpc.StatusRuntimeException

import org.apache.spark.sql.connect.client.util.RemoteSparkSession
import org.apache.spark.sql.types.{DoubleType, LongType, StructType}

class CatalogSuite extends RemoteSparkSession with SQLHelper {

  test("Database APIs") {
    val currentDb = spark.catalog.currentDatabase
    assert(currentDb == "default")
    withTempDatabase { db =>
      try {
        spark.catalog.setCurrentDatabase(db)
        assert(spark.catalog.currentDatabase == db)
        val dbs = spark.catalog.listDatabases().collect().sortBy(_.name)
        assert(dbs.length == 2)
        assert(dbs.map(_.name) sameElements Array(db, currentDb))
        assert(dbs.map(_.catalog).distinct sameElements Array("spark_catalog"))
        val database = spark.catalog.getDatabase(db)
        assert(database.name == db)
        val message = intercept[StatusRuntimeException] {
          spark.catalog.getDatabase("notExists")
        }.getMessage
        assert(message.contains("SCHEMA_NOT_FOUND"))
        assert(spark.catalog.databaseExists(db))
        assert(!spark.catalog.databaseExists("notExists"))
      } finally {
        spark.catalog.setCurrentDatabase(currentDb)
        assert(spark.catalog.currentDatabase == currentDb)
      }
    }
  }

  test("CatalogMetadata APIs") {
    val currentCatalog = spark.catalog.currentCatalog()
    assert(currentCatalog == "spark_catalog")
    try {
      spark.catalog.setCurrentCatalog("spark_catalog")
      val message = intercept[StatusRuntimeException] {
        spark.catalog.setCurrentCatalog("notExists")
      }.getMessage
      assert(message.contains("plugin class not found"))
      val catalogs = spark.catalog.listCatalogs().collect()
      assert(catalogs.length == 1)
      assert(catalogs.map(_.name) sameElements Array("spark_catalog"))
    } finally {
      spark.catalog.setCurrentCatalog(currentCatalog)
    }
  }

  test("Table APIs") {
    assert(spark.catalog.listTables().collect().isEmpty)
    val parquetTableName = "parquet_table"
    val orcTableName = "orc_table"
    val jsonTableName = "json_table"
    withTable(parquetTableName) {
      withTempPath { table1Dir =>
        val session = spark
        import session.implicits._
        val df1 = Seq("Bob", "Alice", "Nico", "Bob", "Alice").toDF("name")
        df1.write.parquet(table1Dir.getPath)
        spark.catalog.createTable(parquetTableName, table1Dir.getPath).collect()
        withTable(orcTableName, jsonTableName) {
          withTempPath { table2Dir =>
            val df2 = Seq("Bob", "Alice", "Nico", "Bob", "Alice").zipWithIndex.toDF("name", "id")
            df2.write.orc(table2Dir.getPath)
            spark.catalog.createTable(orcTableName, table2Dir.getPath, "orc").collect()
            val orcTable = spark.catalog.getTable(orcTableName)
            assert(!orcTable.isTemporary)
            assert(orcTable.name == orcTableName)
            assert(orcTable.tableType == "EXTERNAL")
            assert(
              spark.catalog.listColumns(orcTableName).collect().map(_.name).toSet == Set(
                "name",
                "id"))
          }
          val schema = new StructType().add("id", LongType).add("a", DoubleType)
          spark.catalog
            .createTable(jsonTableName, "json", schema, Map.empty[String, String])
            .collect()
          val jsonTable = spark.catalog.getTable(jsonTableName)
          assert(!jsonTable.isTemporary)
          assert(jsonTable.name == jsonTableName)
          assert(jsonTable.tableType == "MANAGED")
          assert(spark.catalog.tableExists(jsonTableName))
          assert(
            spark.catalog.listTables().collect().map(_.name).toSet == Set(
              parquetTableName,
              orcTableName,
              jsonTableName))
        }
        assert(spark.catalog.tableExists(parquetTableName))
        assert(!spark.catalog.tableExists(orcTableName))
        assert(!spark.catalog.tableExists(jsonTableName))
        assert(spark.catalog.listTables().collect().map(_.name).toSet == Set(parquetTableName))
      }
    }
    assert(spark.catalog.listTables().collect().isEmpty)
  }

}
