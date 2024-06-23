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

import java.io.{File, FilenameFilter}

import org.apache.commons.io.FileUtils

import org.apache.spark.SparkException
import org.apache.spark.sql.test.{ConnectFunSuite, RemoteSparkSession, SQLHelper}
import org.apache.spark.sql.types.{DoubleType, LongType, StructType}
import org.apache.spark.storage.StorageLevel

class CatalogSuite extends ConnectFunSuite with RemoteSparkSession with SQLHelper {

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
        var databasesWithPattern = spark.catalog.listDatabases("def*").collect().sortBy(_.name)
        assert(databasesWithPattern.length == 1)
        assert(databasesWithPattern.map(_.name) sameElements Array(currentDb))
        databasesWithPattern = spark.catalog.listDatabases("def2*").collect().sortBy(_.name)
        assert(databasesWithPattern.length == 0)
        val database = spark.catalog.getDatabase(db)
        assert(database.name == db)
        val message = intercept[AnalysisException] {
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
      val catalogs = spark.catalog.listCatalogs().collect()
      assert(catalogs.length == 1)
      assert(catalogs.map(_.name) sameElements Array("spark_catalog"))
      val exception = intercept[SparkException] {
        spark.catalog.setCurrentCatalog("notExists")
      }
      assert(exception.getErrorClass == "CATALOG_NOT_FOUND")
      spark.catalog.setCurrentCatalog("testcat")
      assert(spark.catalog.currentCatalog().equals("testcat"))
      val catalogsAfterChange = spark.catalog.listCatalogs().collect()
      assert(catalogsAfterChange.length == 2)
      assert(catalogsAfterChange.map(_.name).toSet == Set("testcat", "spark_catalog"))
      var catalogsWithPattern = spark.catalog.listCatalogs("spark*").collect()
      assert(catalogsWithPattern.length == 1)
      assert(catalogsWithPattern.map(_.name) sameElements Array("spark_catalog"))
      catalogsWithPattern = spark.catalog.listCatalogs("hive*").collect()
      assert(catalogsWithPattern.length == 0)
    } finally {
      spark.catalog.setCurrentCatalog(currentCatalog)
      assert(spark.catalog.currentCatalog() == "spark_catalog")
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
          val jsonTable = spark.catalog.getTable("default", jsonTableName)
          assert(!jsonTable.isTemporary)
          assert(jsonTable.name == jsonTableName)
          assert(jsonTable.tableType == "MANAGED")
          assert(spark.catalog.tableExists(jsonTableName))
          assert(
            spark.catalog.listTables().collect().map(_.name).toSet == Set(
              parquetTableName,
              orcTableName,
              jsonTableName))
          assert(
            spark.catalog
              .listTables(spark.catalog.currentDatabase, "par*")
              .collect()
              .map(_.name)
              .toSet == Set(parquetTableName))
          assert(
            spark.catalog.listTables(spark.catalog.currentDatabase, "txt*").collect().isEmpty)
        }
        assert(spark.catalog.tableExists(parquetTableName))
        assert(!spark.catalog.tableExists(orcTableName))
        assert(!spark.catalog.tableExists(jsonTableName))
        assert(spark.catalog.listTables().collect().map(_.name).toSet == Set(parquetTableName))
      }
    }
    val message = intercept[AnalysisException] {
      spark.catalog.getTable(parquetTableName)
    }.getMessage
    assert(message.contains("TABLE_OR_VIEW_NOT_FOUND"))

    assert(spark.catalog.listTables().collect().isEmpty)
  }

  test("Cache Table APIs") {
    val parquetTableName = "parquet_table"
    withTable(parquetTableName) {
      withTempPath { table1Dir =>
        val session = spark
        import session.implicits._
        val df1 = Seq("Bob", "Alice", "Nico", "Bob", "Alice").toDF("name")
        df1.write.parquet(table1Dir.getPath)
        spark.catalog.createTable(parquetTableName, table1Dir.getPath).collect()

        // Test cache and uncacheTable
        spark.catalog.cacheTable(parquetTableName)
        assert(spark.catalog.isCached(parquetTableName))
        spark.catalog.uncacheTable(parquetTableName)
        assert(!spark.catalog.isCached(parquetTableName))

        // Test cache with `StorageLevel` and clearCache
        spark.catalog.cacheTable(parquetTableName, StorageLevel.MEMORY_ONLY)
        assert(spark.catalog.isCached(parquetTableName))
        spark.catalog.clearCache()
        assert(!spark.catalog.isCached(parquetTableName))
      }
    }
  }

  test("TempView APIs") {
    val viewName = "view1"
    val globalViewName = "g_view1"
    try {
      spark.range(100).createTempView(viewName)
      val view = spark.catalog.getTable(viewName)
      assert(view.name == viewName)
      assert(view.isTemporary)
      assert(view.tableType == "TEMPORARY")
      spark.range(100).createGlobalTempView(globalViewName)
      val globalView = spark.catalog.getTable(s"global_temp.$globalViewName")
      assert(globalView.name == globalViewName)
      assert(globalView.isTemporary)
      assert(globalView.tableType == "TEMPORARY")
    } finally {
      spark.catalog.dropTempView(viewName)
      spark.catalog.dropGlobalTempView(globalViewName)
      assert(!spark.catalog.tableExists(viewName))
      assert(!spark.catalog.tableExists(globalViewName))
    }
  }

  test("Function API") {
    val dbName = spark.catalog.currentDatabase
    val functions1 = spark.catalog.listFunctions().collect()
    assert(functions1.nonEmpty)
    val functions2 = spark.catalog.listFunctions(dbName).collect()
    assert(functions1.map(_.name) sameElements functions2.map(_.name))
    val absFunctionName = "abs"
    assert(spark.catalog.functionExists(absFunctionName))
    assert(spark.catalog.getFunction(absFunctionName).name == absFunctionName)
    val notExistsFunction = "notExists"
    assert(!spark.catalog.functionExists(notExistsFunction))
    val message = intercept[AnalysisException] {
      spark.catalog.getFunction(notExistsFunction)
    }.getMessage
    assert(message.contains("UNRESOLVED_ROUTINE"))

    val functionsWithPattern1 = spark.catalog.listFunctions(dbName, "to*").collect()
    assert(functionsWithPattern1.nonEmpty)
    assert(functionsWithPattern1.exists(f => f.name == "to_date"))
    val functionsWithPattern2 =
      spark.catalog.listFunctions(dbName, "*not_existing_func*").collect()
    assert(functionsWithPattern2.isEmpty)
  }

  test("recoverPartitions") {
    val tableName = "test"
    withTable(tableName) {
      withTempPath { dir =>
        spark
          .range(5)
          .selectExpr("id as fieldOne", "id as partCol")
          .write
          .partitionBy("partCol")
          .mode("overwrite")
          .save(dir.getAbsolutePath)
        spark.sql(s"""
             |create table $tableName (fieldOne long, partCol int)
             |using parquet
             |options (path "${dir.toURI}")
             |partitioned by (partCol)""".stripMargin)
        spark.sql(s"show partitions $tableName").count()
        assert(spark.sql(s"select * from $tableName").count() == 0)
        spark.catalog.recoverPartitions(tableName)
        assert(spark.sql(s"select * from $tableName").count() == 5)
      }
    }
  }

  test("refreshTable") {
    withTempPath { dir =>
      val tableName = "spark_catalog.default.my_table"
      withTable(tableName) {
        try {
          spark.sql(s"""
               | CREATE TABLE $tableName(col STRING) USING TEXT
               | LOCATION '${dir.getAbsolutePath}'
               |""".stripMargin)
          spark.sql(s"""INSERT INTO $tableName SELECT 'abc'""".stripMargin)
          spark.catalog.cacheTable(tableName)
          assert(spark.table(tableName).collect().length == 1)

          FileUtils.deleteDirectory(dir)
          assert(spark.table(tableName).collect().length == 1)

          spark.catalog.refreshTable(tableName)
          assert(spark.table(tableName).collect().length == 0)
        } finally {
          spark.catalog.clearCache()
        }
      }
    }
  }

  test("refreshByPath") {
    withTempPath { dir =>
      val tableName = "spark_catalog.default.my_table"
      withTable(tableName) {
        try {
          spark.sql(s"""
               | CREATE TABLE $tableName(col STRING) USING TEXT
               | LOCATION '${dir.getAbsolutePath}'
               |""".stripMargin)
          spark.sql(s"""INSERT INTO $tableName SELECT 'abc'""".stripMargin)
          spark.catalog.cacheTable(tableName)
          assert(spark.table(tableName).collect().length == 1)

          // delete one
          new File(dir.getAbsolutePath)
            .listFiles(new FilenameFilter() {
              override def accept(dir: File, name: String): Boolean = name.endsWith(".txt")
            })
            .head
            .delete()

          assert(spark.table(tableName).collect().length == 1)

          spark.catalog.refreshByPath(dir.getAbsolutePath)
          assert(spark.table(tableName).collect().length == 0)
        } finally {
          spark.catalog.clearCache()
        }
      }
    }
  }
}
