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
}
