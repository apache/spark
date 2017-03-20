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

import java.net.URI

import org.apache.hadoop.conf.Configuration

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.SimpleFunctionRegistry
import org.apache.spark.sql.catalyst.catalog.CatalogDatabase
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.catalyst.plans.logical.Range
import org.apache.spark.sql.hive.test.TestHiveSingleton
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.util.Utils

class HiveSessionCatalogSuite extends TestHiveSingleton {

  test("clone HiveSessionCatalog") {
    val original = spark.sessionState.catalog.asInstanceOf[HiveSessionCatalog]

    val tempTableName1 = "copytest1"
    val tempTableName2 = "copytest2"
    try {
      val tempTable1 = Range(1, 10, 1, 10)
      original.createTempView(tempTableName1, tempTable1, overrideIfExists = false)

      // check if tables copied over
      val clone = original.newSessionCatalogWith(
        spark,
        new SQLConf,
        new Configuration(),
        new SimpleFunctionRegistry,
        CatalystSqlParser)
      assert(original ne clone)
      assert(clone.getTempView(tempTableName1) == Some(tempTable1))

      // check if clone and original independent
      clone.dropTable(TableIdentifier(tempTableName1), ignoreIfNotExists = false, purge = false)
      assert(original.getTempView(tempTableName1) == Some(tempTable1))

      val tempTable2 = Range(1, 20, 2, 10)
      original.createTempView(tempTableName2, tempTable2, overrideIfExists = false)
      assert(clone.getTempView(tempTableName2).isEmpty)
    } finally {
      // Drop the created temp views from the global singleton HiveSession.
      original.dropTable(TableIdentifier(tempTableName1), ignoreIfNotExists = true, purge = true)
      original.dropTable(TableIdentifier(tempTableName2), ignoreIfNotExists = true, purge = true)
    }
  }

  test("clone SessionCatalog - current db") {
    val original = spark.sessionState.catalog.asInstanceOf[HiveSessionCatalog]
    val originalCurrentDatabase = original.getCurrentDatabase
    val db1 = "db1"
    val db2 = "db2"
    val db3 = "db3"
    try {
      original.createDatabase(newDb(db1), ignoreIfExists = true)
      original.createDatabase(newDb(db2), ignoreIfExists = true)
      original.createDatabase(newDb(db3), ignoreIfExists = true)

      original.setCurrentDatabase(db1)

      // check if tables copied over
      val clone = original.newSessionCatalogWith(
        spark,
        new SQLConf,
        new Configuration(),
        new SimpleFunctionRegistry,
        CatalystSqlParser)

      // check if current db copied over
      assert(original ne clone)
      assert(clone.getCurrentDatabase == db1)

      // check if clone and original independent
      clone.setCurrentDatabase(db2)
      assert(original.getCurrentDatabase == db1)
      original.setCurrentDatabase(db3)
      assert(clone.getCurrentDatabase == db2)
    } finally {
      // Drop the created databases from the global singleton HiveSession.
      original.dropDatabase(db1, ignoreIfNotExists = true, cascade = true)
      original.dropDatabase(db2, ignoreIfNotExists = true, cascade = true)
      original.dropDatabase(db3, ignoreIfNotExists = true, cascade = true)
      original.setCurrentDatabase(originalCurrentDatabase)
    }
  }

  def newUriForDatabase(): URI = new URI(Utils.createTempDir().toURI.toString.stripSuffix("/"))

  def newDb(name: String): CatalogDatabase = {
    CatalogDatabase(name, name + " description", newUriForDatabase(), Map.empty)
  }
}
