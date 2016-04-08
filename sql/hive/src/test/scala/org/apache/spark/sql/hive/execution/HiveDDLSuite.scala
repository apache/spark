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

import org.apache.hadoop.fs.Path
import org.scalatest.BeforeAndAfterEach

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.catalog.CatalogDatabase
import org.apache.spark.sql.hive.test.TestHiveSingleton
import org.apache.spark.sql.test.SQLTestUtils

class HiveDDLSuite
  extends QueryTest with SQLTestUtils with TestHiveSingleton with BeforeAndAfterEach {

  override def afterEach(): Unit = {
    try {
      // drop all databases, tables and functions after each test
      sqlContext.sessionState.catalog.reset()
    } finally {
      super.afterEach()
    }
  }

  test("create/drop database - checking directory") {
    val catalog = sqlContext.sessionState.catalog
    val dbName = "db1"
    val path = catalog.getDatabasePath(dbName, None)
    val dbPath = new Path(path)
    val fs = dbPath.getFileSystem(hiveContext.hiveconf)
    // the database directory does not exist
    assert (!fs.exists(dbPath))

    sql("CREATE DATABASE db1")
    val db1 = catalog.getDatabase(dbName)
    assert(db1 == CatalogDatabase(
      dbName,
      "",
      path,
      Map.empty))
    // the database directory was created
    assert (fs.exists(dbPath) && fs.isDirectory(dbPath))
    sql("DROP DATABASE db1")
    // the database directory was removed
    assert (!fs.exists(dbPath))
  }
}
