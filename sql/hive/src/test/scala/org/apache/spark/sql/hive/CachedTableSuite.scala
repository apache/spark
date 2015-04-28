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

import org.apache.spark.sql.columnar.{InMemoryColumnarTableScan, InMemoryRelation}
import org.apache.spark.sql.hive.test.TestHive
import org.apache.spark.sql.hive.test.TestHive._
import org.apache.spark.sql.{SaveMode, AnalysisException, DataFrame, QueryTest}
import org.apache.spark.storage.RDDBlockId
import org.apache.spark.util.Utils

class CachedTableSuite extends QueryTest {

  def rddIdOf(tableName: String): Int = {
    val executedPlan = table(tableName).queryExecution.executedPlan
    executedPlan.collect {
      case InMemoryColumnarTableScan(_, _, relation) =>
        relation.cachedColumnBuffers.id
      case _ =>
        fail(s"Table $tableName is not cached\n" + executedPlan)
    }.head
  }

  def isMaterialized(rddId: Int): Boolean = {
    sparkContext.env.blockManager.get(RDDBlockId(rddId, 0)).nonEmpty
  }

  test("cache table") {
    val preCacheResults = sql("SELECT * FROM src").collect().toSeq

    cacheTable("src")
    assertCached(sql("SELECT * FROM src"))

    checkAnswer(
      sql("SELECT * FROM src"),
      preCacheResults)

    assertCached(sql("SELECT * FROM src s"))

    checkAnswer(
      sql("SELECT * FROM src s"),
      preCacheResults)
    
    uncacheTable("src")
    assertCached(sql("SELECT * FROM src"), 0)
  }

  test("cache invalidation") {
    sql("CREATE TABLE cachedTable(key INT, value STRING)")

    sql("INSERT INTO TABLE cachedTable SELECT * FROM src")
    checkAnswer(sql("SELECT * FROM cachedTable"), table("src").collect().toSeq)

    cacheTable("cachedTable")
    checkAnswer(sql("SELECT * FROM cachedTable"), table("src").collect().toSeq)

    sql("INSERT INTO TABLE cachedTable SELECT * FROM src")
    checkAnswer(
      sql("SELECT * FROM cachedTable"),
      table("src").collect().toSeq ++ table("src").collect().toSeq)

    sql("DROP TABLE cachedTable")
  }

  test("Drop cached table") {
    sql("CREATE TABLE cachedTableTest(a INT)")
    cacheTable("cachedTableTest")
    sql("SELECT * FROM cachedTableTest").collect()
    sql("DROP TABLE cachedTableTest")
    intercept[AnalysisException] {
      sql("SELECT * FROM cachedTableTest").collect()
    }
  }

  test("DROP nonexistant table") {
    sql("DROP TABLE IF EXISTS nonexistantTable")
  }

  test("correct error on uncache of non-cached table") {
    intercept[IllegalArgumentException] {
      TestHive.uncacheTable("src")
    }
  }

  test("'CACHE TABLE' and 'UNCACHE TABLE' HiveQL statement") {
    TestHive.sql("CACHE TABLE src")
    assertCached(table("src"))
    assert(TestHive.isCached("src"), "Table 'src' should be cached")

    TestHive.sql("UNCACHE TABLE src")
    assertCached(table("src"), 0)
    assert(!TestHive.isCached("src"), "Table 'src' should not be cached")
  }

  test("CACHE TABLE tableName AS SELECT * FROM anotherTable") {
    sql("CACHE TABLE testCacheTable AS SELECT * FROM src")
    assertCached(table("testCacheTable"))

    val rddId = rddIdOf("testCacheTable")
    assert(
      isMaterialized(rddId),
      "Eagerly cached in-memory table should have already been materialized")

    uncacheTable("testCacheTable")
    assert(!isMaterialized(rddId), "Uncached in-memory table should have been unpersisted")
  }

  test("CACHE TABLE tableName AS SELECT ...") {
    sql("CACHE TABLE testCacheTable AS SELECT key FROM src LIMIT 10")
    assertCached(table("testCacheTable"))

    val rddId = rddIdOf("testCacheTable")
    assert(
      isMaterialized(rddId),
      "Eagerly cached in-memory table should have already been materialized")

    uncacheTable("testCacheTable")
    assert(!isMaterialized(rddId), "Uncached in-memory table should have been unpersisted")
  }

  test("CACHE LAZY TABLE tableName") {
    sql("CACHE LAZY TABLE src")
    assertCached(table("src"))

    val rddId = rddIdOf("src")
    assert(
      !isMaterialized(rddId),
      "Lazily cached in-memory table shouldn't be materialized eagerly")

    sql("SELECT COUNT(*) FROM src").collect()
    assert(
      isMaterialized(rddId),
      "Lazily cached in-memory table should have been materialized")

    uncacheTable("src")
    assert(!isMaterialized(rddId), "Uncached in-memory table should have been unpersisted")
  }

  test("CACHE TABLE with Hive UDF") {
    sql("CACHE TABLE udfTest AS SELECT * FROM src WHERE floor(key) = 1")
    assertCached(table("udfTest"))
    uncacheTable("udfTest")
  }

  test("REFRESH TABLE also needs to recache the data (data source tables)") {
    val tempPath: File = Utils.createTempDir()
    tempPath.delete()
    table("src").save(tempPath.toString, "parquet", SaveMode.Overwrite)
    sql("DROP TABLE IF EXISTS refreshTable")
    createExternalTable("refreshTable", tempPath.toString, "parquet")
    checkAnswer(
      table("refreshTable"),
      table("src").collect())
    // Cache the table.
    sql("CACHE TABLE refreshTable")
    assertCached(table("refreshTable"))
    // Append new data.
    table("src").save(tempPath.toString, "parquet", SaveMode.Append)
    // We are still using the old data.
    assertCached(table("refreshTable"))
    checkAnswer(
      table("refreshTable"),
      table("src").collect())
    // Refresh the table.
    sql("REFRESH TABLE refreshTable")
    // We are using the new data.
    assertCached(table("refreshTable"))
    checkAnswer(
      table("refreshTable"),
      table("src").unionAll(table("src")).collect())

    // Drop the table and create it again.
    sql("DROP TABLE refreshTable")
    createExternalTable("refreshTable", tempPath.toString, "parquet")
    // It is not cached.
    assert(!isCached("refreshTable"), "refreshTable should not be cached.")
    // Refresh the table. REFRESH TABLE command should not make a uncached
    // table cached.
    sql("REFRESH TABLE refreshTable")
    checkAnswer(
      table("refreshTable"),
      table("src").unionAll(table("src")).collect())
    // It is not cached.
    assert(!isCached("refreshTable"), "refreshTable should not be cached.")

    sql("DROP TABLE refreshTable")
    Utils.deleteRecursively(tempPath)
  }
}
