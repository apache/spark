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

import org.apache.spark.sql.{AnalysisException, QueryTest, SaveMode}
import org.apache.spark.sql.columnar.InMemoryColumnarTableScan
import org.apache.spark.sql.hive.test.SharedHiveContext
import org.apache.spark.storage.RDDBlockId
import org.apache.spark.util.Utils

class CachedTableSuite extends QueryTest with SharedHiveContext {

  def rddIdOf(tableName: String): Int = {
    val executedPlan = ctx.table(tableName).queryExecution.executedPlan
    executedPlan.collect {
      case InMemoryColumnarTableScan(_, _, relation) =>
        relation.cachedColumnBuffers.id
      case _ =>
        fail(s"Table $tableName is not cached\n" + executedPlan)
    }.head
  }

  def isMaterialized(rddId: Int): Boolean = {
    ctx.sparkContext.env.blockManager.get(RDDBlockId(rddId, 0)).nonEmpty
  }

  test("cache table") {
    val preCacheResults = ctx.sql("SELECT * FROM src").collect().toSeq

    ctx.cacheTable("src")
    assertCached(ctx.sql("SELECT * FROM src"))

    checkAnswer(
      ctx.sql("SELECT * FROM src"),
      preCacheResults)

    assertCached(ctx.sql("SELECT * FROM src s"))

    checkAnswer(
      ctx.sql("SELECT * FROM src s"),
      preCacheResults)

    ctx.uncacheTable("src")
    assertCached(ctx.sql("SELECT * FROM src"), 0)
  }

  test("cache invalidation") {
    ctx.sql("CREATE TABLE cachedTable(key INT, value STRING)")

    ctx.sql("INSERT INTO TABLE cachedTable SELECT * FROM src")
    checkAnswer(ctx.sql("SELECT * FROM cachedTable"), ctx.table("src").collect().toSeq)

    ctx.cacheTable("cachedTable")
    checkAnswer(ctx.sql("SELECT * FROM cachedTable"), ctx.table("src").collect().toSeq)

    ctx.sql("INSERT INTO TABLE cachedTable SELECT * FROM src")
    checkAnswer(
      ctx.sql("SELECT * FROM cachedTable"),
      ctx.table("src").collect().toSeq ++ ctx.table("src").collect().toSeq)

    ctx.sql("DROP TABLE cachedTable")
  }

  test("Drop cached table") {
    ctx.sql("CREATE TABLE cachedTableTest(a INT)")
    ctx.cacheTable("cachedTableTest")
    ctx.sql("SELECT * FROM cachedTableTest").collect()
    ctx.sql("DROP TABLE cachedTableTest")
    intercept[AnalysisException] {
      ctx.sql("SELECT * FROM cachedTableTest").collect()
    }
  }

  test("DROP nonexistant table") {
    ctx.sql("DROP TABLE IF EXISTS nonexistantTable")
  }

  test("correct error on uncache of non-cached table") {
    intercept[IllegalArgumentException] {
      ctx.uncacheTable("src")
    }
  }

  test("'CACHE TABLE' and 'UNCACHE TABLE' HiveQL statement") {
    ctx.sql("CACHE TABLE src")
    assertCached(ctx.table("src"))
    assert(ctx.isCached("src"), "Table 'src' should be cached")

    ctx.sql("UNCACHE TABLE src")
    assertCached(ctx.table("src"), 0)
    assert(!ctx.isCached("src"), "Table 'src' should not be cached")
  }

  test("CACHE TABLE tableName AS SELECT * FROM anotherTable") {
    ctx.sql("CACHE TABLE testCacheTable AS SELECT * FROM src")
    assertCached(ctx.table("testCacheTable"))

    val rddId = rddIdOf("testCacheTable")
    assert(
      isMaterialized(rddId),
      "Eagerly cached in-memory table should have already been materialized")

    ctx.uncacheTable("testCacheTable")
    assert(!isMaterialized(rddId), "Uncached in-memory table should have been unpersisted")
  }

  test("CACHE TABLE tableName AS SELECT ...") {
    ctx.sql("CACHE TABLE testCacheTable AS SELECT key FROM src LIMIT 10")
    assertCached(ctx.table("testCacheTable"))

    val rddId = rddIdOf("testCacheTable")
    assert(
      isMaterialized(rddId),
      "Eagerly cached in-memory table should have already been materialized")

    ctx.uncacheTable("testCacheTable")
    assert(!isMaterialized(rddId), "Uncached in-memory table should have been unpersisted")
  }

  test("CACHE LAZY TABLE tableName") {
    ctx.sql("CACHE LAZY TABLE src")
    assertCached(ctx.table("src"))

    val rddId = rddIdOf("src")
    assert(
      !isMaterialized(rddId),
      "Lazily cached in-memory table shouldn't be materialized eagerly")

    ctx.sql("SELECT COUNT(*) FROM src").collect()
    assert(
      isMaterialized(rddId),
      "Lazily cached in-memory table should have been materialized")

    ctx.uncacheTable("src")
    assert(!isMaterialized(rddId), "Uncached in-memory table should have been unpersisted")
  }

  test("CACHE TABLE with Hive UDF") {
    ctx.sql("CACHE TABLE udfTest AS SELECT * FROM src WHERE floor(key) = 1")
    assertCached(ctx.table("udfTest"))
    ctx.uncacheTable("udfTest")
  }

  test("REFRESH TABLE also needs to recache the data (data source tables)") {
    val tempPath: File = Utils.createTempDir()
    tempPath.delete()
    ctx.table("src").write.mode(SaveMode.Overwrite).parquet(tempPath.toString)
    ctx.sql("DROP TABLE IF EXISTS refreshTable")
    ctx.createExternalTable("refreshTable", tempPath.toString, "parquet")
    checkAnswer(
      ctx.table("refreshTable"),
      ctx.table("src").collect())
    // Cache the table.
    ctx.sql("CACHE TABLE refreshTable")
    assertCached(ctx.table("refreshTable"))
    // Append new data.
    ctx.table("src").write.mode(SaveMode.Append).parquet(tempPath.toString)
    // We are still using the old data.
    assertCached(ctx.table("refreshTable"))
    checkAnswer(
      ctx.table("refreshTable"),
      ctx.table("src").collect())
    // Refresh the table.
    ctx.sql("REFRESH TABLE refreshTable")
    // We are using the new data.
    assertCached(ctx.table("refreshTable"))
    checkAnswer(
      ctx.table("refreshTable"),
      ctx.table("src").unionAll(ctx.table("src")).collect())

    // Drop the table and create it again.
    ctx.sql("DROP TABLE refreshTable")
    ctx.createExternalTable("refreshTable", tempPath.toString, "parquet")
    // It is not cached.
    assert(!ctx.isCached("refreshTable"), "refreshTable should not be cached.")
    // Refresh the table. REFRESH TABLE command should not make a uncached
    // table cached.
    ctx.sql("REFRESH TABLE refreshTable")
    checkAnswer(
      ctx.table("refreshTable"),
      ctx.table("src").unionAll(ctx.table("src")).collect())
    // It is not cached.
    assert(!ctx.isCached("refreshTable"), "refreshTable should not be cached.")

    ctx.sql("DROP TABLE refreshTable")
    Utils.deleteRecursively(tempPath)
  }
}
