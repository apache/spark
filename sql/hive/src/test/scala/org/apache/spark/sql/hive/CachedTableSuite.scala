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

import org.apache.spark.sql.{QueryTest, SchemaRDD}
import org.apache.spark.sql.columnar.{InMemoryRelation, InMemoryColumnarTableScan}
import org.apache.spark.sql.hive.test.TestHive

class CachedTableSuite extends QueryTest {
  import TestHive._

  /**
   * Throws a test failed exception when the number of cached tables differs from the expected
   * number.
   */
  def assertCached(query: SchemaRDD, numCachedTables: Int = 1): Unit = {
    val planWithCaching = query.queryExecution.withCachedData
    val cachedData = planWithCaching collect {
      case cached: InMemoryRelation => cached
    }

    if (cachedData.size != numCachedTables) {
      fail(
        s"Expected query to contain $numCachedTables, but it actually had ${cachedData.size}\n" +
          planWithCaching)
    }
  }

  test("cache table") {
    val preCacheResults = sql("SELECT * FROM src").collect().toSeq

    cacheTable("src")
    assertCached(sql("SELECT * FROM src"))

    checkAnswer(
      sql("SELECT * FROM src"),
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
    sql("CREATE TABLE test(a INT)")
    cacheTable("test")
    sql("SELECT * FROM test").collect()
    sql("DROP TABLE test")
    intercept[org.apache.hadoop.hive.ql.metadata.InvalidTableException] {
      sql("SELECT * FROM test").collect()
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

  test("CACHE TABLE AS SELECT") {
    assertCached(sql("SELECT * FROM src"), 0)
    sql("CACHE TABLE test AS SELECT key FROM src")

    checkAnswer(
      sql("SELECT * FROM test"),
      sql("SELECT key FROM src").collect().toSeq)

    assertCached(sql("SELECT * FROM test"))

    assertCached(sql("SELECT * FROM test JOIN test"), 2)
  }
}
