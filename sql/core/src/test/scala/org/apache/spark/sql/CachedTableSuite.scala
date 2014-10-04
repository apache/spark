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

import org.apache.spark.sql.TestData._
import org.apache.spark.sql.columnar.{InMemoryRelation, InMemoryColumnarTableScan}
import org.apache.spark.sql.test.TestSQLContext

case class BigData(s: String)

class CachedTableSuite extends QueryTest {
  import TestSQLContext._
  TestData // Load test tables.

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

  test("too big for memory") {
    val data = "*" * 10000
    sparkContext.parallelize(1 to 1000000, 1).map(_ => BigData(data)).registerTempTable("bigData")
    cacheTable("bigData")
    assert(table("bigData").count() === 1000000L)
    uncacheTable("bigData")
  }

  test("calling .cache() should use inmemory columnar caching") {
    table("testData").cache()

    assertCached(table("testData"))
  }

  test("SPARK-1669: cacheTable should be idempotent") {
    assume(!table("testData").logicalPlan.isInstanceOf[InMemoryRelation])

    cacheTable("testData")
    assertCached(table("testData"))

    cacheTable("testData")
    table("testData").queryExecution.analyzed match {
      case InMemoryRelation(_, _, _, _, _: InMemoryColumnarTableScan) =>
        fail("cacheTable is not idempotent")

      case _ =>
    }
  }

  test("read from cached table and uncache") {
    cacheTable("testData")

    checkAnswer(
      table("testData"),
      testData.collect().toSeq
    )

    assertCached(table("testData"))

    uncacheTable("testData")

    checkAnswer(
      table("testData"),
      testData.collect().toSeq
    )

    assertCached(table("testData"), 0)
  }

  test("correct error on uncache of non-cached table") {
    intercept[IllegalArgumentException] {
      uncacheTable("testData")
    }
  }

  test("SELECT Star Cached Table") {
    sql("SELECT * FROM testData").registerTempTable("selectStar")
    cacheTable("selectStar")
    sql("SELECT * FROM selectStar WHERE key = 1").collect()
    uncacheTable("selectStar")
  }

  test("Self-join cached") {
    val unCachedAnswer =
      sql("SELECT * FROM testData a JOIN testData b ON a.key = b.key").collect()
    cacheTable("testData")
    checkAnswer(
      sql("SELECT * FROM testData a JOIN testData b ON a.key = b.key"),
      unCachedAnswer.toSeq)
    uncacheTable("testData")
  }

  test("'CACHE TABLE' and 'UNCACHE TABLE' SQL statement") {
    sql("CACHE TABLE testData")
    assertCached(table("testData"))

    assert(isCached("testData"), "Table 'testData' should be cached")

    sql("UNCACHE TABLE testData")
    assertCached(table("testData"), 0)
    assert(!isCached("testData"), "Table 'testData' should not be cached")
  }
  
  test("CACHE TABLE tableName AS SELECT Star Table") {
    sql("CACHE TABLE testCacheTable AS SELECT * FROM testData")
    sql("SELECT * FROM testCacheTable WHERE key = 1").collect()
    assert(isCached("testCacheTable"), "Table 'testCacheTable' should be cached")
    uncacheTable("testCacheTable")
  }
  
  test("'CACHE TABLE tableName AS SELECT ..'") {
    sql("CACHE TABLE testCacheTable AS SELECT * FROM testData")
    assert(isCached("testCacheTable"), "Table 'testCacheTable' should be cached")
    uncacheTable("testCacheTable")
  }
}
