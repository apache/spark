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
import org.apache.spark.sql.test.TestSQLContext._

case class BigData(s: String)

class CachedTableSuite extends QueryTest {
  TestData // Load test tables.

  test("too big for memory") {
    val data = "*" * 10000
    sparkContext.parallelize(1 to 1000000, 1).map(_ => BigData(data)).registerTempTable("bigData")
    cacheTable("bigData")
    assert(table("bigData").count() === 1000000L)
    uncacheTable("bigData")
  }

  test("SPARK-1669: cacheTable should be idempotent") {
    assume(!table("testData").logicalPlan.isInstanceOf[InMemoryRelation])

    cacheTable("testData")
    table("testData").queryExecution.analyzed match {
      case _: InMemoryRelation =>
      case _ =>
        fail("testData should be cached")
    }

    cacheTable("testData")
    table("testData").queryExecution.analyzed match {
      case InMemoryRelation(_, _, _, _: InMemoryColumnarTableScan) =>
        fail("cacheTable is not idempotent")

      case _ =>
    }
  }

  test("read from cached table and uncache") {
    TestSQLContext.cacheTable("testData")

    checkAnswer(
      TestSQLContext.table("testData"),
      testData.collect().toSeq
    )

    TestSQLContext.table("testData").queryExecution.analyzed match {
      case _ : InMemoryRelation => // Found evidence of caching
      case noCache => fail(s"No cache node found in plan $noCache")
    }

    TestSQLContext.uncacheTable("testData")

    checkAnswer(
      TestSQLContext.table("testData"),
      testData.collect().toSeq
    )

    TestSQLContext.table("testData").queryExecution.analyzed match {
      case cachePlan: InMemoryRelation =>
        fail(s"Table still cached after uncache: $cachePlan")
      case noCache => // Table uncached successfully
    }
  }

  test("correct error on uncache of non-cached table") {
    intercept[IllegalArgumentException] {
      TestSQLContext.uncacheTable("testData")
    }
  }

  test("SELECT Star Cached Table") {
    TestSQLContext.sql("SELECT * FROM testData").registerTempTable("selectStar")
    TestSQLContext.cacheTable("selectStar")
    TestSQLContext.sql("SELECT * FROM selectStar WHERE key = 1").collect()
    TestSQLContext.uncacheTable("selectStar")
  }

  test("Self-join cached") {
    val unCachedAnswer =
      TestSQLContext.sql("SELECT * FROM testData a JOIN testData b ON a.key = b.key").collect()
    TestSQLContext.cacheTable("testData")
    checkAnswer(
      TestSQLContext.sql("SELECT * FROM testData a JOIN testData b ON a.key = b.key"),
      unCachedAnswer.toSeq)
    TestSQLContext.uncacheTable("testData")
  }

  test("'CACHE TABLE' and 'UNCACHE TABLE' SQL statement") {
    TestSQLContext.sql("CACHE TABLE testData")
    TestSQLContext.table("testData").queryExecution.executedPlan match {
      case _: InMemoryColumnarTableScan => // Found evidence of caching
      case _ => fail(s"Table 'testData' should be cached")
    }
    assert(TestSQLContext.isCached("testData"), "Table 'testData' should be cached")

    TestSQLContext.sql("UNCACHE TABLE testData")
    TestSQLContext.table("testData").queryExecution.executedPlan match {
      case _: InMemoryColumnarTableScan => fail(s"Table 'testData' should not be cached")
      case _ => // Found evidence of uncaching
    }
    assert(!TestSQLContext.isCached("testData"), "Table 'testData' should not be cached")
  }
}
