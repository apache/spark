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

import org.apache.spark.sql.execution.SparkLogicalPlan
import org.apache.spark.sql.columnar.{InMemoryRelation, InMemoryColumnarTableScan}
import org.apache.spark.sql.hive.execution.HiveComparisonTest
import org.apache.spark.sql.hive.test.TestHive

class CachedTableSuite extends HiveComparisonTest {
  import TestHive._

  TestHive.loadTestTable("src")

  test("cache table") {
    TestHive.cacheTable("src")
  }

  createQueryTest("read from cached table",
    "SELECT * FROM src LIMIT 1", reset = false)

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

  test("check that table is cached and uncache") {
    TestHive.table("src").queryExecution.analyzed match {
      case _ : InMemoryRelation => // Found evidence of caching
      case noCache => fail(s"No cache node found in plan $noCache")
    }
    TestHive.uncacheTable("src")
  }

  createQueryTest("read from uncached table",
    "SELECT * FROM src LIMIT 1", reset = false)

  test("make sure table is uncached") {
    TestHive.table("src").queryExecution.analyzed match {
      case cachePlan: InMemoryRelation =>
        fail(s"Table still cached after uncache: $cachePlan")
      case noCache => // Table uncached successfully
    }
  }

  test("correct error on uncache of non-cached table") {
    intercept[IllegalArgumentException] {
      TestHive.uncacheTable("src")
    }
  }

  test("'CACHE TABLE' and 'UNCACHE TABLE' HiveQL statement") {
    TestHive.sql("CACHE TABLE src")
    TestHive.table("src").queryExecution.executedPlan match {
      case _: InMemoryColumnarTableScan => // Found evidence of caching
      case _ => fail(s"Table 'src' should be cached")
    }
    assert(TestHive.isCached("src"), "Table 'src' should be cached")

    TestHive.sql("UNCACHE TABLE src")
    TestHive.table("src").queryExecution.executedPlan match {
      case _: InMemoryColumnarTableScan => fail(s"Table 'src' should not be cached")
      case _ => // Found evidence of uncaching
    }
    assert(!TestHive.isCached("src"), "Table 'src' should not be cached")
  }
}
