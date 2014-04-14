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
import org.apache.spark.sql.columnar.InMemoryColumnarTableScan
import org.apache.spark.sql.execution.SparkLogicalPlan
import org.apache.spark.sql.test.TestSQLContext

class CachedTableSuite extends QueryTest {
  TestData // Load test tables.

  test("read from cached table and uncache") {
    TestSQLContext.cacheTable("testData")

    checkAnswer(
      TestSQLContext.table("testData"),
      testData.collect().toSeq
    )

    TestSQLContext.table("testData").queryExecution.analyzed match {
      case SparkLogicalPlan(_ : InMemoryColumnarTableScan) => // Found evidence of caching
      case noCache => fail(s"No cache node found in plan $noCache")
    }

    TestSQLContext.uncacheTable("testData")

    checkAnswer(
      TestSQLContext.table("testData"),
      testData.collect().toSeq
    )

    TestSQLContext.table("testData").queryExecution.analyzed match {
      case cachePlan @ SparkLogicalPlan(_ : InMemoryColumnarTableScan) =>
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
    TestSQLContext.sql("SELECT * FROM testData").registerAsTable("selectStar")
    TestSQLContext.cacheTable("selectStar")
    TestSQLContext.sql("SELECT * FROM selectStar")
    TestSQLContext.uncacheTable("selectStar")
  }

  test("Self-join cached") {
    TestSQLContext.cacheTable("testData")
    TestSQLContext.sql("SELECT * FROM testData a JOIN testData b ON a.key = b.key")
    TestSQLContext.uncacheTable("testData")
  }
}
