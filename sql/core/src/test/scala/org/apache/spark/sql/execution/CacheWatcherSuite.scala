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
package org.apache.spark.sql.execution

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.columnar.{InMemoryRelation, InMemoryTableScanExec}
import org.apache.spark.sql.test.SharedSQLContext

class CacheWatcherSuite extends QueryTest with SharedSQLContext {
  import testImplicits._

  private def assertCachedPlan(plan: LogicalPlan, numCachedTables: Int = 1): Unit = {
    val cachedData = plan.collect {
      case cached: InMemoryRelation => cached
    }
    assert(cachedData.size == numCachedTables)
  }

  private def assertCachedExecPlan(plan: SparkPlan, numCachedTables: Int = 1): Unit = {
    val cachedData = plan.collect {
      case cached: InMemoryTableScanExec => cached
    }
    assert(cachedData.size == numCachedTables)
  }

  test("Query plans with cached/uncached nodes after persist/unpersist") {
    withTable("t") {
      Seq("1", "2").toDF().write.saveAsTable("t")
      val ds1 = spark.table("t")
      val ds2 = spark.table("t")

      val watcher1 = ds1.queryExecution.cacheWatcher
      val watcher2 = ds1.queryExecution.cacheWatcher

      assertCachedPlan(watcher1.withCachedData, 0)
      assertCachedPlan(watcher2.withCachedData, 0)
      assertCachedExecPlan(watcher1.executedPlan, 0)
      assertCachedExecPlan(watcher2.executedPlan, 0)
      assertCached(ds1, 0)
      assertCached(ds2, 0)

      ds1.persist()
      assertCachedPlan(watcher1.withCachedData, 1)
      assertCachedPlan(watcher2.withCachedData, 1)
      assertCachedExecPlan(watcher1.executedPlan, 1)
      assertCachedExecPlan(watcher2.executedPlan, 1)
      assertCached(ds1, 1)
      assertCached(ds2, 1)

      ds2.unpersist()
      assertCachedPlan(watcher1.withCachedData, 0)
      assertCachedPlan(watcher2.withCachedData, 0)
      assertCachedExecPlan(watcher1.executedPlan, 0)
      assertCachedExecPlan(watcher2.executedPlan, 0)
      assertCached(ds1, 0)
      assertCached(ds2, 0)
    }
  }
}
