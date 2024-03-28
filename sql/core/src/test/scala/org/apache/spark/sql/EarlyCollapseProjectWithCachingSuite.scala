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

import org.apache.spark.sql.execution.analysis.EarlyCollapseProject
import org.apache.spark.sql.execution.columnar.{InMemoryRelation, InMemoryTableScanExec}
import org.apache.spark.sql.internal.SQLConf

class EarlyCollapseProjectWithCachingSuite extends EarlyCollapseProjectSuite {
  import testImplicits._
  override val useCaching: Boolean = true

  test("check for nested InMemoryRelations") {
    val baseDfCreator = () => spark.range(1000).select($"id" as "a", $"id" as "b").
      select($"a" + 1 as "c", $"a", $"b").filter($"a" > 4).filter($"c" * $"b" < 60)

    checkProjectCollapseAndCacheUse(baseDfCreator, df => df.filter($"b" < 100).
      select($"c" + $"a" as "c", $"a" + 3 as "a", $"b", $"c" + 7 as "d", $"a" - $"b" as "e"))

    // there is already a cached base Df
    val df1 = baseDfCreator().filter($"b" < 100).
      select($"c" + $"a" as "c", $"a" + 3 as "a", $"b", $"c" + 7 as "d", $"a" - $"b" as "e")
    df1.cache()

    val df2 = baseDfCreator().filter($"b" < 100).
      select($"c" + $"a" as "c", $"a" + 3 as "a", $"b", $"c" + 7 as "d", $"a" - $"b" as "e").
      select($"c" * $"a" as "c", $"c" * $"b" as "a", $"e").filter($"c" > 73).
      filter($"d" < 300)
    val rows = df2.collect()
    assert(rows.length > 0)
    // there should be 2 nested In Memory Relations
    val optimizedPlan = df2.queryExecution.optimizedPlan
    val leaf1 = optimizedPlan.collectLeaves().head
    assert(leaf1.isInstanceOf[InMemoryRelation])
    val imr1 = leaf1.asInstanceOf[InMemoryRelation]
    val leaf2 = imr1.queryExecution.optimizedPlan.collectLeaves().head
    assert(leaf2.isInstanceOf[InMemoryRelation])
    df1.unpersist()
    baseDfCreator().unpersist()
    val fullyUnopt = withSQLConf(
      SQLConf.EXCLUDE_POST_ANALYSIS_RULES.key -> EarlyCollapseProject.ruleName) {
      baseDfCreator().filter($"b" < 100).
        select($"c" + $"a" as "c", $"a" + 3 as "a", $"b", $"c" + 7 as "d", $"a" - $"b" as "e").
        select($"c" * $"a" as "c", $"c" * $"b" as "a", $"e").filter($"c" > 73).
        filter($"d" < 300)
    }
    checkAnswer(fullyUnopt, rows)
  }

  test("check cached plan invalidation when subplan is uncached") {
    val baseDf = spark.range(1000).select($"id" as "a", $"id" as "b").
      select($"a" + 1 as "c", $"a", $"b").filter($"a" > 4)
    val df1 = baseDf.withColumn("d", $"a" + 1 + $"b")
    baseDf.cache()
    // Add df1 to the CacheManager; the buffer is currently empty.
    df1.cache()
    assertCacheDependency(df1, 1)
    // removal of InMemoryRelation of base Df should result in the removal of dependency of df1
    baseDf.unpersist(blocking = true)
    assertCacheDependency(df1.limit(1000), 0)
  }


  private def assertCacheDependency(df: DataFrame, numOfCachesDependedUpon: Int = 1): Unit = {

    val cachedPlan = df.queryExecution.withCachedData.collectFirst {
      case i: InMemoryRelation => i.cacheBuilder.cachedPlan
    }
    assert(cachedPlan.isDefined)

    assert(find(cachedPlan.get)(_.isInstanceOf[InMemoryTableScanExec]).size
      == numOfCachesDependedUpon)
  }
}
