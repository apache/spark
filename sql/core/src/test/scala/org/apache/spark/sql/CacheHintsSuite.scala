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

import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.execution.CachedData
import org.apache.spark.sql.execution.analysis.ResolveCacheHints.NO_RESULT_CACHE_TAG
import org.apache.spark.sql.test.SharedSparkSession

class CacheHintsSuite extends PlanTest with SharedSparkSession {
  private def lookupCachedData(df: DataFrame): Option[CachedData] = {
    spark.sharedState.cacheManager.lookupCachedData(df)
  }

  test("RESULT_CACHE Hint") {
    val e = intercept[AnalysisException](
      sql("SELECT /*+ RESULT_CACHE(abc) */ id from range(0, 1)").collect())
    assert(e.getMessage.contains("RESULT_CACHE Hint does not accept any parameter"))
    val df = sql("SELECT /*+ RESULT_CACHE */ id from range(0, 1)")
    assert(lookupCachedData(df).nonEmpty)
    sql("SELECT * FROM range(2, 3) WHERE EXISTS (SELECT /*+ RESULT_CACHE */ id from range(1, 2))")
    assert(lookupCachedData(sql("SELECT id from range(1, 2)")).nonEmpty)
  }

  test("RESULT_UNCACHE Hint") {
    val e = intercept[AnalysisException](
      sql("SELECT /*+ RESULT_UNCACHE(abc) */ id from range(0, 1)").collect())
    assert(e.getMessage.contains("RESULT_UNCACHE Hint does not accept any parameter"))
    sql("SELECT /*+ RESULT_CACHE */ id from range(0, 1)")
    assert(lookupCachedData(sql("SELECT id from range(0, 1)")).nonEmpty)
    sql("SELECT /*+ RESULT_UNCACHE */ id from range(0, 1)")
    assert(lookupCachedData(sql("SELECT id from range(0, 1)")).isEmpty)

    sql("CACHE TABLE abc AS SELECT id from range(1, 2)")
    sql("SELECT /*+ RESULT_UNCACHE */ id from range(1, 2)")
    assert(!spark.catalog.isCached("abc"))
  }

  test("NO_RESULT_CACHE Hint") {
    val e = intercept[AnalysisException](
      sql("SELECT /*+ NO_RESULT_CACHE(abc) */ id from range(0, 1)").collect())
    assert(e.getMessage.contains("NO_RESULT_CACHE Hint does not accept any parameter"))
    val resultCache = sql("SELECT /*+ RESULT_CACHE */ id from range(0, 1)").logicalPlan
    assert(resultCache.getTagValue(NO_RESULT_CACHE_TAG).isEmpty)
    val noResultCache = sql("SELECT /*+ NO_RESULT_CACHE */ id from range(0, 1)").logicalPlan
    assert(noResultCache.getTagValue(NO_RESULT_CACHE_TAG).contains(true))
  }

  test("Unrecognized hints for ResolveCacheHints rule") {
    val logAppender = new LogAppender("RESULT_CACHE_UNKNOWN")

    withLogAppender(logAppender) {
      val analyzed =
        sql("SELECT /*+ RESULT_CACHE_UNKNOWN */ id from range(0, 1)").queryExecution.analyzed
    }
    assert(logAppender.loggingEvents.exists(
      _.getMessage.toString.contains("Unrecognized hint: RESULT_CACHE_UNKNOWN()")))

  }
}
