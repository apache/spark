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

import org.apache.spark.sql.{DataFrame, QueryTest}
import org.apache.spark.sql.catalyst.plans.physical.{RangePartitioning, UnknownPartitioning}
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanHelper, DisableAdaptiveExecutionSuite, EnableAdaptiveExecutionSuite}
import org.apache.spark.sql.execution.joins.ShuffledJoin
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession


abstract class RemoveRedundantSortsSuiteBase
    extends QueryTest
    with SharedSparkSession
    with AdaptiveSparkPlanHelper {
  import testImplicits._

  private def checkNumSorts(df: DataFrame, count: Int): Unit = {
    val plan = df.queryExecution.executedPlan
    assert(collectWithSubqueries(plan) { case s: SortExec => s }.length == count)
  }

  private def checkSorts(query: String, enabledCount: Int, disabledCount: Int): Unit = {
    withSQLConf(SQLConf.REMOVE_REDUNDANT_SORTS_ENABLED.key -> "true") {
      val df = sql(query)
      checkNumSorts(df, enabledCount)
      val result = df.collect()
      withSQLConf(SQLConf.REMOVE_REDUNDANT_SORTS_ENABLED.key -> "false") {
        val df = sql(query)
        checkNumSorts(df, disabledCount)
        checkAnswer(df, result)
      }
    }
  }

  test("remove redundant sorts with limit") {
    withTempView("t") {
      spark.range(100).select($"id" as "key").createOrReplaceTempView("t")
      val query =
        """
          |SELECT key FROM
          | (SELECT key FROM t WHERE key > 10 ORDER BY key DESC LIMIT 10)
          |ORDER BY key DESC
          |""".stripMargin
      checkSorts(query, 0, 1)
    }
  }

  test("remove redundant sorts with broadcast hash join") {
    withTempView("t1", "t2") {
      spark.range(1000).select($"id" as "key").createOrReplaceTempView("t1")
      spark.range(1000).select($"id" as "key").createOrReplaceTempView("t2")

      val queryTemplate = """
        |SELECT /*+ BROADCAST(%s) */ t1.key FROM
        | (SELECT key FROM t1 WHERE key > 10 ORDER BY key DESC LIMIT 10) t1
        |JOIN
        | (SELECT key FROM t2 WHERE key > 50 ORDER BY key DESC LIMIT 100) t2
        |ON t1.key = t2.key
        |ORDER BY %s
      """.stripMargin

      // No sort should be removed since the stream side (t2) order DESC
      // does not satisfy the required sort order ASC.
      val buildLeftOrderByRightAsc = queryTemplate.format("t1", "t2.key ASC")
      checkSorts(buildLeftOrderByRightAsc, 1, 1)

      // The top sort node should be removed since the stream side (t2) order DESC already
      // satisfies the required sort order DESC.
      val buildLeftOrderByRightDesc = queryTemplate.format("t1", "t2.key DESC")
      checkSorts(buildLeftOrderByRightDesc, 0, 1)

      // No sort should be removed since the sort ordering from broadcast-hash join is based
      // on the stream side (t2) and the required sort order is from t1.
      val buildLeftOrderByLeftDesc = queryTemplate.format("t1", "t1.key DESC")
      checkSorts(buildLeftOrderByLeftDesc, 1, 1)

      // The top sort node should be removed since the stream side (t1) order DESC already
      // satisfies the required sort order DESC.
      val buildRightOrderByLeftDesc = queryTemplate.format("t2", "t1.key DESC")
      checkSorts(buildRightOrderByLeftDesc, 0, 1)
    }
  }

  test("remove redundant sorts with sort merge join") {
    withTempView("t1", "t2") {
      spark.range(1000).select($"id" as "key").createOrReplaceTempView("t1")
      spark.range(1000).select($"id" as "key").createOrReplaceTempView("t2")
      val query = """
        |SELECT /*+ MERGE(t1) */ t1.key FROM
        | (SELECT key FROM t1 WHERE key > 10 ORDER BY key DESC LIMIT 10) t1
        |JOIN
        | (SELECT key FROM t2 WHERE key > 50 ORDER BY key DESC LIMIT 100) t2
        |ON t1.key = t2.key
        |ORDER BY t1.key
      """.stripMargin

      val queryAsc = query + " ASC"
      checkSorts(queryAsc, 2, 3)

      // The top level sort should not be removed since the child output ordering is ASC and
      // the required ordering is DESC.
      val queryDesc = query + " DESC"
      checkSorts(queryDesc, 3, 3)
    }
  }

  test("cached sorted data doesn't need to be re-sorted") {
    withSQLConf(SQLConf.REMOVE_REDUNDANT_SORTS_ENABLED.key -> "true") {
      val df = spark.range(1000).select($"id" as "key").sort($"key".desc).cache()
      df.collect()
      val resorted = df.sort($"key".desc)
      val sortedAsc = df.sort($"key".asc)
      checkNumSorts(df, 0)
      checkNumSorts(resorted, 0)
      checkNumSorts(sortedAsc, 1)
      val result = resorted.collect()
      withSQLConf(SQLConf.REMOVE_REDUNDANT_SORTS_ENABLED.key -> "false") {
        val resorted = df.sort($"key".desc)
        resorted.collect()
        checkNumSorts(resorted, 1)
        checkAnswer(resorted, result)
      }
    }
  }

  test("SPARK-33472: shuffled join with different left and right side partition numbers") {
    withTempView("t1", "t2") {
      spark.range(0, 100, 1, 2).select($"id" as "key").createOrReplaceTempView("t1")
      (0 to 100).toDF("key").createOrReplaceTempView("t2")

      val queryTemplate = """
        |SELECT /*+ %s(t1) */ t1.key
        |FROM t1 JOIN t2 ON t1.key = t2.key
        |WHERE t1.key > 10 AND t2.key < 50
        |ORDER BY t1.key ASC
      """.stripMargin

      Seq(("MERGE", 3), ("SHUFFLE_HASH", 1)).foreach { case (hint, count) =>
        val query = queryTemplate.format(hint)
        val df = sql(query)
        val sparkPlan = df.queryExecution.sparkPlan
        val join = sparkPlan.collect { case j: ShuffledJoin => j }.head
        val leftPartitioning = join.left.outputPartitioning
        assert(leftPartitioning.isInstanceOf[RangePartitioning])
        assert(leftPartitioning.numPartitions == 2)
        assert(join.right.outputPartitioning == UnknownPartitioning(0))
        checkSorts(query, count, count)
      }
    }
  }
}

class RemoveRedundantSortsSuite extends RemoveRedundantSortsSuiteBase
  with DisableAdaptiveExecutionSuite

class RemoveRedundantSortsSuiteAE extends RemoveRedundantSortsSuiteBase
  with EnableAdaptiveExecutionSuite
