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

import scala.reflect.ClassTag

import org.apache.spark.sql.{DataFrame, QueryTest}
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanHelper, DisableAdaptiveExecutionSuite, EnableAdaptiveExecutionSuite}
import org.apache.spark.sql.execution.aggregate.{HashAggregateExec, ObjectHashAggregateExec, SortAggregateExec}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

abstract class CollapseAggregatesSuiteBase
  extends QueryTest
    with SharedSparkSession
    with AdaptiveSparkPlanHelper {

  import testImplicits._

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark.sparkContext.parallelize(1 to 100, 10).toDF("id")
      .selectExpr("id as col1", "id % 10 as col2").createOrReplaceTempView("t1")
    spark.sparkContext.parallelize(1 to 100, 10).toDF("id")
      .selectExpr("id as col1", "id % 10 as col2").createOrReplaceTempView("t2")
  }

  override def afterAll(): Unit = {
    try spark.catalog.dropTempView("t1") catch {
      case _: NoSuchTableException =>
    }
    try spark.catalog.dropTempView("t2") catch {
      case _: NoSuchTableException =>
    }
    super.afterAll()
  }

  private def assertAggregateExecCount[T: ClassTag](df: DataFrame, expected: Int) = {
    withClue(df.queryExecution) {
      val plan = df.queryExecution.executedPlan
      val actual = collectWithSubqueries(plan) { case agg: T => agg }.size
      assert(actual == expected)
    }
  }

  private def assertBaseAggregateExec[T: ClassTag](
      query: String,
      enabled: Int,
      disabled: Int): Unit = {

    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      withSQLConf(SQLConf.COLLAPSE_AGGREGATE_NODES_ENABLED.key -> "false") {
        val df1 = sql(query)
        assertAggregateExecCount[T](df1, disabled)
        val result = df1.collect()
        withSQLConf(SQLConf.COLLAPSE_AGGREGATE_NODES_ENABLED.key -> "true") {
          val df2 = sql(query)
          assertAggregateExecCount[T](df2, enabled)
          checkAnswer(df2, result)
        }
      }
    }
  }

  private def assertHashAggregateExec(query: String, enabled: Int, disabled: Int): Unit = {
    assertBaseAggregateExec[HashAggregateExec](query, enabled, disabled)
  }

  private def assertObjectHashAggregateExec(query: String, enabled: Int, disabled: Int): Unit = {
    withSQLConf(SQLConf.USE_OBJECT_HASH_AGG.key -> "true") {
      assertBaseAggregateExec[ObjectHashAggregateExec](query, enabled, disabled)
    }
  }

  private def assertSortAggregateExec(query: String, enabled: Int, disabled: Int): Unit = {
    withSQLConf(SQLConf.USE_OBJECT_HASH_AGG.key -> "false") {
      assertBaseAggregateExec[SortAggregateExec](query, enabled, disabled)
    }
  }

  test("hash aggregate") {
    val query = "select max(col1), col2 from t1 group by col2"
    assertHashAggregateExec(query, 2, 2)
  }

  test("Aggregate after Aggregate where aggregates cannot be merged (HashAggregateExec)") {
    val query = "select max(col1), min(c1), col2 " +
      "from (select count(*) as c1, col1, col2 from t1 group by col1, col2) temp " +
      "group by col2"
    assertHashAggregateExec(query, 4, 4)
  }

  test("Aggregate after aggregate where aggregates can be merged (HashAggregateExec)") {
    val query = "select sum(c2), col1, c1 " +
      "from (select col1, count(*) as c1, max(col2) as c2 from t1 group by col1) temp " +
      "group by col1, c1"
    assertHashAggregateExec(query, 3, 4)
  }

  test("Aggregate after Join when it cannot be merged (HashAggregateExec)") {
    val query = "select sum(t1.col1), t1.col2 " +
      "from t1, t2 where t1.col1 = t2.col1 " +
      "group by t1.col2"
    assertHashAggregateExec(query, 2, 2)
  }

  test("Aggregate after Join when it can be merged (HashAggregateExec)") {
    val query = "select sum(t2.col1), max(t2.col2), t1.col1, t1.col2 " +
      "from t1, t2 where t1.col1 = t2.col1 " +
      "group by t1.col1, t1.col2"
    assertHashAggregateExec(query, 1, 2)
  }

  Seq(true, false).foreach { useObjectHashAgg =>
    test(s"object/sort aggregate [useObjectHashAgg: $useObjectHashAgg]") {
      val query = "select collect_list(col1), col2 from t1 group by col2"
      if (useObjectHashAgg) {
        assertObjectHashAggregateExec(query, 2, 2)
      } else {
        assertSortAggregateExec(query, 2, 2)
      }
    }

    test("Aggregate after Aggregate where aggregates cannot be merged " +
      s"[useObjectHashAgg: $useObjectHashAgg]") {
      val query = "select max(col1), collect_list(c1), col2 " +
        "from (select collect_list(1) as c1, col1, col2 from t1 group by col1, col2) temp " +
        "group by col2"

      if (useObjectHashAgg) {
        assertObjectHashAggregateExec(query, 4, 4)
      } else {
        assertSortAggregateExec(query, 4, 4)
      }
    }

    test("Aggregate after aggregate where aggregates can be merged " +
      s"[useObjectHashAgg: $useObjectHashAgg]") {
      val query = "select collect_list(c2), col1, c1 " +
        "from (select col1, count(*) as c1, collect_list(col2) as c2 from t1 group by col1) temp " +
        "group by col1, c1"
      if (useObjectHashAgg) {
        assertObjectHashAggregateExec(query, 3, 4)
      } else {
        assertSortAggregateExec(query, 3, 4)
      }
    }

    test(s"Aggregate after Join when it cannot be merged " +
      s"[useObjectHashAgg: $useObjectHashAgg]") {
      val query = "select collect_list(t1.col1), t1.col2 " +
        "from t1, t2 where t1.col1 = t2.col1 " +
        "group by t1.col2"
      if (useObjectHashAgg) {
        assertObjectHashAggregateExec(query, 2, 2)
      } else {
        assertSortAggregateExec(query, 2, 2)
      }
    }

    test("Aggregate after Join when it can be merged " +
      s"[useObjectHashAgg: $useObjectHashAgg]") {
      val query = "select collect_list(t2.col1), max(t2.col2), t1.col1, t1.col2 " +
        "from t1, t2 where t1.col1 = t2.col1 " +
        "group by t1.col1, t1.col2"
      if (useObjectHashAgg) {
        assertObjectHashAggregateExec(query, 1, 2)
      } else {
        assertSortAggregateExec(query, 1, 2)
      }
    }
  }
}

class CollapseAggregatesSuite extends CollapseAggregatesSuiteBase
  with DisableAdaptiveExecutionSuite

class CollapseAggregatesSuiteAE extends CollapseAggregatesSuiteBase
  with EnableAdaptiveExecutionSuite
