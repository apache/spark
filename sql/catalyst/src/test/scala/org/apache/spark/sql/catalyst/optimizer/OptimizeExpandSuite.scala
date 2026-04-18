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

package org.apache.spark.sql.catalyst.optimizer

import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules._
import org.apache.spark.sql.internal.SQLConf

class OptimizeExpandSuite extends PlanTest {

  private object Optimize extends RuleExecutor[LogicalPlan] {
    val batches = Batch("Rewrite", Once,
      RewriteDistinctAggregates, OptimizeExpand) :: Nil
  }

  val testRelation = LocalRelation(
    $"key".string, $"col1".int, $"col2".int, $"col3".int,
    $"col4".int, $"col5".int, $"value".double)

  private def hasPreAggBeforeExpand(plan: LogicalPlan): Boolean = {
    plan.collect {
      case e: Expand => e.child.isInstanceOf[Aggregate]
    }.exists(identity)
  }

  test("inserts pre-aggregate when expand ratio >= threshold (pure distinct)") {
    // 3 distinct groups -> Expand ratio = 3, threshold set to 3
    withSQLConf(SQLConf.OPTIMIZE_EXPAND_RATIO.key -> "3") {
      val query = testRelation
        .groupBy($"key")(
          countDistinct($"col1").as("cd1"),
          countDistinct($"col2").as("cd2"),
          countDistinct($"col3").as("cd3"))
        .analyze
      val optimized = Optimize.execute(query)
      assert(hasPreAggBeforeExpand(optimized),
        "Should insert pre-aggregation for pure distinct query above threshold")
    }
  }

  test("does not insert pre-aggregate when below threshold") {
    withSQLConf(SQLConf.OPTIMIZE_EXPAND_RATIO.key -> "10") {
      val query = testRelation
        .groupBy($"key")(
          countDistinct($"col1").as("cd1"),
          countDistinct($"col2").as("cd2"))
        .analyze
      val optimized = Optimize.execute(query)
      assert(!hasPreAggBeforeExpand(optimized),
        "Should not insert pre-aggregation when below threshold")
    }
  }

  test("disabled when threshold is -1") {
    withSQLConf(SQLConf.OPTIMIZE_EXPAND_RATIO.key -> "-1") {
      val query = testRelation
        .groupBy($"key")(
          countDistinct($"col1").as("cd1"),
          countDistinct($"col2").as("cd2"),
          countDistinct($"col3").as("cd3"),
          countDistinct($"col4").as("cd4"),
          countDistinct($"col5").as("cd5"))
        .analyze
      val optimized = Optimize.execute(query)
      assert(!hasPreAggBeforeExpand(optimized),
        "Should not insert pre-aggregation when disabled")
    }
  }

  test("skips when non-distinct agg references columns outside group by") {
    withSQLConf(SQLConf.OPTIMIZE_EXPAND_RATIO.key -> "2") {
      // sum(value) references 'value' which is not in groupBy or distinct cols
      val query = testRelation
        .groupBy($"key")(
          countDistinct($"col1").as("cd1"),
          countDistinct($"col2").as("cd2"),
          sum($"value").as("total"))
        .analyze
      val optimized = Optimize.execute(query)
      assert(!hasPreAggBeforeExpand(optimized),
        "Should skip when non-distinct agg children are outside group by + distinct")
    }
  }

  test("applies for pure count distinct (no non-distinct aggs)") {
    withSQLConf(SQLConf.OPTIMIZE_EXPAND_RATIO.key -> "2") {
      val query = testRelation
        .groupBy($"key")(
          countDistinct($"col1").as("cd1"),
          countDistinct($"col2").as("cd2"))
        .analyze
      val optimized = Optimize.execute(query)
      assert(hasPreAggBeforeExpand(optimized),
        "Should apply for pure count distinct queries")
    }
  }

  test("idempotent: does not insert double pre-aggregate") {
    withSQLConf(SQLConf.OPTIMIZE_EXPAND_RATIO.key -> "2") {
      val query = testRelation
        .groupBy($"key")(
          countDistinct($"col1").as("cd1"),
          countDistinct($"col2").as("cd2"),
          countDistinct($"col3").as("cd3"))
        .analyze
      val optimized = Optimize.execute(query)
      assert(hasPreAggBeforeExpand(optimized))
      // Run again - should not insert another Aggregate
      val optimizedAgain = Optimize.execute(optimized)
      val expandChildren = optimizedAgain.collect {
        case e: Expand => e.child
      }
      assert(expandChildren.nonEmpty)
      // The Expand's child should be an Aggregate, but its child
      // should NOT be another Aggregate (no double pre-agg)
      expandChildren.foreach { child =>
        assert(child.isInstanceOf[Aggregate])
        assert(!child.asInstanceOf[Aggregate].child.isInstanceOf[Aggregate],
          "Should not insert double pre-aggregation")
      }
    }
  }

  test("pre-aggregate groups by all expand input attributes") {
    withSQLConf(SQLConf.OPTIMIZE_EXPAND_RATIO.key -> "2") {
      val query = testRelation
        .groupBy($"key")(
          countDistinct($"col1").as("cd1"),
          countDistinct($"col2").as("cd2"))
        .analyze
      val optimized = Optimize.execute(query)
      val preAgg = optimized.collect {
        case e: Expand => e.child
      }.head.asInstanceOf[Aggregate]
      val groupByNames = preAgg.groupingExpressions
        .collect { case a: Attribute => a.name }.toSet
      assert(groupByNames.contains("key"),
        "Pre-aggregate should include grouping key")
      assert(groupByNames.contains("col1"),
        "Pre-aggregate should include distinct col1")
      assert(groupByNames.contains("col2"),
        "Pre-aggregate should include distinct col2")
    }
  }

  test("skips when distinct expression is composite (col1 + col2)") {
    // col1 + col2 fans out into leaf attributes (col1, col2), inflating
    // the pre-aggregate's Cartesian product beyond effective dedup.
    withSQLConf(SQLConf.OPTIMIZE_EXPAND_RATIO.key -> "2") {
      val query = testRelation
        .groupBy($"key")(
          countDistinct($"col1" + $"col2").as("cd_expr"),
          countDistinct($"col3").as("cd3"))
        .analyze
      val optimized = Optimize.execute(query)
      assert(!hasPreAggBeforeExpand(optimized),
        "Should skip optimization for expression-based distinct")
    }
  }
}
