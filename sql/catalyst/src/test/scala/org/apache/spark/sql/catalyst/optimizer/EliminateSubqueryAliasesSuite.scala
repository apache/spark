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

import org.apache.spark.sql.catalyst.analysis
import org.apache.spark.sql.catalyst.analysis.EliminateSubqueryAliases
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.Literal.TrueLiteral
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules._


class EliminateSubqueryAliasesSuite extends PlanTest with PredicateHelper {

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches = Batch("EliminateSubqueryAliases", Once, EliminateSubqueryAliases) :: Nil
  }

  private def afterOptimization(plan: LogicalPlan): LogicalPlan = {
    Optimize.execute(analysis.SimpleAnalyzer.execute(plan))
  }

  test("eliminate top level subquery") {
    val input = LocalRelation('a.int, 'b.int)
    val query = SubqueryAlias("a", input)
    comparePlans(afterOptimization(query), input)
  }

  test("eliminate mid-tree subquery") {
    val input = LocalRelation('a.int, 'b.int)
    val query = Filter(TrueLiteral, SubqueryAlias("a", input))
    comparePlans(
      afterOptimization(query),
      Filter(TrueLiteral, LocalRelation('a.int, 'b.int)))
  }

  test("eliminate multiple subqueries") {
    val input = LocalRelation('a.int, 'b.int)
    val query = Filter(TrueLiteral,
      SubqueryAlias("c", SubqueryAlias("b", SubqueryAlias("a", input))))
    comparePlans(
      afterOptimization(query),
      Filter(TrueLiteral, LocalRelation('a.int, 'b.int)))
  }
}
