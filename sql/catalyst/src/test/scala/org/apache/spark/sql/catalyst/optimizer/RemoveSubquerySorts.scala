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

import org.apache.spark.sql.catalyst.analysis.{EliminateSubqueryAliases, SimpleAnalyzer}
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules._

class RemoveSubquerySortsSuite extends PlanTest {

  private object Optimize extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("Subqueries", Once,
        RemoveSubquerySorts,
        EliminateSubqueryAliases) :: Nil
  }

  private val testRelation = LocalRelation('a.int, 'b.int, 'c.int)

  private def analyzeAndCompare(plan: LogicalPlan, correct: LogicalPlan) {
    // We can't use the implicit analyze method, that tests usually use, for 'plan'
    // because it explicitly calls EliminateSubqueryAliases.
    comparePlans(Optimize.execute(SimpleAnalyzer.execute(plan)), correct.analyze)
  }

  test("Remove top-level sort") {
    val query = testRelation.orderBy('a.asc).subquery('x)
    analyzeAndCompare(query, testRelation)
  }

  test("Remove sort behind filter and project") {
    val query = testRelation.orderBy('a.asc).where('a.attr > 10).select('b).subquery('x)
    analyzeAndCompare(query, testRelation.where('a.attr > 10).select('b))
  }

  test("Remove sort below subquery that is not at root") {
    val query = testRelation.orderBy('a.asc).subquery('x).groupBy('a)(sum('b))
    analyzeAndCompare(query, testRelation.groupBy('a)(sum('b)))
  }

  test("Sorts with limits must not be removed from subqueries") {
    val query = testRelation.orderBy('a.asc).limit(10).subquery('x)
    analyzeAndCompare(query, testRelation.orderBy('a.asc).limit(10))
  }

  test("Remove more than one sort") {
    val query = testRelation.orderBy('a.asc).orderBy('b.desc).subquery('x)
    analyzeAndCompare(query, testRelation)
  }

  test("Nested subqueries") {
    val query = testRelation.orderBy('a.asc).subquery('x).orderBy('b.desc).subquery('y)
    analyzeAndCompare(query, testRelation)
  }

  test("Sorts below non-project / filter operators don't get removed") {
    val query = testRelation.orderBy('a.asc).groupBy('a)(sum('b)).subquery('x)
    analyzeAndCompare(query, testRelation.orderBy('a.asc).groupBy('a)(sum('b)))
  }
}
