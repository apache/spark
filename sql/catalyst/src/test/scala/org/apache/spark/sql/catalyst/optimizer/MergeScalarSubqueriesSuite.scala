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
import org.apache.spark.sql.catalyst.expressions.{GetStructField, MultiScalarSubquery, ScalarSubquery}
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules._

class MergeScalarSubqueriesSuite extends PlanTest {

  private object Optimize extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("MergeScalarSubqueries", Once, MergeScalarSubqueries) :: Nil
  }

  test("Simple non-correlated scalar subquery merge") {
    val testRelation = LocalRelation('a.int, 'b.int)

    val subquery1 = testRelation
      .groupBy('b)(max('a))
    val subquery2 = testRelation
      .groupBy('b)(sum('a))
    val originalQuery = testRelation
      .select(ScalarSubquery(subquery1), ScalarSubquery(subquery2))

    val multiSubquery = testRelation
      .groupBy('b)(max('a), sum('a)).analyze
    val correctAnswer = testRelation
      .select(GetStructField(MultiScalarSubquery(multiSubquery), 0).as("scalarsubquery()"),
        GetStructField(MultiScalarSubquery(multiSubquery), 1).as("scalarsubquery()"))

    // checkAnalysis is disabled because `Analizer` is not prepared for `MultiScalarSubquery` nodes
    // as only `Optimizer` can insert such a node to the plan
    comparePlans(Optimize.execute(originalQuery.analyze), correctAnswer, false)
  }
}
