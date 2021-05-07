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
import org.apache.spark.sql.catalyst.expressions.{CreateStruct, GetStructField, ScalarSubquery}
import org.apache.spark.sql.catalyst.expressions.aggregate.{CollectList, CollectSet}
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules._

class MergeScalarSubqueriesSuite extends PlanTest {
  private object Optimize extends RuleExecutor[LogicalPlan] {
    val batches = Batch("MergeScalarSubqueries", Once, MergeScalarSubqueries) :: Nil
  }

  val testRelation = LocalRelation('a.int, 'b.int, 'c.string)

  test("Simple non-correlated scalar subquery merge") {
    val subquery1 = testRelation
      .groupBy('b)(max('a).as("max_a"))
    val subquery2 = testRelation
      .groupBy('b)(sum('a).as("sum_a"))
    val originalQuery = testRelation
      .select(ScalarSubquery(subquery1), ScalarSubquery(subquery2))

    val multiSubquery = testRelation
      .groupBy('b)(max('a).as("max_a"), sum('a).as("sum_a"))
      .select(CreateStruct(Seq('max_a, 'sum_a)).as("mergedValue"))
    val correctAnswer = testRelation
      .select(GetStructField(ScalarSubquery(multiSubquery), 0).as("scalarsubquery()"),
        GetStructField(ScalarSubquery(multiSubquery), 1).as("scalarsubquery()"))

    comparePlans(Optimize.execute(originalQuery.analyze), correctAnswer.analyze)
  }

  test("Aggregate and group expression merge") {
    val subquery1 = testRelation
      .groupBy('b)(max('a).as("max_a"))
    val subquery2 = testRelation
      .groupBy('b)('b)
    val originalQuery = testRelation
      .select(ScalarSubquery(subquery1), ScalarSubquery(subquery2))

    val multiSubquery = testRelation
      .groupBy('b)(max('a).as("max_a"), 'b)
      .select(CreateStruct(Seq('max_a, 'b)).as("mergedValue"))
    val correctAnswer = testRelation
      .select(GetStructField(ScalarSubquery(multiSubquery), 0).as("scalarsubquery()"),
        GetStructField(ScalarSubquery(multiSubquery), 1).as("scalarsubquery()"))

    comparePlans(Optimize.execute(originalQuery.analyze), correctAnswer.analyze)
  }

  test("Do not merge different aggregate implementations") {
    // supports HashAggregate
    val subquery1 = testRelation
      .groupBy('b)(max('a).as("max_a"))
    val subquery2 = testRelation
      .groupBy('b)(min('a).as("min_a"))

    // supports ObjectHashAggregate
    val subquery3 = testRelation
      .groupBy('b)(CollectList('a).toAggregateExpression(isDistinct = false).as("collectlist_a"))
    val subquery4 = testRelation
      .groupBy('b)(CollectSet('a).toAggregateExpression(isDistinct = false).as("collectset_a"))

    // supports SortAggregate
    val subquery5 = testRelation
      .groupBy('b)(max('c).as("max_c"))
    val subquery6 = testRelation
      .groupBy('b)(min('c).as("min_c"))

    val originalQuery = testRelation
      .select(ScalarSubquery(subquery1), ScalarSubquery(subquery2), ScalarSubquery(subquery3),
        ScalarSubquery(subquery4), ScalarSubquery(subquery5), ScalarSubquery(subquery6))

    val hashAggregates = testRelation
      .groupBy('b)(max('a).as("max_a"), min('a).as("min_a"))
      .select(CreateStruct(Seq('max_a, 'min_a)).as("mergedValue"))
    val objectHashAggregates = testRelation
      .groupBy('b)(CollectList('a).toAggregateExpression(isDistinct = false).as("collectlist_a"),
        CollectSet('a).toAggregateExpression(isDistinct = false).as("collectset_a"))
      .select(CreateStruct(Seq('collectlist_a, 'collectset_a)).as("mergedValue"))
    val sortAggregates = testRelation
      .groupBy('b)(max('c).as("max_c"), min('c).as("min_c"))
      .select(CreateStruct(Seq('max_c, 'min_c)).as("mergedValue"))
    val correctAnswer = testRelation
      .select(GetStructField(ScalarSubquery(hashAggregates), 0).as("scalarsubquery()"),
        GetStructField(ScalarSubquery(hashAggregates), 1).as("scalarsubquery()"),
        GetStructField(ScalarSubquery(objectHashAggregates), 0).as("scalarsubquery()"),
        GetStructField(ScalarSubquery(objectHashAggregates), 1).as("scalarsubquery()"),
        GetStructField(ScalarSubquery(sortAggregates), 0).as("scalarsubquery()"),
        GetStructField(ScalarSubquery(sortAggregates), 1).as("scalarsubquery()"))

    comparePlans(Optimize.execute(originalQuery.analyze), correctAnswer.analyze)
  }
}
