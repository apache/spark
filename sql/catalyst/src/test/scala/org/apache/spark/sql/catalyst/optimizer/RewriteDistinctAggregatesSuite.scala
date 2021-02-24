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
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.expressions.aggregate.CollectSet
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Expand, LocalRelation, LogicalPlan}
import org.apache.spark.sql.types.{IntegerType, StringType}

class RewriteDistinctAggregatesSuite extends PlanTest {
  val nullInt = Literal(null, IntegerType)
  val nullString = Literal(null, StringType)
  val testRelation =
    LocalRelation("a".attr.string, "b".attr.string, "c".attr.string, "d".attr.string, "e".attr.int)

  private def checkRewrite(rewrite: LogicalPlan): Unit = rewrite match {
    case Aggregate(_, _, Aggregate(_, _, _: Expand)) =>
    case _ => fail(s"Plan is not rewritten:\n$rewrite")
  }

  test("single distinct group") {
    val input = testRelation
      .groupBy("a".attr)(countDistinct("e".attr))
      .analyze
    val rewrite = RewriteDistinctAggregates(input)
    comparePlans(input, rewrite)
  }

  test("single distinct group with partial aggregates") {
    val input = testRelation
      .groupBy("a".attr, "d".attr)(
        countDistinct("e".attr, "c".attr).as("agg1"),
        max("b".attr).as("agg2"))
      .analyze
    val rewrite = RewriteDistinctAggregates(input)
    comparePlans(input, rewrite)
  }

  test("multiple distinct groups") {
    val input = testRelation
      .groupBy("a".attr)(countDistinct("b".attr, "c".attr), countDistinct("d".attr))
      .analyze
    checkRewrite(RewriteDistinctAggregates(input))
  }

  test("multiple distinct groups with partial aggregates") {
    val input = testRelation
      .groupBy("a".attr)(countDistinct("b".attr, "c".attr), countDistinct("d".attr), sum("e".attr))
      .analyze
    checkRewrite(RewriteDistinctAggregates(input))
  }

  test("multiple distinct groups with non-partial aggregates") {
    val input = testRelation
      .groupBy("a".attr)(
        countDistinct("b".attr, "c".attr),
        countDistinct("d".attr),
        CollectSet("b".attr).toAggregateExpression())
      .analyze
    checkRewrite(RewriteDistinctAggregates(input))
  }
}
