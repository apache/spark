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
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules._

class EliminateJoinBaseUniquenessSuite extends PlanTest {
  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches = Batch("EliminateJoin", FixedPoint(100),
      FoldablePropagation,
      CollapseProject,
      RemoveLiteralFromGroupExpressions,
      EliminateJoinBaseUniqueness,
      RemoveRepetitionFromGroupExpressions,
      ReplaceDistinctWithAggregate) :: Nil
  }

  private val testRelation = LocalRelation('a.int, 'b.int, 'c.int)
  private val x = testRelation.subquery('x)
  private val y = testRelation.subquery('y)

  test("SPARK-34808: Remove left join if it only has distinct on left side") {
    val query = Distinct(x.join(y, LeftOuter, Some("x.a".attr === "y.a".attr)).select("x.b".attr))
    val correctAnswer = x.select("x.b".attr).groupBy("x.b".attr)("x.b".attr)

    comparePlans(Optimize.execute(query.analyze), correctAnswer.analyze)
  }

  test("SPARK-34808: Remove right join if it only has distinct on right side") {
    val query = Distinct(x.join(y, RightOuter, Some("x.a".attr === "y.a".attr)).select("y.b".attr))
    val correctAnswer = y.select("y.b".attr).groupBy("y.b".attr)("y.b".attr)

    comparePlans(Optimize.execute(query.analyze), correctAnswer.analyze)
  }

  test("SPARK-34808: Should not remove left join if select 2 join sides") {
    val query = Distinct(x.join(y, RightOuter, Some("x.a".attr === "y.a".attr))
      .select("x.b".attr, "y.c".attr))
    val correctAnswer = Aggregate(query.child.output, query.child.output, query.child)

    comparePlans(Optimize.execute(query.analyze), correctAnswer.analyze)
  }

  test("SPARK-34808: aggregateExpressions only contains groupingExpressions") {
    comparePlans(
      Optimize.execute(
        Distinct(x.join(y, LeftOuter, Some("x.a".attr === "y.a".attr))
          .select("x.b".attr, "x.b".attr)).analyze),
      x.select("x.b".attr, "x.b".attr).groupBy("x.b".attr)("x.b".attr, "x.b".attr).analyze)

    comparePlans(
      Optimize.execute(
        x.join(y, LeftOuter, Some("x.a".attr === "y.a".attr))
          .groupBy("x.a".attr, "x.b".attr)("x.b".attr, "x.a".attr).analyze),
      x.groupBy("x.a".attr, "x.b".attr)("x.b".attr, "x.a".attr).analyze)

    comparePlans(
      Optimize.execute(
        x.join(y, LeftOuter, Some("x.a".attr === "y.a".attr))
          .groupBy("x.a".attr)("x.a".attr, Literal(1)).analyze),
      x.join(y, LeftOuter, Some("x.a".attr === "y.a".attr))
        .groupBy("x.a".attr)("x.a".attr, Literal(1)).analyze)
  }

  test("SPARK-36155: Left semi join to inner join") {
    val query = x.join(y.groupBy("y.a".attr)("y.a".attr),
      LeftSemi,
      Some("x.a".attr === "y.a".attr)).select("x.b".attr)
    val correctAnswer = x.join(y.groupBy("y.a".attr)("y.a".attr),
      Inner,
      Some("x.a".attr === "y.a".attr)).select("x.b".attr)

    comparePlans(Optimize.execute(query.analyze), correctAnswer.analyze)
  }

  test("SPARK-36155: Should not rewrite Left semi join to inner join") {
    val query = x.join(y.groupBy("y.a".attr, "y.b".attr)("y.a".attr, "y.b".attr),
      LeftSemi,
      Some("x.a".attr === "y.a".attr)).select("x.b".attr)

    comparePlans(Optimize.execute(query.analyze), query.analyze)
  }

  test("SPARK-36155: Remove left join if uniqueness can be guaranteed on the buffered side") {
    val query = x.join(y.groupBy("y.a".attr)("y.a".attr),
      LeftOuter,
      Some("x.a".attr === "y.a".attr)).select("x.b".attr)
    val correctAnswer = x.select("x.b".attr)

    comparePlans(Optimize.execute(query.analyze), correctAnswer.analyze)
  }

  test("SPARK-36155: Should not remove left join") {
    val query = x.join(y.groupBy("y.a".attr, "y.b".attr)("y.a".attr, "y.b".attr),
      LeftOuter,
      Some("x.a".attr === "y.a".attr)).select("x.b".attr)

    comparePlans(Optimize.execute(query.analyze), query.analyze)
  }

  test("SPARK-36155: Remove right join if uniqueness can be guaranteed on the buffered side") {
    val query = x.groupBy("x.a".attr, "x.a".attr)("x.a".attr).join(y,
      RightOuter,
      Some("x.a".attr === "y.a".attr)).select("y.b".attr)
    val correctAnswer = y.select("y.b".attr)

    comparePlans(Optimize.execute(query.analyze), correctAnswer.analyze)
  }

  test("SPARK-36155: Should not remove right join") {
    val query = x.groupBy("x.a".attr, "x.b".attr)("x.a".attr, "x.b".attr).join(y,
      RightOuter,
      Some("x.a".attr === "y.a".attr)).select("y.b".attr)

    comparePlans(Optimize.execute(query.analyze), query.analyze)
  }
}
