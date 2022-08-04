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
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules._

class PushLocalTopKThroughOuterJoinSuite extends PlanTest {

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("Push TopK Through Outer Join", FixedPoint(1),
        PushDownPredicates,
        PushLocalTopKThroughOuterJoin) :: Nil
  }

  private val testRelation1 = RelationWithoutMaxRows(Seq($"a".int, $"b".int, $"c".int))
  private val testRelation2 = RelationWithoutMaxRows(Seq($"x".int, $"y".int, $"z".int))
  private val testRelation3 = RelationWithoutMaxRows(Seq($"l".int, $"m".int, $"n".int))
  private val emptyRelation = EmptyRelation(Seq($"x".int, $"y".int, $"z".int))

  test("left outer join") {
    val x = testRelation1.subquery("x")
    val y = testRelation2.subquery("y")

    def checkPushThrough(f: LogicalPlan => LogicalPlan): Unit = {
      val input = f(x.join(y, LeftOuter, Some($"a" === $"x")))
        .where($"a" > 1)
        .orderBy($"a".asc, $"b".asc).limit(10)
      val expected =
        f(LocalLimit(10, x.where($"a" > 1).sortBy($"a".asc, $"b".asc))
          .join(y, LeftOuter, Some($"a" === $"x")))
          .orderBy($"a".asc, $"b".asc).limit(10)
      comparePlans(Optimize.execute(input.analyze), expected.analyze)
    }

    checkPushThrough(identity)
    checkPushThrough(Project(Seq($"a", $"a" as "a1"), _))
    checkPushThrough(Repartition(2, true, _))
    checkPushThrough(RepartitionByExpression(Seq.empty, _, None))
    checkPushThrough(RebalancePartitions(Seq.empty, _))
  }

  test("right outer join") {
    val x = testRelation1.subquery("x")
    val y = testRelation2.subquery("y")

    def checkPushThrough(f: LogicalPlan => LogicalPlan): Unit = {
      val input = f(x.join(y, RightOuter, Some($"a" === $"x")))
        .where($"x" > 0)
        .orderBy($"x".asc).limit(10)
      val expected = f(x.join(
        LocalLimit(10, y.where($"x" > 0).sortBy($"x".asc)), RightOuter, Some($"a" === $"x")))
        .orderBy($"x".asc).limit(10)
      comparePlans(Optimize.execute(input.analyze), expected.analyze)
    }

    checkPushThrough(identity)
    checkPushThrough(Project(Seq($"x", $"x" as "x1"), _))
    checkPushThrough(Repartition(2, true, _))
    checkPushThrough(RepartitionByExpression(Seq.empty, _, None))
    checkPushThrough(RebalancePartitions(Seq.empty, _))
  }

  test("union outer join") {
    val x = testRelation1.subquery("x")
    val y = testRelation2.subquery("y")
    val z = testRelation3.subquery("z")

    val input = Union(
      x.join(y, LeftOuter, Some($"a" === $"x"))
        .select($"a", $"x", $"y").orderBy($"a".asc, $"b".asc).limit(10),
      x)
    val expected = Union(
      LocalLimit(10, x.sortBy($"a".asc, $"b".asc)).join(y, LeftOuter, Some($"a" === $"x"))
        .select($"a", $"x", $"y").orderBy($"a".asc, $"b".asc).limit(10),
      x)
    comparePlans(Optimize.execute(input.analyze), expected.analyze)

    val input2 = Union(
      x.join(y, LeftOuter, Some($"a" === $"x")).orderBy($"a".asc, $"b".asc).limit(10),
      x.join(y, RightOuter, Some($"a" === $"x")).orderBy($"x".asc).limit(10))
    val expected2 = Union(
      LocalLimit(10, x.sortBy($"a".asc, $"b".asc)).join(y, LeftOuter, Some($"a" === $"x"))
        .orderBy($"a".asc, $"b".asc).limit(10),
      x.join(LocalLimit(10, y.sortBy($"x".asc)), RightOuter, Some($"a" === $"x"))
        .orderBy($"x".asc).limit(10))
    comparePlans(Optimize.execute(input2.analyze), expected2.analyze)

    val input3 = Union(x, y).join(z, LeftOuter, Some($"a" === $"l"))
      .orderBy($"a".asc).limit(10)
    val expected3 = LocalLimit(10, Union(x, y).sortBy($"a".asc))
      .join(z, LeftOuter, Some($"a" === $"l"))
      .orderBy($"a".asc).limit(10)
    comparePlans(Optimize.execute(input3.analyze), expected3.analyze)
  }

  test("multi outer join") {
    val x = testRelation1.subquery("x")
    val y = testRelation2.subquery("y")
    val z = testRelation3.subquery("z")

    // GlobalLimit
    //   LocalLimit
    //     Sort global
    //       Join LeftOuter
    //         Join LeftOuter
    //           x
    //           y
    //       z
    // ==========>
    // GlobalLimit
    //   LocalLimit
    //     Sort global
    //       Join LeftOuter
    //         Join LeftOuter
    //           LocalLimit
    //             Sort local
    //               x
    //           y
    //       z
    val input = x.join(y, LeftOuter, Some($"a" === $"x"))
      .join(z, LeftOuter, Some($"a" === $"l"))
      .orderBy($"a".asc, $"b".asc).limit(10)
    val expected = LocalLimit(10, x.sortBy($"a".asc, $"b".asc))
      .join(y, LeftOuter, Some($"a" === $"x"))
      .join(z, LeftOuter, Some($"a" === $"l"))
      .orderBy($"a".asc, $"b".asc).limit(10)
    comparePlans(Optimize.execute(input.analyze), expected.analyze)
  }

  test("negative case with push through node") {
    val x = testRelation1.subquery("x")
    val y = testRelation2.subquery("y")

    val input = x.join(y, LeftOuter, Some($"a" === $"x")).groupBy($"x")(count("*"))
      .orderBy($"x".asc).limit(10)
    comparePlans(Optimize.execute(input.analyze), input.analyze)

    val input2 = x.join(y, LeftOuter, Some($"a" === $"x")).where($"x" > 0)
      .orderBy($"x".asc).limit(10)
    comparePlans(Optimize.execute(input2.analyze), input2.analyze)
  }

  test("negative case with reference") {
    val x = testRelation1.subquery("x")
    val y = testRelation2.subquery("y")

    val input = x.join(y, LeftOuter, Some($"a" === $"x")).orderBy($"x".asc).limit(10)
    comparePlans(Optimize.execute(input.analyze), input.analyze)

    val input2 = x.join(y, RightOuter, Some($"a" === $"x")).orderBy($"b".asc).limit(10)
    comparePlans(Optimize.execute(input2.analyze), input2.analyze)

    val input3 = x.join(y, Inner, Some($"a" === $"x")).orderBy($"b".asc).limit(10)
    comparePlans(Optimize.execute(input3.analyze), input3.analyze)
  }

  test("negative case with max rows") {
    val x = testRelation1.subquery("x")
    val y = emptyRelation.subquery("y")

    val input = y.join(x, LeftOuter, Some($"a" === $"x")).orderBy($"a".asc).limit(10)
    comparePlans(Optimize.execute(input.analyze), input.analyze)

    val input2 = x.limit(5).join(y, LeftOuter, Some($"a" === $"x")).orderBy($"a".asc).limit(10)
    comparePlans(Optimize.execute(input2.analyze), input2.analyze)
  }
}
