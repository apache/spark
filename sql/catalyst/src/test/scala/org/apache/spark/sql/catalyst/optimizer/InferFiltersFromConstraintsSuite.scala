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
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{IntegerType, LongType}

class InferFiltersFromConstraintsSuite extends PlanTest {

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("InferAndPushDownFilters", FixedPoint(100),
        PushPredicateThroughJoin,
        PushPredicateThroughNonJoin,
        InferFiltersFromConstraints,
        CombineFilters,
        SimplifyBinaryComparison,
        BooleanSimplification,
        PruneFilters) :: Nil
  }

  val testRelation = LocalRelation('a.int, 'b.int, 'c.int)

  private def testConstraintsAfterJoin(
      x: LogicalPlan,
      y: LogicalPlan,
      expectedLeft: LogicalPlan,
      expectedRight: LogicalPlan,
      joinType: JoinType,
      condition: Option[Expression] = Some("x.a".attr === "y.a".attr)) = {
    val originalQuery = x.join(y, joinType, condition).analyze
    val correctAnswer = expectedLeft.join(expectedRight, joinType, condition).analyze
    val optimized = Optimize.execute(originalQuery)
    comparePlans(optimized, correctAnswer)
  }

  test("filter: filter out constraints in condition") {
    val originalQuery = testRelation.where('a === 1 && 'a === 'b).analyze
    val correctAnswer = testRelation
      .where(IsNotNull('a) && IsNotNull('b) && 'a === 'b && 'a === 1 && 'b === 1).analyze
    val optimized = Optimize.execute(originalQuery)
    comparePlans(optimized, correctAnswer)
  }

  test("single inner join: filter out values on either side on equi-join keys") {
    val x = testRelation.subquery('x)
    val y = testRelation.subquery('y)
    val originalQuery = x.join(y,
      condition = Some(("x.a".attr === "y.a".attr) && ("x.a".attr === 1) && ("y.c".attr > 5)))
      .analyze
    val left = x.where(IsNotNull('a) && "x.a".attr === 1)
    val right = y.where(IsNotNull('a) && IsNotNull('c) && "y.c".attr > 5 && "y.a".attr === 1)
    val correctAnswer = left.join(right, condition = Some("x.a".attr === "y.a".attr)).analyze
    val optimized = Optimize.execute(originalQuery)
    comparePlans(optimized, correctAnswer)
  }

  test("single inner join: filter out nulls on either side on non equal keys") {
    val x = testRelation.subquery('x)
    val y = testRelation.subquery('y)
    val originalQuery = x.join(y,
      condition = Some(("x.a".attr =!= "y.a".attr) && ("x.b".attr === 1) && ("y.c".attr > 5)))
      .analyze
    val left = x.where(IsNotNull('a) && IsNotNull('b) && "x.b".attr === 1)
    val right = y.where(IsNotNull('a) && IsNotNull('c) && "y.c".attr > 5)
    val correctAnswer = left.join(right, condition = Some("x.a".attr =!= "y.a".attr)).analyze
    val optimized = Optimize.execute(originalQuery)
    comparePlans(optimized, correctAnswer)
  }

  test("single inner join with pre-existing filters: filter out values on either side") {
    val x = testRelation.subquery('x)
    val y = testRelation.subquery('y)
    val originalQuery = x.where('b > 5).join(y.where('a === 10),
      condition = Some("x.a".attr === "y.a".attr && "x.b".attr === "y.b".attr)).analyze
    val left = x.where(IsNotNull('a) && 'a === 10 && IsNotNull('b) && 'b > 5)
    val right = y.where(IsNotNull('a) && IsNotNull('b) && 'a === 10 && 'b > 5)
    val correctAnswer = left.join(right,
      condition = Some("x.a".attr === "y.a".attr && "x.b".attr === "y.b".attr)).analyze
    val optimized = Optimize.execute(originalQuery)
    comparePlans(optimized, correctAnswer)
  }

  test("single outer join: no null filters are generated") {
    val x = testRelation.subquery('x)
    val y = testRelation.subquery('y)
    val originalQuery = x.join(y, FullOuter,
      condition = Some("x.a".attr === "y.a".attr)).analyze
    val optimized = Optimize.execute(originalQuery)
    comparePlans(optimized, originalQuery)
  }

  test("multiple inner joins: filter out values on all sides on equi-join keys") {
    val t1 = testRelation.subquery('t1)
    val t2 = testRelation.subquery('t2)
    val t3 = testRelation.subquery('t3)
    val t4 = testRelation.subquery('t4)

    val originalQuery = t1.where('b > 5)
      .join(t2, condition = Some("t1.b".attr === "t2.b".attr))
      .join(t3, condition = Some("t2.b".attr === "t3.b".attr))
      .join(t4, condition = Some("t3.b".attr === "t4.b".attr)).analyze
    val correctAnswer = t1.where(IsNotNull('b) && 'b > 5)
      .join(t2.where(IsNotNull('b) && 'b > 5), condition = Some("t1.b".attr === "t2.b".attr))
      .join(t3.where(IsNotNull('b) && 'b > 5), condition = Some("t2.b".attr === "t3.b".attr))
      .join(t4.where(IsNotNull('b) && 'b > 5), condition = Some("t3.b".attr === "t4.b".attr))
      .analyze
    val optimized = Optimize.execute(originalQuery)
    comparePlans(optimized, correctAnswer)
  }

  test("inner join with filter: filter out values on all sides on equi-join keys") {
    val x = testRelation.subquery('x)
    val y = testRelation.subquery('y)

    val originalQuery =
      x.join(y, Inner, Some("x.a".attr === "y.a".attr)).where("x.a".attr > 5).analyze
    val correctAnswer = x.where(IsNotNull('a) && 'a.attr > 5)
      .join(y.where(IsNotNull('a) && 'a.attr > 5), Inner, Some("x.a".attr === "y.a".attr)).analyze
    val optimized = Optimize.execute(originalQuery)
    comparePlans(optimized, correctAnswer)
  }

  test("inner join with alias: alias contains multiple attributes") {
    val t1 = testRelation.subquery('t1)
    val t2 = testRelation.subquery('t2)

    val originalQuery = t1.select('a, Coalesce(Seq('a, 'b)).as('int_col)).as("t")
      .join(t2, Inner, Some("t.a".attr === "t2.a".attr && "t.int_col".attr === "t2.a".attr))
      .analyze
    val correctAnswer = t1
      .where(IsNotNull('a) && IsNotNull(Coalesce(Seq('a, 'b))) && 'a === Coalesce(Seq('a, 'b)))
      .select('a, Coalesce(Seq('a, 'b)).as('int_col)).as("t")
      .join(t2.where(IsNotNull('a)), Inner,
        Some("t.a".attr === "t2.a".attr && "t.int_col".attr === "t2.a".attr))
      .analyze
    val optimized = Optimize.execute(originalQuery)
    comparePlans(optimized, correctAnswer)
  }

  test("inner join with alias: alias contains single attributes") {
    val t1 = testRelation.subquery('t1)
    val t2 = testRelation.subquery('t2)

    val originalQuery = t1.select('a, 'b.as('d)).as("t")
      .join(t2, Inner, Some("t.a".attr === "t2.a".attr && "t.d".attr === "t2.a".attr))
      .analyze
    val correctAnswer = t1
      .where(IsNotNull('a) && IsNotNull('b) &&'a === 'b)
      .select('a, 'b.as('d)).as("t")
      .join(t2.where(IsNotNull('a)), Inner,
        Some("t.a".attr === "t2.a".attr && "t.d".attr === "t2.a".attr))
      .analyze
    val optimized = Optimize.execute(originalQuery)
    comparePlans(optimized, correctAnswer)
  }

  test("generate correct filters for alias that don't produce recursive constraints") {
    val t1 = testRelation.subquery('t1)

    val originalQuery = t1.select('a.as('x), 'b.as('y)).where('x === 1 && 'x === 'y).analyze
    val correctAnswer =
      t1.where('a === 1 && 'b === 1 && 'a === 'b && IsNotNull('a) && IsNotNull('b))
        .select('a.as('x), 'b.as('y)).analyze
    val optimized = Optimize.execute(originalQuery)
    comparePlans(optimized, correctAnswer)
  }

  test("No inferred filter when constraint propagation is disabled") {
    withSQLConf(SQLConf.CONSTRAINT_PROPAGATION_ENABLED.key -> "false") {
      val originalQuery = testRelation.where('a === 1 && 'a === 'b).analyze
      val optimized = Optimize.execute(originalQuery)
      comparePlans(optimized, originalQuery)
    }
  }

  test("constraints should be inferred from aliased literals") {
    val originalLeft = testRelation.subquery('left).as("left")
    val optimizedLeft = testRelation.subquery('left).where(IsNotNull('a) && 'a <=> 2).as("left")

    val right = Project(Seq(Literal(2).as("two")), testRelation.subquery('right)).as("right")
    val condition = Some("left.a".attr === "right.two".attr)

    val original = originalLeft.join(right, Inner, condition)
    val correct = optimizedLeft.join(right, Inner, condition)

    comparePlans(Optimize.execute(original.analyze), correct.analyze)
  }

  test("SPARK-23405: left-semi equal-join should filter out null join keys on both sides") {
    val x = testRelation.subquery('x)
    val y = testRelation.subquery('y)
    testConstraintsAfterJoin(x, y, x.where(IsNotNull('a)), y.where(IsNotNull('a)), LeftSemi)
  }

  test("SPARK-21479: Outer join after-join filters push down to null-supplying side") {
    val x = testRelation.subquery('x)
    val y = testRelation.subquery('y)
    val condition = Some("x.a".attr === "y.a".attr)
    val originalQuery = x.join(y, LeftOuter, condition).where("x.a".attr === 2).analyze
    val left = x.where(IsNotNull('a) && 'a === 2)
    val right = y.where(IsNotNull('a) && 'a === 2)
    val correctAnswer = left.join(right, LeftOuter, condition).analyze
    val optimized = Optimize.execute(originalQuery)
    comparePlans(optimized, correctAnswer)
  }

  test("SPARK-21479: Outer join pre-existing filters push down to null-supplying side") {
    val x = testRelation.subquery('x)
    val y = testRelation.subquery('y)
    val condition = Some("x.a".attr === "y.a".attr)
    val originalQuery = x.join(y.where("y.a".attr > 5), RightOuter, condition).analyze
    val left = x.where(IsNotNull('a) && 'a > 5)
    val right = y.where(IsNotNull('a) && 'a > 5)
    val correctAnswer = left.join(right, RightOuter, condition).analyze
    val optimized = Optimize.execute(originalQuery)
    comparePlans(optimized, correctAnswer)
  }

  test("SPARK-21479: Outer join no filter push down to preserved side") {
    val x = testRelation.subquery('x)
    val y = testRelation.subquery('y)
    testConstraintsAfterJoin(
      x, y.where("a".attr === 1),
      x, y.where(IsNotNull('a) && 'a === 1),
      LeftOuter)
  }

  test("SPARK-23564: left anti join should filter out null join keys on right side") {
    val x = testRelation.subquery('x)
    val y = testRelation.subquery('y)
    testConstraintsAfterJoin(x, y, x, y.where(IsNotNull('a)), LeftAnti)
  }

  test("SPARK-23564: left outer join should filter out null join keys on right side") {
    val x = testRelation.subquery('x)
    val y = testRelation.subquery('y)
    testConstraintsAfterJoin(x, y, x, y.where(IsNotNull('a)), LeftOuter)
  }

  test("SPARK-23564: right outer join should filter out null join keys on left side") {
    val x = testRelation.subquery('x)
    val y = testRelation.subquery('y)
    testConstraintsAfterJoin(x, y, x.where(IsNotNull('a)), y, RightOuter)
  }

  test("Constraints should be inferred from cast equality constraint(filter higher data type)") {
    val testRelation1 = LocalRelation('a.int)
    val testRelation2 = LocalRelation('b.long)
    val originalLeft = testRelation1.subquery('left)
    val originalRight = testRelation2.where('b === 1L).subquery('right)

    val left = testRelation1.where(IsNotNull('a) && 'a.cast(LongType) === 1L).subquery('left)
    val right = testRelation2.where(IsNotNull('b) && 'b === 1L).subquery('right)

    Seq(Some("left.a".attr.cast(LongType) === "right.b".attr),
      Some("right.b".attr === "left.a".attr.cast(LongType))).foreach { condition =>
      testConstraintsAfterJoin(originalLeft, originalRight, left, right, Inner, condition)
    }

    Seq(Some("left.a".attr === "right.b".attr.cast(IntegerType)),
      Some("right.b".attr.cast(IntegerType) === "left.a".attr)).foreach { condition =>
      testConstraintsAfterJoin(
        originalLeft,
        originalRight,
        testRelation1.where(IsNotNull('a)).subquery('left),
        right,
        Inner,
        condition)
    }
  }

  test("Constraints shouldn't be inferred from cast equality constraint(filter lower data type)") {
    val testRelation1 = LocalRelation('a.int)
    val testRelation2 = LocalRelation('b.long)
    val originalLeft = testRelation1.where('a === 1).subquery('left)
    val originalRight = testRelation2.subquery('right)

    val left = testRelation1.where(IsNotNull('a) && 'a === 1).subquery('left)
    val right = testRelation2.where(IsNotNull('b)).subquery('right)

    Seq(Some("left.a".attr.cast(LongType) === "right.b".attr),
      Some("right.b".attr === "left.a".attr.cast(LongType))).foreach { condition =>
      testConstraintsAfterJoin(originalLeft, originalRight, left, right, Inner, condition)
    }

    Seq(Some("left.a".attr === "right.b".attr.cast(IntegerType)),
      Some("right.b".attr.cast(IntegerType) === "left.a".attr)).foreach { condition =>
      testConstraintsAfterJoin(
        originalLeft,
        originalRight,
        left,
        testRelation2.where(IsNotNull('b) && 'b.attr.cast(IntegerType) === 1).subquery('right),
        Inner,
        condition)
    }
  }

  test("Constraints inferred from inequality constraints: basic") {
    Seq(('a < 'b && 'b < 3, 'a < 'b && 'b < 3 && 'a < 3), // a < b && b < 3 => a < 3
      ('a < 'b && 'b <= 3, 'a < 'b && 'b <= 3 && 'a < 3), // a < b && b <= 3 => a < 3
      ('a < 'b && 'b === 3, 'a < 'b && 'b === 3 && 'a < 3), // a < b && b = 3 => a < 3
      ('a <= 'b && 'b < 3, 'a <= 'b && 'b < 3 && 'a < 3), // a <= b && b < 3 => a < 3
      ('a <= 'b && 'b <= 3, 'a <= 'b && 'b <= 3 && 'a <= 3), // a <= b && b <= 3 => a <= 3
      ('a <= 'b && 'b === 3, 'a <= 'b && 'b === 3 && 'a <= 3), // a <= b && b = 3 => a <= 3
      ('a > 'b && 'b > 3, 'a > 'b && 'b > 3 && 'a > 3), // a > b && b > 3 => a > 3
      ('a > 'b && 'b >= 3, 'a > 'b && 'b >= 3 && 'a > 3), // a > b && b >= 3 => a > 3
      ('a > 'b && 'b === 3, 'a > 'b && 'b === 3 && 'a > 3), // a > b && b = 3 => a > 3
      ('a >= 'b && 'b > 3, 'a >= 'b && 'b > 3 && 'a > 3), // a >= b && b > 3 => a > 3
      ('a >= 'b && 'b >= 3, 'a >= 'b && 'b >= 3 && 'a >= 3), // a >= b && b >= 3 => a >= 3
      ('a >= 'b && 'b === 3, 'a >= 'b && 'b === 3 && 'a >= 3) // a >= b && b = 3 => a >= 3
    ).foreach {
      case (filter, inferred) =>
        val original = testRelation.where(filter)
        val optimized = testRelation.where(IsNotNull('a) && IsNotNull('b) && inferred)
        comparePlans(Optimize.execute(original.analyze), optimized.analyze)
    }
  }

  test("Constraints inferred from inequality constraints: join") {
    Seq(("left.b".attr < "right.b".attr, 'b < 1, 'b < 1),
      ("left.b".attr < "right.b".attr, 'b === 1, 'b < 1),
      ("left.b".attr < "right.b".attr, 'b <= 1, 'b < 1),
      ("left.b".attr <= "right.b".attr, 'b <= 1, 'b <= 1),
      ("left.b".attr <= "right.b".attr, 'b === 1, 'b <= 1),
      ("left.b".attr > "right.b".attr, 'b > 1, 'b > 1),
      ("left.b".attr > "right.b".attr, 'b === 1, 'b > 1),
      ("left.b".attr > "right.b".attr, 'b >= 1, 'b > 1),
      ("left.b".attr >= "right.b".attr, 'b >= 1, 'b >= 1),
      ("left.b".attr >= "right.b".attr, 'b === 1, 'b >= 1)
    ).foreach {
      case (cond, filter, inferred) =>
        val originalLeft = testRelation.subquery('left)
        val originalRight = testRelation.where(filter).subquery('right)

        val left = testRelation.where(IsNotNull('a) && IsNotNull('b) && inferred).subquery('left)
        val right = testRelation.where(IsNotNull('a) && IsNotNull('b) && filter).subquery('right)
        val condition = Some("left.a".attr === "right.a".attr && cond)
        testConstraintsAfterJoin(originalLeft, originalRight, left, right, Inner, condition)
    }
  }

  test("Constraints inferred from inequality constraints with cast") {
    Seq(('a < 'b && 'b < 3L, 'a.cast(LongType) < 'b && 'b < 3L && 'a.cast(LongType) < 3L),
      ('a < 'b && 'b <= 3L, 'a.cast(LongType) < 'b && 'b <= 3L && 'a.cast(LongType) < 3L),
      ('a < 'b && 'b === 3L, 'a.cast(LongType) < 'b && 'b === 3L && 'a.cast(LongType) < 3L),
      ('a <= 'b && 'b < 3L, 'a.cast(LongType) <= 'b && 'b < 3L && 'a.cast(LongType) < 3L),
      ('a <= 'b && 'b <= 3L, 'a.cast(LongType) <= 'b && 'b <= 3L && 'a.cast(LongType) <= 3L),
      ('a <= 'b && 'b === 3L, 'a.cast(LongType) <= 'b && 'b === 3L && 'a.cast(LongType) <= 3L),
      ('a < 'b && 'b < 3, 'a.cast(LongType) < 'b && 'b < Literal(3).cast(LongType)
        && 'a.cast(LongType) < Literal(3).cast(LongType)),
      ('a > 'b && 'b > 3L, 'a.cast(LongType) > 'b && 'b > 3L && 'a.cast(LongType) > 3L),
      ('a > 'b && 'b >= 3L, 'a.cast(LongType) > 'b && 'b >= 3L && 'a.cast(LongType) > 3L),
      ('a > 'b && 'b === 3L, 'a.cast(LongType) > 'b && 'b === 3L && 'a.cast(LongType) > 3L),
      ('a >= 'b && 'b > 3L, 'a.cast(LongType) >= 'b && 'b > 3L && 'a.cast(LongType) > 3L),
      ('a >= 'b && 'b >= 3L, 'a.cast(LongType) >= 'b && 'b >= 3L && 'a.cast(LongType) >= 3L),
      ('a >= 'b && 'b === 3L, 'a.cast(LongType) >= 'b && 'b === 3L && 'a.cast(LongType) >= 3L),
      ('a > 'b && 'b > 3, 'a.cast(LongType) > 'b && 'b > Literal(3).cast(LongType)
        && 'a.cast(LongType) > Literal(3).cast(LongType))
    ).foreach {
      case (filter, inferred) =>
        val testRelation = LocalRelation('a.int, 'b.long)
        val original = testRelation.where(filter)
        val optimized = testRelation.where(IsNotNull('a) && IsNotNull('b) && inferred)
        comparePlans(Optimize.execute(original.analyze), optimized.analyze)
    }
  }

  test("Constraints inferred from inequality attributes: case1") {
    val condition = Some("x.a".attr > "y.a".attr)
    val optimizedLeft = testRelation.where(IsNotNull('a) && 'a === 1).as("x")
    val optimizedRight = testRelation.where('a < 1 && IsNotNull('a) ).as("y")
    val correct = optimizedLeft.join(optimizedRight, Inner, condition)

    Seq(Literal(1) === 'a, 'a === Literal(1)).foreach { filter =>
      val original = testRelation.where(filter).as("x").join(testRelation.as("y"), Inner, condition)
      comparePlans(Optimize.execute(original.analyze), correct.analyze)
    }
  }

  test("Constraints inferred from inequality attributes: case2") {
    val original = testRelation.where('a < 'b && 'b < 'c && 'c < 5)
    val optimized = testRelation.where(IsNotNull('a) && IsNotNull('b) && IsNotNull('c)
      && 'a < 'b && 'b < 'c && 'a < 5 && 'b < 5 && 'c < 5)
    comparePlans(Optimize.execute(original.analyze), optimized.analyze)
  }

  test("Constraints inferred from inequality attributes: case3") {
    val left = testRelation.where('b >= 3 && 'b <= 13).as("x")
    val right = testRelation.as("y")

    val optimizedLeft = testRelation.where(IsNotNull('a) && IsNotNull('b)
      && 'b >= 3 && 'b <= 13).as("x")
    val optimizedRight = testRelation.where(IsNotNull('a) && IsNotNull('b) && IsNotNull('c)
      && 'c > 3 && 'b <= 13).as("y")
    val condition = Some("x.a".attr === "y.a".attr
      && "x.b".attr >= "y.b".attr && "x.b".attr < "y.c".attr)
    val original = left.join(right, Inner, condition)
    val optimized = optimizedLeft.join(optimizedRight, Inner, condition)
    comparePlans(Optimize.execute(original.analyze), optimized.analyze)
  }

  test("Constraints inferred from inequality attributes: case4") {
    val testRelation1 = LocalRelation('a.long, 'b.long, 'c.long).as("x")
    val testRelation2 = LocalRelation('a.int, 'b.int, 'c.int).as("y")

    // y.b < 13 inferred from y.b < x.b && x.b <= 13
    val left = testRelation1.where('b <= 13L).as("x")
    val right = testRelation2.as("y")

    val optimizedLeft = testRelation1.where(IsNotNull('a) && IsNotNull('b) && 'b <= 13L).as("x")
    val optimizedRight = testRelation2.where(IsNotNull('a) && IsNotNull('b)
      && 'b.cast(LongType) < 13L).as("y")

    val condition = Some("x.a".attr === "y.a".attr && "y.b".attr < "x.b".attr)
    val original = left.join(right, Inner, condition)
    val optimized = optimizedLeft.join(optimizedRight, Inner, condition)
    comparePlans(Optimize.execute(original.analyze), optimized.analyze)
  }
}
