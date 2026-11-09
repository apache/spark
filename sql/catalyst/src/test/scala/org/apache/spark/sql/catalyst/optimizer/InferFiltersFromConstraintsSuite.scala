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
import org.apache.spark.sql.types._

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

  val testRelation = LocalRelation($"a".int, $"b".int, $"c".int)

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
    val originalQuery = testRelation.where($"a" === 1 && $"a" === $"b").analyze
    val correctAnswer = testRelation.where(IsNotNull($"a") && IsNotNull($"b") &&
      $"a" === $"b" && $"a" === 1 && $"b" === 1).analyze
    val optimized = Optimize.execute(originalQuery)
    comparePlans(optimized, correctAnswer)
  }

  test("single inner join: filter out values on either side on equi-join keys") {
    val x = testRelation.subquery("x")
    val y = testRelation.subquery("y")
    val originalQuery = x.join(y,
      condition = Some(("x.a".attr === "y.a".attr) && ("x.a".attr === 1) && ("y.c".attr > 5)))
      .analyze
    val left = x.where(IsNotNull($"a") && "x.a".attr === 1)
    val right = y.where(IsNotNull($"a") && IsNotNull($"c") && "y.c".attr > 5 && "y.a".attr === 1)
    val correctAnswer = left.join(right, condition = Some("x.a".attr === "y.a".attr)).analyze
    val optimized = Optimize.execute(originalQuery)
    comparePlans(optimized, correctAnswer)
  }

  test("single inner join: filter out nulls on either side on non equal keys") {
    val x = testRelation.subquery("x")
    val y = testRelation.subquery("y")
    val originalQuery = x.join(y,
      condition = Some(("x.a".attr =!= "y.a".attr) && ("x.b".attr === 1) && ("y.c".attr > 5)))
      .analyze
    val left = x.where(IsNotNull($"a") && IsNotNull($"b") && "x.b".attr === 1)
    val right = y.where(IsNotNull($"a") && IsNotNull($"c") && "y.c".attr > 5)
    val correctAnswer = left.join(right, condition = Some("x.a".attr =!= "y.a".attr)).analyze
    val optimized = Optimize.execute(originalQuery)
    comparePlans(optimized, correctAnswer)
  }

  test("single inner join with pre-existing filters: filter out values on either side") {
    val x = testRelation.subquery("x")
    val y = testRelation.subquery("y")
    val originalQuery = x.where($"b" > 5).join(y.where($"a" === 10),
      condition = Some("x.a".attr === "y.a".attr && "x.b".attr === "y.b".attr)).analyze
    val left = x.where(IsNotNull($"a") && $"a" === 10 && IsNotNull($"b") && $"b" > 5)
    val right = y.where(IsNotNull($"a") && IsNotNull($"b") && $"a" === 10 && $"b" > 5)
    val correctAnswer = left.join(right,
      condition = Some("x.a".attr === "y.a".attr && "x.b".attr === "y.b".attr)).analyze
    val optimized = Optimize.execute(originalQuery)
    comparePlans(optimized, correctAnswer)
  }

  test("single outer join: no null filters are generated") {
    val x = testRelation.subquery("x")
    val y = testRelation.subquery("y")
    val originalQuery = x.join(y, FullOuter,
      condition = Some("x.a".attr === "y.a".attr)).analyze
    val optimized = Optimize.execute(originalQuery)
    comparePlans(optimized, originalQuery)
  }

  test("multiple inner joins: filter out values on all sides on equi-join keys") {
    val t1 = testRelation.subquery("t1")
    val t2 = testRelation.subquery("t2")
    val t3 = testRelation.subquery("t3")
    val t4 = testRelation.subquery("t4")

    val originalQuery = t1.where($"b" > 5)
      .join(t2, condition = Some("t1.b".attr === "t2.b".attr))
      .join(t3, condition = Some("t2.b".attr === "t3.b".attr))
      .join(t4, condition = Some("t3.b".attr === "t4.b".attr)).analyze
    val correctAnswer = t1.where(IsNotNull($"b") && $"b" > 5)
      .join(t2.where(IsNotNull($"b") && $"b" > 5), condition = Some("t1.b".attr === "t2.b".attr))
      .join(t3.where(IsNotNull($"b") && $"b" > 5), condition = Some("t2.b".attr === "t3.b".attr))
      .join(t4.where(IsNotNull($"b") && $"b" > 5), condition = Some("t3.b".attr === "t4.b".attr))
      .analyze
    val optimized = Optimize.execute(originalQuery)
    comparePlans(optimized, correctAnswer)
  }

  test("inner join with filter: filter out values on all sides on equi-join keys") {
    val x = testRelation.subquery("x")
    val y = testRelation.subquery("y")

    val originalQuery =
      x.join(y, Inner, Some("x.a".attr === "y.a".attr)).where("x.a".attr > 5).analyze
    val correctAnswer = x.where(IsNotNull($"a") && $"a".attr > 5)
      .join(y.where(IsNotNull($"a") && $"a".attr > 5), Inner, Some("x.a".attr === "y.a".attr))
      .analyze
    val optimized = Optimize.execute(originalQuery)
    comparePlans(optimized, correctAnswer)
  }

  test("inner join with alias: alias contains multiple attributes") {
    val t1 = testRelation.subquery("t1")
    val t2 = testRelation.subquery("t2")

    val originalQuery = t1.select($"a", Coalesce(Seq($"a", $"b")).as("int_col")).as("t")
      .join(t2, Inner, Some("t.a".attr === "t2.a".attr && "t.int_col".attr === "t2.a".attr))
      .analyze
    val correctAnswer = t1
      .where(IsNotNull($"a") && IsNotNull(Coalesce(Seq($"a", $"b"))) &&
        $"a" === Coalesce(Seq($"a", $"b")))
      .select($"a", Coalesce(Seq($"a", $"b")).as("int_col")).as("t")
      .join(t2.where(IsNotNull($"a")), Inner,
        Some("t.a".attr === "t2.a".attr && "t.int_col".attr === "t2.a".attr))
      .analyze
    val optimized = Optimize.execute(originalQuery)
    comparePlans(optimized, correctAnswer)
  }

  test("inner join with alias: alias contains single attributes") {
    val t1 = testRelation.subquery("t1")
    val t2 = testRelation.subquery("t2")

    val originalQuery = t1.select($"a", $"b".as("d")).as("t")
      .join(t2, Inner, Some("t.a".attr === "t2.a".attr && "t.d".attr === "t2.a".attr))
      .analyze
    val correctAnswer = t1
      .where(IsNotNull($"a") && IsNotNull($"b") &&$"a" === $"b")
      .select($"a", $"b".as("d")).as("t")
      .join(t2.where(IsNotNull($"a")), Inner,
        Some("t.a".attr === "t2.a".attr && "t.d".attr === "t2.a".attr))
      .analyze
    val optimized = Optimize.execute(originalQuery)
    comparePlans(optimized, correctAnswer)
  }

  test("generate correct filters for alias that don't produce recursive constraints") {
    val t1 = testRelation.subquery("t1")

    val originalQuery = t1.select($"a".as("x"), $"b".as("y"))
      .where($"x" === 1 && $"x" === $"y").analyze
    val correctAnswer =
      t1.where($"a" === 1 && $"b" === 1 && $"a" === $"b" && IsNotNull($"a") && IsNotNull($"b"))
        .select($"a".as("x"), $"b".as("y")).analyze
    val optimized = Optimize.execute(originalQuery)
    comparePlans(optimized, correctAnswer)
  }

  test("No inferred filter when constraint propagation is disabled") {
    withSQLConf(SQLConf.CONSTRAINT_PROPAGATION_ENABLED.key -> "false") {
      val originalQuery = testRelation.where($"a" === 1 && $"a" === $"b").analyze
      val optimized = Optimize.execute(originalQuery)
      comparePlans(optimized, originalQuery)
    }
  }

  test("constraints should be inferred from aliased literals") {
    val originalLeft = testRelation.subquery("left").as("left")
    val optimizedLeft = testRelation.subquery("left")
      .where(IsNotNull($"a") && $"a" <=> 2).as("left")

    val right = Project(Seq(Literal(2).as("two")),
      testRelation.subquery("right")).as("right")
    val condition = Some("left.a".attr === "right.two".attr)

    val original = originalLeft.join(right, Inner, condition)
    val correct = optimizedLeft.join(right, Inner, condition)

    comparePlans(Optimize.execute(original.analyze), correct.analyze)
  }

  test("SPARK-23405: left-semi equal-join should filter out null join keys on both sides") {
    val x = testRelation.subquery("x")
    val y = testRelation.subquery("y")
    testConstraintsAfterJoin(x, y, x.where(IsNotNull($"a")), y.where(IsNotNull($"a")), LeftSemi)
  }

  test("SPARK-21479: Outer join after-join filters push down to null-supplying side") {
    val x = testRelation.subquery("x")
    val y = testRelation.subquery("y")
    val condition = Some("x.a".attr === "y.a".attr)
    val originalQuery = x.join(y, LeftOuter, condition).where("x.a".attr === 2).analyze
    val left = x.where(IsNotNull($"a") && $"a" === 2)
    val right = y.where(IsNotNull($"a") && $"a" === 2)
    val correctAnswer = left.join(right, LeftOuter, condition).analyze
    val optimized = Optimize.execute(originalQuery)
    comparePlans(optimized, correctAnswer)
  }

  test("SPARK-21479: Outer join pre-existing filters push down to null-supplying side") {
    val x = testRelation.subquery("x")
    val y = testRelation.subquery("y")
    val condition = Some("x.a".attr === "y.a".attr)
    val originalQuery = x.join(y.where("y.a".attr > 5), RightOuter, condition).analyze
    val left = x.where(IsNotNull($"a") && $"a" > 5)
    val right = y.where(IsNotNull($"a") && $"a" > 5)
    val correctAnswer = left.join(right, RightOuter, condition).analyze
    val optimized = Optimize.execute(originalQuery)
    comparePlans(optimized, correctAnswer)
  }

  test("SPARK-21479: Outer join no filter push down to preserved side") {
    val x = testRelation.subquery("x")
    val y = testRelation.subquery("y")
    testConstraintsAfterJoin(
      x, y.where("a".attr === 1),
      x, y.where(IsNotNull($"a") && $"a" === 1),
      LeftOuter)
  }

  test("SPARK-23564: left anti join should filter out null join keys on right side") {
    val x = testRelation.subquery("x")
    val y = testRelation.subquery("y")
    testConstraintsAfterJoin(x, y, x, y.where(IsNotNull($"a")), LeftAnti)
  }

  test("SPARK-23564: left outer join should filter out null join keys on right side") {
    val x = testRelation.subquery("x")
    val y = testRelation.subquery("y")
    testConstraintsAfterJoin(x, y, x, y.where(IsNotNull($"a")), LeftOuter)
  }

  test("SPARK-23564: right outer join should filter out null join keys on left side") {
    val x = testRelation.subquery("x")
    val y = testRelation.subquery("y")
    testConstraintsAfterJoin(x, y, x.where(IsNotNull($"a")), y, RightOuter)
  }

  test("Constraints should be inferred from cast equality constraint(filter higher data type)") {
    val testRelation1 = LocalRelation($"a".int)
    val testRelation2 = LocalRelation($"b".long)
    val originalLeft = testRelation1.subquery("left")
    val originalRight = testRelation2.where($"b" === 1L).subquery("right")

    val left = testRelation1.where(IsNotNull($"a") && $"a".cast(LongType) === 1L)
      .subquery("left")
    val right = testRelation2.where(IsNotNull($"b") && $"b" === 1L).subquery("right")

    Seq(Some("left.a".attr.cast(LongType) === "right.b".attr),
      Some("right.b".attr === "left.a".attr.cast(LongType))).foreach { condition =>
      testConstraintsAfterJoin(originalLeft, originalRight, left, right, Inner, condition)
    }

    Seq(Some("left.a".attr === "right.b".attr.cast(IntegerType)),
      Some("right.b".attr.cast(IntegerType) === "left.a".attr)).foreach { condition =>
      testConstraintsAfterJoin(
        originalLeft,
        originalRight,
        testRelation1.where(IsNotNull($"a")).subquery("left"),
        right,
        Inner,
        condition)
    }
  }

  test("Constraints shouldn't be inferred from cast equality constraint(filter lower data type)") {
    val testRelation1 = LocalRelation($"a".int)
    val testRelation2 = LocalRelation($"b".long)
    val originalLeft = testRelation1.where($"a" === 1).subquery("left")
    val originalRight = testRelation2.subquery("right")

    val left = testRelation1.where(IsNotNull($"a") && $"a" === 1).subquery("left")
    val right = testRelation2.where(IsNotNull($"b")).subquery("right")

    Seq(Some("left.a".attr.cast(LongType) === "right.b".attr),
      Some("right.b".attr === "left.a".attr.cast(LongType))).foreach { condition =>
      testConstraintsAfterJoin(originalLeft, originalRight, left, right, Inner, condition)
    }

    // A narrowing cast can fail in ANSI mode, so it is not inferred.
    withSQLConf(SQLConf.ANSI_ENABLED.key -> "true") {
      Seq(Some("left.a".attr === "right.b".attr.cast(IntegerType)),
        Some("right.b".attr.cast(IntegerType) === "left.a".attr)).foreach { condition =>
        testConstraintsAfterJoin(originalLeft, originalRight, left, right, Inner, condition)
      }
    }
  }

  private def failable(e: Expression): Expression =
    Divide(Literal(10.0), e, EvalMode.ANSI) > 0.0

  test("SPARK-60985: a constraint that can fail is not inferred as a filter") {
    val relation = LocalRelation($"x".double, $"y".double)
    val query = relation.where($"x" === $"y" && failable($"y")).analyze
    val notNullAndQuery = IsNotNull($"x") && IsNotNull($"y") && $"x" === $"y" && failable($"y")

    comparePlans(InferFiltersFromConstraints(query), relation.where(notNullAndQuery).analyze)
    withSQLConf(SQLConf.LEGACY_INFER_FAILABLE_FILTERS_FROM_CONSTRAINTS.key -> "true") {
      comparePlans(
        InferFiltersFromConstraints(query),
        relation.where(failable($"x") && notNullAndQuery).analyze)
    }
  }

  test("SPARK-60985: a constraint that can fail is not inferred on the other side of a join") {
    val left = LocalRelation($"x".double)
    val right = LocalRelation($"y".double)
    testConstraintsAfterJoin(
      left.subquery("l"),
      right.where(failable($"y")).subquery("r"),
      left.where(IsNotNull($"x")).subquery("l"),
      right.where(IsNotNull($"y") && failable($"y")).subquery("r"),
      Inner,
      Some("l.x".attr === "r.y".attr))
  }

  test("SPARK-60985: a constraint on an uncorrelated subquery is inferred as a filter") {
    val relation = LocalRelation($"x".int, $"y".int)
    val values = LocalRelation($"a".int).select($"a")
    val scalar = ScalarSubquery(LocalRelation($"a".int).select(max($"a")))
    def in(e: Expression): Expression = InSubquery(Seq(e), ListQuery(values))
    val query = relation.where($"x" === $"y" && in($"y") && $"y" > scalar).analyze
    val inferred = InferFiltersFromConstraints(query).collectFirst {
      case Filter(condition, _) => splitConjunctivePredicates(condition)
    }.get
    assert(inferred.count(_.isInstanceOf[InSubquery]) === 2, inferred)
    assert(inferred.count(_.isInstanceOf[GreaterThan]) === 2, inferred)
  }

  test("SPARK-60985: an integral remainder by a non-zero constant is inferred") {
    val relation = LocalRelation($"x".int, $"y".int)
    def even(e: Expression): Expression = Remainder(e, Literal(2), EvalMode.ANSI) === 0
    val query = relation.where($"x" === $"y" && even($"y")).analyze
    comparePlans(
      InferFiltersFromConstraints(query),
      relation.where(IsNotNull($"x") && IsNotNull($"y") && $"x" === $"y" &&
        even($"y") && even($"x")).analyze)
  }

  test("SPARK-60985: a constraint whose canonical form is MultiCommutativeOp is inferred") {
    val relation = LocalRelation($"x".int, $"y".int)
    def anyOf(e: Expression): Expression = e === 1 || e === 2 || e === 3
    val query = relation.where($"x" === $"y" && anyOf($"y")).analyze
    comparePlans(
      InferFiltersFromConstraints(query),
      relation.where(IsNotNull($"x") && IsNotNull($"y") && $"x" === $"y" &&
        anyOf($"y") && anyOf($"x")).analyze)
  }

  test("SPARK-60882: a constraint that can not fail is inferred as a filter") {
    val legacy = NumericEvalContext(EvalMode.LEGACY)
    val ansi = NumericEvalContext(EvalMode.ANSI)
    val dates = LocalRelation($"x".date, $"y".date)
    val strings = LocalRelation($"x".string, $"y".string)
    val ints = LocalRelation($"x".int, $"y".int)
    val doubles = LocalRelation($"x".double, $"y".double)
    def infer(relation: LocalRelation, condition: Expression => Expression): LogicalPlan =
      InferFiltersFromConstraints(
        LikeSimplification(relation.where($"x" === $"y" && condition($"y")).analyze))
    def notNullAnd(relation: LocalRelation, condition: Expression => Expression): LogicalPlan =
      relation.where(IsNotNull($"x") && IsNotNull($"y") && $"x" === $"y" && condition($"y"))
    // `LIKE 'a%'` is simplified to a `StartsWith` before this rule runs.
    Seq[(LocalRelation, Expression => Expression, Expression => Expression)](
        (dates, Year(_) === 2024, Year(_) === 2024),
        (strings, Like(_, Literal("a%"), '\\'), StartsWith(_, Literal("a"))),
        (ints, Add(_, Literal(1), legacy) > 5, Add(_, Literal(1), legacy) > 5),
        (doubles, Multiply(_, Literal(1.1), ansi) > 100.0, Multiply(_, Literal(1.1), ansi) > 100.0)
    ).foreach { case (relation, condition, simplified) =>
      comparePlans(infer(relation, condition), relation.where(simplified($"x") && IsNotNull($"x") &&
        IsNotNull($"y") && $"x" === $"y" && simplified($"y")).analyze)
    }
    // Not `upper`, which can not fail but is not accepted, nor an addition in ANSI mode, which can.
    // Nor a cast of a DOUBLE to a string: on a row with `x` = 0.0 and `y` = -0.0, `x = y` holds,
    // but `CAST(x AS STRING)` is '0.0'. Nor `length` of a string of a trim collation: on a row
    // with `x` = 'a' and `y` = 'a ', `x = y` holds, but their lengths differ.
    val trimmed = LocalRelation(AttributeReference("x", StringType("UTF8_BINARY_RTRIM"))(),
      AttributeReference("y", StringType("UTF8_BINARY_RTRIM"))())
    // Nor a `try_cast` of a UDT to a string: on a row with vectors (0.0) and (-0.0), `x = y`
    // holds, but their strings differ.
    val udt = new TestUDT.MyDenseVectorUDT()
    val vectors = LocalRelation(AttributeReference("x", udt)(), AttributeReference("y", udt)())
    Seq[(LocalRelation, Expression => Expression)](
        (strings, Upper(_) === "A"), (ints, Add(_, Literal(1), ansi) > 5),
        (doubles, Cast(_, StringType) === "-0.0"), (trimmed, Length(_) === 2),
        (vectors, Cast(_, StringType, evalMode = EvalMode.TRY) === "(-0.0)")).foreach {
      case (relation, condition) =>
        comparePlans(infer(relation, condition), notNullAnd(relation, condition).analyze)
    }
  }

  test("SPARK-60882: a constraint through a widening of a decimal of a negative scale is " +
    "inferred") {
    withSQLConf(SQLConf.LEGACY_ALLOW_NEGATIVE_SCALE_OF_DECIMAL_ENABLED.key -> "true") {
      val relation = LocalRelation(AttributeReference("x", DecimalType(3, -2))(),
        AttributeReference("y", DecimalType(10, 0))())
      val query = relation.where($"x" === $"y" && $"y" > 5).analyze
      val Filter(condition, _) = InferFiltersFromConstraints(query)
      // Type coercion widens `x` to DECIMAL(10, 0), and `CAST(x AS DECIMAL(10, 0)) > 5` follows.
      assert(splitConjunctivePredicates(condition).exists {
        case GreaterThan(Cast(a: AttributeReference, _: DecimalType, _, _), _) => a.name == "x"
        case _ => false
      }, condition)
    }
  }

  test("SPARK-60882: a constraint that can not fail is inferred on the other side of a join") {
    val left = LocalRelation($"x".date)
    val right = LocalRelation($"y".date)
    testConstraintsAfterJoin(
      left.subquery("l"),
      right.where(Year($"y") === 2024).subquery("r"),
      left.where(IsNotNull($"x") && Year($"x") === 2024).subquery("l"),
      right.where(IsNotNull($"y") && Year($"y") === 2024).subquery("r"),
      Inner,
      Some("l.x".attr === "r.y".attr))
  }

  test("SPARK-36978: IsNotNull constraints on structs should apply at the member field " +
    "instead of the root level nested type") {
    val structTestRelation = LocalRelation($"a".struct(StructType(
      StructField("cstruct", StructType(StructField("cstr", StringType) :: Nil))
        :: StructField("cint", IntegerType) :: Nil)))
    val originalQuery = structTestRelation
      .where($"a".getField("cint") === 1 && $"a".getField("cstruct")
        .getField("cstr") === "abc").analyze

    val correctAnswer = structTestRelation
      .where(IsNotNull($"a".getField("cint"))
        && IsNotNull($"a".getField("cstruct").getField("cstr"))
        && $"a".getField("cint") === 1 && $"a".getField("cstruct").getField("cstr") === "abc")
      .analyze
    val optimized = Optimize.execute(originalQuery)
    comparePlans(optimized, correctAnswer)
  }

  test("SPARK-36978: IsNotNull constraints on array of structs should apply at the member field " +
    "instead of the root level nested type") {
    val intStructField = StructField("cint", IntegerType)
    val arrayOfStructsTestRelation = LocalRelation($"a".array(StructType(intStructField :: Nil)))
    val getArrayStructField = GetArrayStructFields($"a", intStructField, 0, 1, containsNull = true)
    val originalQuery = arrayOfStructsTestRelation
      .where(GetArrayItem(getArrayStructField, 0) === 1).analyze

    val correctAnswer = arrayOfStructsTestRelation
      .where(IsNotNull(getArrayStructField) && GetArrayItem(getArrayStructField, 0) === 1)
      .analyze
    val optimized = Optimize.execute(originalQuery)
    comparePlans(optimized, correctAnswer)
  }

  test("SPARK-36978: IsNotNull constraints for nested types apply to the ExtractValue which " +
    "only has ExtractValue/Attribute children") {
    val arrayTestRelation = LocalRelation($"a".array(IntegerType))
    val originalQuery = arrayTestRelation
      .where(GetArrayItem(ArrayDistinct($"a"), 0) === 1 && GetArrayItem($"a", 1) === 1)
      .analyze

    val correctAnswer = arrayTestRelation
      .where(IsNotNull($"a") && GetArrayItem(ArrayDistinct($"a"), 0) === 1
        && GetArrayItem($"a", 1) === 1)
      .analyze
    val optimized = Optimize.execute(originalQuery)
    comparePlans(optimized, correctAnswer)
  }

  test("SPARK-43095: Avoid Once strategy's idempotence is broken for batch: Infer Filters") {
    val x = testRelation.as("x")
    val y = testRelation.as("y")
    val z = testRelation.as("z")

    // Removes EqualNullSafe when constructing candidate constraints
    comparePlans(
      InferFiltersFromConstraints(x.select($"x.a", $"x.a".as("xa"))
        .where($"xa" <=> $"x.a" && $"xa" === $"x.a").analyze),
      x.select($"x.a", $"x.a".as("xa"))
        .where($"xa".isNotNull && $"x.a".isNotNull && $"xa" <=> $"x.a" && $"xa" === $"x.a").analyze)

    // Once strategy's idempotence is not broken
    val originalQuery =
      x.join(y, condition = Some($"x.a" === $"y.a"))
        .select($"x.a", $"x.a".as("xa")).as("xy")
        .join(z, condition = Some($"xy.a" === $"z.a")).analyze

    val correctAnswer =
      x.where($"a".isNotNull).join(y.where($"a".isNotNull), condition = Some($"x.a" === $"y.a"))
        .select($"x.a", $"x.a".as("xa")).as("xy")
        .join(z.where($"a".isNotNull), condition = Some($"xy.a" === $"z.a")).analyze

    val optimizedQuery = InferFiltersFromConstraints(originalQuery)
    comparePlans(optimizedQuery, correctAnswer)
    comparePlans(InferFiltersFromConstraints(optimizedQuery), correctAnswer)
  }

  test("correlated range predicate in left-outer join ON clause is pushed to right side " +
    "when left attr is bound to a literal") {
    testRangePredicatePushDown(LeftOuter)
  }

  test("correlated range predicate in inner join ON clause is pushed to right side " +
    "when left attr is bound to a literal") {
    testRangePredicatePushDown(Inner)
  }

  private def testRangePredicatePushDown(joinType: JoinType): Unit = {
    // Simulates: SELECT * FROM a JOIN b ON a.key = b.key AND b.v >= a.k AND b.v <= a.k + 10
    // WHERE a.k = 5
    // The constant binding a.k = 5 should be substituted into the range predicates, producing
    // b.v >= 5 and b.v <= (5 + 10) that get pushed into the right scan.
    val left = LocalRelation($"k".int, $"key".int).subquery("a")
    val right = LocalRelation($"v".int, $"key".int).subquery("b")

    val joinCond = ("a.key".attr === "b.key".attr) &&
      ("b.v".attr >= "a.k".attr) &&
      ("b.v".attr <= ("a.k".attr + 10))

    val originalQuery = left
      .where("a.k".attr === 5)
      .join(right, joinType, Some(joinCond))
      .analyze

    val optimized = Optimize.execute(originalQuery)

    // The optimized right side must carry the inferred range filters.
    var joinFound = false
    optimized.foreach {
      case Join(_, rightChild, `joinType`, _, _) =>
        joinFound = true
        val rightFilters = rightChild.collect { case Filter(cond, _) => cond }
        val allConds = rightFilters.flatMap(splitConjunctivePredicates)
        // b.v >= 5 and b.v <= (5 + 10) must appear (arithmetic not folded without ConstantFolding)
        assert(allConds.exists {
          case GreaterThanOrEqual(_: Attribute, Literal(v, IntegerType)) => v == 5
          case _ => false
        }, s"Expected b.v >= 5 in right filter; found: $allConds")
        assert(allConds.exists {
          case LessThanOrEqual(_: Attribute, add: Add)
            if add.left == Literal(5, IntegerType) && add.right == Literal(10, IntegerType) => true
          case _ => false
        }, s"Expected b.v <= (5 + 10) in right filter; found: $allConds")
      case _ =>
    }
    assert(joinFound, "Expected a Join node in the optimized plan")
  }

  test("SPARK-59719: infer IsNotNull through null-intolerant CheckOverflow") {
    val dt = DecimalType(18, 0)
    val decimalRelation = LocalRelation($"d".decimal(18, 0))
    val query = decimalRelation
      .where(CheckOverflow($"d", dt, nullOnOverflow = true) > Literal.create(Decimal(0), dt))
      .analyze
    val optimized = Optimize.execute(query)
    val correctAnswer = decimalRelation
      .where(IsNotNull($"d") &&
        (CheckOverflow($"d", dt, nullOnOverflow = true) > Literal.create(Decimal(0), dt)))
      .analyze
    comparePlans(optimized, correctAnswer)
  }

}
