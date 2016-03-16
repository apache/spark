package org.apache.spark.sql.catalyst.optimizer

import org.apache.spark.sql.catalyst.analysis.EliminateSubQueries
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.rules._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.dsl.expressions._

class TransitiveClosureSuite extends PlanTest {
  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("Subqueries", Once,
        EliminateSubQueries) ::
      Batch("Filter Pushdown", FixedPoint(4),
        SamplePushDown,
        CombineFilters,
        PushPredicateThroughProject,
        BooleanSimplification,
        TransitiveClosure,
        PushPredicateThroughJoin,
        PushPredicateThroughGenerate,
        ColumnPruning,
        ProjectCollapsing) :: Nil
  }

  val testRelation = LocalRelation('a.int, 'b.int, 'c.int)

  val testRelation1 = LocalRelation('d.int)

  test("Binary comparison in where clause together with join condition") {
    val x = testRelation.subquery('x)
    val y = testRelation1.subquery('y)

    val originalQuery = {
      x.join(y)
        .where("x.a".attr === "y.d".attr && "x.a".attr >= 1)
    }

    val optimized = Optimize.execute(originalQuery.analyze)
    val left = testRelation.where('a >= 1)
    val right = testRelation1.where('d >= 1)
    val correctAnswer =
      left.join(right, condition = Some("a".attr === "d".attr)).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("Not and mixed join condition with where clause filter") {
    val x = testRelation.subquery('x)
    val y = testRelation1.subquery('y)

    val originalQuery = {
      x.join(y, condition = Option("x.a".attr === "y.d".attr))
        .where("x.a".attr !== 1)
    }

    val optimized = Optimize.execute(originalQuery.analyze)
    val left = testRelation.where('a !== 1)
    val right = testRelation1.where('d !== 1)
    val correctAnswer =
      left.join(right, condition = Some("a".attr === "d".attr)).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("In and all conditions are join conditions") {
    val x = testRelation.subquery('x)
    val y = testRelation1.subquery('y)

    val originalQuery = {
      x.join(y, condition = Option("x.a".attr === "y.d".attr && ("x.a".attr in (1, 2))))
    }

    val optimized = Optimize.execute(originalQuery.analyze)
    val left = testRelation.where('a in (1, 2))
    val right = testRelation1.where('d in (1, 2))
    val correctAnswer =
      left.join(right, condition = Some("a".attr === "d".attr)).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("Or condition") {
    val x = testRelation.subquery('x)
    val y = testRelation1.subquery('y)

    val originalQuery = {
      x.join(y, condition = Option("x.a".attr === "y.d".attr))
        .where("x.a".attr === 1 || "x.a".attr === 2)
    }

    val optimized = Optimize.execute(originalQuery.analyze)
    val left = testRelation.where('a === 1 || 'a === 2)
    val right = testRelation1.where('d === 1 || 'd === 2)
    val correctAnswer =
      left.join(right, condition = Some("a".attr === "d".attr)).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("Mixed join and where clause conditions") {
    val x = testRelation.subquery('x)
    val y = testRelation1.subquery('y)

    val originalQuery = {
      x.join(y, condition = Option("x.a".attr === "y.d".attr && "y.d".attr === 2))
        .where("x.a".attr === 1)
    }

    val optimized = Optimize.execute(originalQuery.analyze)
    val left = testRelation.where('a === 1 && 'a === 2)
    val right = testRelation1.where('d === 1 && 'd === 2)
    val correctAnswer =
      left.join(right, condition = Some("a".attr === "d".attr)).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("Left join works one way") {
    val x = testRelation.subquery('x)
    val y = testRelation1.subquery('y)

    val originalQuery = {
      x.join(y, LeftOuter, Option("x.a".attr === "y.d".attr))
        .where("x.a".attr === 1 && "y.d".attr === 2)
    }

    val optimized = Optimize.execute(originalQuery.analyze)
    val left = testRelation.where('a === 1)
    val right = testRelation1
    val correctAnswer =
      left.join(right, LeftOuter, Some("a".attr === "d".attr))
        .where("d".attr === 1 && "d".attr === 2)
        .analyze

    comparePlans(optimized, correctAnswer)
  }

  test("Right join works one way") {
    val x = testRelation.subquery('x)
    val y = testRelation1.subquery('y)

    val originalQuery = {
      x.join(y, RightOuter, Option("x.a".attr === "y.d".attr))
        .where("y.d".attr === 2 && "x.a".attr === 1)
    }

    val optimized = Optimize.execute(originalQuery.analyze)
    val left = testRelation
    val right = testRelation1.where('d === 2)
    val correctAnswer =
      left.join(right, RightOuter, Some("a".attr === "d".attr))
        .where("a".attr === 2 && "a".attr === 1)
        .analyze

    comparePlans(optimized, correctAnswer)
  }

  test("Full join does not do anything") {
    val x = testRelation.subquery('x)
    val y = testRelation1.subquery('y)

    val originalQuery = {
      x.join(y, FullOuter, Option("x.a".attr === "y.d".attr && "y.d".attr === 2))
        .where("x.a".attr === 1)
    }

    val optimized = Optimize.execute(originalQuery.analyze)
    val left = testRelation
    val right = testRelation1
    val correctAnswer =
      left.join(right, FullOuter, Some("a".attr === "d".attr && "d".attr === 2))
        .where("a".attr === 1).analyze

    comparePlans(optimized, correctAnswer)
  }

}
