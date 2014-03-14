package org.apache.spark.sql
package catalyst
package optimizer

import expressions._
import plans.logical._
import rules._
import util._

import dsl._
import dsl.expressions._

class FilterPushdownSuite extends OptimizerTest {

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("Subqueries", Once,
        EliminateSubqueries) ::
      Batch("Filter Pushdown", Once,
        EliminateSubqueries,
        CombineFilters,
        PushPredicateThroughProject,
        PushPredicateThroughInnerJoin) :: Nil
  }

  val testRelation = LocalRelation('a.int, 'b.int, 'c.int)

  // This test already passes.
  test("eliminate subqueries") {
    val originalQuery =
      testRelation
        .subquery('y)
        .select('a)

    val optimized = Optimize(originalQuery.analyze)
    val correctAnswer =
      testRelation
        .select('a.attr)
        .analyze

    comparePlans(optimized, correctAnswer)
  }

  // After this line is unimplemented.
  test("simple push down") {
    val originalQuery =
      testRelation
        .select('a)
        .where('a === 1)

    val optimized = Optimize(originalQuery.analyze)
    val correctAnswer =
      testRelation
        .where('a === 1)
        .select('a)
        .analyze

    comparePlans(optimized, correctAnswer)
  }

  test("can't push without rewrite") {
    val originalQuery =
      testRelation
        .select('a + 'b as 'e)
        .where('e === 1)
        .analyze

    val optimized = Optimize(originalQuery.analyze)
    val correctAnswer =
      testRelation
        .where('a + 'b === 1)
        .select('a + 'b as 'e)
        .analyze

    comparePlans(optimized, correctAnswer)
  }

  test("filters: combines filters") {
    val originalQuery = testRelation
      .select('a)
      .where('a === 1)
      .where('a === 2)

    val optimized = Optimize(originalQuery.analyze)
    val correctAnswer =
      testRelation
        .where('a === 1 && 'a === 2)
        .select('a).analyze


    comparePlans(optimized, correctAnswer)
  }


  test("joins: push to either side") {
    val x = testRelation.subquery('x)
    val y = testRelation.subquery('y)

    val originalQuery = {
      x.join(y)
        .where("x.b".attr === 1)
        .where("y.b".attr === 2)
    }

    val optimized = Optimize(originalQuery.analyze)
    val left = testRelation.where('b === 1)
    val right = testRelation.where('b === 2)
    val correctAnswer =
      left.join(right).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("joins: push to one side") {
    val x = testRelation.subquery('x)
    val y = testRelation.subquery('y)

    val originalQuery = {
      x.join(y)
        .where("x.b".attr === 1)
    }

    val optimized = Optimize(originalQuery.analyze)
    val left = testRelation.where('b === 1)
    val right = testRelation
    val correctAnswer =
      left.join(right).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("joins: rewrite filter to push to either side") {
    val x = testRelation.subquery('x)
    val y = testRelation.subquery('y)

    val originalQuery = {
      x.join(y)
        .where("x.b".attr === 1 && "y.b".attr === 2)
    }

    val optimized = Optimize(originalQuery.analyze)
    val left = testRelation.where('b === 1)
    val right = testRelation.where('b === 2)
    val correctAnswer =
      left.join(right).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("joins: can't push down") {
    val x = testRelation.subquery('x)
    val y = testRelation.subquery('y)

    val originalQuery = {
      x.join(y, condition = Some("x.b".attr === "y.b".attr))
    }
    val optimized = Optimize(originalQuery.analyze)

    comparePlans(optimizer.EliminateSubqueries(originalQuery.analyze), optimized)
  }

  test("joins: conjunctive predicates") {
    val x = testRelation.subquery('x)
    val y = testRelation.subquery('y)

    val originalQuery = {
      x.join(y)
        .where(("x.b".attr === "y.b".attr) && ("x.a".attr === 1) && ("y.a".attr === 1))
    }

    val optimized = Optimize(originalQuery.analyze)
    val left = testRelation.where('a === 1).subquery('x)
    val right = testRelation.where('a === 1).subquery('y)
    val correctAnswer =
      left.join(right, condition = Some("x.b".attr === "y.b".attr))
        .analyze

    comparePlans(optimized, optimizer.EliminateSubqueries(correctAnswer))
  }

  test("joins: conjunctive predicates #2") {
    val x = testRelation.subquery('x)
    val y = testRelation.subquery('y)

    val originalQuery = {
      x.join(y)
        .where(("x.b".attr === "y.b".attr) && ("x.a".attr === 1))
    }

    val optimized = Optimize(originalQuery.analyze)
    val left = testRelation.where('a === 1).subquery('x)
    val right = testRelation.subquery('y)
    val correctAnswer =
      left.join(right, condition = Some("x.b".attr === "y.b".attr))
        .analyze

    comparePlans(optimized, optimizer.EliminateSubqueries(correctAnswer))
  }

  test("joins: conjunctive predicates #3") {
    val x = testRelation.subquery('x)
    val y = testRelation.subquery('y)
    val z = testRelation.subquery('z)

    val originalQuery = {
      z.join(x.join(y))
        .where(("x.b".attr === "y.b".attr) && ("x.a".attr === 1) && ("z.a".attr >= 3) && ("z.a".attr === "x.b".attr))
    }

    val optimized = Optimize(originalQuery.analyze)
    val lleft = testRelation.where('a >= 3).subquery('z)
    val left = testRelation.where('a === 1).subquery('x)
    val right = testRelation.subquery('y)
    val correctAnswer =
      lleft.join(
        left.join(right, condition = Some("x.b".attr === "y.b".attr)),
          condition = Some("z.a".attr === "x.b".attr))
        .analyze

    comparePlans(optimized, optimizer.EliminateSubqueries(correctAnswer))
  }
}