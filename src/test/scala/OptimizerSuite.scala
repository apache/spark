package catalyst

import org.scalatest.FunSuite

import analysis._
import expressions._
import optimizer.Optimize
import plans.logical._
import types._
import util._

/* Implicit conversions for creating query plans */
import dsl._

class OptimizerSuite extends FunSuite {
  // Test relations.  Feel free to create more.
  val testRelation = LocalRelation('a.int, 'b.int, 'c.int)

  // Helper functions for comparing plans.

  /**
   * Since attribute references are given globally unique ids during analysis we must normalize them to check if two
   * different queries are identical.
   */
  protected def normalizeExprIds(plan: LogicalPlan) = {
    val minId = plan.flatMap(_.expressions.flatMap(_.references).map(_.exprId.id)).min
    plan transformAllExpressions {
      case a: AttributeReference =>
        AttributeReference(a.name, a.dataType, a.nullable)(exprId = ExprId(a.exprId.id - minId))
    }
  }

  /** Fails the test if the two plans do not match */
  protected def comparePlans(plan1: LogicalPlan, plan2: LogicalPlan) {
    val normalized1 = normalizeExprIds(plan1)
    val normalized2 = normalizeExprIds(plan2)
    if(normalized1 != normalized2)
      fail(
        s"""
          |== FAIL: Plans do not match ===
          |${sideBySide(normalized1.treeString, normalized2.treeString).mkString("\n")}
        """.stripMargin)
  }

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

    /* Your code here */
    fail("not implemented")
  }

  test("joins: push to either side") {
    val x = testRelation.subquery('x)
    val y = testRelation.subquery('y)

    val originalQuery = {
      x.join(y)
        .where("x.b".attr === 1)
        .where("y.b".attr === 2)
        .analyze
    }

    fail("not implemented")
  }

  test("joins: can't push down") {
    val x = testRelation.subquery('x)
    val y = testRelation.subquery('y)

    val originalQuery = {
      x.join(y)
        .where("x.b".attr === "y.b".attr)
        .analyze
    }
    val optimized = Optimize(originalQuery.analyze)

    comparePlans(optimizer.EliminateSubqueries(originalQuery), optimized)
  }

  test("joins: conjunctive predicates") {
    val x = testRelation.subquery('x)
    val y = testRelation.subquery('y)

    val originalQuery = {
      x.join(y)
        .where(("x.b".attr === "y.b".attr) && ("x.a".attr === 1) && ("y.a".attr === 1))
        .analyze
    }

    fail("not implemented")
  }
}