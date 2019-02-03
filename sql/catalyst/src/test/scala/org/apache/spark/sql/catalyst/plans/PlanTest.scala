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

package org.apache.spark.sql.catalyst.plans

import org.scalactic.source
import org.scalatest.Suite
import org.scalatest.Tag

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.analysis.SimpleAnalyzer
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.CodegenObjectFactoryMode
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.util._
import org.apache.spark.sql.internal.SQLConf

/**
 * Provides helper methods for comparing plans.
 */
trait PlanTest extends SparkFunSuite with PlanTestBase

trait CodegenInterpretedPlanTest extends PlanTest {

  override protected def test(
      testName: String,
      testTags: Tag*)(testFun: => Any)(implicit pos: source.Position): Unit = {
    val codegenMode = CodegenObjectFactoryMode.CODEGEN_ONLY.toString
    val interpretedMode = CodegenObjectFactoryMode.NO_CODEGEN.toString

    withSQLConf(SQLConf.CODEGEN_FACTORY_MODE.key -> codegenMode) {
      super.test(testName + " (codegen path)", testTags: _*)(testFun)(pos)
    }
    withSQLConf(SQLConf.CODEGEN_FACTORY_MODE.key -> interpretedMode) {
      super.test(testName + " (interpreted path)", testTags: _*)(testFun)(pos)
    }
  }
}

/**
 * Provides helper methods for comparing plans, but without the overhead of
 * mandating a FunSuite.
 */
trait PlanTestBase extends PredicateHelper with SQLHelper { self: Suite =>

  // TODO(gatorsmile): remove this from PlanTest and all the analyzer rules
  protected def conf = SQLConf.get

  /**
   * Since attribute references are given globally unique ids during analysis,
   * we must normalize them to check if two different queries are identical.
   */
  protected def normalizeExprIds(plan: LogicalPlan) = {
    plan transformAllExpressions {
      case s: ScalarSubquery =>
        s.copy(exprId = ExprId(0))
      case e: Exists =>
        e.copy(exprId = ExprId(0))
      case l: ListQuery =>
        l.copy(exprId = ExprId(0))
      case a: AttributeReference =>
        AttributeReference(a.name, a.dataType, a.nullable)(exprId = ExprId(0))
      case a: Alias =>
        Alias(a.child, a.name)(exprId = ExprId(0))
      case ae: AggregateExpression =>
        ae.copy(resultId = ExprId(0))
      case lv: NamedLambdaVariable =>
        lv.copy(exprId = ExprId(0), value = null)
    }
  }

  /**
   * Normalizes plans:
   * - Filter the filter conditions that appear in a plan. For instance,
   *   ((expr 1 && expr 2) && expr 3), (expr 1 && expr 2 && expr 3), (expr 3 && (expr 1 && expr 2)
   *   etc., will all now be equivalent.
   * - Sample the seed will replaced by 0L.
   * - Join conditions will be resorted by hashCode.
   */
  protected def normalizePlan(plan: LogicalPlan): LogicalPlan = {
    plan transform {
      case Filter(condition: Expression, child: LogicalPlan) =>
        Filter(splitConjunctivePredicates(condition).map(rewriteEqual).sortBy(_.hashCode())
          .reduce(And), child)
      case sample: Sample =>
        sample.copy(seed = 0L)
      case Join(left, right, joinType, condition, hint) if condition.isDefined =>
        val newCondition =
          splitConjunctivePredicates(condition.get).map(rewriteEqual).sortBy(_.hashCode())
            .reduce(And)
        Join(left, right, joinType, Some(newCondition), hint)
    }
  }

  /**
   * Rewrite [[EqualTo]] and [[EqualNullSafe]] operator to keep order. The following cases will be
   * equivalent:
   * 1. (a = b), (b = a);
   * 2. (a <=> b), (b <=> a).
   */
  private def rewriteEqual(condition: Expression): Expression = condition match {
    case eq @ EqualTo(l: Expression, r: Expression) =>
      Seq(l, r).sortBy(_.hashCode()).reduce(EqualTo)
    case eq @ EqualNullSafe(l: Expression, r: Expression) =>
      Seq(l, r).sortBy(_.hashCode()).reduce(EqualNullSafe)
    case _ => condition // Don't reorder.
  }

  /** Fails the test if the two plans do not match */
  protected def comparePlans(
      plan1: LogicalPlan,
      plan2: LogicalPlan,
      checkAnalysis: Boolean = true): Unit = {
    if (checkAnalysis) {
      // Make sure both plan pass checkAnalysis.
      SimpleAnalyzer.checkAnalysis(plan1)
      SimpleAnalyzer.checkAnalysis(plan2)
    }

    val normalized1 = normalizePlan(normalizeExprIds(plan1))
    val normalized2 = normalizePlan(normalizeExprIds(plan2))
    if (normalized1 != normalized2) {
      fail(
        s"""
          |== FAIL: Plans do not match ===
          |${sideBySide(normalized1.treeString, normalized2.treeString).mkString("\n")}
         """.stripMargin)
    }
  }

  /** Fails the test if the two expressions do not match */
  protected def compareExpressions(e1: Expression, e2: Expression): Unit = {
    comparePlans(Filter(e1, OneRowRelation()), Filter(e2, OneRowRelation()), checkAnalysis = false)
  }

  /** Fails the test if the join order in the two plans do not match */
  protected def compareJoinOrder(plan1: LogicalPlan, plan2: LogicalPlan) {
    val normalized1 = normalizePlan(normalizeExprIds(plan1))
    val normalized2 = normalizePlan(normalizeExprIds(plan2))
    if (!sameJoinPlan(normalized1, normalized2)) {
      fail(
        s"""
           |== FAIL: Plans do not match ===
           |${sideBySide(normalized1.treeString, normalized2.treeString).mkString("\n")}
         """.stripMargin)
    }
  }

  /** Consider symmetry for joins when comparing plans. */
  private def sameJoinPlan(plan1: LogicalPlan, plan2: LogicalPlan): Boolean = {
    (plan1, plan2) match {
      case (j1: Join, j2: Join) =>
        (sameJoinPlan(j1.left, j2.left) && sameJoinPlan(j1.right, j2.right)
          && j1.hint.leftHint == j2.hint.leftHint && j1.hint.rightHint == j2.hint.rightHint) ||
          (sameJoinPlan(j1.left, j2.right) && sameJoinPlan(j1.right, j2.left)
            && j1.hint.leftHint == j2.hint.rightHint && j1.hint.rightHint == j2.hint.leftHint)
      case (p1: Project, p2: Project) =>
        p1.projectList == p2.projectList && sameJoinPlan(p1.child, p2.child)
      case _ =>
        plan1 == plan2
    }
  }
}
