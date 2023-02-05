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
import org.apache.spark.sql.catalyst.SQLConfHelper
import org.apache.spark.sql.catalyst.analysis.{GetViewColumnByNameAndOrdinal, SimpleAnalyzer}
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

    super.test(testName + " (codegen path)", testTags: _*)(
      withSQLConf(SQLConf.CODEGEN_FACTORY_MODE.key -> codegenMode) { testFun })(pos)
    super.test(testName + " (interpreted path)", testTags: _*)(
      withSQLConf(SQLConf.CODEGEN_FACTORY_MODE.key -> interpretedMode) { testFun })(pos)
  }

  protected def testFallback(
      testName: String,
      testTags: Tag*)(testFun: => Any)(implicit pos: source.Position): Unit = {
    val codegenMode = CodegenObjectFactoryMode.FALLBACK.toString
    super.test(testName + " (codegen fallback mode)", testTags: _*)(
      withSQLConf(SQLConf.CODEGEN_FACTORY_MODE.key -> codegenMode) { testFun })(pos)
  }
}

/**
 * Provides helper methods for comparing plans, but without the overhead of
 * mandating a FunSuite.
 */
trait PlanTestBase extends PredicateHelper with SQLHelper with SQLConfHelper { self: Suite =>

  /**
   * Since attribute references are given globally unique ids during analysis,
   * we must normalize them to check if two different queries are identical.
   */
  protected def normalizeExprIds(plan: LogicalPlan): LogicalPlan = {
    plan transformAllExpressions {
      case s: ScalarSubquery =>
        s.copy(plan = normalizeExprIds(s.plan), exprId = ExprId(0))
      case s: LateralSubquery =>
        s.copy(plan = normalizeExprIds(s.plan), exprId = ExprId(0))
      case e: Exists =>
        e.copy(plan = normalizeExprIds(e.plan), exprId = ExprId(0))
      case l: ListQuery =>
        l.copy(
          plan = normalizeExprIds(l.plan),
          exprId = ExprId(0),
          childOutputs = l.childOutputs.map(_.withExprId(ExprId(0))))
      case a: AttributeReference =>
        AttributeReference(a.name, a.dataType, a.nullable)(exprId = ExprId(0))
      case OuterReference(a: AttributeReference) =>
        OuterReference(AttributeReference(a.name, a.dataType, a.nullable)(exprId = ExprId(0)))
      case a: Alias =>
        Alias(a.child, a.name)(exprId = ExprId(0))
      case OuterReference(a: Alias) =>
        OuterReference(Alias(a.child, a.name)(exprId = ExprId(0)))
      case ae: AggregateExpression =>
        ae.copy(resultId = ExprId(0))
      case lv: NamedLambdaVariable =>
        lv.copy(exprId = ExprId(0), value = null)
      case udf: PythonUDF =>
        udf.copy(resultId = ExprId(0))
    }
  }

  protected def rewriteNameFromAttrNullability(plan: LogicalPlan): LogicalPlan = {
    plan.transformAllExpressions {
      case a @ AttributeReference(name, _, false, _) =>
        a.copy(name = s"*$name")(exprId = a.exprId, qualifier = a.qualifier)
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
        Filter(splitConjunctivePredicates(condition).map(rewriteBinaryComparison)
          .sortBy(_.hashCode()).reduce(And), child)
      case sample: Sample =>
        sample.copy(seed = 0L)
      case Join(left, right, joinType, condition, hint) if condition.isDefined =>
        val newJoinType = joinType match {
          case ExistenceJoin(a: Attribute) =>
            val newAttr = AttributeReference(a.name, a.dataType, a.nullable)(exprId = ExprId(0))
            ExistenceJoin(newAttr)
          case other => other
        }

        val newCondition =
          splitConjunctivePredicates(condition.get).map(rewriteBinaryComparison)
            .sortBy(_.hashCode()).reduce(And)
        Join(left, right, newJoinType, Some(newCondition), hint)
      case Project(projectList, child) =>
        val projList = projectList.map { e =>
          e.transformUp {
            case g: GetViewColumnByNameAndOrdinal => g.copy(viewDDL = None)
          }
        }.asInstanceOf[Seq[NamedExpression]]
        Project(projList, child)
      case c: KeepAnalyzedQuery => c.storeAnalyzedQuery()
    }
  }

  /**
   * Rewrite [[BinaryComparison]] operator to keep order. The following cases will be
   * equivalent:
   * 1. (a = b), (b = a);
   * 2. (a <=> b), (b <=> a).
   * 3. (a > b), (b < a)
   */
  private def rewriteBinaryComparison(condition: Expression): Expression = condition match {
    case EqualTo(l, r) => Seq(l, r).sortBy(_.hashCode()).reduce(EqualTo)
    case EqualNullSafe(l, r) => Seq(l, r).sortBy(_.hashCode()).reduce(EqualNullSafe)
    case GreaterThan(l, r) if l.hashCode() > r.hashCode() => LessThan(r, l)
    case LessThan(l, r) if l.hashCode() > r.hashCode() => GreaterThan(r, l)
    case GreaterThanOrEqual(l, r) if l.hashCode() > r.hashCode() => LessThanOrEqual(r, l)
    case LessThanOrEqual(l, r) if l.hashCode() > r.hashCode() => GreaterThanOrEqual(r, l)
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
          |${sideBySide(
            rewriteNameFromAttrNullability(normalized1).treeString,
            rewriteNameFromAttrNullability(normalized2).treeString).mkString("\n")}
         """.stripMargin)
    }
  }

  /** Fails the test if the two expressions do not match */
  protected def compareExpressions(e1: Expression, e2: Expression): Unit = {
    comparePlans(Filter(e1, OneRowRelation()), Filter(e2, OneRowRelation()), checkAnalysis = false)
  }
}
