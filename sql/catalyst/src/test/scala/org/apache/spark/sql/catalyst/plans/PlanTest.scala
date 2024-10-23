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
import org.apache.spark.sql.catalyst.analysis.SimpleAnalyzer
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.CodegenObjectFactoryMode
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

  protected def normalizeExprIds(plan: LogicalPlan): LogicalPlan =
    NormalizePlan.normalizeExprIds(plan)

  protected def rewriteNameFromAttrNullability(plan: LogicalPlan): LogicalPlan = {
    plan.transformAllExpressions {
      case a @ AttributeReference(name, _, false, _) =>
        a.copy(name = s"*$name")(exprId = a.exprId, qualifier = a.qualifier)
    }
  }

  protected def normalizePlan(plan: LogicalPlan): LogicalPlan =
    NormalizePlan.normalizePlan(plan)

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
