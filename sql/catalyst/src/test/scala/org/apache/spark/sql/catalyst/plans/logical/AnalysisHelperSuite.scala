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

package org.apache.spark.sql.catalyst.plans.logical

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.expressions.{Alias, Expression, Literal, NamedExpression}


class AnalysisHelperSuite extends SparkFunSuite {

  private var invocationCount = 0
  private val function: PartialFunction[LogicalPlan, LogicalPlan] = {
    case p: Project =>
      invocationCount += 1
      p
  }

  private val exprFunction: PartialFunction[Expression, Expression] = {
    case e: Literal =>
      invocationCount += 1
      Literal.TrueLiteral
  }

  private def projectExprs: Seq[NamedExpression] = Alias(Literal.TrueLiteral, "A")() :: Nil

  test("setAnalyze is recursive") {
    val plan = Project(Nil, LocalRelation())
    plan.setAnalyzed()
    assert(!plan.exists(!_.analyzed))
  }

  test("resolveOperator runs on operators recursively") {
    invocationCount = 0
    val plan = Project(Nil, Project(Nil, LocalRelation()))
    plan.resolveOperators(function)
    assert(invocationCount === 2)
  }

  test("resolveOperatorsDown runs on operators recursively") {
    invocationCount = 0
    val plan = Project(Nil, Project(Nil, LocalRelation()))
    plan.resolveOperatorsDown(function)
    assert(invocationCount === 2)
  }

  test("resolveExpressions runs on operators recursively") {
    invocationCount = 0
    val plan = Project(projectExprs, Project(projectExprs, LocalRelation()))
    plan.resolveExpressions(exprFunction)
    assert(invocationCount === 2)
  }

  test("resolveOperator skips all ready resolved plans") {
    invocationCount = 0
    val plan = Project(Nil, Project(Nil, LocalRelation()))
    plan.setAnalyzed()
    plan.resolveOperators(function)
    assert(invocationCount === 0)
  }

  test("resolveOperatorsDown skips all ready resolved plans") {
    invocationCount = 0
    val plan = Project(Nil, Project(Nil, LocalRelation()))
    plan.setAnalyzed()
    plan.resolveOperatorsDown(function)
    assert(invocationCount === 0)
  }

  test("resolveExpressions skips all ready resolved plans") {
    invocationCount = 0
    val plan = Project(projectExprs, Project(projectExprs, LocalRelation()))
    plan.setAnalyzed()
    plan.resolveExpressions(exprFunction)
    assert(invocationCount === 0)
  }

  test("resolveOperator skips partially resolved plans") {
    invocationCount = 0
    val plan1 = Project(Nil, LocalRelation())
    val plan2 = Project(Nil, plan1)
    plan1.setAnalyzed()
    plan2.resolveOperators(function)
    assert(invocationCount === 1)
  }

  test("resolveOperatorsDown skips partially resolved plans") {
    invocationCount = 0
    val plan1 = Project(Nil, LocalRelation())
    val plan2 = Project(Nil, plan1)
    plan1.setAnalyzed()
    plan2.resolveOperatorsDown(function)
    assert(invocationCount === 1)
  }

  test("resolveExpressions skips partially resolved plans") {
    invocationCount = 0
    val plan1 = Project(projectExprs, LocalRelation())
    val plan2 = Project(projectExprs, plan1)
    plan1.setAnalyzed()
    plan2.resolveExpressions(exprFunction)
    assert(invocationCount === 1)
  }

  test("do not allow transform in analyzer") {
    val plan = Project(Nil, LocalRelation())
    // These should be OK since we are not in the analyzer
    plan.transform { case p: Project => p }
    plan.transformUp { case p: Project => p }
    plan.transformDown { case p: Project => p }
    plan.transformAllExpressions { case lit: Literal => lit }

    // The following should fail in the analyzer scope
    AnalysisHelper.markInAnalyzer {
      intercept[RuntimeException] { plan.transform { case p: Project => p } }
      intercept[RuntimeException] { plan.transformUp { case p: Project => p } }
      intercept[RuntimeException] { plan.transformDown { case p: Project => p } }
      intercept[RuntimeException] { plan.transformAllExpressions { case lit: Literal => lit } }
    }
  }

  test("allow transform in resolveOperators in the analyzer") {
    val plan = Project(Nil, LocalRelation())
    AnalysisHelper.markInAnalyzer {
      plan.resolveOperators { case p: Project => p.transform { case p: Project => p } }
      plan.resolveOperatorsDown { case p: Project => p.transform { case p: Project => p } }
      plan.resolveExpressions { case lit: Literal =>
        Project(Nil, LocalRelation()).transform { case p: Project => p }
        lit
      }
    }
  }

  test("allow transform with allowInvokingTransformsInAnalyzer in the analyzer") {
    val plan = Project(Nil, LocalRelation())
    AnalysisHelper.markInAnalyzer {
      AnalysisHelper.allowInvokingTransformsInAnalyzer {
        plan.transform { case p: Project => p }
        plan.transformUp { case p: Project => p }
        plan.transformDown { case p: Project => p }
        plan.transformAllExpressions { case lit: Literal => lit }
      }
    }
  }
}
