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

import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.analysis.{EmptyFunctionRegistry, FakeV2SessionCatalog, UnresolvedAttribute}
import org.apache.spark.sql.catalyst.catalog.{InMemoryCatalog, SessionCatalog}
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Filter, LocalRelation, LogicalPlan, OneRowRelation, Project}
import org.apache.spark.sql.catalyst.rules._
import org.apache.spark.sql.connector.catalog.CatalogManager
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{BooleanType, StringType, StructType}


class OptimizerStructuralIntegrityCheckerSuite extends PlanTest {

  object OptimizeRuleBreakSI extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = plan transform {
      case Project(projectList, child) =>
        val newAttr = UnresolvedAttribute("unresolvedAttr")
        Project(projectList ++ Seq(newAttr), child)
      case agg @ Aggregate(Nil, aggregateExpressions, child, _) =>
        // Project cannot host AggregateExpression
        Project(aggregateExpressions, child)
      case Filter(cond, child) =>
        val newCond = cond.transform {
          case g @ GetStructField(a: AttributeReference, _, _) =>
            g.copy(child = a.withDataType(StringType))
          case g @ GetArrayStructFields(a: AttributeReference, _, _, _, _) =>
            g.copy(child = a.withDataType(StringType))
          case g @ GetArrayItem(a: AttributeReference, _, _) =>
            g.copy(child = a.withDataType(StringType))
          case g @ GetMapValue(a: AttributeReference, _) =>
            g.copy(child = a.withDataType(StringType))
        }
        Filter(newCond, child)
    }
  }

  object Optimize extends Optimizer(
    new CatalogManager(
      FakeV2SessionCatalog,
      new SessionCatalog(new InMemoryCatalog, EmptyFunctionRegistry))) {
    val newBatch = Batch("OptimizeRuleBreakSI", Once, OptimizeRuleBreakSI)
    override def defaultBatches: Seq[Batch] = Seq(newBatch) ++ super.defaultBatches
  }

  test("check for invalid plan after execution of rule - unresolved attribute") {
    val analyzed = Project(Alias(Literal(10), "attr")() :: Nil, OneRowRelation()).analyze
    assert(analyzed.resolved)
    val message = intercept[SparkException] {
      Optimize.execute(analyzed)
    }.getMessage
    val ruleName = OptimizeRuleBreakSI.ruleName
    assert(message.contains(s"Rule $ruleName in batch OptimizeRuleBreakSI"))
    assert(message.contains("generated an invalid plan"))
  }

  test("check for invalid plan after execution of rule - special expression in wrong operator") {
    val analyzed =
      Aggregate(Nil, Seq[NamedExpression](max($"id") as "m"),
        LocalRelation($"id".long)).analyze
    assert(analyzed.resolved)

    // Should fail verification with the OptimizeRuleBreakSI rule
    val message = intercept[SparkException] {
      Optimize.execute(analyzed)
    }.getMessage
    val ruleName = OptimizeRuleBreakSI.ruleName
    assert(message.contains(s"Rule $ruleName in batch OptimizeRuleBreakSI"))
    assert(message.contains("generated an invalid plan"))

    // Should not fail verification with the regular optimizer
    SimpleTestOptimizer.execute(analyzed)
  }

  test("check for invalid plan after execution of rule - bad ExtractValue") {
    val input = LocalRelation(
      $"c1".struct(new StructType().add("f1", "boolean")),
      $"c2".array(new StructType().add("f1", "boolean")),
      $"c3".array(BooleanType),
      new DslAttr($"c4").map(StringType, BooleanType)
    )

    def assertCheckFailed(expr: Expression): Unit = {
      val analyzed = Filter(expr, input).analyze
      assert(analyzed.resolved)
      // Should fail verification with the OptimizeRuleBreakSI rule
      val message = intercept[SparkException] {
        Optimize.execute(analyzed)
      }.getMessage
      val ruleName = OptimizeRuleBreakSI.ruleName
      assert(message.contains(s"Rule $ruleName in batch OptimizeRuleBreakSI"))
      assert(message.contains("generated an invalid plan"))
    }

    // This resolution validation should be included in the lightweight
    // validator so that it's validated in production.
    withSQLConf(
      SQLConf.PLAN_CHANGE_VALIDATION.key -> "false",
      SQLConf.LIGHTWEIGHT_PLAN_CHANGE_VALIDATION.key -> "true") {
      assertCheckFailed($"c1.f1")
      assertCheckFailed($"c2.f1".getItem(0))
      assertCheckFailed($"c3".getItem(0))
      assertCheckFailed($"c4".getItem("key"))
    }
  }

  test("check for invalid plan before execution of any rule") {
    val analyzed =
      Aggregate(Nil, Seq[NamedExpression](max($"id") as "m"),
        LocalRelation($"id".long)).analyze
    val invalidPlan = OptimizeRuleBreakSI.apply(analyzed)

    // Should fail verification right at the beginning
    val message = intercept[SparkException] {
      Optimize.execute(invalidPlan)
    }.getMessage
    assert(message.contains("The input plan of"))
  }
}
