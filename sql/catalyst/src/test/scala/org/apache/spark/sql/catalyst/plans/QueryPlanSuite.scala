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

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeReference, Expression, ExprId, ListQuery, Literal, NamedExpression, PlanExpression, Rand}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LocalRelation, LogicalPlan, Project, Union}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.{CurrentOrigin, Origin, TreePattern}
import org.apache.spark.sql.types.{DataType, IntegerType}

class QueryPlanSuite extends SparkFunSuite {

  test("origin remains the same after mapExpressions (SPARK-23823)") {
    CurrentOrigin.setPosition(0, 0)
    val column = AttributeReference("column", IntegerType)(NamedExpression.newExprId)
    val query = DslLogicalPlan(table("table")).select(column)
    CurrentOrigin.reset()

    val mappedQuery = query mapExpressions {
      case _: Expression => Literal(1)
    }

    val mappedOrigin = mappedQuery.expressions.apply(0).origin
    assert(mappedOrigin == Origin.apply(Some(0), Some(0)))
  }

  test("collectInPlanAndSubqueries") {
    val a: NamedExpression = AttributeReference("a", IntegerType)()
    val plan =
      Union(
        Seq(
          Project(
            Seq(a),
            Filter(
              ListQuery(Project(
                Seq(a),
                Filter(
                  ListQuery(Project(
                    Seq(a),
                    UnresolvedRelation(TableIdentifier("t", None))
                  )),
                  UnresolvedRelation(TableIdentifier("t", None))
                )
              )),
              UnresolvedRelation(TableIdentifier("t", None))
            )
          ),
          Project(
            Seq(a),
            Filter(
              ListQuery(Project(
                Seq(a),
                UnresolvedRelation(TableIdentifier("t", None))
              )),
              UnresolvedRelation(TableIdentifier("t", None))
            )
          )
        )
      )

    val countRelationsInPlan = plan.collect({ case _: UnresolvedRelation => 1 }).sum
    val countRelationsInPlanAndSubqueries =
      plan.collectWithSubqueries({ case _: UnresolvedRelation => 1 }).sum

    assert(countRelationsInPlan == 2)
    assert(countRelationsInPlanAndSubqueries == 5)
  }

  test("SPARK-33035: consecutive attribute updates in parent plan nodes") {
    val testRule = new Rule[LogicalPlan] {
      override def apply(plan: LogicalPlan): LogicalPlan = plan.transformUpWithNewOutput {
        case p @ Project(projList, _) =>
          // Assigns new `ExprId`s for output references
          val newPlan = p.copy(projectList = projList.map { ne => Alias(ne, ne.name)() })
          val attrMapping = p.output.zip(newPlan.output)
          newPlan -> attrMapping
      }
    }

    val t = LocalRelation($"a".int, $"b".int)
    val plan = t.select($"a", $"b").select($"a", $"b").select($"a", $"b").analyze
    assert(testRule(plan).resolved)
  }

  test("SPARK-37199: add a deterministic field to QueryPlan") {
    val a: NamedExpression = AttributeReference("a", IntegerType)()
    val aRand: NamedExpression = Alias(a + Rand(1), "aRand")()
    val deterministicPlan = Project(
      Seq(a),
      Filter(
        ListQuery(Project(
          Seq(a),
          UnresolvedRelation(TableIdentifier("t", None))
        )),
        UnresolvedRelation(TableIdentifier("t", None))
      )
    )
    assert(deterministicPlan.deterministic)

    val nonDeterministicPlan = Project(
      Seq(aRand),
      Filter(
        ListQuery(Project(
          Seq(a),
          UnresolvedRelation(TableIdentifier("t", None))
        )),
        UnresolvedRelation(TableIdentifier("t", None))
      )
    )
    assert(!nonDeterministicPlan.deterministic)
  }

  test("SPARK-38347: Nullability propagation in transformUpWithNewOutput") {
    // A test rule that replaces Attributes in Project's project list.
    val testRule = new Rule[LogicalPlan] {
      override def apply(plan: LogicalPlan): LogicalPlan = plan.transformUpWithNewOutput {
        case p @ Project(projectList, _) =>
          val newProjectList = projectList.map {
            case a: AttributeReference => a.newInstance()
            case ne => ne
          }
          val newProject = p.copy(projectList = newProjectList)
          newProject -> p.output.zip(newProject.output)
      }
    }

    // Test a Left Outer Join plan in which right-hand-side input attributes are not nullable.
    // Those attributes should be nullable after join even with a `transformUpWithNewOutput`
    // started below the Left Outer join.
    val t1 = LocalRelation($"a".int.withNullability(false),
      $"b".int.withNullability(false), $"c".int.withNullability(false))
    val t2 = LocalRelation($"c".int.withNullability(false),
      $"d".int.withNullability(false), $"e".int.withNullability(false))
    val plan = t1.select($"a", $"b")
      .join(t2.select($"c", $"d"), LeftOuter, Some($"a" === $"c"))
      .select($"a" + $"d").analyze
    // The output Attribute of `plan` is nullable even though `d` is not nullable before the join.
    assert(plan.output(0).nullable)
    // The test rule with `transformUpWithNewOutput` should not change the nullability.
    val planAfterTestRule = testRule(plan)
    assert(planAfterTestRule.output(0).nullable)
  }

  test("SPARK-54865: pruning works correctly in foreachWithSubqueriesAndPruning") {
    val a: NamedExpression = AttributeReference("a", IntegerType)()
    val plan = Project(
      Seq(a),
      Filter(
        ListQuery(Project(
          Seq(a),
          UnresolvedRelation(TableIdentifier("t", None))
        )),
        UnresolvedRelation(TableIdentifier("t", None))
      )
    )

    val visited = ArrayBuffer[LogicalPlan]()
    plan.foreachWithSubqueriesAndPruning(_.containsPattern(TreePattern.FILTER)) { p =>
      visited += p
    }

    // Only 2 nodes contain FILTER pattern: outer Project and Filter
    assert(visited.size == 2)
    assert(visited.forall(_.containsPattern(TreePattern.FILTER)))
  }

  test("SPARK-54560: subqueries includes plans of the same plan family") {
    val a: NamedExpression = AttributeReference("a", IntegerType)()
    val innerPlan = Project(Seq(a), UnresolvedRelation(TableIdentifier("t", None)))

    // Baseline: a LogicalPlan containing a ListQuery (whose `.plan` is a LogicalPlan)
    // should expose the subquery in `subqueries`. This verifies the common path is
    // unchanged by the SPARK-54560 fix.
    val logicalOuter = Filter(ListQuery(innerPlan), UnresolvedRelation(TableIdentifier("t", None)))
    assert(logicalOuter.subqueries.size === 1)
    assert(logicalOuter.subqueries.head eq innerPlan)
  }

  test("SPARK-54560: subqueries excludes plans of an incompatible plan family") {
    // This is the regression test for SPARK-54560. In AQE, a SparkPlan can
    // transiently hold a PlanExpression whose `.plan` is still a LogicalPlan
    // (or vice-versa) because logical-to-physical planning runs separately for
    // the main and sub-queries. Before the fix, `_subqueries` blindly cast such
    // plans to the enclosing plan's type, and the resulting unchecked generic
    // Seq[PlanType] would cause a ClassCastException as soon as downstream code
    // used a PlanType-specific method on the element.
    //
    // We simulate the cross-family scenario with a minimal alternative plan
    // family (QueryPlanSuite.FakePhysicalPlan) holding a PlanExpression whose
    // `.plan` is a LogicalPlan. With the fix, `subqueries` filters the
    // incompatible plan out; without the fix, it would be included.

    val a: NamedExpression = AttributeReference("a", IntegerType)()
    val logicalChild: LogicalPlan =
      Project(Seq(a), UnresolvedRelation(TableIdentifier("t", None)))

    val fakePhysicalOuter = QueryPlanSuite.FakePhysicalPlan(
      Seq(QueryPlanSuite.FakePlanExpr(logicalChild)))

    assert(fakePhysicalOuter.subqueries.isEmpty,
      "LogicalPlan subquery should be filtered out of FakePhysicalPlan.subqueries " +
        "(regression for SPARK-54560)")
  }
}

object QueryPlanSuite {
  /**
   * A minimal alternative plan family used only by the SPARK-54560 regression
   * test. Distinct from [[LogicalPlan]] and [[SparkPlan]]. The `exprs` field is
   * a product member so `QueryPlan.expressions` surfaces them automatically.
   */
  case class FakePhysicalPlan(exprs: Seq[Expression]) extends QueryPlan[FakePhysicalPlan] {
    override def output: Seq[AttributeReference] = Nil
    override def children: Seq[FakePhysicalPlan] = Nil
    override protected def withNewChildrenInternal(
        newChildren: IndexedSeq[FakePhysicalPlan]): FakePhysicalPlan = this
  }

  /**
   * A minimal [[PlanExpression]] whose wrapped plan is a [[LogicalPlan]]. Used
   * only by the SPARK-54560 regression test to construct a cross-family
   * embedding. `eval` and `doGenCode` are not exercised by the test; they
   * throw to make misuse loud.
   */
  case class FakePlanExpr(plan: LogicalPlan) extends PlanExpression[LogicalPlan] {
    override def exprId: ExprId = ExprId(0)
    override def dataType: DataType = IntegerType
    override def nullable: Boolean = true
    override def children: Seq[Expression] = Nil
    override def withNewPlan(newPlan: LogicalPlan): FakePlanExpr = copy(plan = newPlan)
    override protected def withNewChildrenInternal(
        newChildren: IndexedSeq[Expression]): FakePlanExpr = this
    override def eval(input: org.apache.spark.sql.catalyst.InternalRow): Any =
      throw new UnsupportedOperationException("FakePlanExpr is test-only and not evaluable")
    override protected def doGenCode(
        ctx: org.apache.spark.sql.catalyst.expressions.codegen.CodegenContext,
        ev: org.apache.spark.sql.catalyst.expressions.codegen.ExprCode):
      org.apache.spark.sql.catalyst.expressions.codegen.ExprCode =
      throw new UnsupportedOperationException("FakePlanExpr is test-only and not codegen-able")
  }
}
