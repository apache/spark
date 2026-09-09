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

package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.SparkThrowable
import org.apache.spark.sql.catalyst.expressions.{Add, AttributeReference, Expression, LessThanOrEqual, Literal, Rand}
import org.apache.spark.sql.catalyst.plans.{GreaterThanOp, GreaterThanOrEqualOp, Inner, JoinType, LeftOuter, LessThanOp, LessThanOrEqualOp, MatchComparisonOperator}
import org.apache.spark.sql.catalyst.plans.logical.{AsOfJoin, LocalRelation, LogicalPlan, Project}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

/**
 * Unit tests for the fixed-point analyzer rule [[ResolveAsOfJoin]], which materializes a SQL
 * `MATCH_CONDITION` clause into an [[AsOfJoin]]'s executable fields and expands `USING` column
 * lists into equi-join predicates. Operand resolution itself is done by the generic
 * `ResolveReferences` rule, so these tests feed already-resolved operands and exercise
 * `ResolveAsOfJoin` directly, mirroring `ResolveBinBySuite`.
 */
class ResolveAsOfJoinSuite extends AnalysisTest {

  // Left is nullable, right is non-nullable, so LEFT OUTER's nullability change is observable.
  private val lk = AttributeReference("k", IntegerType)()
  private val la = AttributeReference("a", IntegerType)()
  private val rk = AttributeReference("k", IntegerType, nullable = false)()
  private val rb = AttributeReference("b", IntegerType, nullable = false)()
  private val left: LogicalPlan = LocalRelation(lk, la)
  private val right: LogicalPlan = LocalRelation(rk, rb)

  // Non-orderable (MAP) operands for the invalid-type case.
  private val lmap = AttributeReference("m", MapType(StringType, IntegerType))()
  private val rmap = AttributeReference("m", MapType(StringType, IntegerType))()
  private val leftMap: LogicalPlan = LocalRelation(lmap)
  private val rightMap: LogicalPlan = LocalRelation(rmap)

  /** Build an [[AsOfJoin]] from a `MATCH_CONDITION` whose operands are already resolved. */
  private def asOf(
      leftExpr: Expression = la,
      operator: MatchComparisonOperator = GreaterThanOrEqualOp,
      rightExpr: Expression = rb,
      condition: Option[Expression] = None,
      joinType: JoinType = Inner,
      usingColumns: Option[Seq[String]] = None,
      l: LogicalPlan = left,
      r: LogicalPlan = right): AsOfJoin =
    AsOfJoin.fromMatchCondition(l, r, leftExpr, operator, rightExpr, condition, joinType,
      usingColumns)

  private def expectError(plan: LogicalPlan, condition: String): Unit = {
    val ex = intercept[SparkThrowable](ResolveAsOfJoin.apply(plan))
    assert(ex.getCondition == condition,
      s"expected condition '$condition' but got '${ex.getCondition}'")
  }

  test("materializes MATCH_CONDITION into asOfCondition and clears the match fields") {
    val resolved = ResolveAsOfJoin.apply(asOf()).asInstanceOf[AsOfJoin]
    assert(resolved.matchLeftOperand.isEmpty)
    assert(resolved.matchOperator.isEmpty)
    assert(resolved.matchRightOperand.isEmpty)
    assert(resolved.asOfCondition.dataType == BooleanType)
    assert(resolved.asOfCondition.resolved)
    assert(resolved.resolved)
  }

  test("materializes each MATCH_CONDITION comparison operator") {
    Seq(GreaterThanOrEqualOp, GreaterThanOp, LessThanOrEqualOp, LessThanOp).foreach { op =>
      val r = ResolveAsOfJoin.apply(asOf(operator = op)).asInstanceOf[AsOfJoin]
      assert(r.matchOperator.isEmpty, s"operator $op should be materialized")
      assert(r.asOfCondition.dataType == BooleanType, s"operator $op")
      assert(r.resolved, s"operator $op")
    }
  }

  test("expands USING into an equi-join predicate wrapped in a Project") {
    val result = ResolveAsOfJoin.apply(asOf(usingColumns = Some(Seq("k"))))
    val project = result.asInstanceOf[Project]
    val join = project.child.asInstanceOf[AsOfJoin]
    assert(join.usingColumns.isEmpty, "USING should be consumed")
    assert(join.condition.isDefined, "USING should expand into an equi-join condition")
    assert(join.matchOperator.isEmpty, "the match condition should still be materialized")
    assert(project.getTagValue(Project.hiddenOutputTag).isDefined,
      "USING columns should be tagged as hidden output")
  }

  test("keeps an explicit ON condition and does not add a USING projection") {
    val onCond = LessThanOrEqual(la, rb)
    val join = ResolveAsOfJoin.apply(asOf(condition = Some(onCond))).asInstanceOf[AsOfJoin]
    assert(join.condition.contains(onCond))
    assert(join.usingColumns.isEmpty)
    assert(join.matchOperator.isEmpty)
  }

  test("Inner preserves right-side nullability; LEFT OUTER makes the right side nullable") {
    val inner = ResolveAsOfJoin.apply(asOf(joinType = Inner)).asInstanceOf[AsOfJoin]
    assert(inner.output.map(_.nullable) == Seq(true, true, false, false))
    val leftOuter = ResolveAsOfJoin.apply(asOf(joinType = LeftOuter)).asInstanceOf[AsOfJoin]
    assert(leftOuter.output.map(_.nullable) == Seq(true, true, true, true))
  }

  test("rejects a MATCH_CONDITION operand referencing both join sides") {
    expectError(asOf(leftExpr = Add(la, rb)), "ASOF_JOIN_MATCH_CONDITION_TABLE_REFERENCE")
  }

  test("rejects a non-deterministic MATCH_CONDITION operand") {
    expectError(asOf(leftExpr = Rand(Literal(1L))),
      "ASOF_JOIN_MATCH_CONDITION_INVALID_EXPRESSION")
  }

  test("rejects non-orderable MATCH_CONDITION operand types") {
    expectError(
      asOf(leftExpr = lmap, rightExpr = rmap, l = leftMap, r = rightMap),
      "ASOF_JOIN_MATCH_CONDITION_INVALID_TYPE")
  }

  test("AsOfJoin survives the full Analyzer and CheckAnalysis") {
    withSQLConf(SQLConf.SQL_ASOF_JOIN_ENABLED.key -> "true") {
      // Unresolved operands here: the generic ResolveReferences resolves them, then
      // ResolveAsOfJoin materializes the match condition.
      val plan = AsOfJoin.fromMatchCondition(
        left, right, UnresolvedAttribute("a"), GreaterThanOrEqualOp,
        UnresolvedAttribute("b"), None, Inner)
      assertAnalysisSuccess(plan)
    }
  }
}
