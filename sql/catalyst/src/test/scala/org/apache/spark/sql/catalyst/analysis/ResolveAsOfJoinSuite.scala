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
import org.apache.spark.sql.catalyst.expressions.{Add, AttributeReference, CreateNamedStruct, EqualTo, Expression, GreaterThan, GreaterThanOrEqual, If, LambdaFunction, LessThan, LessThanOrEqual, Literal, Rand, Subtract, ZipWith}
import org.apache.spark.sql.catalyst.plans.{GreaterThanOp, GreaterThanOrEqualOp, Inner, JoinType, LeftOuter, LessThanOp, LessThanOrEqualOp, MatchComparisonOperator}
import org.apache.spark.sql.catalyst.plans.logical.{AsOfJoin, LocalRelation, LogicalPlan, Project}
import org.apache.spark.sql.types._

/**
 * Unit tests for the analyzer rule [[ResolveAsOfJoin]], which materializes a SQL
 * `MATCH_CONDITION` into an [[AsOfJoin]]'s executable fields and expands `USING` into equi-join
 * predicates. Operands are resolved by the generic `ResolveReferences`, so these tests feed
 * already-resolved operands, mirroring `ResolveBinBySuite`.
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

  // Incompatible but orderable operands (timestamp vs boolean) for the areOperandsCompatible arm.
  private val lts = AttributeReference("t", TimestampType)()
  private val rbool = AttributeReference("bl", BooleanType, nullable = false)()
  private val leftTs: LogicalPlan = LocalRelation(lts)
  private val rightBool: LogicalPlan = LocalRelation(rbool)

  // Two shared key columns on each side (plus a distinct match operand) for multi-column USING.
  private val lOp = AttributeReference("lop", IntegerType)()
  private val lKey1 = AttributeReference("k1", IntegerType)()
  private val lKey2 = AttributeReference("k2", IntegerType)()
  private val rOp = AttributeReference("rop", IntegerType, nullable = false)()
  private val rKey1 = AttributeReference("k1", IntegerType, nullable = false)()
  private val rKey2 = AttributeReference("k2", IntegerType, nullable = false)()
  private val leftKeys: LogicalPlan = LocalRelation(lOp, lKey1, lKey2)
  private val rightKeys: LogicalPlan = LocalRelation(rOp, rKey1, rKey2)

  // String operands: not subtractable, so the distance is a signed -1 / 0 / +1 rank.
  private val lstr = AttributeReference("s", StringType)()
  private val rstr = AttributeReference("s", StringType, nullable = false)()
  private val leftStr: LogicalPlan = LocalRelation(lstr)
  private val rightStr: LogicalPlan = LocalRelation(rstr)

  // Array operands: the distance is computed element-wise via ZipWith.
  private val larr = AttributeReference("arr", ArrayType(IntegerType))()
  private val rarr = AttributeReference("arr", ArrayType(IntegerType))()
  private val leftArr: LogicalPlan = LocalRelation(larr)
  private val rightArr: LogicalPlan = LocalRelation(rarr)

  // Positional struct operands (different field names): the distance is flattened per field.
  private val lstruct = AttributeReference(
    "st", StructType(StructField("f1", IntegerType) :: StructField("f2", IntegerType) :: Nil))()
  private val rstruct = AttributeReference(
    "st", StructType(StructField("g1", IntegerType) :: StructField("g2", IntegerType) :: Nil))()
  private val leftStruct: LogicalPlan = LocalRelation(lstruct)
  private val rightStruct: LogicalPlan = LocalRelation(rstruct)

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

  test("materializes MATCH_CONDITION into the executable fields and clears the match fields") {
    val resolved = ResolveAsOfJoin.apply(asOf()).asInstanceOf[AsOfJoin]
    assert(resolved.matchLeftOperand.isEmpty)
    assert(resolved.matchOperator.isEmpty)
    assert(resolved.matchRightOperand.isEmpty)
    assert(resolved.asOfCondition.dataType == BooleanType)
    assert(resolved.asOfCondition.resolved)
    // `resolved` ignores the sort lists (empty passes), so pin them and the SQL factory's flag:
    // the sort-merge operator needs these sort exprs, and a dropped flag would reroute SQL ASOF.
    assert(resolved.leftSortExprs == Seq(la))
    assert(resolved.rightSortExprs == Seq(rb))
    assert(resolved.requiresSortMergeAsOfJoin)
    assert(resolved.resolved)
  }

  test("materializes each MATCH_CONDITION comparison operator into its exact comparison") {
    // The operator is the only thing that distinguishes the four cases, so pin the comparison
    // node itself: a regression that always emits `>=` would pass a Boolean-type check.
    Seq(
      GreaterThanOrEqualOp -> GreaterThanOrEqual(la, rb),
      GreaterThanOp -> GreaterThan(la, rb),
      LessThanOrEqualOp -> LessThanOrEqual(la, rb),
      LessThanOp -> LessThan(la, rb)).foreach { case (op, expected) =>
      val r = ResolveAsOfJoin.apply(asOf(operator = op)).asInstanceOf[AsOfJoin]
      assert(r.matchOperator.isEmpty, s"operator $op should be materialized")
      assert(r.asOfCondition == expected, s"operator $op")
      assert(r.resolved, s"operator $op")
    }
  }

  test("materializes a subtractable leaf operand into a Subtract distance") {
    val ge = ResolveAsOfJoin.apply(asOf(operator = GreaterThanOrEqualOp)).asInstanceOf[AsOfJoin]
    assert(ge.matchOperator.isEmpty)
    assert(ge.orderExpression == Subtract(la, rb))
    // `<` / `<=` flip the operands so the distance stays non-negative on the matching side.
    val le = ResolveAsOfJoin.apply(asOf(operator = LessThanOrEqualOp)).asInstanceOf[AsOfJoin]
    assert(le.orderExpression == Subtract(rb, la))
  }

  test("swaps operands written on the opposite join side and flips the operator") {
    // Writing the right column first (rb <= la) must normalize to the same plan as la >= rb:
    // normalizeMatchOperands swaps the pair and flips `<=` into `>=`. Every other test writes
    // the left operand first, so this is the only case that exercises that branch.
    val resolved = ResolveAsOfJoin.apply(
      asOf(leftExpr = rb, operator = LessThanOrEqualOp, rightExpr = la)).asInstanceOf[AsOfJoin]
    assert(resolved.matchOperator.isEmpty)
    assert(resolved.asOfCondition == GreaterThanOrEqual(la, rb))
    assert(resolved.orderExpression == Subtract(la, rb))
  }

  test("materializes a non-subtractable String operand into a signed comparison distance") {
    val resolved = ResolveAsOfJoin.apply(
      asOf(leftExpr = lstr, rightExpr = rstr, l = leftStr, r = rightStr)).asInstanceOf[AsOfJoin]
    assert(resolved.matchOperator.isEmpty)
    // Strings cannot be subtracted, so the distance is a -1 / 0 / +1 rank.
    val expected = If(
      EqualTo(lstr, rstr),
      Literal(0),
      If(GreaterThan(lstr, rstr), Literal(1), Literal(-1)))
    assert(resolved.orderExpression == expected)
  }

  test("flips the signed comparison distance for a less-than match operator") {
    val resolved = ResolveAsOfJoin.apply(
      asOf(leftExpr = lstr, operator = LessThanOrEqualOp, rightExpr = rstr,
        l = leftStr, r = rightStr)).asInstanceOf[AsOfJoin]
    val expected = If(
      EqualTo(lstr, rstr),
      Literal(0),
      If(GreaterThan(lstr, rstr), Literal(-1), Literal(1)))
    assert(resolved.orderExpression == expected)
  }

  test("materializes an array operand into an element-wise ZipWith distance") {
    val resolved = ResolveAsOfJoin.apply(
      asOf(leftExpr = larr, rightExpr = rarr, l = leftArr, r = rightArr)).asInstanceOf[AsOfJoin]
    assert(resolved.matchOperator.isEmpty)
    val order = resolved.orderExpression
    assert(order.isInstanceOf[ZipWith], s"expected ZipWith, got ${order.getClass.getSimpleName}")
    val zip = order.asInstanceOf[ZipWith]
    assert(zip.left == larr && zip.right == rarr)
    // Each element pair reuses the subtractable-leaf distance (Subtract) inside the lambda.
    val body = zip.function.asInstanceOf[LambdaFunction].function
    assert(body.isInstanceOf[Subtract], s"expected a Subtract element distance, got $body")
  }

  test("materializes a positional struct operand into a flattened per-field distance") {
    val resolved = ResolveAsOfJoin.apply(
      asOf(leftExpr = lstruct, rightExpr = rstruct, l = leftStruct, r = rightStruct))
      .asInstanceOf[AsOfJoin]
    assert(resolved.matchOperator.isEmpty)
    val order = resolved.orderExpression
    assert(order.isInstanceOf[CreateNamedStruct],
      s"expected CreateNamedStruct, got ${order.getClass.getSimpleName}")
    val fields = order.asInstanceOf[CreateNamedStruct].valExprs
    // One Subtract distance per struct field, kept together in a composite struct.
    assert(fields.size == 2)
    assert(fields.forall(_.isInstanceOf[Subtract]), s"expected per-field Subtracts, got $fields")
    // Field names differ, so each side is decomposed into a positional struct for the compare,
    // not the raw struct. A regression that ordered fields right but compared raw structs would
    // slip past the orderExpression checks above, so pin the comparison operands too.
    val ge = resolved.asOfCondition.asInstanceOf[GreaterThanOrEqual]
    assert(ge.left.isInstanceOf[CreateNamedStruct] && ge.right.isInstanceOf[CreateNamedStruct],
      s"expected decomposed struct operands, got ${ge.left} >= ${ge.right}")
    assert(ge.left != lstruct && ge.right != rstruct)
  }

  test("expands USING into an equi-join predicate wrapped in a Project") {
    val result = ResolveAsOfJoin.apply(asOf(usingColumns = Some(Seq("k"))))
    val project = result.asInstanceOf[Project]
    val join = project.child.asInstanceOf[AsOfJoin]
    assert(join.usingColumns.isEmpty, "USING should be consumed")
    assert(join.condition.contains(EqualTo(lk, rk)),
      s"USING (k) should expand into lk = rk, got ${join.condition}")
    assert(join.matchOperator.isEmpty, "the match condition should still be materialized")
    assert(project.getTagValue(Project.hiddenOutputTag).isDefined,
      "USING columns should be tagged as hidden output")
  }

  test("expands multi-column USING into ANDed equi-join predicates") {
    val result = ResolveAsOfJoin.apply(
      asOf(leftExpr = lOp, rightExpr = rOp, usingColumns = Some(Seq("k1", "k2")),
        l = leftKeys, r = rightKeys))
    val project = result.asInstanceOf[Project]
    val join = project.child.asInstanceOf[AsOfJoin]
    assert(join.usingColumns.isEmpty)
    // USING (k1, k2) pairs each side's column by name: k1 = k1 AND k2 = k2.
    val equiPredicates = join.condition.get.collect { case e: EqualTo => e }
    assert(equiPredicates == Seq(EqualTo(lKey1, rKey1), EqualTo(lKey2, rKey2)),
      s"expected lKey1 = rKey1 and lKey2 = rKey2, got $equiPredicates")
    assert(project.getTagValue(Project.hiddenOutputTag).isDefined)
  }

  test("keeps an explicit ON condition and does not add a USING projection") {
    val onCond = LessThanOrEqual(la, rb)
    val join = ResolveAsOfJoin.apply(asOf(condition = Some(onCond))).asInstanceOf[AsOfJoin]
    assert(join.condition.contains(onCond))
    assert(join.usingColumns.isEmpty)
    assert(join.matchOperator.isEmpty)
  }

  test("Inner preserves right-side nullability; LEFT OUTER makes the right side nullable") {
    // Nullability comes from AsOfJoin.computeOutput, not the rule, so also assert the match
    // fields were cleared: that ties these checks to ResolveAsOfJoin having actually run.
    val inner = ResolveAsOfJoin.apply(asOf(joinType = Inner)).asInstanceOf[AsOfJoin]
    assert(inner.matchOperator.isEmpty)
    assert(inner.output.map(_.nullable) == Seq(true, true, false, false))
    val leftOuter = ResolveAsOfJoin.apply(asOf(joinType = LeftOuter)).asInstanceOf[AsOfJoin]
    assert(leftOuter.matchOperator.isEmpty)
    assert(leftOuter.output.map(_.nullable) == Seq(true, true, true, true))
  }

  test("rejects a MATCH_CONDITION operand referencing both join sides") {
    expectError(asOf(leftExpr = Add(la, rb)), "ASOF_JOIN_MATCH_CONDITION_TABLE_REFERENCE")
  }

  test("rejects a MATCH_CONDITION with both operands on the same join side") {
    // A distinct throw site from the case above: normalizeMatchOperands rejects operands that
    // do not straddle the two relations, rather than validateMatchConditionTableReferences.
    expectError(asOf(leftExpr = la, rightExpr = la), "ASOF_JOIN_MATCH_CONDITION_TABLE_REFERENCE")
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

  test("rejects incompatible MATCH_CONDITION operand types that are each orderable") {
    // timestamp and boolean are each orderable, so this hits areOperandsCompatible, not the
    // isValidOperandType arm the MAP case above exercises.
    expectError(
      asOf(leftExpr = lts, rightExpr = rbool, l = leftTs, r = rightBool),
      "ASOF_JOIN_MATCH_CONDITION_INVALID_TYPE")
  }

  test("AsOfJoin survives the full Analyzer and CheckAnalysis") {
    // No withSQLConf needed: SQL_ASOF_JOIN_ENABLED is read only in the parser, and this builds
    // the node directly. ResolveAsOfJoin is always in the analyzer batch.
    // Unresolved operands here: the generic ResolveReferences resolves them, then
    // ResolveAsOfJoin materializes the match condition.
    val plan = AsOfJoin.fromMatchCondition(
      left, right, UnresolvedAttribute("a"), GreaterThanOrEqualOp,
      UnresolvedAttribute("b"), None, Inner)
    assertAnalysisSuccess(plan)
  }
}
