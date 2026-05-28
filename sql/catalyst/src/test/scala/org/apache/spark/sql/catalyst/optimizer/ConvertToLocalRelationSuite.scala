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

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions.{Alias, Expression, GenericInternalRow, LessThan, Literal, UnaryExpression}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.plans.{Inner, LeftOuter, PlanTest}
import org.apache.spark.sql.catalyst.plans.logical.{Join, LocalRelation, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.types.{DataType, StructType}


class ConvertToLocalRelationSuite extends PlanTest {

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("LocalRelation", FixedPoint(100),
        ConvertToLocalRelation) :: Nil
  }

  test("Project on LocalRelation should be turned into a single LocalRelation") {
    val testRelation = LocalRelation(
      LocalRelation($"a".int, $"b".int).output,
      InternalRow(1, 2) :: InternalRow(4, 5) :: Nil)

    val correctAnswer = LocalRelation(
      LocalRelation($"a1".int, $"b1".int).output,
      InternalRow(1, 3) :: InternalRow(4, 6) :: Nil)

    val projectOnLocal = testRelation.select(
      UnresolvedAttribute("a").as("a1"),
      (UnresolvedAttribute("b") + 1).as("b1"))

    val optimized = Optimize.execute(projectOnLocal.analyze)

    comparePlans(optimized, correctAnswer)
  }

  test("Filter on LocalRelation should be turned into a single LocalRelation") {
    val testRelation = LocalRelation(
      LocalRelation($"a".int, $"b".int).output,
      InternalRow(1, 2) :: InternalRow(4, 5) :: Nil)

    val correctAnswer = LocalRelation(
      LocalRelation($"a1".int, $"b1".int).output,
      InternalRow(1, 3) :: Nil)

    val filterAndProjectOnLocal = testRelation
      .select(UnresolvedAttribute("a").as("a1"), (UnresolvedAttribute("b") + 1).as("b1"))
      .where(LessThan(UnresolvedAttribute("b1"), Literal.create(6)))

    val optimized = Optimize.execute(filterAndProjectOnLocal.analyze)

    comparePlans(optimized, correctAnswer)
  }

  test("SPARK-27798: Expression reusing output shouldn't override values in local relation") {
    val testRelation = LocalRelation(
      LocalRelation($"a".int).output,
      InternalRow(1) :: InternalRow(2) :: Nil)

    val correctAnswer = LocalRelation(
      LocalRelation($"a".struct($"a1".int)).output,
      InternalRow(InternalRow(1)) :: InternalRow(InternalRow(2)) :: Nil)

    val projected = testRelation.select(ExprReuseOutput(UnresolvedAttribute("a")).as("a"))
    val optimized = Optimize.execute(projected.analyze)

    comparePlans(optimized, correctAnswer)
  }

  // ---- SPARK-57039: Inner Join + single-row LocalRelation -> Project [Filter] ----

  private val tbl = LocalRelation($"a".int, $"b".int)
  private val singleRow = LocalRelation(
    LocalRelation($"c1".int, $"c2".boolean).output,
    InternalRow(1, true) :: Nil)
  private val multiRow = LocalRelation(
    LocalRelation($"c1".int, $"c2".boolean).output,
    InternalRow(1, true) :: InternalRow(2, false) :: Nil)

  test("SPARK-57039: InnerJoin with single-row LocalRelation on right -> Project") {
    val plan = tbl.join(singleRow, Inner, None).analyze
    val optimized = Optimize.execute(plan)
    val c1Attr = singleRow.output(0)
    val c2Attr = singleRow.output(1)
    val expected = Optimize.execute(tbl.select(
      $"a", $"b",
      Alias(Literal.create(1, c1Attr.dataType), c1Attr.name)(c1Attr.exprId),
      Alias(Literal.create(true, c2Attr.dataType), c2Attr.name)(c2Attr.exprId)).analyze)
    comparePlans(optimized, expected)
  }

  test("SPARK-57039: InnerJoin with single-row LocalRelation on left -> Project") {
    val plan = singleRow.join(tbl, Inner, None).analyze
    val optimized = Optimize.execute(plan)
    val c1Attr = singleRow.output(0)
    val c2Attr = singleRow.output(1)
    val expected = Optimize.execute(tbl.select(
      Alias(Literal.create(1, c1Attr.dataType), c1Attr.name)(c1Attr.exprId),
      Alias(Literal.create(true, c2Attr.dataType), c2Attr.name)(c2Attr.exprId),
      $"a", $"b").analyze)
    comparePlans(optimized, expected)
  }

  test("SPARK-57039: InnerJoin with single-row LocalRelation + condition -> Project + Filter") {
    val plan = tbl.join(singleRow, Inner, Some($"a" > UnresolvedAttribute("c1"))).analyze
    val optimized = Optimize.execute(plan)
    val c1Attr = singleRow.output(0)
    val c2Attr = singleRow.output(1)
    val expected = Optimize.execute(tbl.select(
        $"a", $"b",
        Alias(Literal.create(1, c1Attr.dataType), c1Attr.name)(c1Attr.exprId),
        Alias(Literal.create(true, c2Attr.dataType), c2Attr.name)(c2Attr.exprId))
      .where($"a" > UnresolvedAttribute("c1")).analyze)
    comparePlans(optimized, expected)
  }

  test("SPARK-57039: do NOT fold non-Inner join") {
    val plan = tbl.join(singleRow, LeftOuter, None).analyze
    val optimized = Optimize.execute(plan)
    comparePlans(optimized, plan)
  }

  test("SPARK-57039: do NOT fold multi-row LocalRelation") {
    val plan = tbl.join(multiRow, Inner, None).analyze
    val optimized = Optimize.execute(plan)
    comparePlans(optimized, plan)
  }

  test("SPARK-57039: preserve exprId across single-row fold") {
    val plan = tbl.join(singleRow, Inner, None).analyze
    val optimized = Optimize.execute(plan)
    val outIds = optimized.output.map(_.exprId).toSet
    assert(outIds.contains(singleRow.output(0).exprId))
    assert(outIds.contains(singleRow.output(1).exprId))
  }

  test("SPARK-57039: fold actually removes Join and preserves singleRow exprIds in literals") {
    val plan = tbl.join(singleRow, Inner, None).analyze
    val optimized = Optimize.execute(plan)
    // Strong assertion #1: fold actually happened - no Join survives
    assert(optimized.collectFirst { case _: Join => () }.isEmpty,
      "Expected Join to be folded away")
    // Strong assertion #2: output schema width = tbl + singleRow columns (4 cols total)
    assert(optimized.output.length == 4,
      "Expected 4-column output after fold")
    // Strong assertion #3: singleRow exprIds preserved through fold
    val outIds = optimized.output.map(_.exprId).toSet
    assert(outIds.contains(singleRow.output(0).exprId),
      "singleRow col 0 exprId should survive fold via Alias-preserved exprId")
    assert(outIds.contains(singleRow.output(1).exprId),
      "singleRow col 1 exprId should survive fold via Alias-preserved exprId")
  }

  test("SPARK-57039: do NOT fold when both sides are single-row LocalRelation (cartesian)") {
    val l = LocalRelation.fromExternalRows(
      Seq(Symbol("a").int, Symbol("b").int), Seq(Row(1, 2)))
    val r = LocalRelation.fromExternalRows(
      Seq(Symbol("c").int, Symbol("d").int), Seq(Row(3, 4)))
    val plan = l.join(r, Inner, None).analyze
    val optimized = Optimize.execute(plan)
    // Must NOT collapse: CheckCartesianProducts must still see the Join.
    assert(optimized.collectFirst { case _: Join => () }.isDefined,
      "Join should be preserved when both sides are single-row")
  }

  test("SPARK-57039: do NOT fold when other side is Project over single-row LocalRelation") {
    // SPARK-33100 CliSuite regression: "CREATE TEMPORARY VIEW t AS SELECT * FROM VALUES(...)"
    // parses to Project(LocalRelation(1 row)); during optimization one side may already have
    // collapsed to a bare LR while the other still wears a Project wrapper. A leaf-only
    // single-row guard misses this and folds the join, hiding a 1x1 cartesian.
    val l = LocalRelation.fromExternalRows(
      Seq(Symbol("a").int, Symbol("b").int), Seq(Row(1, 2)))
    val r = LocalRelation.fromExternalRows(
      Seq(Symbol("c").int, Symbol("d").int), Seq(Row(3, 4)))
    // Wrap r in a Project so r itself is not a bare LocalRelation pattern match.
    val rProj = r.select(Symbol("c"), Symbol("d"))
    val plan = l.join(rProj, Inner, None).analyze
    val optimized = Optimize.execute(plan)
    assert(optimized.collectFirst { case _: Join => () }.isDefined,
      "Join must be preserved when other side is Project(single-row LR)")
  }
}


// Dummy expression used for testing. It reuses output row. Assumes child expr outputs an integer.
case class ExprReuseOutput(child: Expression) extends UnaryExpression {
  override def dataType: DataType = StructType.fromDDL("a1 int")
  override def nullable: Boolean = true

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
    throw new UnsupportedOperationException("Should not trigger codegen")

  private val row: InternalRow = new GenericInternalRow(1)

  override def eval(input: InternalRow): Any = {
    row.update(0, child.eval(input))
    row
  }

  override protected def withNewChildInternal(newChild: Expression): ExprReuseOutput =
    copy(child = newChild)
}
