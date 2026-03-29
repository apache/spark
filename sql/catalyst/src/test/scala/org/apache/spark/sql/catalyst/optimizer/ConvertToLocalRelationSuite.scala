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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions.{Expression, GenericInternalRow, LessThan, Literal, UnaryExpression}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan}
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
