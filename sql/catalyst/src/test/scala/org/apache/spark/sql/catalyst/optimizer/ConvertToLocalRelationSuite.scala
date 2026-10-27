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

import java.util.concurrent.atomic.AtomicInteger

import org.apache.spark.SparkArithmeticException
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions.{Add, Alias, ArrayTransform, AttributeReference, Cast, Divide, EqualTo, EvalMode, Expression, GenericInternalRow, If, LambdaFunction, LeafExpression, LessThan, Literal, NamedLambdaVariable, Nondeterministic, UnaryExpression}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, CodegenFallback, ExprCode}
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{ArrayType, DataType, DoubleType, IntegerType, MetadataBuilder, StructType}


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

  test("SPARK-58208: ConvertToLocalRelation uses fresh stateful project expressions") {
    val element = NamedLambdaVariable("x", IntegerType, nullable = false)
    val transform = ArrayTransform(
      Literal.create(Seq(1, 2), ArrayType(IntegerType, containsNull = false)),
      LambdaFunction(Add(element, Literal(1)), Seq(element)))
    val project = Project(Seq(Alias(transform, "v")()), LocalRelation(Nil, Seq(InternalRow.empty)))

    Optimize.execute(project)

    assert(element.value.get() == null)
  }

  gridTest("materialize complete foldable projected values once")(Seq(1, 4)) { rowCount =>
    val counter = new AtomicInteger()
    val value = CountedLocalEvaluation(Add(Literal(1), Literal(2)), counter)
    val input = LocalRelation(Nil, Seq.fill(rowCount)(InternalRow.empty))
    val project = Project(Seq(Alias(value, "v")()), input)

    val result = Optimize.execute(project).asInstanceOf[LocalRelation]

    assert(result.data.map(_.getInt(0)) == Seq.fill(rowCount)(3))
    assert(counter.get() == 1)
  }

  test("empty local input does not evaluate projected constants") {
    withSQLConf(SQLConf.ANSI_ENABLED.key -> "true") {
      val counter = new AtomicInteger()
      val project = Project(Seq(
        Alias(CountedLocalEvaluation(Literal(1), counter), "v")(),
        Alias(Divide(Literal(1d), Literal(0d)), "failure")()), LocalRelation(Nil))

      assert(Optimize.execute(project).asInstanceOf[LocalRelation].data.isEmpty)
      assert(counter.get() == 0)
    }
  }

  test("row-dependent expressions retain guards around failing constants") {
    withSQLConf(SQLConf.ANSI_ENABLED.key -> "true") {
      val input = AttributeReference("v", DoubleType)()
      val failure = Divide(Literal(1d), Literal(0d))
      val guarded = If(EqualTo(input, Literal(1d)), Literal(7d), failure)
      val project = Project(Seq(Alias(guarded, "v")()),
        LocalRelation(Seq(input), Seq(InternalRow(1d), InternalRow(1d))))
      val result = Optimize.execute(project).asInstanceOf[LocalRelation]
      assert(result.data.map(_.getDouble(0)) == Seq(7d, 7d))

      val nullGuarded = Project(Seq(Alias(Add(input, failure), "v")()),
        LocalRelation(Seq(input), Seq(InternalRow(null), InternalRow(null))))
      val nullResult = Optimize.execute(nullGuarded).asInstanceOf[LocalRelation]
      assert(nullResult.data.forall(_.isNullAt(0)))
    }
  }

  test("local materialization retains original output attributes") {
    val metadata = new MetadataBuilder().putString("description", "original output").build()
    val value = Cast(Literal("7"), IntegerType, evalMode = EvalMode.LEGACY)
    val alias = Alias(value, "original_name")(
      qualifier = Seq("original_qualifier"), explicitMetadata = Some(metadata))
    val project = Project(Seq(alias), LocalRelation(Nil, Seq(InternalRow.empty)))
    val expected = project.output.head
    assert(expected.nullable)

    val result = Optimize.execute(project).asInstanceOf[LocalRelation]
    val actual = result.output.head

    assert(actual.exprId == expected.exprId)
    assert(actual.name == expected.name)
    assert(actual.qualifier == expected.qualifier)
    assert(actual.metadata == expected.metadata)
    assert(actual.dataType == expected.dataType)
    assert(actual.nullable == expected.nullable)
    assert(result.data.head.getInt(0) == 7)
  }

  test("local projections isolate stateful per-row evaluation") {
    val sequence = LocalProjectionSequence()
    val project = Project(Seq(Alias(sequence, "v")()),
      LocalRelation(Nil, Seq.fill(3)(InternalRow.empty)))

    (1 to 2).foreach { _ =>
      val result = Optimize.execute(project).asInstanceOf[LocalRelation]
      assert(result.data.map(_.getInt(0)) == Seq(1, 2, 3))
    }
    sequence.initialize(0)
    assert(sequence.eval(InternalRow.empty) == 1)
  }

  test("folded and row-dependent mutable projection outputs are copied") {
    val input = AttributeReference("v", IntegerType)()
    val project = Project(Seq(
      Alias(ExprReuseOutput(Literal(7)), "constant")(),
      Alias(ExprReuseOutput(input), "dependent")()),
      LocalRelation(Seq(input), Seq(InternalRow(1), InternalRow(2))))

    val result = Optimize.execute(project).asInstanceOf[LocalRelation]
    assert(result.data.map(_.getStruct(1, 1).getInt(0)) == Seq(1, 2))
    result.data.head.getStruct(0, 1).update(0, 99)
    result.data.head.getStruct(1, 1).update(0, 99)
    assert(result.data(1).getStruct(0, 1).getInt(0) == 7)
    assert(result.data(1).getStruct(1, 1).getInt(0) == 2)
  }

  test("local constant materialization respects static evaluation exceptions") {
    val counter = new AtomicInteger()
    val value = CountedLocalEvaluation(Literal(7), counter)
    value.setTagValue(ConstantFolding.FAILED_TO_EVALUATE, ())
    val project = Project(Seq(Alias(value, "v")()),
      LocalRelation(Nil, Seq.fill(3)(InternalRow.empty)))

    val result = Optimize.execute(project).asInstanceOf[LocalRelation]
    assert(result.data.map(_.getInt(0)) == Seq(7, 7, 7))
    assert(counter.get() == 3)

    withSQLConf(SQLConf.ANSI_ENABLED.key -> "true") {
      val failure = Project(Seq(Alias(Divide(Literal(1d), Literal(0d)), "v")()),
        LocalRelation(Nil, Seq(InternalRow.empty)))
      assert(intercept[SparkArithmeticException](Optimize.execute(failure)).getCondition ==
        "DIVIDE_BY_ZERO")
    }
  }
}

case class CountedLocalEvaluation(child: Expression, counter: AtomicInteger)
    extends UnaryExpression with CodegenFallback {
  override def dataType: DataType = child.dataType
  override def eval(input: InternalRow): Any = {
    counter.incrementAndGet()
    child.eval(input)
  }
  override protected def withNewChildInternal(newChild: Expression): CountedLocalEvaluation =
    copy(child = newChild)
}

case class LocalProjectionSequence() extends LeafExpression with Nondeterministic
    with CodegenFallback {
  private var next = 0
  override def stateful: Boolean = true
  override def dataType: DataType = IntegerType
  override def nullable: Boolean = false
  override protected def initializeInternal(partitionIndex: Int): Unit = { next = 0 }
  override protected def evalInternal(input: InternalRow): Any = {
    next += 1
    next
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
