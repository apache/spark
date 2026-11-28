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

import java.util.Collections
import java.util.concurrent.atomic.AtomicInteger

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.Literal.{FalseLiteral, TrueLiteral}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.optimizer.ConstantFolding
import org.apache.spark.sql.catalyst.plans.{LeftAnti, PlanTest}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.connector.catalog.InMemoryRowLevelOperationTable
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{BooleanType, DataType, DateType, DoubleType, LongType, StructType}

class RewriteMergeIntoTableSuite extends PlanTest {
  import RewriteMergeIntoTableSuite.CountingConstant

  private val source = LocalRelation($"sourceId".long)
  private val key = source.output.head
  private val table = new InMemoryRowLevelOperationTable(
    "target", new StructType().add("id", LongType), Array.empty, Collections.emptyMap())
  private val target = DataSourceV2Relation.create(table, None, None)
  private val joinCondition = key === target.output.head

  private def rewrite(actions: Seq[InsertAction]): InsertOnlyMerge = {
    val plan = MergeIntoTable(target, source, joinCondition, Nil, actions, Nil,
      withSchemaEvolution = false)
    assert(plan.resolved && plan.aligned)
    RewriteMergeIntoTable(plan).asInstanceOf[InsertOnlyMerge]
  }

  private def insert(condition: Expression, value: Expression = key): InsertAction = {
    InsertAction(Some(condition), Seq(Assignment(target.output.head, value)))
  }

  private val currentDateCondition =
    CurrentDate(Some("UTC")) === Cast(Literal("today"), DateType, Some("UTC"))
  private val currentTimezoneCondition = key > Cast(Length(CurrentTimeZone()), LongType)

  gridTest("preserve the projection when prepared constants fold")(Seq(
    (key > Cast(Literal("5"), LongType), key > Literal(5L)),
    (Cast(key, DoubleType) > new TryDivide(Literal(10.0), Literal(2.0)),
      Cast(key, DoubleType) > Literal(5.0)),
    (currentDateCondition, currentDateCondition),
    (currentTimezoneCondition, currentTimezoneCondition)
  )) { case (condition, expected) =>
    withSQLConf(SQLConf.ANSI_ENABLED.key -> "true") {
      val result = rewrite(Seq(insert(condition)))
      val project = result.query.asInstanceOf[Project]
      val join = project.child.asInstanceOf[Join]
      assert(join.joinType == LeftAnti)
      comparePlans(join.left, Filter(expected, source))
      assert(project.projectList.head.asInstanceOf[Alias].child == key)
    }
  }

  gridTest("keep failed folds in MergeRows")(Seq("condition", "value", "if input", "if branch")) {
      position =>
    withSQLConf(SQLConf.ANSI_ENABLED.key -> "true") {
      val bad = IntegralDivide(Literal(1L), Literal(0L))
      val action = position match {
        case "condition" => insert(bad === 1L)
        case "value" => insert(TrueLiteral, bad)
        case "if input" => insert(If(bad === 1L, key > 0L, key < 0L))
        case "if branch" => insert(If(key > 0L, bad === 1L, FalseLiteral))
      }
      val result = rewrite(Seq(action))
      assert(result.query.isInstanceOf[MergeRows])
      assert(!action.exists(_.containsTag(ConstantFolding.FAILED_TO_EVALUATE)))
    }
  }

  test("multiple insert actions keep MergeRows") {
    val result = rewrite(Seq(insert(key < 0L, Literal(0L)), insert(TrueLiteral)))
    assert(result.query.isInstanceOf[MergeRows])
  }

  test("a conditional insert value keeps its own failed folds protected") {
    withSQLConf(SQLConf.ANSI_ENABLED.key -> "true") {
      val value = If(key > 0L, key, IntegralDivide(Literal(1L), Literal(0L)))
      val result = rewrite(Seq(insert(TrueLiteral, value)))
      val project = result.query.asInstanceOf[Project]
      assert(project.projectList.head.asInstanceOf[Alias].child == value)
    }
  }

  gridTest("preserve false and null insert conditions")(
      Seq(FalseLiteral, Literal.create(null, BooleanType), Literal(null))
        .map(c => (c, c.dataType))) { case (condition, _) =>
    val result = rewrite(Seq(insert(condition)))
    val project = result.query.asInstanceOf[Project]
    val expected = if (condition.dataType == BooleanType) condition else {
      Cast(condition, BooleanType)
    }
    comparePlans(project.child.asInstanceOf[Join].left, Filter(expected, source))
  }

  test("push a safe condition below the anti join when only a value fails to fold") {
    withSQLConf(SQLConf.ANSI_ENABLED.key -> "true") {
      val condition = key > Cast(Literal("5"), LongType)
      val result = rewrite(Seq(insert(condition, IntegralDivide(Literal(1L), Literal(0L)))))
      val rows = result.query.asInstanceOf[MergeRows]
      comparePlans(rows.child.asInstanceOf[Join].left, Filter(key > Literal(5L), source))
      assert(rows.notMatchedInstructions.head.condition == TrueLiteral)
    }
  }

  gridTest("keep catalog and user functions in conditional evaluation")(for {
    current <- Seq(CurrentDatabase(), CurrentCatalog(), CurrentUser(), CurrentPath())
    inCondition <- Seq(false, true)
  } yield (current, inCondition)) { case (current, inCondition) =>
    val value = Cast(current, LongType)
    val action = if (inCondition) insert(key > value) else insert(TrueLiteral, value)
    assert(rewrite(Seq(action)).query.isInstanceOf[MergeRows])
  }

  test("reuse successful constant folds in the planned values") {
    val counter = new AtomicInteger()
    val result = rewrite(Seq(insert(TrueLiteral, CountingConstant(counter))))
    val optimized = ConstantFolding(result).asInstanceOf[InsertOnlyMerge]
    val project = optimized.query.asInstanceOf[Project]
    assert(project.projectList.head.asInstanceOf[Alias].child == Literal(1L))
    assert(counter.get() == 1)
  }

  test("leave current timestamp values for the optimizer's shared snapshot") {
    val value = Cast(CurrentTimestamp(), LongType)
    val result = rewrite(Seq(insert(TrueLiteral, value)))
    val project = result.query.asInstanceOf[Project]
    assert(project.projectList.head.asInstanceOf[Alias].child == value)
  }
}

object RewriteMergeIntoTableSuite {
  case class CountingConstant(counter: AtomicInteger) extends LeafExpression with CodegenFallback {
    override def dataType: DataType = LongType
    override def nullable: Boolean = false
    override def foldable: Boolean = true
    override def eval(input: InternalRow): Any = counter.incrementAndGet().toLong
  }
}
