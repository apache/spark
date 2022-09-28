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

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.types.IntegerType

/**
 * This suite is used to test [[LogicalPlan]]'s `transformUp/transformDown`.
 */
class LogicalPlanSuite extends SparkFunSuite {
  private var invocationCount = 0
  private val function: PartialFunction[LogicalPlan, LogicalPlan] = {
    case p: Project =>
      invocationCount += 1
      p
  }

  private val testRelation = LocalRelation()

  test("transformUp runs on operators") {
    invocationCount = 0
    val plan = Project(Nil, testRelation)
    plan transformUp function

    assert(invocationCount === 1)

    invocationCount = 0
    plan transformDown function
    assert(invocationCount === 1)
  }

  test("transformUp runs on operators recursively") {
    invocationCount = 0
    val plan = Project(Nil, Project(Nil, testRelation))
    plan transformUp function

    assert(invocationCount === 2)

    invocationCount = 0
    plan transformDown function
    assert(invocationCount === 2)
  }

  test("isStreaming") {
    val relation = LocalRelation(AttributeReference("a", IntegerType, nullable = true)())
    val incrementalRelation = LocalRelation(
      Seq(AttributeReference("a", IntegerType, nullable = true)()), isStreaming = true)

    case class TestBinaryRelation(left: LogicalPlan, right: LogicalPlan) extends BinaryNode {
      override def output: Seq[Attribute] = left.output ++ right.output
      override protected def withNewChildrenInternal(
          newLeft: LogicalPlan, newRight: LogicalPlan): LogicalPlan =
        copy(left = newLeft, right = newRight)
    }

    require(relation.isStreaming === false)
    require(incrementalRelation.isStreaming)
    assert(TestBinaryRelation(relation, relation).isStreaming === false)
    assert(TestBinaryRelation(incrementalRelation, relation).isStreaming)
    assert(TestBinaryRelation(relation, incrementalRelation).isStreaming)
    assert(TestBinaryRelation(incrementalRelation, incrementalRelation).isStreaming)
  }

  test("transformExpressions works with a Stream") {
    val id1 = NamedExpression.newExprId
    val id2 = NamedExpression.newExprId
    val plan = Project(Stream(
      Alias(Literal(1), "a")(exprId = id1),
      Alias(Literal(2), "b")(exprId = id2)),
      OneRowRelation())
    val result = plan.transformExpressions {
      case Literal(v: Int, IntegerType) if v != 1 =>
        Literal(v + 1, IntegerType)
    }
    val expected = Project(Stream(
      Alias(Literal(1), "a")(exprId = id1),
      Alias(Literal(3), "b")(exprId = id2)),
      OneRowRelation())
    assert(result.sameResult(expected))
  }

  test("SPARK-35231: logical.Range override maxRowsPerPartition") {
    assert(Range(0, 100, 1, 3).maxRowsPerPartition === Some(34))
    assert(Range(0, 100, 1, 4).maxRowsPerPartition === Some(25))
    assert(Range(0, 100, 1, 3).select($"id").maxRowsPerPartition === Some(34))
    assert(Range(0, 100, 1, 3).where($"id" % 2 === 1).maxRowsPerPartition === Some(34))
  }

  test("SPARK-38286: Union's maxRows and maxRowsPerPartition may overflow") {
    val query1 = Range(0, Long.MaxValue, 1, 1)
    val query2 = Range(0, 100, 1, 10)
    val query = query1.union(query2)
    assert(query.maxRows.isEmpty)
    assert(query.maxRowsPerPartition.isEmpty)
  }

  test("SPARK-37961: add maxRows/maxRowsPerPartition for some logical nodes") {
    val range = Range(0, 100, 1, 3)
    assert(range.maxRows === Some(100))
    assert(range.maxRowsPerPartition === Some(34))

    val sort = Sort(Seq('id.asc), false, range)
    assert(sort.maxRows === Some(100))
    assert(sort.maxRowsPerPartition === Some(34))
    val sort2 = Sort(Seq('id.asc), true, range)
    assert(sort2.maxRows === Some(100))
    assert(sort2.maxRowsPerPartition === Some(100))

    val c1 = Literal(1).as('a).toAttribute.newInstance().withNullability(true)
    val c2 = Literal(2).as('b).toAttribute.newInstance().withNullability(true)
    val expand = Expand(
      Seq(Seq(Literal(null), 'b), Seq('a, Literal(null))),
      Seq(c1, c2),
      sort.select('id as 'a, 'id + 1 as 'b))
    assert(expand.maxRows === Some(200))
    assert(expand.maxRowsPerPartition === Some(68))

    val sample = Sample(0.1, 0.9, false, 42, expand)
    assert(sample.maxRows === Some(200))
    assert(sample.maxRowsPerPartition === Some(68))
  }
}
