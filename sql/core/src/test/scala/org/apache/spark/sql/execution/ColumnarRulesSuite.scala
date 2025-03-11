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

package org.apache.spark.sql.execution

import org.apache.spark.SparkUnsupportedOperationException
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.vectorized.ColumnarBatch

class ColumnarRulesSuite extends PlanTest with SharedSparkSession {

  test("Idempotency of columnar rules - RowToColumnar/ColumnarToRow") {
    val rules = ApplyColumnarRulesAndInsertTransitions(
      spark.sessionState.columnarRules, false)

    val plan = UnaryOp(UnaryOp(LeafOp(false), true), false)
    val expected =
      UnaryOp(ColumnarToRowExec(UnaryOp(RowToColumnarExec(LeafOp(false)), true)), false)
    val appliedOnce = rules.apply(plan)
    assert(appliedOnce == expected)
    val appliedTwice = rules.apply(appliedOnce)
    assert(appliedTwice == expected)
  }

  test("Idempotency of columnar rules - ColumnarToRow/RowToColumnar") {
    val rules = ApplyColumnarRulesAndInsertTransitions(
      spark.sessionState.columnarRules, false)

    val plan = UnaryOp(UnaryOp(LeafOp(true), false), true)
    val expected = ColumnarToRowExec(
      UnaryOp(RowToColumnarExec(UnaryOp(ColumnarToRowExec(LeafOp(true)), false)), true))
    val appliedOnce = rules.apply(plan)
    assert(appliedOnce == expected)
    val appliedTwice = rules.apply(appliedOnce)
    assert(appliedTwice == expected)
  }

  test("SPARK-51474: Don't insert redundant ColumnarToRowExec") {
    val rules = ApplyColumnarRulesAndInsertTransitions(
      spark.sessionState.columnarRules, false)

    val plan = CanDoColumnarAndRowOp(UnaryOp(LeafOp(true), true))
    val appliedOnce = rules.apply(plan)
    assert(appliedOnce == plan)
  }
}

case class LeafOp(override val supportsColumnar: Boolean) extends LeafExecNode {
  override protected def doExecute(): RDD[InternalRow] = throw SparkUnsupportedOperationException()
  override def output: Seq[Attribute] = Seq.empty
}

case class UnaryOp(child: SparkPlan, override val supportsColumnar: Boolean) extends UnaryExecNode {
  override protected def doExecute(): RDD[InternalRow] = throw SparkUnsupportedOperationException()
  override def output: Seq[Attribute] = child.output
  override protected def withNewChildInternal(newChild: SparkPlan): UnaryOp = copy(child = newChild)
}

case class CanDoColumnarAndRowOp(child: SparkPlan) extends UnaryExecNode {
  override val supportsRowBased: Boolean = true
  override val supportsColumnar: Boolean = true

  override protected def doExecute(): RDD[InternalRow] = throw SparkUnsupportedOperationException()
  override protected def doExecuteColumnar(): RDD[ColumnarBatch] =
    throw SparkUnsupportedOperationException()
  override def output: Seq[Attribute] = child.output
  override protected def withNewChildInternal(newChild: SparkPlan): CanDoColumnarAndRowOp =
    copy(child = newChild)
}
