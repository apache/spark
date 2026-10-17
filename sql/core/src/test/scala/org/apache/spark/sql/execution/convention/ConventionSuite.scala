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

package org.apache.spark.sql.execution.convention

import org.apache.spark.SparkException
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.physical.SinglePartition
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.adaptive.{AQEShuffleReadExec, ShuffleQueryStageExec}
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.vectorized.ColumnarBatch

class ConventionSuite extends SharedSparkSession {
  import ConventionSuite._
  private val transitionGraph = TransitionGraph()
  transitionGraph.register(FooBatchType)
  transitionGraph.register(BarBatchType)
  transitionGraph.register(IsolatedBatchType)

  private def insert(plan: SparkPlan, outputsColumnar: Boolean = false): SparkPlan =
    ApplyColumnarRulesAndInsertTransitions(Nil, outputsColumnar, transitionGraph).apply(plan)

  test("Spark plans keep the existing transitions") {
    assert(insert(RowLeaf(), outputsColumnar = true) == RowToColumnarExec(RowLeaf()))
    assert(insert(SparkBatchLeaf()) == ColumnarToRowExec(SparkBatchLeaf()))
    assert(insert(RowUnary(SparkBatchLeaf())) ==
      RowUnary(ColumnarToRowExec(SparkBatchLeaf())))
    assert(insert(SparkBatchUnary(RowLeaf()), outputsColumnar = true) ==
      SparkBatchUnary(RowToColumnarExec(RowLeaf())))
    // A dual-mode plan's children follow the requested execution mode.
    assert(insert(RowUnary(DualUnary(SparkBatchLeaf()))) ==
      RowUnary(DualUnary(ColumnarToRowExec(SparkBatchLeaf()))))
    assert(insert(DualUnary(RowLeaf())) == DualUnary(RowLeaf()))
    assert(insert(DualUnary(RowLeaf()), outputsColumnar = true) ==
      DualUnary(RowToColumnarExec(RowLeaf())))
    // Existing transitions are kept.
    val c2r = ColumnarToRowExec(SparkBatchLeaf())
    assert(insert(c2r) eq c2r)
  }

  test("union aligns mixed row-based and columnar-only children") {
    assert(insert(UnionExec(Seq(RowLeaf(), SparkBatchLeaf()))) ==
      UnionExec(Seq(RowLeaf(), ColumnarToRowExec(SparkBatchLeaf()))))
    assert(insert(UnionExec(Seq(RowLeaf(), FooLeaf())), outputsColumnar = true) ==
      RowToColumnarExec(UnionExec(Seq(RowLeaf(), FooToRowExec(FooLeaf())))))
  }

  test("union keeps dual-mode custom batch children row-based for row output") {
    val plan = UnionExec(Seq(DualFooLeaf(), DualFooLeaf()))
    assert(plan.supportsRowBased && plan.supportsColumnar)

    val columnarPlan = insert(plan, outputsColumnar = true)
    assert(columnarPlan.supportsColumnar && !columnarPlan.supportsRowBased)
    assert(columnarPlan == UnionExec(Seq.fill(2)(
      RowToColumnarExec(FooToRowExec(DualFooLeaf())))))

    val rowPlan = insert(plan)
    assert(rowPlan == plan)
  }

  test("union aligns columnar-only custom batch children to its batch type") {
    val plan = UnionExec(Seq(FooLeaf(), FooLeaf()))
    val aligned = UnionExec(Seq.fill(2)(RowToColumnarExec(FooToRowExec(FooLeaf()))))
    assert(insert(plan, outputsColumnar = true) == aligned)
    assert(insert(plan) == ColumnarToRowExec(aligned))
  }

  test("plans must declare a row or batch convention") {
    val plan = MixedBinary(RowLeaf(), SparkBatchLeaf())
    val e = intercept[IllegalArgumentException](insert(plan))
    assert(e.getMessage.contains("not registered"))
    intercept[IllegalArgumentException](insert(plan, outputsColumnar = true))
  }

  test("custom batch type: transitions from / to Spark layouts") {
    assert(insert(RowUnary(FooLeaf())) ==
      RowUnary(FooToRowExec(FooLeaf())))
    assert(insert(FooUnary(SparkBatchLeaf())) ==
      FooToRowExec(FooUnary(RowToFooExec(ColumnarToRowExec(SparkBatchLeaf())))))
    assert(insert(FooUnary(RowLeaf()), outputsColumnar = true) ==
      RowToColumnarExec(FooToRowExec(FooUnary(RowToFooExec(RowLeaf())))))
    // Adjacent plans with the same custom type need no transition.
    assert(insert(FooUnary(FooLeaf()), outputsColumnar = true) ==
      RowToColumnarExec(FooToRowExec(FooUnary(FooLeaf()))))
  }

  test("Arrow batch type uses row transitions") {
    assert(insert(RowUnary(ArrowLeaf())) == RowUnary(ColumnarToRowExec(ArrowLeaf())))
    assert(insert(SparkBatchUnary(ArrowLeaf()), outputsColumnar = true) ==
      SparkBatchUnary(RowToColumnarExec(ColumnarToRowExec(ArrowLeaf()))))
  }

  test("different batch layouts transition through rows") {
    assert(transitionGraph.findPathOrThrow(FooBatchType, BarBatchType).size == 2)
    assert(insert(BarUnary(FooLeaf()), outputsColumnar = true) ==
      RowToColumnarExec(BarToRowExec(BarUnary(RowToBarExec(FooToRowExec(FooLeaf()))))))
  }

  test("direct batch-to-batch transitions are unsupported") {
    val e = intercept[IllegalArgumentException] {
      transitionGraph.addEdge(FooBatchType, BarBatchType, Transition.empty)
    }
    assert(e.getMessage.contains("Direct batch-to-batch transitions are not supported"))
  }

  test("registration and graph insertions reject duplicates") {
    assert(transitionGraph.registeredTypes.count(_ == FooBatchType) == 1)
    intercept[IllegalArgumentException] {
      transitionGraph.register(FooBatchType)
    }
    intercept[IllegalArgumentException] {
      transitionGraph.addEdge(FooBatchType, RowType.SparkRowType, Transition.empty)
    }
  }

  test("type registration uses the supplied graph") {
    val graph = TransitionGraph()
    val otherGraph = TransitionGraph()
    graph.register(FooBatchType)
    assert(graph.findPathOrThrow(FooBatchType, RowType.SparkRowType).size == 1)
    assert(!otherGraph.registeredTypes.contains(FooBatchType))
    otherGraph.register(FooBatchType)
    assert(otherGraph.findPathOrThrow(RowType.SparkRowType, FooBatchType).size == 1)
    intercept[IllegalArgumentException] {
      graph.register(FooBatchType)
    }
  }

  test("custom batch transitions are preserved when the rule runs again") {
    val plan = insert(BarUnary(FooLeaf()), outputsColumnar = true)
    assert(insert(plan, outputsColumnar = true) == plan)
  }

  test("adding an edge requires registered vertices") {
    val unregistered = new RowType {}
    intercept[IllegalArgumentException] {
      transitionGraph.addEdge(unregistered, FooBatchType, Transition.empty)
    }
    intercept[IllegalArgumentException] {
      transitionGraph.addEdge(FooBatchType, unregistered, Transition.empty)
    }
    assert(!transitionGraph.registeredTypes.contains(unregistered))
  }

  test("path lookup does not register types") {
    val unregistered = new RowType {}
    intercept[IllegalArgumentException] {
      transitionGraph.findPath(unregistered, RowType.SparkRowType)
    }
    intercept[IllegalArgumentException] {
      transitionGraph.findPath(RowType.SparkRowType, unregistered)
    }
    assert(!transitionGraph.registeredTypes.contains(unregistered))
  }

  test("no transition path") {
    val e = intercept[SparkException](insert(RowUnary(IsolatedLeaf())))
    assert(e.getMessage.contains("No transition"))
  }

  test("AQE shuffle reads preserve custom conventions and their stage child") {
    val shuffle = new ShuffleExchangeExec(SinglePartition, FooLeaf()) with BatchOnly {
      override def batchType: BatchType = FooBatchType
    }
    val stage = ShuffleQueryStageExec(0, shuffle, shuffle)
    val reader = AQEShuffleReadExec(stage, Seq(CoalescedPartitionSpec(0, 1)))
    assert(reader.convention == stage.convention)
    assert(reader.supportsColumnar && !reader.supportsRowBased)

    val rowPlan = insert(reader)
    assert(rowPlan == FooToRowExec(reader))
    assert(rowPlan.children.head.asInstanceOf[AQEShuffleReadExec].child eq stage)
    assert(insert(reader, outputsColumnar = true) ==
      RowToColumnarExec(FooToRowExec(reader)))
  }
}

object ConventionSuite {
  case object FooBatchType extends BatchType {
    override protected[convention] def registerTransitions(
        implicit graph: TransitionGraph): Unit = {
      fromRow(RowType.SparkRowType, Transition(RowToFooExec(_)))
      toRow(RowType.SparkRowType, Transition(FooToRowExec(_)))
    }
  }

  case object BarBatchType extends BatchType {
    override protected[convention] def registerTransitions(
        implicit graph: TransitionGraph): Unit = {
      fromRow(RowType.SparkRowType, Transition(RowToBarExec(_)))
      toRow(RowType.SparkRowType, Transition(BarToRowExec(_)))
    }
  }

  case object IsolatedBatchType extends BatchType

  trait MockExec extends SparkPlan {
    override def output: Seq[Attribute] = Nil
    override protected def doExecute(): RDD[InternalRow] = throw new UnsupportedOperationException
    override protected def doExecuteColumnar(): RDD[ColumnarBatch] =
      throw new UnsupportedOperationException
  }

  trait MockLeaf extends LeafExecNode with MockExec

  abstract class MockUnary extends UnaryExecNode with MockExec {
    override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
      getClass.getConstructors.head.newInstance(newChild).asInstanceOf[SparkPlan]
  }

  trait BatchOnly extends SparkPlan {
    def batchType: BatchType
    override def supportsColumnar: Boolean = true
    override def supportsRowBased: Boolean = false
    override def convention: Convention = Convention(RowType.None, batchType)
  }

  case class RowLeaf() extends MockLeaf
  case class SparkBatchLeaf() extends MockLeaf {
    override def supportsColumnar: Boolean = true
  }
  case class FooLeaf() extends MockLeaf with BatchOnly {
    override def batchType: BatchType = FooBatchType
  }
  case class DualFooLeaf() extends MockLeaf {
    override def supportsColumnar: Boolean = true
    override def supportsRowBased: Boolean = true
    override def convention: Convention = Convention(RowType.SparkRowType, FooBatchType)
  }
  case class ArrowLeaf() extends MockLeaf with BatchOnly {
    override def batchType: BatchType = BatchType.ArrowBatchType
  }
  case class IsolatedLeaf() extends MockLeaf with BatchOnly {
    override def batchType: BatchType = IsolatedBatchType
  }

  case class MixedBinary(left: SparkPlan, right: SparkPlan) extends BinaryExecNode with MockExec {
    override def supportsColumnar: Boolean = false
    override def supportsRowBased: Boolean = false
    override protected def withNewChildrenInternal(
        newLeft: SparkPlan, newRight: SparkPlan): SparkPlan = copy(left = newLeft, right = newRight)
  }

  case class RowUnary(child: SparkPlan) extends MockUnary
  case class SparkBatchUnary(child: SparkPlan) extends MockUnary {
    override def supportsColumnar: Boolean = true
  }
  case class DualUnary(child: SparkPlan) extends MockUnary {
    override def supportsColumnar: Boolean = true
    override def supportsRowBased: Boolean = true
  }
  case class FooUnary(child: SparkPlan) extends MockUnary with BatchOnly {
    override def batchType: BatchType = FooBatchType
  }
  case class BarUnary(child: SparkPlan) extends MockUnary with BatchOnly {
    override def batchType: BatchType = BarBatchType
  }

  case class RowToFooExec(child: SparkPlan)
      extends MockUnary with BatchOnly with RowToColumnarTransition {
    override def batchType: BatchType = FooBatchType
  }
  case class FooToRowExec(child: SparkPlan) extends MockUnary with ColumnarToRowTransition

  case class RowToBarExec(child: SparkPlan)
      extends MockUnary with BatchOnly with RowToColumnarTransition {
    override def batchType: BatchType = BarBatchType
  }
  case class BarToRowExec(child: SparkPlan) extends MockUnary with ColumnarToRowTransition
}
