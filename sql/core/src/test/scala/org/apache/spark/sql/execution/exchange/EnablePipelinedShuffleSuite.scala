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

package org.apache.spark.sql.execution.exchange

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.SparkFunSuite
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.catalyst.plans.physical.RoundRobinPartitioning
import org.apache.spark.sql.execution.{CoalesceExec, CollectLimitExec, CollectTailExec, LocalTableScanExec, SparkPlan, TakeOrderedAndProjectExec, UnionExec}
import org.apache.spark.sql.execution.joins.CartesianProductExec
import org.apache.spark.sql.execution.python.AttachDistributedSequenceExec
import org.apache.spark.sql.types.LongType

class EnablePipelinedShuffleSuite extends SparkFunSuite {

  import EnablePipelinedShuffle.readsShuffleThroughUnsupportedConsumer

  private def leaf: SparkPlan = LocalTableScanExec(Nil, Nil, None)

  private def shuffle(child: SparkPlan): SparkPlan =
    ShuffleExchangeExec(RoundRobinPartitioning(2), child)

  private def attach(child: SparkPlan, cache: Boolean): SparkPlan =
    AttachDistributedSequenceExec(AttributeReference("sequence", LongType)(), child, cache)

  test("distributed sequence rejects a shuffle input only when caching is enabled") {
    assert(!readsShuffleThroughUnsupportedConsumer(shuffle(leaf)))
    assert(!readsShuffleThroughUnsupportedConsumer(attach(leaf, cache = true)))
    assert(!readsShuffleThroughUnsupportedConsumer(shuffle(attach(leaf, cache = true))))
    assert(readsShuffleThroughUnsupportedConsumer(attach(shuffle(leaf), cache = true)))
    assert(!readsShuffleThroughUnsupportedConsumer(attach(shuffle(leaf), cache = false)))
  }

  test("cached distributed sequence finds a shuffle in either union branch") {
    val branches = Seq(shuffle(leaf), leaf)
    for (children <- Seq(branches, branches.reverse)) {
      val plan = attach(UnionExec(children), cache = true)
      assert(readsShuffleThroughUnsupportedConsumer(plan))
    }
  }

  test("a shuffle in a sibling does not reject a cached distributed sequence") {
    val branches = Seq(attach(shuffle(leaf), cache = false), attach(leaf, cache = true))
    for (children <- Seq(branches, branches.reverse)) {
      assert(!readsShuffleThroughUnsupportedConsumer(UnionExec(children)))
    }
  }

  test("finding a shuffle in one branch does not skip an unsupported consumer in a later branch") {
    val supported = attach(shuffle(leaf), cache = false)
    val unsupported = attach(shuffle(leaf), cache = true)
    assert(readsShuffleThroughUnsupportedConsumer(UnionExec(Seq(supported, unsupported))))
  }

  test("unsupported consumers below an outer exchange are still checked") {
    val plan = shuffle(attach(shuffle(leaf), cache = true))
    assert(readsShuffleThroughUnsupportedConsumer(plan))
  }

  test("coalesce, cartesian product and limit consumers require a shuffle input") {
    def consumers(child: SparkPlan): Seq[SparkPlan] = Seq(
      CoalesceExec(1, child),
      CartesianProductExec(child, leaf, None),
      CartesianProductExec(leaf, child, None),
      CollectLimitExec(1, child),
      CollectTailExec(1, child),
      TakeOrderedAndProjectExec(1, Nil, Nil, child))

    for (plan <- consumers(leaf)) {
      withClue(s"${plan.nodeName} with no shuffle input: ") {
        assert(!readsShuffleThroughUnsupportedConsumer(plan))
        assert(!readsShuffleThroughUnsupportedConsumer(shuffle(plan)))
      }
    }
    for (plan <- consumers(shuffle(leaf))) {
      withClue(s"${plan.nodeName} with a shuffle input: ") {
        assert(readsShuffleThroughUnsupportedConsumer(plan))
      }
    }
  }

  gridTest("nested cached distributed sequences visit each child once")(Seq(1, 32, 128)) { depth =>
    val wrappers = ArrayBuffer.empty[CountingChildrenExec]
    val nested = (0 until depth).foldLeft(leaf) { (child, _) =>
      val wrapper = CountingChildrenExec(child)
      wrappers += wrapper
      attach(wrapper, cache = true)
    }
    // The exchange is above the cached chain, so no consumer can reject the plan early.
    val plan = shuffle(nested)
    wrappers.foreach(_.childrenAccesses = 0)

    assert(!readsShuffleThroughUnsupportedConsumer(plan))
    wrappers.zipWithIndex.foreach { case (wrapper, index) =>
      assert(wrapper.childrenAccesses == 1,
        s"Wrapper $index at depth $depth had ${wrapper.childrenAccesses} child reads")
    }
  }
}

private case class CountingChildrenExec(child: SparkPlan) extends SparkPlan {
  var childrenAccesses: Int = 0

  override def children: Seq[SparkPlan] = {
    childrenAccesses += 1
    Seq(child)
  }

  override def output: Seq[Attribute] = child.output

  override protected def doExecute(): RDD[InternalRow] =
    throw new UnsupportedOperationException("Test plan must not execute")

  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[SparkPlan]): CountingChildrenExec = copy(child = newChildren.head)
}
