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

package org.apache.spark.sql.execution.streaming

import scala.collection.mutable

import org.apache.spark.internal.Logging
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.streaming.WatermarkPropagator.DEFAULT_WATERMARK_MS
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.util.Utils

trait WatermarkPropagator {
  def isInitialized(batchId: Long): Boolean
  def propagate(batchId: Long, plan: SparkPlan, originWatermark: Long): Unit
  def getInputWatermark(batchId: Long, stateOpId: Long): Long
  def evictBatch(batchId: Long): Unit
}

class NoOpWatermarkPropagator extends WatermarkPropagator {
  def isInitialized(batchId: Long): Boolean = false
  def propagate(batchId: Long, plan: SparkPlan, originWatermark: Long): Unit = {}
  def getInputWatermark(batchId: Long, stateOpId: Long): Long = Long.MinValue
  def evictBatch(batchId: Long): Unit = {}
}

class UseSingleWatermarkPropagator extends WatermarkPropagator {
  private val batchIdToWatermark: mutable.Map[Long, Long] = mutable.Map[Long, Long]()

  override def isInitialized(batchId: Long): Boolean = batchIdToWatermark.contains(batchId)

  override def propagate(batchId: Long, plan: SparkPlan, originWatermark: Long): Unit = {
    if (isInitialized(batchId)) {
      val cached = batchIdToWatermark(batchId)
      assert(cached == originWatermark,
        s"Watermark has been changed for the same batch ID! Batch ID: $batchId, " +
          s"Value in cache: $cached, value given: $originWatermark")
    } else {
      batchIdToWatermark.put(batchId, originWatermark)
    }
  }

  override def getInputWatermark(batchId: Long, stateOpId: Long): Long = {
    assert(isInitialized(batchId), s"Watermark for batch ID $batchId is not yet set!")
    batchIdToWatermark(batchId)
  }

  // FIXME: evict up to the batch ID?
  override def evictBatch(batchId: Long): Unit = batchIdToWatermark.remove(batchId)
}

class PropagateWatermarkSimulator extends WatermarkPropagator with Logging {
  private val batchIdToWatermark: mutable.Map[Long, Long] = mutable.Map[Long, Long]()
  private val inputWatermarks: mutable.Map[Long, Map[Long, Long]] =
    mutable.Map[Long, Map[Long, Long]]()

  override def isInitialized(batchId: Long): Boolean = batchIdToWatermark.contains(batchId)

  private def getInputWatermarks(
      node: SparkPlan,
      nodeToOutputWatermark: mutable.Map[Int, Long]): Seq[Long] = {
    node.children.map { child =>
      nodeToOutputWatermark.getOrElse(child.id, {
        throw new IllegalStateException(
          s"watermark for the node ${child.id} should be registered")
      })
    }.filter { case curr =>
      // This path is to exclude children from watermark calculation
      // which don't have watermark information
      curr != DEFAULT_WATERMARK_MS
    }
  }

  private def doSimulate(batchId: Long, plan: SparkPlan, originWatermark: Long): Unit = {
    val statefulOperatorIdToNodeId = mutable.HashMap[Long, Int]()
    val nodeToOutputWatermark = mutable.HashMap[Int, Long]()
    val nextStatefulOperatorToWatermark = mutable.HashMap[Long, Long]()

    // This calculation relies on post-order traversal of the query plan.
    plan.transformUp {
      case node: EventTimeWatermarkExec =>
        val inputWatermarks = getInputWatermarks(node, nodeToOutputWatermark)
        if (inputWatermarks.nonEmpty) {
          throw new AnalysisException("Redefining watermark is disallowed. You can set the " +
            s"config '${SQLConf.STATEFUL_OPERATOR_ALLOW_MULTIPLE}' to 'false' to restore the " +
            "previous behavior. Note that multiple stateful operators will be disallowed.")
        }

        nodeToOutputWatermark.put(node.id, originWatermark)
        node

      case node: StateStoreWriter =>
        val stOpId = node.stateInfo.get.operatorId
        statefulOperatorIdToNodeId.put(stOpId, node.id)

        val inputWatermarks = getInputWatermarks(node, nodeToOutputWatermark)
        val finalInputWatermarkMs = if (inputWatermarks.nonEmpty) {
          inputWatermarks.min
        } else {
          DEFAULT_WATERMARK_MS
        }

        val newWatermarkMs = node.produceWatermark(finalInputWatermarkMs)
        nodeToOutputWatermark.put(node.id, newWatermarkMs)
        nextStatefulOperatorToWatermark.put(stOpId, newWatermarkMs)
        node

      case node =>
        // pass-through, but also consider multiple children like the case of union
        val inputWatermarks = getInputWatermarks(node, nodeToOutputWatermark)
        val finalInputWatermarkMs = if (inputWatermarks.nonEmpty) {
          val minCurrInputWatermarkMs = inputWatermarks.min
          minCurrInputWatermarkMs
        } else {
          DEFAULT_WATERMARK_MS
        }

        nodeToOutputWatermark.put(node.id, finalInputWatermarkMs)
        node
    }

    inputWatermarks.put(batchId, nextStatefulOperatorToWatermark.toMap)
    batchIdToWatermark.put(batchId, originWatermark)

    logDebug(s"global watermark for batch ID $batchId is set to $originWatermark")
    logDebug(s"input watermarks for batch ID $batchId is set to $nextStatefulOperatorToWatermark")
  }

  override def propagate(batchId: Long, plan: SparkPlan, originWatermark: Long): Unit = {
    if (isInitialized(batchId)) {
      val cached = batchIdToWatermark(batchId)
      assert(cached == originWatermark,
        s"Watermark has been changed for the same batch ID! Batch ID: $batchId, " +
          s"Value in cache: $cached, value given: $originWatermark")
    } else {
      logDebug(s"watermark for batch ID $batchId is received as $originWatermark, " +
        s"call site: ${Utils.getCallSite().longForm}")
      doSimulate(batchId, plan, originWatermark)
    }
  }

  override def getInputWatermark(batchId: Long, stateOpId: Long): Long = {
    assert(isInitialized(batchId), s"Watermark for batch ID $batchId is not yet set!")
    batchIdToWatermark(batchId)
  }

  // FIXME: evict up to the batch ID?
  override def evictBatch(batchId: Long): Unit = {
    batchIdToWatermark.remove(batchId)
    inputWatermarks.remove(batchId)
  }
}

object WatermarkPropagator {
  // FIXME: Can we change this to -1L?
  // this has to be equal or higher than 0 due to check condition of FlatMapGroupsWithState.
  val DEFAULT_WATERMARK_MS = -1L

  def apply(conf: SQLConf): WatermarkPropagator = {
    if (conf.getConf(SQLConf.STATEFUL_OPERATOR_ALLOW_MULTIPLE)) {
      new PropagateWatermarkSimulator
    } else {
      new UseSingleWatermarkPropagator
    }
  }

  def noop(): WatermarkPropagator = new NoOpWatermarkPropagator
}
