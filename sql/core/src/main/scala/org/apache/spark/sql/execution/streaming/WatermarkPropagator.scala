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

import java.{util => jutil}

import scala.collection.mutable

import org.apache.spark.internal.Logging
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.util.Utils

/**
 * Interface for propagating watermark. The implementation is not required to be thread-safe,
 * as all methods are expected to be called from the query execution thread.
 * (The guarantee may change on further improvements on Structured Streaming - update
 * implementations if we change the guarantee.)
 */
sealed trait WatermarkPropagator {
  /**
   * Request to propagate watermark among operators based on origin watermark value. The result
   * should be input watermark per stateful operator, which Spark will request the value by calling
   * getInputWatermarkXXX with operator ID.
   *
   * It is recommended for implementation to cache the result, as Spark can request the propagation
   * multiple times with the same batch ID and origin watermark value.
   */
  def propagate(batchId: Long, plan: SparkPlan, originWatermark: Long): Unit

  /** Provide the calculated input watermark for late events for given stateful operator. */
  def getInputWatermarkForLateEvents(batchId: Long, stateOpId: Long): Long

  /** Provide the calculated input watermark for eviction for given stateful operator. */
  def getInputWatermarkForEviction(batchId: Long, stateOpId: Long): Long

  /**
   * Request to clean up cached result on propagation. Spark will call this method when the given
   * batch ID will be likely to be not re-executed.
   */
  def purge(batchId: Long): Unit
}

/**
 * Do nothing. This is dummy implementation to help creating a dummy IncrementalExecution instance.
 */
object NoOpWatermarkPropagator extends WatermarkPropagator {
  def propagate(batchId: Long, plan: SparkPlan, originWatermark: Long): Unit = {}
  def getInputWatermarkForLateEvents(batchId: Long, stateOpId: Long): Long = Long.MinValue
  def getInputWatermarkForEviction(batchId: Long, stateOpId: Long): Long = Long.MinValue
  def purge(batchId: Long): Unit = {}
}

/**
 * This implementation uses a single global watermark for late events and eviction.
 *
 * This implementation provides the behavior before Structured Streaming supports multiple stateful
 * operators. (prior to SPARK-40925) This is only used for compatibility mode.
 */
class UseSingleWatermarkPropagator extends WatermarkPropagator {
  // We use treemap to sort the key (batchID) and evict old batch IDs efficiently.
  private val batchIdToWatermark: jutil.TreeMap[Long, Long] = new jutil.TreeMap[Long, Long]()

  private def isInitialized(batchId: Long): Boolean = batchIdToWatermark.containsKey(batchId)

  override def propagate(batchId: Long, plan: SparkPlan, originWatermark: Long): Unit = {
    if (batchId < 0) {
      // no-op
    } else if (isInitialized(batchId)) {
      val cached = batchIdToWatermark.get(batchId)
      assert(cached == originWatermark,
        s"Watermark has been changed for the same batch ID! Batch ID: $batchId, " +
          s"Value in cache: $cached, value given: $originWatermark")
    } else {
      batchIdToWatermark.put(batchId, originWatermark)
    }
  }

  private def getInputWatermark(batchId: Long, stateOpId: Long): Long = {
    if (batchId < 0) {
      0
    } else {
      assert(isInitialized(batchId), s"Watermark for batch ID $batchId is not yet set!")
      batchIdToWatermark.get(batchId)
    }
  }

  def getInputWatermarkForLateEvents(batchId: Long, stateOpId: Long): Long =
    getInputWatermark(batchId, stateOpId)

  def getInputWatermarkForEviction(batchId: Long, stateOpId: Long): Long =
    getInputWatermark(batchId, stateOpId)

  override def purge(batchId: Long): Unit = {
    val keyIter = batchIdToWatermark.keySet().iterator()
    var stopIter = false
    while (keyIter.hasNext && !stopIter) {
      val currKey = keyIter.next()
      if (currKey <= batchId) {
        keyIter.remove()
      } else {
        stopIter = true
      }
    }
  }
}

/**
 * This implementation simulates propagation of watermark among operators.
 *
 * It is considered a "simulation" because watermarks are not being physically sent between
 * operators, but rather propagated up the tree via post-order (children first) traversal of
 * the query plan. This allows Structured Streaming to determine the new (input watermark, output
 * watermark) for all nodes.
 *
 * For each node, below logic is applied:
 *
 * - Input watermark for specific node is decided by `min(output watermarks from all children)`.
 *   -- Children providing no input watermark (DEFAULT_WATERMARK_MS) are excluded.
 *   -- If there is no valid input watermark from children, input watermark = DEFAULT_WATERMARK_MS.
 * - Output watermark for specific node is decided as following:
 *   -- watermark nodes: origin watermark value
 *      This could be individual origin watermark value, but we decide to retain global watermark
 *      to keep the watermark model be simple.
 *   -- stateless nodes: same as input watermark
 *   -- stateful nodes: the return value of `op.produceOutputWatermark(input watermark)`.
 *
 *      @see [[StateStoreWriter.produceOutputWatermark]]
 *
 * Note that this implementation will throw an exception if watermark node sees a valid input
 * watermark from children, meaning that we do not support re-definition of watermark.
 *
 * Once the algorithm traverses the physical plan tree, the association between stateful operator
 * and input watermark will be constructed. Spark will request the input watermark for specific
 * stateful operator, which this implementation will give the value from the association.
 *
 * We skip simulation of propagation for the value of watermark as 0. Input watermark for every
 * operator will be 0. (This may not be expected for the case op.produceOutputWatermark returns
 * higher than the input watermark, but it won't happen in most practical cases.)
 */
class PropagateWatermarkSimulator extends WatermarkPropagator with Logging {
  // We use treemap to sort the key (batchID) and evict old batch IDs efficiently.
  private val batchIdToWatermark: jutil.TreeMap[Long, Long] = new jutil.TreeMap[Long, Long]()

  // contains the association for batchId -> (stateful operator ID -> input watermark)
  private val inputWatermarks: mutable.Map[Long, Map[Long, Option[Long]]] =
    mutable.Map[Long, Map[Long, Option[Long]]]()

  private def isInitialized(batchId: Long): Boolean = batchIdToWatermark.containsKey(batchId)

  /**
   * Retrieve the available input watermarks for specific node in the plan. Every child will
   * produce an output watermark, which we capture as input watermark. If the child provides
   * default watermark value (no watermark info), it is excluded.
   */
  private def getInputWatermarks(
      node: SparkPlan,
      nodeToOutputWatermark: mutable.Map[Int, Option[Long]]): Seq[Long] = {
    node.children.flatMap { child =>
      nodeToOutputWatermark.getOrElse(child.id, {
        throw new IllegalStateException(
          s"watermark for the node ${child.id} should be registered")
      })
      // Since we use flatMap here, this will exclude children from watermark calculation
      // which don't have watermark information.
    }
  }

  private def doSimulate(batchId: Long, plan: SparkPlan, originWatermark: Long): Unit = {
    val nodeToOutputWatermark = mutable.HashMap[Int, Option[Long]]()
    val nextStatefulOperatorToWatermark = mutable.HashMap[Long, Option[Long]]()

    // This calculation relies on post-order traversal of the query plan.
    plan.transformUp {
      case node: EventTimeWatermarkExec =>
        val inputWatermarks = getInputWatermarks(node, nodeToOutputWatermark)
        if (inputWatermarks.nonEmpty) {
          throw new AnalysisException(
            errorClass = "_LEGACY_ERROR_TEMP_3076",
            messageParameters = Map("config" -> SQLConf.STATEFUL_OPERATOR_ALLOW_MULTIPLE.key))
        }

        nodeToOutputWatermark.put(node.id, Some(originWatermark))
        node

      case node: StateStoreWriter =>
        val stOpId = node.stateInfo.get.operatorId

        val inputWatermarks = getInputWatermarks(node, nodeToOutputWatermark)

        val finalInputWatermarkMs = if (inputWatermarks.nonEmpty) {
          Some(inputWatermarks.min)
        } else {
          // We can't throw exception here, as we allow stateful operator to process without
          // watermark. E.g. streaming aggregation with update/complete mode.
          None
        }

        val outputWatermarkMs = finalInputWatermarkMs.flatMap { wm =>
          node.produceOutputWatermark(wm)
        }
        nodeToOutputWatermark.put(node.id, outputWatermarkMs)
        nextStatefulOperatorToWatermark.put(stOpId, finalInputWatermarkMs)
        node

      case node =>
        // pass-through, but also consider multiple children like the case of union
        val inputWatermarks = getInputWatermarks(node, nodeToOutputWatermark)
        val finalInputWatermarkMs = if (inputWatermarks.nonEmpty) {
          Some(inputWatermarks.min)
        } else {
          None
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
    if (batchId < 0) {
      // no-op
    } else if (isInitialized(batchId)) {
      val cached = batchIdToWatermark.get(batchId)
      assert(cached == originWatermark,
        s"Watermark has been changed for the same batch ID! Batch ID: $batchId, " +
          s"Value in cache: $cached, value given: $originWatermark")
    } else {
      logDebug(s"watermark for batch ID $batchId is received as $originWatermark, " +
        s"call site: ${Utils.getCallSite().longForm}")

      if (originWatermark == 0) {
        logDebug(s"skipping the propagation for batch $batchId as origin watermark is 0.")
        batchIdToWatermark.put(batchId, 0L)
        inputWatermarks.put(batchId, Map.empty[Long, Option[Long]])
      } else {
        doSimulate(batchId, plan, originWatermark)
      }
    }
  }

  private def getInputWatermark(batchId: Long, stateOpId: Long): Long = {
    if (batchId < 0) {
      0
    } else {
      assert(isInitialized(batchId), s"Watermark for batch ID $batchId is not yet set!")
      inputWatermarks(batchId).get(stateOpId) match {
        case Some(wmOpt) =>
          // In current Spark's logic, event time watermark cannot go down to negative. So even
          // there is no input watermark for operator, the final input watermark for operator should
          // be 0L.
          Math.max(wmOpt.getOrElse(0L), 0L)
        case None =>
          if (batchIdToWatermark.get(batchId) == 0L) {
            // We skip the propagation when the origin watermark is produced as 0L. This is safe,
            // as output watermark is not expected to be later than the input watermark. That said,
            // all operators would have the input watermark as 0L.
            0L
          } else {
            throw new IllegalStateException(s"Watermark for batch ID $batchId and " +
              s"stateOpId $stateOpId is not yet set!")
          }
      }
    }
  }

  override def getInputWatermarkForLateEvents(batchId: Long, stateOpId: Long): Long = {
    // We use watermark for previous microbatch to determine late events.
    getInputWatermark(batchId - 1, stateOpId)
  }

  override def getInputWatermarkForEviction(batchId: Long, stateOpId: Long): Long =
    getInputWatermark(batchId, stateOpId)

  override def purge(batchId: Long): Unit = {
    val keyIter = batchIdToWatermark.keySet().iterator()
    var stopIter = false
    while (keyIter.hasNext && !stopIter) {
      val currKey = keyIter.next()
      if (currKey <= batchId) {
        keyIter.remove()
        inputWatermarks.remove(currKey)
      } else {
        stopIter = true
      }
    }
  }
}

object WatermarkPropagator {
  def apply(conf: SQLConf): WatermarkPropagator = {
    if (conf.getConf(SQLConf.STATEFUL_OPERATOR_ALLOW_MULTIPLE)) {
      new PropagateWatermarkSimulator
    } else {
      new UseSingleWatermarkPropagator
    }
  }

  def noop(): WatermarkPropagator = NoOpWatermarkPropagator
}
