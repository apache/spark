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

import java.util.Locale

import scala.collection.mutable

import org.apache.spark.internal.Logging
import org.apache.spark.sql.RuntimeConfig
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.internal.SQLConf

/**
 * Policy to define how to choose a new global watermark value if there are
 * multiple watermark operators in a streaming query.
 */
sealed trait MultipleWatermarkPolicy {
  def chooseGlobalWatermark(operatorWatermarks: Seq[Long]): Long
}

object MultipleWatermarkPolicy {
  val DEFAULT_POLICY_NAME = "min"

  def apply(policyName: String): MultipleWatermarkPolicy = {
    policyName.toLowerCase(Locale.ROOT) match {
      case DEFAULT_POLICY_NAME => MinWatermark
      case "max" => MaxWatermark
      case _ =>
        throw new IllegalArgumentException(s"Could not recognize watermark policy '$policyName'")
    }
  }
}

/**
 * Policy to choose the *min* of the operator watermark values as the global watermark value.
 * Note that this is the safe (hence default) policy as the global watermark will advance
 * only if all the individual operator watermarks have advanced. In other words, in a
 * streaming query with multiple input streams and watermarks defined on all of them,
 * the global watermark will advance as slowly as the slowest input. So if there is watermark
 * based state cleanup or late-data dropping, then this policy is the most conservative one.
 */
case object MinWatermark extends MultipleWatermarkPolicy {
  def chooseGlobalWatermark(operatorWatermarks: Seq[Long]): Long = {
    assert(operatorWatermarks.nonEmpty)
    operatorWatermarks.min
  }
}

/**
 * Policy to choose the *max* of the operator watermark values as the global watermark value. So the
 * global watermark will advance if any of the individual operator watermarks has advanced.
 * In other words, in a streaming query with multiple input streams and watermarks defined on all
 * of them, the global watermark will advance as fast as the fastest input. So if there is watermark
 * based state cleanup or late-data dropping, then this policy is the most aggressive one and
 * may lead to unexpected behavior if the data of the slow stream is delayed.
 */
case object MaxWatermark extends MultipleWatermarkPolicy {
  def chooseGlobalWatermark(operatorWatermarks: Seq[Long]): Long = {
    assert(operatorWatermarks.nonEmpty)
    operatorWatermarks.max
  }
}

/** Tracks the watermark value of a streaming query based on a given `policy` */
case class WatermarkTracker(policy: MultipleWatermarkPolicy) extends Logging {
  import WatermarkTracker._

  // FIXME: we won't use this anymore, but need to consider about backward compatibility as well...
  private val operatorToWatermarkMap = mutable.HashMap[Int, Long]()
  private var globalWatermarkMs: Long = 0

  private val statefulOperatorToWatermarkForLateEvents = mutable.HashMap[Long, Long]()
  private val statefulOperatorToWatermarkForEviction = mutable.HashMap[Long, Long]()

  def setOperatorWatermarks(
      operatorWatermarksForLateEvents: Map[Long, Long],
      operatorWatermarksForEviction: Map[Long, Long]): Unit = synchronized {
    // FIXME: take max between two instead of simply clearing out old version of watermarks
    statefulOperatorToWatermarkForLateEvents.clear()
    statefulOperatorToWatermarkForEviction.clear()
    statefulOperatorToWatermarkForLateEvents ++= operatorWatermarksForLateEvents
    statefulOperatorToWatermarkForEviction ++= operatorWatermarksForEviction
  }

  def setWatermark(newWatermarkMs: Long): Unit = synchronized {
    globalWatermarkMs = newWatermarkMs
  }

  def updateWatermark(executedPlan: SparkPlan): Unit = synchronized {
    updateGlobalWatermark(executedPlan)

    // FIXME: need to find a way to retain backward compatibility, like when global watermark is
    //  only available in the checkpoint.

    val statefulOperatorIdToNodeId = mutable.HashMap[Long, Int]()
    val nodeToOutputWatermark = mutable.HashMap[Int, Long]()
    val nextStatefulOperatorToWatermark = mutable.HashMap[Long, (Long, Long)]()

    // This calculation relies on post-order traversal of the query plan.
    executedPlan.transformUp {
      case node: EventTimeWatermarkExec =>
        val stOpId = node.stateInfo.get.operatorId
        statefulOperatorIdToNodeId.put(stOpId, node.id)

        val oldWatermarkMs = statefulOperatorToWatermarkForEviction
          .getOrElse(stOpId, DEFAULT_WATERMARK_MS)

        val newWatermarkMs = if (node.eventTimeStats.value.count > 0) {
          logDebug(s"Observed event time stats from watermark op " +
            s"${stOpId * -1}: ${node.eventTimeStats.value}")
          node.eventTimeStats.value.max - node.delayMs
        } else {
          DEFAULT_WATERMARK_MS
        }

        val finalWatermarkMs = Math.max(oldWatermarkMs, newWatermarkMs)
        nextStatefulOperatorToWatermark.put(stOpId, (oldWatermarkMs, finalWatermarkMs))
        nodeToOutputWatermark.put(node.id, finalWatermarkMs)
        node

      case node: StateStoreWriter =>
        val stOpId = node.stateInfo.get.operatorId
        statefulOperatorIdToNodeId.put(stOpId, node.id)

        val oldWatermarkMs = statefulOperatorToWatermarkForEviction
          .getOrElse(stOpId, DEFAULT_WATERMARK_MS)

        val inputWatermarks = node.children.map { child =>
          nodeToOutputWatermark.getOrElse(child.id, {
            throw new IllegalStateException(
              s"watermark for the node ${child.id} should be registered")
          })
        }.filter { case curr =>
          // This path is to exclude children from watermark calculation
          // which don't have watermark information
          curr >= 0
        }

        val minCurrInputWatermarkMs = if (inputWatermarks.nonEmpty) {
          inputWatermarks.min
        } else {
          DEFAULT_WATERMARK_MS
        }

        val newWatermarkMs = node.produceWatermark(minCurrInputWatermarkMs)
        val finalWatermarkMs = Math.max(oldWatermarkMs, newWatermarkMs)
        nodeToOutputWatermark.put(node.id, finalWatermarkMs)
        nextStatefulOperatorToWatermark.put(stOpId,
          (oldWatermarkMs, newWatermarkMs))
        node

      case node =>
        // pass-through, but also consider multiple children like the case of union
        val inputWatermarks = node.children.map { child =>
          nodeToOutputWatermark.getOrElse(child.id, {
            throw new IllegalStateException(
              s"watermark for the node ${child.id} should be registered")
          })
        }.filter { case curr =>
          // This path is to exclude children from watermark calculation
          // which don't have watermark information
          curr >= 0
        }

        val finalWatermarkMs = if (inputWatermarks.nonEmpty) {
          val minCurrInputWatermarkMs = inputWatermarks.min
          minCurrInputWatermarkMs
        } else {
          DEFAULT_WATERMARK_MS
        }

        nodeToOutputWatermark.put(node.id, finalWatermarkMs)
        node
    }

    logDebug("watermark update ----------------------------------")
    logDebug("BEFORE ===================================================")
    logDebug(s"late events: $statefulOperatorToWatermarkForLateEvents")
    logDebug(s"eviction: $statefulOperatorToWatermarkForEviction")

    statefulOperatorToWatermarkForLateEvents.clear()
    statefulOperatorToWatermarkForLateEvents ++= nextStatefulOperatorToWatermark.mapValues(_._1)

    statefulOperatorToWatermarkForEviction.clear()
    statefulOperatorToWatermarkForEviction ++= nextStatefulOperatorToWatermark.mapValues(_._2)

    logDebug("AFTER ===================================================")
    logDebug(s"late events: $statefulOperatorToWatermarkForLateEvents")
    logDebug(s"eviction: $statefulOperatorToWatermarkForEviction")
  }

  def operatorWatermarkForLateEvents(id: Long): Option[Long] = synchronized {
    statefulOperatorToWatermarkForLateEvents.get(id)
  }

  def operatorWatermarkForEviction(id: Long): Option[Long] = synchronized {
    statefulOperatorToWatermarkForEviction.get(id)
  }

  // FIXME: temporarily left to make tests pass... we need to fix all tests depending on
  //  global watermark, but then we will also need to expose watermark information per operator
  private def updateGlobalWatermark(executedPlan: SparkPlan): Unit = synchronized {
    val watermarkOperators = executedPlan.collect {
      case e: EventTimeWatermarkExec => e
    }
    if (watermarkOperators.isEmpty) return

    watermarkOperators.zipWithIndex.foreach {
      case (e, index) if e.eventTimeStats.value.count > 0 =>
        logDebug(s"Observed event time stats $index: ${e.eventTimeStats.value}")
        val newWatermarkMs = e.eventTimeStats.value.max - e.delayMs
        val prevWatermarkMs = operatorToWatermarkMap.get(index)
        if (prevWatermarkMs.isEmpty || newWatermarkMs > prevWatermarkMs.get) {
          operatorToWatermarkMap.put(index, newWatermarkMs)
        }

      // Populate 0 if we haven't seen any data yet for this watermark node.
      case (_, index) =>
        if (!operatorToWatermarkMap.isDefinedAt(index)) {
          operatorToWatermarkMap.put(index, 0)
        }
    }

    // Update the global watermark accordingly to the chosen policy. To find all available policies
    // and their semantics, please check the comments of
    // `org.apache.spark.sql.execution.streaming.MultipleWatermarkPolicy` implementations.
    val chosenGlobalWatermark = policy.chooseGlobalWatermark(operatorToWatermarkMap.values.toSeq)
    if (chosenGlobalWatermark > globalWatermarkMs) {
      logInfo(s"Updating event-time watermark from $globalWatermarkMs to $chosenGlobalWatermark ms")
      globalWatermarkMs = chosenGlobalWatermark
    } else {
      logDebug(s"Event time watermark didn't move: $chosenGlobalWatermark < $globalWatermarkMs")
    }
  }

  def currentWatermark: Long = synchronized { globalWatermarkMs }

  def currentOperatorWatermarks: (Map[Long, Long], Map[Long, Long]) = synchronized {
    (statefulOperatorToWatermarkForLateEvents.toMap, statefulOperatorToWatermarkForEviction.toMap)
  }
}

object WatermarkTracker {
  // FIXME: Can we change this to -1L?
  // this has to be equal or higher than 0 due to check condition of FlatMapGroupsWithState.
  val DEFAULT_WATERMARK_MS = 0L

  def apply(conf: RuntimeConfig): WatermarkTracker = {
    // If the session has been explicitly configured to use non-default policy then use it,
    // otherwise use the default `min` policy as thats the safe thing to do.
    // When recovering from a checkpoint location, it is expected that the `conf` will already
    // be configured with the value present in the checkpoint. If there is no policy explicitly
    // saved in the checkpoint (e.g., old checkpoints), then the default `min` policy is enforced
    // through defaults specified in OffsetSeqMetadata.setSessionConf().
    val policyName = conf.get(
      SQLConf.STREAMING_MULTIPLE_WATERMARK_POLICY, MultipleWatermarkPolicy.DEFAULT_POLICY_NAME)
    new WatermarkTracker(MultipleWatermarkPolicy(policyName))
  }
}
