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
import org.apache.spark.sql.execution.{LeafExecNode, SparkPlan}
import org.apache.spark.sql.execution.streaming.WatermarkTracker.DEFAULT_WATERMARK_MS

class WatermarkPropagationCalculator(plan: SparkPlan) extends Logging {
  private val statefulOperatorToWatermarkForLateEvents = mutable.HashMap[Long, Long]()
  private val statefulOperatorToWatermarkForEviction = mutable.HashMap[Long, Long]()

  def getWatermarkForLateEvents(stateOpId: Long): Long = {
    statefulOperatorToWatermarkForLateEvents.getOrElse(stateOpId,
      WatermarkTracker.DEFAULT_WATERMARK_MS)
  }

  def getWatermarkForEviction(stateOpId: Long): Long = {
    statefulOperatorToWatermarkForEviction.getOrElse(stateOpId,
      WatermarkTracker.DEFAULT_WATERMARK_MS)
  }

  def calculate(watermarkForLateEvents: Long, watermarkForEviction: Long): Unit = {
    val statefulOperatorIdToNodeId = mutable.HashMap[Long, Int]()
    val nodeToOutputWatermarkForLateEvent = mutable.HashMap[Int, Long]()
    val nodeToOutputWatermarkForEviction = mutable.HashMap[Int, Long]()
    val statefulOperatorToInputWatermark = mutable.HashMap[Long, (Long, Long)]()

    // This calculation relies on post-order traversal of the query plan.
    plan.transformUp {
      case node: StateStoreWriter =>
        val stOpId = node.stateInfo.get.operatorId
        statefulOperatorIdToNodeId.put(stOpId, node.id)

        val inputWatermarkForLateEvents = inputWatermark(node, nodeToOutputWatermarkForLateEvent)
        val inputWatermarkForEviction = inputWatermark(node, nodeToOutputWatermarkForEviction)

        val outputWatermarkForLateEvents = node.produceOutputWatermark(inputWatermarkForLateEvents)
        val outputWatermarkForEviction = Math.max(
          outputWatermarkForLateEvents,
          node.produceOutputWatermark(inputWatermarkForEviction))

        nodeToOutputWatermarkForLateEvent.put(node.id, outputWatermarkForLateEvents)
        nodeToOutputWatermarkForEviction.put(node.id, outputWatermarkForEviction)
        statefulOperatorToInputWatermark.put(stOpId,
          (inputWatermarkForLateEvents, inputWatermarkForEviction))
        node

      case node: LeafExecNode =>
        nodeToOutputWatermarkForLateEvent.put(node.id, watermarkForLateEvents)
        nodeToOutputWatermarkForEviction.put(node.id, watermarkForEviction)
        node

      case node =>
        val inputWatermarkForLateEvents = inputWatermark(node, nodeToOutputWatermarkForLateEvent)
        val inputWatermarkForEviction = inputWatermark(node, nodeToOutputWatermarkForEviction)

        nodeToOutputWatermarkForLateEvent.put(node.id, inputWatermarkForLateEvents)
        nodeToOutputWatermarkForEviction.put(node.id, inputWatermarkForEviction)
        node
    }

    logDebug("watermark update ----------------------------------")
    logDebug("BEFORE ===================================================")
    logDebug(s"late events: $statefulOperatorToWatermarkForLateEvents")
    logDebug(s"eviction: $statefulOperatorToWatermarkForEviction")

    statefulOperatorToWatermarkForLateEvents.clear()
    statefulOperatorToWatermarkForLateEvents ++= statefulOperatorToInputWatermark.mapValues(_._1)

    statefulOperatorToWatermarkForEviction.clear()
    statefulOperatorToWatermarkForEviction ++= statefulOperatorToInputWatermark.mapValues(_._2)

    logDebug("AFTER ===================================================")
    logDebug(s"late events: $statefulOperatorToWatermarkForLateEvents")
    logDebug(s"eviction: $statefulOperatorToWatermarkForEviction")
  }

  private def inputWatermark(
      node: SparkPlan,
      nodeToOutputWatermark: mutable.HashMap[Int, Long]): Long = {
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

    if (inputWatermarks.nonEmpty) {
      val minCurrInputWatermarkMs = inputWatermarks.min
      minCurrInputWatermarkMs
    } else {
      DEFAULT_WATERMARK_MS
    }
  }
}
