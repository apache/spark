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
    policyName.toLowerCase match {
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
  private val operatorToWatermarkMap = mutable.HashMap[Int, Long]()
  private var globalWatermarkMs: Long = 0

  def setWatermark(newWatermarkMs: Long): Unit = synchronized {
    globalWatermarkMs = newWatermarkMs
  }

  def updateWatermark(executedPlan: SparkPlan): Unit = synchronized {
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
}

object WatermarkTracker {
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
