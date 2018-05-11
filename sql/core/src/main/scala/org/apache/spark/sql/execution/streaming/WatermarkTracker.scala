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
import org.apache.spark.sql.execution.SparkPlan

class WatermarkTracker extends Logging {
  private val operatorToWatermarkMap = mutable.HashMap[Int, Long]()
  private var watermarkMs: Long = 0
  private var updated = false

  def setWatermark(newWatermarkMs: Long): Unit = synchronized {
    watermarkMs = newWatermarkMs
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

    // Update the global watermark to the minimum of all watermark nodes.
    // This is the safest option, because only the global watermark is fault-tolerant. Making
    // it the minimum of all individual watermarks guarantees it will never advance past where
    // any individual watermark operator would be if it were in a plan by itself.
    val newWatermarkMs = operatorToWatermarkMap.minBy(_._2)._2
    if (newWatermarkMs > watermarkMs) {
      logInfo(s"Updating eventTime watermark to: $newWatermarkMs ms")
      watermarkMs = newWatermarkMs
      updated = true
    } else {
      logDebug(s"Event time didn't move: $newWatermarkMs < $watermarkMs")
      updated = false
    }
  }

  def currentWatermark: Long = synchronized { watermarkMs }
}
