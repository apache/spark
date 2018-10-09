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
package org.apache.spark.executor

import scala.collection.mutable

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.metrics.ExecutorMetricType

/**
 * :: DeveloperApi ::
 * Metrics tracked for executors and the driver.
 *
 * Executor-level metrics are sent from each executor to the driver as part of the Heartbeat.
 */
@DeveloperApi
class ExecutorMetrics private[spark] extends Serializable {

  private var metrics = mutable.Map.empty[String, Long]

  /** Returns the Map which given a metric's name will return its value. */
  def getMetrics(): mutable.Map[String, Long] = {
    metrics
  }

  /** Returns the value for the specified metric. */
  def getMetricValue(metricName: String): Long = {
    metrics.get(metricName).get
  }

  /** Returns true if the values for the metrics have been set, false otherwise. */
  def isSet(): Boolean = !metrics.isEmpty

  /**
   * Constructor: create the ExecutorMetrics with using a given map.
   *
   * @param executorMetrics map of executor metric name to value
   */
  private[spark] def this(executorMetrics: Map[String, Long]) {
    this()
    for(m <- ExecutorMetricType.definedMetrics) {
      metrics += (m -> executorMetrics.getOrElse(m, 0L))
    }
  }

  // This method is just added for the use of some of the existing tests.
  // IT SHOULDN't BE USED FOR OTHER PURPOSES
  private[spark] def this(metrics: Array[Long]) {
    this()
    val orderedMetrics = Seq(
      "JVMHeapMemory",
      "JVMOffHeapMemory",
      "OnHeapExecutionMemory",
      "OffHeapExecutionMemory",
      "OnHeapStorageMemory",
      "OffHeapStorageMemory",
      "OnHeapUnifiedMemory",
      "OffHeapUnifiedMemory",
      "DirectPoolMemory",
      "MappedPoolMemory",
      "ProcessTreeJVMVMemory",
      "ProcessTreeJVMRSSMemory",
      "ProcessTreePythonVMemory",
      "ProcessTreePythonRSSMemory",
      "ProcessTreeOtherVMemory",
      "ProcessTreeOtherRSSMemory"
    )

    (0 until orderedMetrics.length).foreach{ m =>
      if ( m < metrics.length) {
        this.metrics += (orderedMetrics(m) -> metrics(m))
      }
      else {
        this.metrics += (orderedMetrics(m) -> 0L)
      }
    }
  }

  /**
   * Compare the specified executor metrics values with the current executor metric values,
   * and update the value for any metrics where the new value for the metric is larger.
   *
   * @param executorMetrics the executor metrics to compare
   * @return if there is a new peak value for any metric
   */
  private[spark] def compareAndUpdatePeakValues(executorMetrics: ExecutorMetrics): Boolean = {
    var updated = false
    for(m <- ExecutorMetricType.definedMetrics) {
      if (!metrics.contains(m)) {
        metrics += (m -> 0)
      }
      if (executorMetrics.getMetrics().contains(m)) {
        val mValue = executorMetrics.getMetrics().get(m).get
        if (mValue > metrics.get(m).get) {
          updated = true
          metrics += (m -> mValue)
        }
      }
    }
    updated
  }
}
