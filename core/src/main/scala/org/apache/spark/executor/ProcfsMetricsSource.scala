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

import com.codahale.metrics.{Gauge, MetricRegistry}

import org.apache.spark.SparkEnv
import org.apache.spark.internal.config
import org.apache.spark.metrics.source.Source

private[executor] class ProcfsMetricsSource extends Source {
  override val sourceName = "procfs"
  // We use numMetrics for tracking to only call computAllMetrics once per set of metrics
  var numMetrics: Int = 0
  override val metricRegistry = new MetricRegistry()
  var metrics: Map[String, Long] = Map.empty
  val shouldAddProcessTreeMetricsToMetricsSet =
    SparkEnv.get.conf.get(config.METRICS_PROCESS_TREE_METRICS)

  private def getProcfsMetrics: Map[String, Long] = {
    if (numMetrics == 0) {
      metrics = Map.empty
      val p = ProcfsMetricsGetter.pTreeInfo.computeAllMetrics()
      metrics = Map(
        "JVMVMemory" -> p.jvmVmemTotal,
        "JVMRSSMemory" -> p.jvmRSSTotal,
        "PythonVMemory" -> p.pythonVmemTotal,
        "PythonRSSMemory" -> p.pythonRSSTotal,
        "OtherVMemory" -> p.otherVmemTotal,
        "OtherRSSMemory" -> p.otherRSSTotal)
    }
    numMetrics = numMetrics + 1
    if (numMetrics == 6) {
      numMetrics = 0
    }
    metrics
  }

  private def registerProcfsMetrics[Long](name: String) = {
    metricRegistry.register(MetricRegistry.name("processTree", name), new Gauge[Long] {
      override def getValue: Long = getProcfsMetrics(name).asInstanceOf[Long]
    })
  }

  if (shouldAddProcessTreeMetricsToMetricsSet) {
    registerProcfsMetrics("JVMVMemory")
    registerProcfsMetrics("JVMRSSMemory")
    registerProcfsMetrics("PythonVMemory")
    registerProcfsMetrics("PythonRSSMemory")
    registerProcfsMetrics("OtherVMemory")
    registerProcfsMetrics("OtherRSSMemory")
  }
}
