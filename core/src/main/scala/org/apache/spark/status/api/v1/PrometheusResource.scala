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
package org.apache.spark.status.api.v1

import javax.ws.rs._
import javax.ws.rs.core.MediaType

import org.eclipse.jetty.servlet.{ServletContextHandler, ServletHolder}
import org.glassfish.jersey.server.ServerProperties
import org.glassfish.jersey.servlet.ServletContainer

import org.apache.spark.ui.SparkUI

/**
 * This aims to expose Executor metrics like REST API which is documented in
 *
 *    https://spark.apache.org/docs/3.0.0/monitoring.html#executor-metrics
 *
 * Note that this is based on ExecutorSummary which is different from ExecutorSource.
 */
@Path("/executors")
private[v1] class PrometheusResource extends ApiRequestContext {
  @GET
  @Path("prometheus")
  @Produces(Array(MediaType.TEXT_PLAIN))
  def executors(): String = {
    val sb = new StringBuilder
    val store = uiRoot.asInstanceOf[SparkUI].store
    val appId = store.applicationInfo.id.replaceAll("[^a-zA-Z0-9]", "_")
    store.executorList(true).foreach { executor =>
      val prefix = s"metrics_${appId}_${executor.id}_executor_"
      sb.append(s"${prefix}rddBlocks_Count ${executor.rddBlocks}\n")
      sb.append(s"${prefix}memoryUsed_Count ${executor.memoryUsed}\n")
      sb.append(s"${prefix}diskUsed_Count ${executor.diskUsed}\n")
      sb.append(s"${prefix}totalCores_Count ${executor.totalCores}\n")
      sb.append(s"${prefix}maxTasks_Count ${executor.maxTasks}\n")
      sb.append(s"${prefix}activeTasks_Count ${executor.activeTasks}\n")
      sb.append(s"${prefix}failedTasks_Count ${executor.failedTasks}\n")
      sb.append(s"${prefix}completedTasks_Count ${executor.completedTasks}\n")
      sb.append(s"${prefix}totalTasks_Count ${executor.totalTasks}\n")
      sb.append(s"${prefix}totalDuration_Value ${executor.totalDuration}\n")
      sb.append(s"${prefix}totalGCTime_Value ${executor.totalGCTime}\n")
      sb.append(s"${prefix}totalInputBytes_Count ${executor.totalInputBytes}\n")
      sb.append(s"${prefix}totalShuffleRead_Count ${executor.totalShuffleRead}\n")
      sb.append(s"${prefix}totalShuffleWrite_Count ${executor.totalShuffleWrite}\n")
      sb.append(s"${prefix}maxMemory_Count ${executor.maxMemory}\n")
      executor.executorLogs.foreach { case (k, v) => }
      executor.memoryMetrics.foreach { m =>
        sb.append(s"${prefix}usedOnHeapStorageMemory_Count ${m.usedOnHeapStorageMemory}\n")
        sb.append(s"${prefix}usedOffHeapStorageMemory_Count ${m.usedOffHeapStorageMemory}\n")
        sb.append(s"${prefix}totalOnHeapStorageMemory_Count ${m.totalOnHeapStorageMemory}\n")
        sb.append(s"${prefix}totalOffHeapStorageMemory_Count ${m.totalOffHeapStorageMemory}\n")
      }
      executor.peakMemoryMetrics.foreach { m =>
        val names = Array(
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
          "ProcessTreeOtherRSSMemory",
          "MinorGCCount",
          "MinorGCTime",
          "MajorGCCount",
          "MajorGCTime"
        )
        names.foreach { name =>
          sb.append(s"$prefix${name}_Count ${m.getMetricValue(name)}\n")
        }
      }
    }
    sb.toString
  }
}

private[spark] object PrometheusResource {
  def getServletHandler(uiRoot: UIRoot): ServletContextHandler = {
    val jerseyContext = new ServletContextHandler(ServletContextHandler.NO_SESSIONS)
    jerseyContext.setContextPath("/metrics")
    val holder: ServletHolder = new ServletHolder(classOf[ServletContainer])
    holder.setInitParameter(ServerProperties.PROVIDER_PACKAGES, "org.apache.spark.status.api.v1")
    UIRootFromServletContext.setUiRoot(jerseyContext, uiRoot)
    jerseyContext.addServlet(holder, "/*")
    jerseyContext
  }
}
