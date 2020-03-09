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
    store.executorList(true).foreach { executor =>
      val prefix = "metrics_executor_"
      val labels = Seq(
        "application_id" -> store.applicationInfo.id,
        "application_name" -> store.applicationInfo.name,
        "executor_id" -> executor.id
      ).map { case (k, v) => s"""$k="$v"""" }.mkString("{", ", ", "}")
      sb.append(s"${prefix}rddBlocks_Count$labels ${executor.rddBlocks}\n")
      sb.append(s"${prefix}memoryUsed_Count$labels ${executor.memoryUsed}\n")
      sb.append(s"${prefix}diskUsed_Count$labels ${executor.diskUsed}\n")
      sb.append(s"${prefix}totalCores_Count$labels ${executor.totalCores}\n")
      sb.append(s"${prefix}maxTasks_Count$labels ${executor.maxTasks}\n")
      sb.append(s"${prefix}activeTasks_Count$labels ${executor.activeTasks}\n")
      sb.append(s"${prefix}failedTasks_Count$labels ${executor.failedTasks}\n")
      sb.append(s"${prefix}completedTasks_Count$labels ${executor.completedTasks}\n")
      sb.append(s"${prefix}totalTasks_Count$labels ${executor.totalTasks}\n")
      sb.append(s"${prefix}totalDuration_Value$labels ${executor.totalDuration}\n")
      sb.append(s"${prefix}totalGCTime_Value$labels ${executor.totalGCTime}\n")
      sb.append(s"${prefix}totalInputBytes_Count$labels ${executor.totalInputBytes}\n")
      sb.append(s"${prefix}totalShuffleRead_Count$labels ${executor.totalShuffleRead}\n")
      sb.append(s"${prefix}totalShuffleWrite_Count$labels ${executor.totalShuffleWrite}\n")
      sb.append(s"${prefix}maxMemory_Count$labels ${executor.maxMemory}\n")
      executor.executorLogs.foreach { case (k, v) => }
      executor.memoryMetrics.foreach { m =>
        sb.append(s"${prefix}usedOnHeapStorageMemory_Count$labels ${m.usedOnHeapStorageMemory}\n")
        sb.append(s"${prefix}usedOffHeapStorageMemory_Count$labels ${m.usedOffHeapStorageMemory}\n")
        sb.append(s"${prefix}totalOnHeapStorageMemory_Count$labels ${m.totalOnHeapStorageMemory}\n")
        sb.append(s"${prefix}totalOffHeapStorageMemory_Count$labels " +
          s"${m.totalOffHeapStorageMemory}\n")
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
          sb.append(s"$prefix${name}_Count$labels ${m.getMetricValue(name)}\n")
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
