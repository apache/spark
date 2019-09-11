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

import java.util.Arrays
import javax.ws.rs._
import javax.ws.rs.core.MediaType

import org.eclipse.jetty.servlet.{ServletContextHandler, ServletHolder}
import org.glassfish.jersey.server.ServerProperties
import org.glassfish.jersey.servlet.ServletContainer

import org.apache.spark.ui.SparkUI

/**
 * This aims to expose `Executor Task Metrics` like REST API which is documented in
 *
 *    https://spark.apache.org/docs/latest/monitoring.html#executor-task-metrics.
 *
 * The field order follows `ExecutorSummary` and `StageData`.
 * Note that this is not the same with `ExecutorSource`.
 */
@Path("/detail")
private[v1] class PrometheusResource extends ApiRequestContext {
  @GET
  @Path("prometheus")
  @Produces(Array(MediaType.TEXT_PLAIN))
  def activeExecutorsAndStages(): String = {
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

    store.stageList(null).foreach { stage =>
      val prefix = f"metrics_${appId}_${stage.stageId}_${stage.attemptId}_stage_"
      sb.append(s"${prefix}numTasks_Count ${stage.numTasks}\n")
      sb.append(s"${prefix}numActiveTasks_Count ${stage.numActiveTasks}\n")
      sb.append(s"${prefix}numCompleteTasks_Count ${stage.numCompleteTasks}\n")
      sb.append(s"${prefix}numFailedTasks_Count ${stage.numFailedTasks}\n")
      sb.append(s"${prefix}numKilledTasks_Count ${stage.numKilledTasks}\n")
      sb.append(s"${prefix}numCompletedIndices_Count ${stage.numCompletedIndices}\n")
      sb.append(s"${prefix}executorDeserializeTime_Count ${stage.executorDeserializeTime}\n")
      sb.append(s"${prefix}executorDeserializeCpuTime_Count ${stage.executorDeserializeCpuTime}\n")
      sb.append(s"${prefix}executorRunTime_Count ${stage.executorRunTime}\n")
      sb.append(s"${prefix}executorCpuTime_Count ${stage.executorCpuTime}\n")
      sb.append(s"${prefix}resultSize_Count ${stage.resultSize}\n")
      sb.append(s"${prefix}jvmGcTime_Count ${stage.jvmGcTime}\n")
      sb.append(s"${prefix}resultSerializationTime_Count ${stage.resultSerializationTime}\n")
      sb.append(s"${prefix}memoryBytesSpilled_Count ${stage.memoryBytesSpilled}\n")
      sb.append(s"${prefix}diskBytesSpilled_Count ${stage.diskBytesSpilled}\n")
      sb.append(s"${prefix}peakExecutionMemory_Count ${stage.peakExecutionMemory}\n")
      sb.append(s"${prefix}inputBytes_Count ${stage.inputBytes}\n")
      sb.append(s"${prefix}inputRecords_Count ${stage.inputRecords}\n")
      sb.append(s"${prefix}outputBytes_Count ${stage.outputBytes}\n")
      sb.append(s"${prefix}outputRecords_Count ${stage.outputRecords}\n")
      sb.append(s"${prefix}shuffleRemoteBlocksFetched_Count ${stage.shuffleRemoteBlocksFetched}\n")
      sb.append(s"${prefix}shuffleLocalBlocksFetched_Count ${stage.shuffleLocalBlocksFetched}\n")
      sb.append(s"${prefix}shuffleFetchWaitTime_Count ${stage.shuffleFetchWaitTime}\n")
      sb.append(s"${prefix}shuffleRemoteBytesRead_Count ${stage.shuffleRemoteBytesRead}\n")
      sb.append(
        s"${prefix}shuffleRemoteBytesReadToDisk_Count ${stage.shuffleRemoteBytesReadToDisk}\n")
      sb.append(s"${prefix}shuffleLocalBytesRead_Count ${stage.shuffleLocalBytesRead}\n")
      sb.append(s"${prefix}shuffleReadBytes_Count ${stage.shuffleReadBytes}\n")
      sb.append(s"${prefix}shuffleReadRecords_Count ${stage.shuffleReadRecords}\n")
      sb.append(s"${prefix}shuffleWriteBytes_Count ${stage.shuffleWriteBytes}\n")
      sb.append(s"${prefix}shuffleWriteTime_Count ${stage.shuffleWriteTime}\n")
      sb.append(s"${prefix}shuffleWriteRecords_Count ${stage.shuffleWriteRecords}\n")
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
