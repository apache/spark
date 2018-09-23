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

import java.util.{HashMap, List => JList}
import javax.ws.rs._
import javax.ws.rs.core.{Context, MediaType, MultivaluedMap, UriInfo}

import org.apache.spark.SparkException
import org.apache.spark.scheduler.StageInfo
import org.apache.spark.status.AppStatusUtils
import org.apache.spark.status.api.v1.StageStatus._
import org.apache.spark.status.api.v1.TaskSorting._
import org.apache.spark.ui.SparkUI
import org.apache.spark.ui.jobs.ApiHelper._

@Produces(Array(MediaType.APPLICATION_JSON))
private[v1] class StagesResource extends BaseAppResource {

  @GET
  def stageList(@QueryParam("status") statuses: JList[StageStatus]): Seq[StageData] = {
    withUI(_.store.stageList(statuses))
  }

  @GET
  @Path("{stageId: \\d+}")
  def stageData(
      @PathParam("stageId") stageId: Int,
      @QueryParam("details") @DefaultValue("true") details: Boolean): Seq[StageData] = {
    withUI { ui =>
      var ret = ui.store.stageData(stageId, details = details)
      if (ret.nonEmpty) {
        // Some of the data that we want to display for the tasks like executorLogs,
        // schedulerDelay etc. comes from other sources, thus, we basically add them to the
        // executor and task data object before passing them to the client.
        for (r <- ret) {
          for (execId <- r.executorSummary.get.keys.toArray) {
            val executorLogs = ui.store.executorSummary(execId).executorLogs
            val hostPort = ui.store.executorSummary(execId).hostPort
            val taskDataArray = r.tasks.get.keys.toArray
            var execStageSummary = r.executorSummary.get.get(execId).get
            execStageSummary.executorLogs = executorLogs
            execStageSummary.hostPort = hostPort
            for (taskData <- taskDataArray) {
              var taskDataObject = r.tasks.get.get(taskData).get
              taskDataObject.executorLogs = executorLogs
              taskDataObject.schedulerDelay =
                AppStatusUtils.schedulerDelay(taskDataObject)
              taskDataObject.gettingResultTime =
                AppStatusUtils.gettingResultTime(taskDataObject)
            }
          }
        }
        ret
      } else {
        throw new NotFoundException(s"unknown stage: $stageId")
      }
    }
  }

  @GET
  @Path("{stageId: \\d+}/{stageAttemptId: \\d+}")
  def oneAttemptData(
      @PathParam("stageId") stageId: Int,
      @PathParam("stageAttemptId") stageAttemptId: Int,
      @QueryParam("details") @DefaultValue("true") details: Boolean): StageData = withUI { ui =>
    try {
      ui.store.stageAttempt(stageId, stageAttemptId, details = details)
    } catch {
      case _: NoSuchElementException =>
        // Change the message depending on whether there are any attempts for the requested stage.
        val all = ui.store.stageData(stageId)
        val msg = if (all.nonEmpty) {
          val ids = all.map(_.attemptId)
          s"unknown attempt for stage $stageId.  Found attempts: [${ids.mkString(",")}]"
        } else {
          s"unknown stage: $stageId"
        }
        throw new NotFoundException(msg)
    }
  }

  @GET
  @Path("{stageId: \\d+}/{stageAttemptId: \\d+}/taskSummary")
  def taskSummary(
      @PathParam("stageId") stageId: Int,
      @PathParam("stageAttemptId") stageAttemptId: Int,
      @DefaultValue("0.05,0.25,0.5,0.75,0.95") @QueryParam("quantiles") quantileString: String)
  : TaskMetricDistributions = withUI { ui =>
    val quantiles = quantileString.split(",").map { s =>
      try {
        s.toDouble
      } catch {
        case nfe: NumberFormatException =>
          throw new BadParameterException("quantiles", "double", s)
      }
    }

    ui.store.taskSummary(stageId, stageAttemptId, quantiles).getOrElse(
      throw new NotFoundException(s"No tasks reported metrics for $stageId / $stageAttemptId yet."))
  }

  @GET
  @Path("{stageId: \\d+}/{stageAttemptId: \\d+}/taskList")
  def taskList(
      @PathParam("stageId") stageId: Int,
      @PathParam("stageAttemptId") stageAttemptId: Int,
      @DefaultValue("0") @QueryParam("offset") offset: Int,
      @DefaultValue("20") @QueryParam("length") length: Int,
      @DefaultValue("ID") @QueryParam("sortBy") sortBy: TaskSorting): Seq[TaskData] = {
    withUI(_.store.taskList(stageId, stageAttemptId, offset, length, sortBy))
  }

  // This api needs to stay formatted exactly as it is below, since, it is being used by the
  // datatables for the stages page.
  @GET
  @Path("{stageId: \\d+}/{stageAttemptId: \\d+}/taskTable")
  def taskTable(
    @PathParam("stageId") stageId: Int,
    @PathParam("stageAttemptId") stageAttemptId: Int,
    @QueryParam("details") @DefaultValue("true") details: Boolean,
    @Context uriInfo: UriInfo):
  HashMap[String, Object] = {
    withUI { ui =>
      val uriQueryParameters = uriInfo.getQueryParameters(true)
      val totalRecords = uriQueryParameters.getFirst("numTasks")
      var isSearch = false
      var searchValue: String = null
      var filteredRecords = totalRecords
      var _tasksToShow: Seq[TaskData] = null
      if (uriQueryParameters.getFirst("search[value]") != null &&
        uriQueryParameters.getFirst("search[value]").length > 0) {
        _tasksToShow = doPagination(uriQueryParameters, stageId, stageAttemptId, true,
          totalRecords.toInt)
        isSearch = true
        searchValue = uriQueryParameters.getFirst("search[value]")
      } else {
        _tasksToShow = doPagination(uriQueryParameters, stageId, stageAttemptId, false,
          totalRecords.toInt)
      }
      if (_tasksToShow.nonEmpty) {
        val iterator = _tasksToShow.iterator
        while(iterator.hasNext) {
          val t1: TaskData = iterator.next()
          val execId = t1.executorId
          val executorLogs = ui.store.executorSummary(execId).executorLogs
          t1.executorLogs = executorLogs
          t1.schedulerDelay = AppStatusUtils.schedulerDelay(t1)
          t1.gettingResultTime = AppStatusUtils.gettingResultTime(t1)
        }
        val ret = new HashMap[String, Object]()
        // Performs server-side search based on input from user
        if (isSearch) {
          val filteredTaskList = filterTaskList(_tasksToShow, searchValue)
          filteredRecords = filteredTaskList.length.toString
          if (filteredTaskList.length > 0) {
            val pageStartIndex = uriQueryParameters.getFirst("start").toInt
            val pageLength = uriQueryParameters.getFirst("length").toInt
            ret.put("aaData", filteredTaskList.filter(f =>
              (f.index >= pageStartIndex && f.index < (pageStartIndex + pageLength))))
          } else {
            ret.put("aaData", filteredTaskList)
          }
        } else {
          ret.put("aaData", _tasksToShow)
        }
        ret.put("recordsTotal", totalRecords)
        ret.put("recordsFiltered", filteredRecords)
        ret
      } else {
        throw new NotFoundException(s"unknown stage: $stageId")
      }
    }
  }

  // Performs pagination on the server side
  def doPagination(queryParameters: MultivaluedMap[String, String], stageId: Int,
    stageAttemptId: Int, isSearch: Boolean, totalRecords: Int): Seq[TaskData] = {
    val queryParams = queryParameters.keySet()
    var columnToSort = 0
    if (queryParams.contains("order[0][column]")) {
      columnToSort = queryParameters.getFirst("order[0][column]").toInt
    }
    var columnNameToSort = queryParameters.getFirst("columns[" + columnToSort + "][name]")
    if (columnNameToSort.equalsIgnoreCase("Logs")) {
      columnNameToSort = "Index"
      columnToSort = 0
    }
    val isAscendingStr = queryParameters.getFirst("order[0][dir]")
    var pageStartIndex = 0
    var pageLength = totalRecords
    if (!isSearch) {
      pageStartIndex = queryParameters.getFirst("start").toInt
      pageLength = queryParameters.getFirst("length").toInt
    }
    return withUI(_.store.taskList(stageId, stageAttemptId, pageStartIndex, pageLength,
      indexName(columnNameToSort), isAscendingStr.equalsIgnoreCase("asc")))
  }

  // Filters task list based on search parameter
  def filterTaskList(
    taskDataList: Seq[TaskData],
    searchValue: String): Seq[TaskData] = {
    val defaultOptionString: String = "d"
    val filteredTaskDataSequence: Seq[TaskData] = taskDataList.filter(f =>
      (f.taskId.toString.contains(searchValue) || f.index.toString.contains(searchValue)
        || f.attempt.toString.contains(searchValue) || f.launchTime.toString.contains(searchValue)
        || f.resultFetchStart.getOrElse(defaultOptionString).toString.contains(searchValue)
        || f.duration.getOrElse(defaultOptionString).toString.contains(searchValue)
        || f.executorId.contains(searchValue) || f.host.contains(searchValue)
        || f.status.contains(searchValue) || f.taskLocality.contains(searchValue)
        || f.speculative.toString.contains(searchValue)
        || f.errorMessage.getOrElse(defaultOptionString).contains(searchValue)
        || f.taskMetrics.get.executorDeserializeTime.toString.contains(searchValue)
        || f.taskMetrics.get.executorRunTime.toString.contains(searchValue)
        || f.taskMetrics.get.jvmGcTime.toString.contains(searchValue)
        || f.taskMetrics.get.resultSerializationTime.toString.contains(searchValue)
        || f.taskMetrics.get.memoryBytesSpilled.toString.contains(searchValue)
        || f.taskMetrics.get.diskBytesSpilled.toString.contains(searchValue)
        || f.taskMetrics.get.peakExecutionMemory.toString.contains(searchValue)
        || f.taskMetrics.get.inputMetrics.bytesRead.toString.contains(searchValue)
        || f.taskMetrics.get.inputMetrics.recordsRead.toString.contains(searchValue)
        || f.taskMetrics.get.outputMetrics.bytesWritten.toString.contains(searchValue)
        || f.taskMetrics.get.outputMetrics.recordsWritten.toString.contains(searchValue)
        || f.taskMetrics.get.shuffleReadMetrics.fetchWaitTime.toString.contains(searchValue)
        || f.taskMetrics.get.shuffleReadMetrics.recordsRead.toString.contains(searchValue)
        || f.taskMetrics.get.shuffleWriteMetrics.bytesWritten.toString.contains(searchValue)
        || f.taskMetrics.get.shuffleWriteMetrics.recordsWritten.toString.contains(searchValue)
        || f.taskMetrics.get.shuffleWriteMetrics.writeTime.toString.contains(searchValue)
        || f.schedulerDelay.toString.contains(searchValue)
        || f.gettingResultTime.toString.contains(searchValue)))
    filteredTaskDataSequence
  }

}
