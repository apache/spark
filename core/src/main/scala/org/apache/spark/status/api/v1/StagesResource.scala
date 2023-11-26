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

import java.util.{HashMap, List => JList, Locale}
import javax.ws.rs.{NotFoundException => _, _}
import javax.ws.rs.core.{Context, MediaType, MultivaluedMap, UriInfo}

import scala.collection.JavaConverters._

import org.apache.spark.status.api.v1.TaskStatus._
import org.apache.spark.ui.UIUtils
import org.apache.spark.ui.jobs.ApiHelper._
import org.apache.spark.util.Utils

@Produces(Array(MediaType.APPLICATION_JSON))
private[v1] class StagesResource extends BaseAppResource {

  @GET
  def stageList(
      @QueryParam("status") statuses: JList[StageStatus],
      @QueryParam("details") @DefaultValue("false") details: Boolean,
      @QueryParam("withSummaries") @DefaultValue("false") withSummaries: Boolean,
      @QueryParam("quantiles") @DefaultValue("0.0,0.25,0.5,0.75,1.0") quantileString: String,
      @QueryParam("taskStatus") taskStatus: JList[TaskStatus]): Seq[StageData] = {
    withUI {
      val quantiles = parseQuantileString(quantileString)
      ui => {
        ui.store.stageList(statuses, details, withSummaries, quantiles, taskStatus)
          .filter { stage =>
            if (details && taskStatus.asScala.nonEmpty) {
              taskStatus.asScala.exists {
                case FAILED => stage.numFailedTasks > 0
                case KILLED => stage.numKilledTasks > 0
                case RUNNING => stage.numActiveTasks > 0
                case SUCCESS => stage.numCompleteTasks > 0
                case UNKNOWN => stage.numTasks - stage.numFailedTasks - stage.numKilledTasks -
                  stage.numActiveTasks - stage.numCompleteTasks > 0
              }
            } else {
              true
            }
          }
      }
    }
  }

  @GET
  @Path("{stageId: \\d+}")
  def stageData(
      @PathParam("stageId") stageId: Int,
      @QueryParam("details") @DefaultValue("true") details: Boolean,
      @QueryParam("taskStatus") taskStatus: JList[TaskStatus],
      @QueryParam("withSummaries") @DefaultValue("false") withSummaries: Boolean,
      @QueryParam("quantiles") @DefaultValue("0.0,0.25,0.5,0.75,1.0") quantileString: String):
  Seq[StageData] = {
    withUI { ui =>
      val quantiles = parseQuantileString(quantileString)
      val ret = ui.store.stageData(stageId, details = details, taskStatus = taskStatus,
        withSummaries = withSummaries, unsortedQuantiles = quantiles)
      if (ret.nonEmpty) {
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
      @QueryParam("details") @DefaultValue("true") details: Boolean,
      @QueryParam("taskStatus") taskStatus: JList[TaskStatus],
      @QueryParam("withSummaries") @DefaultValue("false") withSummaries: Boolean,
      @QueryParam("quantiles") @DefaultValue("0.0,0.25,0.5,0.75,1.0") quantileString: String):
  StageData = withUI { ui =>
    try {
      val quantiles = parseQuantileString(quantileString)
      ui.store.stageAttempt(stageId, stageAttemptId, details = details, taskStatus = taskStatus,
        withSummaries = withSummaries, unsortedQuantiles = quantiles)._1
    } catch {
      case _: NoSuchElementException =>
        // Change the message depending on whether there are any attempts for the requested stage.
        val all = ui.store.stageData(stageId, false, taskStatus)
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
    val quantiles = parseQuantileString(quantileString)
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
      @DefaultValue("ID") @QueryParam("sortBy") sortBy: TaskSorting,
      @QueryParam("status") statuses: JList[TaskStatus]): Seq[TaskData] = {
    withUI(_.store.taskList(stageId, stageAttemptId, offset, length, sortBy, statuses))
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
      // Decode URI params twice here to avoid percent-encoding twice
      val uriQueryParameters = UIUtils.decodeURLParameter(uriInfo.getQueryParameters(true))
      val totalRecords = uriQueryParameters.getFirst("numTasks")
      var isSearch = false
      var searchValue: String = null
      var filteredRecords = totalRecords
      // The datatables client API sends a list of query parameters to the server which contain
      // information like the columns to be sorted, search value typed by the user in the search
      // box, pagination index etc. For more information on these query parameters,
      // refer https://datatables.net/manual/server-side.
      if (uriQueryParameters.getFirst("search[value]") != null &&
        uriQueryParameters.getFirst("search[value]").length > 0) {
        isSearch = true
        searchValue = uriQueryParameters.getFirst("search[value]")
      }
      val _tasksToShow: Seq[TaskData] = doPagination(uriQueryParameters, stageId, stageAttemptId,
        isSearch, totalRecords.toInt)
      val ret = new HashMap[String, Object]()
      if (_tasksToShow.nonEmpty) {
        // Performs server-side search based on input from user
        if (isSearch) {
          val filteredTaskList = filterTaskList(_tasksToShow, searchValue)
          filteredRecords = filteredTaskList.length.toString
          if (filteredTaskList.length > 0) {
            val pageStartIndex = uriQueryParameters.getFirst("start").toInt
            val pageLength = uriQueryParameters.getFirst("length").toInt
            ret.put("aaData", filteredTaskList.slice(
              pageStartIndex, pageStartIndex + pageLength))
          } else {
            ret.put("aaData", filteredTaskList)
          }
        } else {
          ret.put("aaData", _tasksToShow)
        }
      } else {
        ret.put("aaData", _tasksToShow)
      }
      ret.put("recordsTotal", totalRecords)
      ret.put("recordsFiltered", filteredRecords)
      ret
    }
  }

  // Performs pagination on the server side
  def doPagination(queryParameters: MultivaluedMap[String, String], stageId: Int,
    stageAttemptId: Int, isSearch: Boolean, totalRecords: Int): Seq[TaskData] = {
    var columnNameToSort = queryParameters.getFirst("columnNameToSort")
    // Sorting on Logs column will default to Index column sort
    if (columnNameToSort.equalsIgnoreCase("Logs")) {
      columnNameToSort = "Index"
    }
    val isAscendingStr = queryParameters.getFirst("order[0][dir]")
    var pageStartIndex = 0
    var pageLength = totalRecords
    // We fetch only the desired rows upto the specified page length for all cases except when a
    // search query is present, in that case, we need to fetch all the rows to perform the search
    // on the entire table
    if (!isSearch) {
      pageStartIndex = queryParameters.getFirst("start").toInt
      pageLength = queryParameters.getFirst("length").toInt
    }
    withUI(_.store.taskList(stageId, stageAttemptId, pageStartIndex, pageLength,
      indexName(columnNameToSort), "asc".equalsIgnoreCase(isAscendingStr)))
  }

  // Filters task list based on search parameter
  def filterTaskList(
    taskDataList: Seq[TaskData],
    searchValue: String): Seq[TaskData] = {
    val defaultOptionString: String = "d"
    val searchValueLowerCase = searchValue.toLowerCase(Locale.ROOT)
    val containsValue = (taskDataParams: Any) => taskDataParams.toString.toLowerCase(
      Locale.ROOT).contains(searchValueLowerCase)
    val taskMetricsContainsValue = (task: TaskData) => task.taskMetrics match {
      case None => false
      case Some(metrics) =>
        (containsValue(UIUtils.formatDuration(task.taskMetrics.get.executorDeserializeTime))
        || containsValue(UIUtils.formatDuration(task.taskMetrics.get.executorRunTime))
        || containsValue(UIUtils.formatDuration(task.taskMetrics.get.jvmGcTime))
        || containsValue(UIUtils.formatDuration(task.taskMetrics.get.resultSerializationTime))
        || containsValue(Utils.bytesToString(task.taskMetrics.get.memoryBytesSpilled))
        || containsValue(Utils.bytesToString(task.taskMetrics.get.diskBytesSpilled))
        || containsValue(Utils.bytesToString(task.taskMetrics.get.peakExecutionMemory))
        || containsValue(Utils.bytesToString(task.taskMetrics.get.inputMetrics.bytesRead))
        || containsValue(task.taskMetrics.get.inputMetrics.recordsRead)
        || containsValue(Utils.bytesToString(
          task.taskMetrics.get.outputMetrics.bytesWritten))
        || containsValue(task.taskMetrics.get.outputMetrics.recordsWritten)
        || containsValue(UIUtils.formatDuration(
          task.taskMetrics.get.shuffleReadMetrics.fetchWaitTime))
        || containsValue(Utils.bytesToString(
          task.taskMetrics.get.shuffleReadMetrics.remoteBytesRead))
        || containsValue(Utils.bytesToString(
          task.taskMetrics.get.shuffleReadMetrics.localBytesRead +
          task.taskMetrics.get.shuffleReadMetrics.remoteBytesRead))
        || containsValue(task.taskMetrics.get.shuffleReadMetrics.recordsRead)
        || containsValue(Utils.bytesToString(
          task.taskMetrics.get.shuffleWriteMetrics.bytesWritten))
        || containsValue(task.taskMetrics.get.shuffleWriteMetrics.recordsWritten)
        || containsValue(UIUtils.formatDuration(
          task.taskMetrics.get.shuffleWriteMetrics.writeTime / 1000000)))
    }
    val filteredTaskDataSequence: Seq[TaskData] = taskDataList.filter(f =>
      (containsValue(f.taskId) || containsValue(f.index) || containsValue(f.attempt)
        || containsValue(UIUtils.formatDate(f.launchTime))
        || containsValue(f.resultFetchStart.getOrElse(defaultOptionString))
        || containsValue(f.executorId) || containsValue(f.host) || containsValue(f.status)
        || containsValue(f.taskLocality) || containsValue(f.speculative)
        || containsValue(f.errorMessage.getOrElse(defaultOptionString))
        || taskMetricsContainsValue(f)
        || containsValue(UIUtils.formatDuration(f.schedulerDelay))
        || containsValue(UIUtils.formatDuration(f.gettingResultTime))))
    filteredTaskDataSequence
  }

  def parseQuantileString(quantileString: String): Array[Double] = {
    quantileString.split(",").map { s =>
      try {
        s.toDouble
      } catch {
        case nfe: NumberFormatException =>
          throw new BadParameterException("quantiles", "double", s)
      }
    }
  }
}
