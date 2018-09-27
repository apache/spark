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
      val ret = ui.store.stageData(stageId, details = details)
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
    // The task metrics dummy object below has been added to avoid throwing exception in cases
    // when task metrics for a particular task do not exist as of yet
    val dummyTaskMetrics: TaskMetrics = new TaskMetrics(0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
      new InputMetrics(0, 0), new OutputMetrics(0, 0),
      new ShuffleReadMetrics(0, 0, 0, 0, 0, 0, 0), new ShuffleWriteMetrics(0, 0, 0))
    val filteredTaskDataSequence: Seq[TaskData] = taskDataList.filter(f =>
      (f.taskId.toString.contains(searchValue) || f.index.toString.contains(searchValue)
        || f.attempt.toString.contains(searchValue) || f.launchTime.toString.contains(searchValue)
        || f.resultFetchStart.getOrElse(defaultOptionString).toString.contains(searchValue)
        || f.duration.getOrElse(defaultOptionString).toString.contains(searchValue)
        || f.executorId.contains(searchValue) || f.host.contains(searchValue)
        || f.status.contains(searchValue) || f.taskLocality.contains(searchValue)
        || f.speculative.toString.contains(searchValue)
        || f.errorMessage.getOrElse(defaultOptionString).contains(searchValue)
        || f.taskMetrics.getOrElse(dummyTaskMetrics).executorDeserializeTime.toString.contains(searchValue)
        || f.taskMetrics.getOrElse(dummyTaskMetrics).executorRunTime.toString.contains(searchValue)
        || f.taskMetrics.getOrElse(dummyTaskMetrics).jvmGcTime.toString.contains(searchValue)
        || f.taskMetrics.getOrElse(dummyTaskMetrics).resultSerializationTime.toString.contains(searchValue)
        || f.taskMetrics.getOrElse(dummyTaskMetrics).memoryBytesSpilled.toString.contains(searchValue)
        || f.taskMetrics.getOrElse(dummyTaskMetrics).diskBytesSpilled.toString.contains(searchValue)
        || f.taskMetrics.getOrElse(dummyTaskMetrics).peakExecutionMemory.toString.contains(searchValue)
        || f.taskMetrics.getOrElse(dummyTaskMetrics).inputMetrics.bytesRead.toString.contains(searchValue)
        || f.taskMetrics.getOrElse(dummyTaskMetrics).inputMetrics.recordsRead.toString.contains(searchValue)
        || f.taskMetrics.getOrElse(dummyTaskMetrics).outputMetrics.bytesWritten.toString.contains(searchValue)
        || f.taskMetrics.getOrElse(dummyTaskMetrics).outputMetrics.recordsWritten.toString.contains(searchValue)
        || f.taskMetrics.getOrElse(dummyTaskMetrics).shuffleReadMetrics.fetchWaitTime.toString.contains(searchValue)
        || f.taskMetrics.getOrElse(dummyTaskMetrics).shuffleReadMetrics.recordsRead.toString.contains(searchValue)
        || f.taskMetrics.getOrElse(dummyTaskMetrics).shuffleWriteMetrics.bytesWritten.toString.contains(searchValue)
        || f.taskMetrics.getOrElse(dummyTaskMetrics).shuffleWriteMetrics.recordsWritten.toString.contains(searchValue)
        || f.taskMetrics.getOrElse(dummyTaskMetrics).shuffleWriteMetrics.writeTime.toString.contains(searchValue)
        || f.schedulerDelay.toString.contains(searchValue)
        || f.gettingResultTime.toString.contains(searchValue)))
    filteredTaskDataSequence
  }

}
