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

import java.util
import java.util.{Collections, Comparator, List => JList}
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
        for (i <- 0 to (ret.length - 1)) {
          var executorIdArray = ret(i).executorSummary.get.keys.toArray
          for (execId <- executorIdArray) {
            var executorLogs = ui.store.executorSummary(execId).executorLogs
            var hostPort = ui.store.executorSummary(execId).hostPort
            var taskDataArray = ret(i).tasks.get.keys.toArray
            var executorStageSummaryArray = ret(i).executorSummary.get.keys.toArray
            ret(i).executorSummary.get.get(execId).get.executorLogs = executorLogs
            ret(i).executorSummary.get.get(execId).get.hostPort = hostPort
            for (taskData <- taskDataArray) {
              ret(i).tasks.get.get(taskData).get.executorLogs = executorLogs
              ret(i).tasks.get.get(taskData).get.schedulerDelay = AppStatusUtils.schedulerDelay(ret(i).tasks.get.get(taskData).get)
              ret(i).tasks.get.get(taskData).get.gettingResultTime = AppStatusUtils.gettingResultTime(ret(i).tasks.get.get(taskData).get)
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

  @GET
  @Path("{stageId: \\d+}/{stageAttemptId: \\d+}/taskTable")
  def taskTable(
    @PathParam("stageId") stageId: Int,
    @PathParam("stageAttemptId") stageAttemptId: Int,
    @QueryParam("details") @DefaultValue("true") details: Boolean, @Context uriInfo: UriInfo): util.HashMap[String, Object] = {
    withUI { ui =>
      val abc = uriInfo.getQueryParameters(true)
      val totalRecords = abc.getFirst("numTasks")
      var isSearch = false
      var searchValue: String = null
      var _tasksToShow: Seq[TaskData] = null
      if (abc.getFirst("search[value]") != null && abc.getFirst("search[value]").length > 0) {
        _tasksToShow = ui.store.taskList(stageId, stageAttemptId, 0, totalRecords.toInt,
          indexName("Index"), true)
        isSearch = true
        searchValue = abc.getFirst("search[value]")
      } else {
        _tasksToShow = doPagination(abc, stageId, stageAttemptId)
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
        val ret5 = new util.HashMap[String, Object]()
        if (isSearch) {
          val filteredTaskList = ui.store.filterTaskList(_tasksToShow, searchValue)
          if (filteredTaskList.length > 0) {
            ret5.put("aaData", filteredTaskList)
          } else {
            _tasksToShow = doPagination(abc, stageId, stageAttemptId)
            val iterator = _tasksToShow.iterator
            while(iterator.hasNext) {
              val t1: TaskData = iterator.next()
              val execId = t1.executorId
              val executorLogs = ui.store.executorSummary(execId).executorLogs
              t1.executorLogs = executorLogs
              t1.schedulerDelay = AppStatusUtils.schedulerDelay(t1)
              t1.gettingResultTime = AppStatusUtils.gettingResultTime(t1)
            }
            ret5.put("aaData", _tasksToShow)
          }
        } else {
          ret5.put("aaData", _tasksToShow)
        }
        ret5.put("recordsTotal", totalRecords)
        ret5.put("recordsFiltered", totalRecords)
        ret5
      } else {
        throw new NotFoundException(s"unknown stage: $stageId")
      }
    }
  }

  def doPagination(queryParameters: MultivaluedMap[String, String], stageId: Int, stageAttemptId: Int): Seq[TaskData] = {
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
    val pageStartIndex = queryParameters.getFirst("start").toInt
    val pageLength = queryParameters.getFirst("length").toInt
    return withUI(_.store.taskList(stageId, stageAttemptId, pageStartIndex, pageLength,
      indexName(columnNameToSort), isAscendingStr.equalsIgnoreCase("asc")))
  }

}
