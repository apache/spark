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

import java.util.{List => JList}
import javax.ws.rs._
import javax.ws.rs.core.MediaType

import org.apache.spark.SparkException
import org.apache.spark.scheduler.StageInfo
import org.apache.spark.status.api.v1.StageStatus._
import org.apache.spark.status.api.v1.TaskSorting._
import org.apache.spark.ui.SparkUI

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

}
