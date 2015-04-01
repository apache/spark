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

import org.apache.spark.SparkException
import org.apache.spark.scheduler.StageInfo
import org.apache.spark.ui.jobs.JobProgressListener
import org.apache.spark.ui.jobs.UIData.StageUIData
import org.apache.spark.util.SparkEnum

@Produces(Array(MediaType.APPLICATION_JSON))
private[v1] class OneStageResource(uiRoot: UIRoot) {

  @GET
  @Path("")
  def stageData(
    @PathParam("appId") appId: String,
    @PathParam("stageId") stageId: Int
  ): Seq[StageData] = {
    forStage(appId, stageId){ (listener,stageAttempts) =>
      stageAttempts.map { case (status, stageInfo) =>
        val stageUiData = listener.synchronized {
          listener.stageIdToData.get((stageInfo.stageId, stageInfo.attemptId)).
            getOrElse(throw new SparkException("failed to get full stage data for stage: " +
            stageInfo.stageId + ":" + stageInfo.attemptId)
            )
        }
        AllStagesResource.stageUiToStageData(status, stageInfo, stageUiData,
          includeDetails = true)
      }
    }
  }

  @GET
  @Path("/{attemptId: \\d+}")
  def oneAttemptData(
    @PathParam("appId") appId: String,
    @PathParam("stageId") stageId: Int,
    @PathParam("attemptId") attemptId: Int
  ): StageData = {
    forStageAttempt(appId, stageId, attemptId) { case (status, stageInfo, stageUiData) =>
      AllStagesResource.stageUiToStageData(status, stageInfo, stageUiData,
        includeDetails = true)
    }
  }

  @GET
  @Path("/{attemptId: \\d+}/taskSummary")
  def stageData(
    @PathParam("appId") appId: String,
    @PathParam("stageId") stageId: Int,
    @PathParam("attemptId") attemptId: Int,
    @DefaultValue("0.05,0.25,0.5,0.75,0.95") @QueryParam("quantiles") quantileString: String
  ): TaskMetricDistributions = {
    forStageAttempt(appId, stageId, attemptId) { case (status, stageInfo, stageUiData) =>
      val quantiles = quantileString.split(",").map{s =>
        try {
          s.toDouble
        } catch {
          case nfe: NumberFormatException =>
            throw new BadParameterException("quantiles", "double", s)
        }
      }
      AllStagesResource.taskMetricDistributions(stageUiData.taskData.values, quantiles)
    }
  }

  @GET
  @Path("/{attemptId: \\d+}/taskList")
  def taskList(
    @PathParam("appId") appId: String,
    @PathParam("stageId") stageId: Int,
    @PathParam("attemptId") attemptId: Int,
    @DefaultValue("0") @QueryParam("offset") offset: Int,
    @DefaultValue("20") @QueryParam("length") length: Int,
    @DefaultValue("ID") @QueryParam("sortBy") sortBy: TaskSorting
  ): Seq[TaskData] = {
    forStageAttempt(appId, stageId, attemptId) { case (status, stageInfo, stageUiData) =>
      val tasks = stageUiData.taskData.values.map{AllStagesResource.convertTaskData}.toIndexedSeq
        .sorted(sortBy.ordering)
      tasks.slice(offset, offset + length)  
    }
  }


  def forStage[T](appId: String, stageId: Int)
      (f: (JobProgressListener, Seq[(StageStatus, StageInfo)]) => T): T = {
    uiRoot.withSparkUI(appId) { ui =>
      val stageAndStatus = AllStagesResource.stagesAndStatus(ui)
      val stageAttempts = stageAndStatus.flatMap { case (status, stages) =>
        val matched = stages.filter { stage => stage.stageId == stageId}
        matched.map {
          status -> _
        }
      }
      if (stageAttempts.isEmpty) {
        throw new NotFoundException("unknown stage: " + stageId)
      } else {
        f(ui.jobProgressListener, stageAttempts)
      }
    }
  }

  def forStageAttempt[T](appId: String, stageId: Int, attemptId: Int)
      (f: (StageStatus, StageInfo, StageUIData) => T): T = {
    forStage(appId, stageId) { case (listener, attempts) =>
        val oneAttempt = attempts.filter{ case (status, stage) =>
            stage.attemptId == attemptId
        }.headOption
        oneAttempt match {
          case Some((status, stageInfo)) =>
            val stageUiData = listener.synchronized {
              listener.stageIdToData.get((stageInfo.stageId, stageInfo.attemptId)).
                getOrElse(throw new SparkException("failed to get full stage data for stage: " +
                stageInfo.stageId + ":" + stageInfo.attemptId)
                )
            }
            f(status, stageInfo, stageUiData)
          case None =>
            val stageAttempts = attempts.map { _._2.attemptId}
            throw new NotFoundException(s"unknown attempt for stage $stageId.  " +
              s"Found attempts: ${stageAttempts.mkString("[", ",", "]")}")
        }
    }
  }
}

sealed abstract class TaskSorting extends SparkEnum {
  def ordering: Ordering[TaskData]
  def alternateNames: Seq[String] = Seq()
}
object TaskSorting extends JerseyEnum[TaskSorting] {
  final val ID = {
    case object ID extends TaskSorting {
      def ordering = Ordering.by{td: TaskData => td.taskId}
    }
    ID
  }

  final val IncreasingRuntime = {
    case object IncreasingRuntime extends TaskSorting {
      def ordering = Ordering.by{td: TaskData =>
        td.taskMetrics.map{_.executorRunTime}.getOrElse(-1L)
      }
      override def alternateNames = Seq("runtime", "+runtime")
    }
    IncreasingRuntime
  }

  final val DecreasingRuntime = {
    case object DecreasingRuntime extends TaskSorting {
      def ordering = IncreasingRuntime.ordering.reverse
      override def alternateNames = Seq("-runtime")
    }
    DecreasingRuntime
  }

  val values = Seq(
    ID,
    IncreasingRuntime,
    DecreasingRuntime
  )

  val alternateNames: Map[String, TaskSorting] = values.flatMap{x => x.alternateNames.map{_ -> x}}.toMap

  override def fromString(s: String): TaskSorting = {
    alternateNames.find { case (k, v) =>
      k.toLowerCase() == s.toLowerCase()
    }.map{_._2}.getOrElse{
      super.fromString(s)
    }
  }
}

