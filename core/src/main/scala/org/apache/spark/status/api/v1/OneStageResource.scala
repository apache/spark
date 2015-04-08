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
import org.apache.spark.status.api.v1.StageStatus._

@Produces(Array(MediaType.APPLICATION_JSON))
private[v1] class OneStageResource(uiRoot: UIRoot) {

  @GET
  @Path("")
  def stageData(
    @PathParam("appId") appId: String,
    @PathParam("stageId") stageId: Int
  ): Seq[StageData] = {
    withStage(appId, stageId){ stageAttempts =>
      stageAttempts.map { stage =>
        AllStagesResource.stageUiToStageData(stage.status, stage.info, stage.ui,
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
    withStageAttempt(appId, stageId, attemptId) { stage =>
      AllStagesResource.stageUiToStageData(stage.status, stage.info, stage.ui,
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
    withStageAttempt(appId, stageId, attemptId) { stage =>
      val quantiles = quantileString.split(",").map { s =>
        try {
          s.toDouble
        } catch {
          case nfe: NumberFormatException =>
            throw new BadParameterException("quantiles", "double", s)
        }
      }
      AllStagesResource.taskMetricDistributions(stage.ui.taskData.values, quantiles)
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
    withStageAttempt(appId, stageId, attemptId) { stage =>
      val tasks = stage.ui.taskData.values.map{AllStagesResource.convertTaskData}.toIndexedSeq
        .sorted(sortBy.ordering)
      tasks.slice(offset, offset + length)  
    }
  }

  private case class StageStatusInfoUi(status: StageStatus, info: StageInfo, ui: StageUIData)

  private def withStage[T](appId: String, stageId: Int)
      (f: Seq[StageStatusInfoUi] => T): T = {
    uiRoot.withSparkUI(appId) { ui =>
      val stageAttempts = findStageStatusUIData(ui.jobProgressListener, stageId)
      if (stageAttempts.isEmpty) {
        throw new NotFoundException("unknown stage: " + stageId)
      } else {
        f(stageAttempts)
      }
    }
  }

  private def findStageStatusUIData(
      listener: JobProgressListener,
      stageId: Int): Seq[StageStatusInfoUi] = {
    listener.synchronized {
      def getStatusInfoUi(status: StageStatus, infos: Seq[StageInfo]): Seq[StageStatusInfoUi] = {
        infos.filter { _.stageId == stageId }.map { info =>
          val ui = listener.stageIdToData.getOrElse((info.stageId, info.attemptId),
            //this is an internal error -- we should always have uiData
            throw new SparkException(
              s"no stage ui data found for stage: ${info.stageId}:${info.attemptId}")
          )
          StageStatusInfoUi(status, info, ui)
        }
      }
      getStatusInfoUi(Active, listener.activeStages.values.toSeq) ++
        getStatusInfoUi(Complete, listener.completedStages) ++
        getStatusInfoUi(Failed, listener.failedStages) ++
        getStatusInfoUi(Pending, listener.pendingStages.values.toSeq)
    }
  }

  private def withStageAttempt[T](appId: String, stageId: Int, attemptId: Int)
      (f: StageStatusInfoUi => T): T = {
    withStage(appId, stageId) { attempts =>
        val oneAttempt = attempts.find { stage => stage.info.attemptId == attemptId }
        oneAttempt match {
          case Some(stage) =>
            f(stage)
          case None =>
            val stageAttempts = attempts.map { _.info.attemptId }
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
      def ordering = Ordering.by { td: TaskData => td.taskId }
    }
    ID
  }

  final val IncreasingRuntime = {
    case object IncreasingRuntime extends TaskSorting {
      def ordering = Ordering.by { td: TaskData =>
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

  val alternateNames: Map[String, TaskSorting] =
    values.flatMap { x => x.alternateNames.map { _ -> x } }.toMap

  override def fromString(s: String): TaskSorting = {
    alternateNames.find { case (k, v) =>
      k.toLowerCase() == s.toLowerCase()
    }.map { _._2 }.getOrElse{
      super.fromString(s)
    }
  }
}
