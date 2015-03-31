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
    forStageAttempt(appId, stageId, attemptId) { case (listener, status, stageInfo) =>
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

  @GET
  @Path("/{attemptId: \\d+}/taskSummary")
  def stageData(
    @PathParam("appId") appId: String,
    @PathParam("stageId") stageId: Int,
    @PathParam("attemptId") attemptId: Int,
    @DefaultValue("0.05,0.25,0.5,0.75,0.95") @QueryParam("quantiles") quantileString: String
  ): TaskMetricDistributions = {
    forStageAttempt(appId, stageId, attemptId) { case (listener, status, stageInfo) =>
      val stageUiData = listener.synchronized {
        listener.stageIdToData.get((stageInfo.stageId, stageInfo.attemptId)).
          getOrElse(throw new SparkException("failed to get full stage data for stage: " +
          stageInfo.stageId + ":" + stageInfo.attemptId)
          )
      }
      //TODO error handling
      val quantiles = quantileString.split(",").map{_.toDouble}
      println("quantiles = " + quantiles.mkString(","))
      AllStagesResource.taskMetricDistributions(stageUiData.taskData.values, quantiles)
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
      (f: (JobProgressListener, StageStatus, StageInfo) => T): T = {
    forStage(appId, stageId) { case (listener, attempts) =>
        val oneAttempt = attempts.filter{ case (status, stage) =>
            stage.attemptId == attemptId
        }.headOption
        oneAttempt match {
          case Some((status, stageInfo)) =>
            f(listener, status, stageInfo)
          case None =>
            val stageAttempts = attempts.map { _._2.attemptId}
            throw new NotFoundException(s"unknown attempt for stage $stageId.  " +
              s"Found attempts: ${stageAttempts.mkString("[", ",", "]")}")
        }
    }
  }
}
