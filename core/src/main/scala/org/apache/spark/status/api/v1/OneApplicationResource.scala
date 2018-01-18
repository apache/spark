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

import java.io.OutputStream
import java.net.URI
import java.util.{List => JList}
import java.util.zip.ZipOutputStream
import javax.ws.rs.{GET, Path, PathParam, Produces, QueryParam}
import javax.ws.rs.core.{MediaType, Response, StreamingOutput}

import scala.util.control.NonFatal

import org.apache.spark.JobExecutionStatus

@Produces(Array(MediaType.APPLICATION_JSON))
private[v1] class AbstractApplicationResource extends BaseAppResource {

  @GET
  @Path("jobs")
  def jobsList(@QueryParam("status") statuses: JList[JobExecutionStatus]): Seq[JobData] = {
    withUI(_.store.jobsList(statuses))
  }

  @GET
  @Path("jobs/{jobId: \\d+}")
  def oneJob(@PathParam("jobId") jobId: Int): JobData = withUI { ui =>
    try {
      ui.store.job(jobId)
    } catch {
      case _: NoSuchElementException =>
        throw new NotFoundException("unknown job: " + jobId)
    }
  }

  @GET
  @Path("executors")
  def executorList(): Seq[ExecutorSummary] = fetchExecutors(true)

  @GET
  @Path("allexecutors")
  def allExecutorList(): Seq[ExecutorSummary] = fetchExecutors(false)

  @Path("stages")
  def stages(): Class[StagesResource] = classOf[StagesResource]

  @GET
  @Path("storage/rdd")
  def rddList(): Seq[RDDStorageInfo] = withUI(_.store.rddList())

  @GET
  @Path("storage/rdd/{rddId: \\d+}")
  def rddData(@PathParam("rddId") rddId: Int): RDDStorageInfo = withUI { ui =>
    try {
      ui.store.rdd(rddId)
    } catch {
      case _: NoSuchElementException =>
        throw new NotFoundException(s"no rdd found w/ id $rddId")
    }
  }

  @GET
  @Path("environment")
  def environmentInfo(): ApplicationEnvironmentInfo = withUI(_.store.environmentInfo())

  @GET
  @Path("logs")
  @Produces(Array(MediaType.APPLICATION_OCTET_STREAM))
  def getEventLogs(): Response = {
    // Retrieve the UI for the application just to do access permission checks. For backwards
    // compatibility, this code also tries with attemptId "1" if the UI without an attempt ID does
    // not exist.
    try {
      withUI { _ => }
    } catch {
      case _: NotFoundException if attemptId == null =>
        attemptId = "1"
        withUI { _ => }
        attemptId = null
    }

    try {
      val fileName = if (attemptId != null) {
        s"eventLogs-$appId-$attemptId.zip"
      } else {
        s"eventLogs-$appId.zip"
      }

      val stream = new StreamingOutput {
        override def write(output: OutputStream): Unit = {
          val zipStream = new ZipOutputStream(output)
          try {
            uiRoot.writeEventLogs(appId, Option(attemptId), zipStream)
          } finally {
            zipStream.close()
          }

        }
      }

      Response.ok(stream)
        .header("Content-Disposition", s"attachment; filename=$fileName")
        .header("Content-Type", MediaType.APPLICATION_OCTET_STREAM)
        .build()
    } catch {
      case NonFatal(e) =>
        Response.serverError()
          .entity(s"Event logs are not available for app: $appId.")
          .status(Response.Status.SERVICE_UNAVAILABLE)
          .build()
    }
  }

  /**
   * This method needs to be last, otherwise it clashes with the paths for the above methods
   * and causes JAX-RS to not find things.
   */
  @Path("{attemptId}")
  def applicationAttempt(): Class[OneApplicationAttemptResource] = {
    if (attemptId != null) {
      throw new NotFoundException(httpRequest.getRequestURI())
    }
    classOf[OneApplicationAttemptResource]
  }

  private def fetchExecutors(activeOnly: Boolean): Seq[ExecutorSummary] = {
    withUI(ui => {
      val tmpExecutorList = ui.store.executorList(activeOnly)
      ui.yarnLogServerUrl.map(lurl =>
        tmpExecutorList.map(withYarnLogServerLogs(toYarnLogServerUrl(lurl, ui.nmRpcPort)))
      ).getOrElse(tmpExecutorList)
    })
  }

  private def toYarnLogServerUrl(logServerUrl: String, nmPort: Int)(nmLogUrl: String): String = {
    val containerSuffixPos = nmLogUrl.indexOf("container_")
    if (containerSuffixPos >= 0) {
      val nodeId = URI.create(nmLogUrl).getHost + ":" + nmPort
      val containerSuffix = nmLogUrl.substring(containerSuffixPos)
      val containerEndPos = containerSuffix.indexOf("/")
      if (containerEndPos >= 0) {
        val container = containerSuffix.substring(0, containerEndPos)
        s"$logServerUrl/$nodeId/$container/$containerSuffix"
      } else {
        nmLogUrl
      }
    } else {
      nmLogUrl
    }
  }

  private def withYarnLogServerLogs(
    logRewrite: String => String)(
    info: ExecutorSummary): ExecutorSummary = {
      new ExecutorSummary(
        id = info.id,
        hostPort = info.hostPort,
        isActive = info.isActive,
        rddBlocks = info.rddBlocks,
        memoryUsed = info.memoryUsed,
        diskUsed = info.diskUsed,
        totalCores = info.totalCores,
        maxTasks = info.maxTasks,
        activeTasks = info.activeTasks,
        failedTasks = info.failedTasks,
        completedTasks = info.completedTasks,
        totalTasks = info.totalTasks,
        totalDuration = info.totalDuration,
        totalGCTime = info.totalGCTime,
        totalInputBytes = info.totalInputBytes,
        totalShuffleRead = info.totalShuffleRead,
        totalShuffleWrite = info.totalShuffleWrite,
        isBlacklisted = info.isBlacklisted,
        maxMemory = info.maxMemory,
        addTime = info.addTime,
        removeTime = info.removeTime,
        removeReason = info.removeReason,
        executorLogs = info.executorLogs.mapValues(logRewrite),
        memoryMetrics = info.memoryMetrics
      )
  }
}

private[v1] class OneApplicationResource extends AbstractApplicationResource {

  @GET
  def getApp(): ApplicationInfo = {
    val app = uiRoot.getApplicationInfo(appId)
    app.getOrElse(throw new NotFoundException("unknown app: " + appId))
  }

}

private[v1] class OneApplicationAttemptResource extends AbstractApplicationResource {

  @GET
  def getAttempt(): ApplicationAttemptInfo = {
    uiRoot.getApplicationInfo(appId)
      .flatMap { app =>
        app.attempts.filter(_.attemptId == attemptId).headOption
      }
      .getOrElse {
        throw new NotFoundException(s"unknown app $appId, attempt $attemptId")
      }
  }

}
