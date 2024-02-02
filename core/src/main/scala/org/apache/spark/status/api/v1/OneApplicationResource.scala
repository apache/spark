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
import java.util.{List => JList}
import java.util.zip.ZipOutputStream

import scala.util.control.NonFatal

import jakarta.ws.rs.{NotFoundException => _, _}
import jakarta.ws.rs.core.{MediaType, Response, StreamingOutput}

import org.apache.spark.{JobExecutionStatus, SparkContext}
import org.apache.spark.status.api.v1
import org.apache.spark.util.Utils

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
  def executorList(): Seq[ExecutorSummary] = withUI(_.store.executorList(true))

  @GET
  @Path("executors/{executorId}/threads")
  def threadDump(@PathParam("executorId") execId: String): Array[ThreadStackTrace] = withUI { ui =>
    checkExecutorId(execId)
    val safeSparkContext = checkAndGetSparkContext()
    ui.store.asOption(ui.store.executorSummary(execId)) match {
      case Some(executorSummary) if executorSummary.isActive =>
          val safeThreadDump = safeSparkContext.getExecutorThreadDump(execId).getOrElse {
            throw new NotFoundException("No thread dump is available.")
          }
          safeThreadDump
      case Some(_) => throw new BadParameterException("Executor is not active.")
      case _ => throw new NotFoundException("Executor does not exist.")
    }
  }

  @GET
  @Path("threads")
  def getTaskThreadDump(
      @QueryParam("taskId") taskId: Long,
      @QueryParam("executorId") execId: String): ThreadStackTrace = {
    checkExecutorId(execId)
    val safeSparkContext = checkAndGetSparkContext()
    safeSparkContext
      .getTaskThreadDump(taskId, execId)
      .getOrElse {
        throw new NotFoundException(
          s"Task '$taskId' is not running on Executor '$execId' right now")
      }
  }

  @GET
  @Path("allexecutors")
  def allExecutorList(): Seq[ExecutorSummary] = withUI(_.store.executorList(false))

  @GET
  @Path("allmiscellaneousprocess")
  def allProcessList(): Seq[ProcessSummary] = withUI(_.store.miscellaneousProcessList(false))

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
  def environmentInfo(): ApplicationEnvironmentInfo = withUI { ui =>
    val envInfo = ui.store.environmentInfo()
    val resourceProfileInfo = ui.store.resourceProfileInfo()
    new v1.ApplicationEnvironmentInfo(
      envInfo.runtime,
      Utils.redact(ui.conf, envInfo.sparkProperties).sortBy(_._1),
      Utils.redact(ui.conf, envInfo.hadoopProperties).sortBy(_._1),
      Utils.redact(ui.conf, envInfo.systemProperties).sortBy(_._1),
      Utils.redact(ui.conf, envInfo.metricsProperties).sortBy(_._1),
      envInfo.classpathEntries.sortBy(_._1),
      resourceProfileInfo)
  }

  @GET
  @Path("logs")
  @Produces(Array(MediaType.APPLICATION_OCTET_STREAM))
  def getEventLogs(): Response = {
    // For backwards compatibility, this code also tries with attemptId "1" if the UI
    // without an attempt ID does not exist.
    try {
      checkUIViewPermissions()
    } catch {
      case _: NotFoundException if attemptId == null =>
        attemptId = "1"
        checkUIViewPermissions()
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
      case NonFatal(_) =>
        throw new ServiceUnavailable(s"Event logs are not available for app: $appId.")
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

  private def checkExecutorId(execId: String): Unit = {
    if (execId != SparkContext.DRIVER_IDENTIFIER && !execId.forall(Character.isDigit)) {
      throw new BadParameterException(
        s"Invalid executorId: neither '${SparkContext.DRIVER_IDENTIFIER}' nor number.")
    }
  }

  private def checkAndGetSparkContext(): SparkContext = withUI { ui =>
    ui.sc.getOrElse {
      throw new ServiceUnavailable("Thread dumps not available through the history server.")
    }
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
        app.attempts.find(_.attemptId.contains(attemptId))
      }
      .getOrElse {
        throw new NotFoundException(s"unknown app $appId, attempt $attemptId")
      }
  }

}
