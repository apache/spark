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

package org.apache.spark.nomad

import java.io.InputStreamReader
import java.math.BigInteger
import java.nio.charset.StandardCharsets.UTF_8
import java.util.Date

import scala.annotation.tailrec
import scala.collection.JavaConverters._

import com.google.common.io.LineReader
import com.hashicorp.nomad.apimodel._
import com.hashicorp.nomad.javasdk.{ServerQueryResponse, WaitStrategy}
import com.hashicorp.nomad.scalasdk._
import org.apache.log4j.{Level, LogManager}

import org.apache.spark.internal.Logging

private[spark] class JobUtils(api: NomadScalaApi) extends Logging {

  LogManager.getLogger(logName).setLevel(Level.DEBUG)

  def pollForNextAllocation(
      jobId: String,
      taskGroupName: String,
      previousAllocation: Option[Allocation],
      waitStrategy: WaitStrategy): AllocationListStub = {
    @tailrec
    def poll(pollIndex: Option[BigInteger]): AllocationListStub = {
      logDebug(s"Polling alloc for task group $taskGroupName in $jobId")
      val response = api.jobs.allocations(jobId, Some(ScalaQueryOptions(
        index = pollIndex,
        waitStrategy = Some(waitStrategy)
      )))
      val untracedAllocations = response.getValue
        .filter(a => a.getTaskGroup == taskGroupName &&
          previousAllocation.forall(_.getCreateIndex.compareTo(a.getCreateIndex) < 0))

      if (untracedAllocations.isEmpty) poll(Some(response.getIndex))
      else untracedAllocations.minBy(_.getCreateIndex)
    }

    poll(None)
  }

  def traceTask(
      jobId: String,
      taskGroupName: String,
      taskName: String,
      waitStrategy: WaitStrategy)(onStarted: => Unit): Allocation = {

    val allocation = pollForNextAllocation(jobId, taskGroupName, None, waitStrategy)
    traceTaskInAllocation(taskName, allocation.getId, waitStrategy, None)(onStarted)
  }

  def pollTaskGroupAllocation[A](
      jobId: String,
      taskGroupName: String,
      waitStrategy: WaitStrategy
  )(poll: Allocation => Option[A]): A = {

    val allocation = pollForNextAllocation(jobId, taskGroupName, None, waitStrategy)
    pollAllocation(allocation.getId, waitStrategy)(poll)
  }

  def pollAllocation[A](allocationId: String, waitStrategy: WaitStrategy)
    (poll: Allocation => Option[A]): A = {

    val response = api.allocations.info(allocationId, Some(ScalaQueryOptions(
      waitStrategy = Some(waitStrategy),
      repeatedPollPredicate = Some(
        (r: ServerQueryResponse[Allocation]) => poll(r.getValue).isDefined
      )
    )))
    poll(response.getValue).get
  }

  // TODO: remove non-tail recursion
  def traceTaskInAllocation(
      taskName: String,
      allocationId: String,
      waitStrategy: WaitStrategy,
      previousResponse: Option[ServerQueryResponse[Allocation]]
  )(onStarted: => Unit): Allocation = {
    logDebug(s"Tracing alloc $allocationId for $taskName")
    val response = api.allocations.info(allocationId, Some(ScalaQueryOptions(
      waitStrategy = Some(waitStrategy),
      index = previousResponse.map(_.getIndex)
    )))

    def extractTaskState(a: ServerQueryResponse[Allocation]): Option[TaskState] =
      Option(a.getValue.getTaskStates).flatMap(states => Option(states.get(taskName)))

    val allocation = response.getValue

    val newEvents = extractTaskState(response)
      .fold(Seq.empty[TaskEvent]) { taskState =>
        val events = taskState.getEvents.asScala
        previousResponse
          .flatMap(extractTaskState)
          .flatMap(_.getEvents.asScala.lastOption)
          .fold(events)(lastSeen => events.dropWhile(_.getTime <= lastSeen.getTime))
      }

    newEvents.foreach { e =>
      val date = new Date(e.getTime / 1000)

      val messages = (e.getType match {
        case "Terminated" => Seq(s"Exit status ${e.getExitCode}")
        case _ => Nil
      }) ++ Seq(e.getMessage).filter(_ != null).filter(_ != "")

      val reasons = Seq(
        e.getKillReason,
        e.getRestartReason,
        e.getTaskSignalReason
      ).filter(_ != null).filter(_ != "")

      val errors = Seq(
        e.getDownloadError,
        e.getDriverError,
        e.getKillError,
        e.getSetupError,
        e.getValidationError,
        e.getVaultError
      ).filter(_ != null).filter(_ != "")

      val message = (
        s"$taskName ${e.getType}" +: (messages ++ reasons ++ errors)
        ).mkString(" -- ")

      if (errors.nonEmpty || e.getFailsTask) {
        logError(message)
      } else {
        logInfo(message)
      }

      if (e.getFailsTask || e.getType == "Killed") {
        throw new RuntimeException(message)
      }

      if (e.getType == "Started") {
        onStarted.getClass
        val client = api.lookupClientApiByNodeId(allocation.getNodeId)
        logInfo("Now we're streaming")

        def stream(logType: String): Unit = {
          new Thread(s"$taskName-$logType") {
            setDaemon(true)

            override def run(): Unit = {
              try {
                val stream = client.logs(allocationId, taskName, follow = true, logType)
                try {
                  val reader = new LineReader(new InputStreamReader(stream, UTF_8))
                  @tailrec def pumpRemainingLines(): Unit =
                    reader.readLine() match {
                      case null =>
                        log.info(s"$logType closed")
                      case line =>
                        log.info(s"$logType: $line")
                        pumpRemainingLines()
                    }
                  pumpRemainingLines()
                } catch {
                  case e: Throwable =>
                    logError(s"Error while streaming $logType: $e", e)
                    try stream.close()
                    catch {
                      case _: Throwable =>
                    }
                }
              } catch {
                case e: Throwable => logError(s"Can't stream $logType: $e", e)
              }
            }
          }.start()
        }

        stream("stdout")
        stream("stderr")
      }
    }

    val clientStatus = allocation.getClientStatus
    if (previousResponse.forall(_.getValue.getClientStatus != clientStatus)) {
      log.info("Allocation {} has client status {}", allocationId: Any, clientStatus)
    }

    JobUtils.extractPortAddress(allocation, taskName, "SparkUI").foreach { address =>
      if (!previousResponse.exists(p =>
        JobUtils.extractPortAddress(p.getValue, taskName, "SparkUI").contains(address)
      )) {
        log.info(s"The driver's Spark UI will be served at http://$address/")
      }
    }

    clientStatus match {
      case "complete" =>
        if (extractTaskState(response).exists(_.getFailed)) {
          sys.error("Completed with error")
        } else {
          allocation
        }
      case _ =>
        traceTaskInAllocation(taskName, allocationId, waitStrategy, Some(response))(onStarted)
    }
  }

}

private[spark] object JobUtils {

  def extractPortAddress(allocation: Allocation, task: String, label: String): Option[String] = {
    allocation.getTaskResources.get(task) match {
      case null => None
      case taskResources => taskResources.getNetworks match {
        case null => None
        case networks => networks.asScala.flatMap { network =>
          network.getDynamicPorts match {
            case null => Nil
            case ports => ports.asScala
              .find(_.getLabel == label)
              .map(port => network.getIp + ":" + port.getValue)
          }
        }.headOption
      }
    }
  }

}
