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

package org.apache.spark.deploy.nomad

import scala.annotation.tailrec
import scala.collection.mutable.ArrayBuffer

import com.hashicorp.nomad.apimodel.Evaluation
import com.hashicorp.nomad.javasdk.WaitStrategy
import com.hashicorp.nomad.scalasdk.NomadScalaApi

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.launcher.{LauncherBackend, SparkAppHandle}
import org.apache.spark.nomad.{EvaluationUtils, JobUtils}
import org.apache.spark.scheduler.cluster.nomad.SparkNomadJobController

/**
 * Launches an application in a Nomad cluster.
 *
 * In cluster mode, a Nomad job is registered with a single task that runs the Nomad driver.
 * The driver process then adds executor task groups to the job as necessary.
 */
private[spark] class NomadClusterModeLauncher(
    conf: SparkConf,
    clusterModeConf: NomadClusterModeConf
) extends Logging {
  import org.apache.spark.deploy.nomad.NomadClusterModeLauncher._

  private val launcherBackend = new LauncherBackend {
    connect()

    override protected def onStopRequest(): Unit = {}
  }

  logInfo(s"Will access Nomad API at ${clusterModeConf.backend.nomadApi.getAddress}")
  private val nomad = NomadScalaApi(clusterModeConf.backend.nomadApi)
  private val jobController = SparkNomadJobController.initialize(clusterModeConf.backend, nomad)
  launcherBackend.setAppId(jobController.jobId)

  val waitForever = new WaitStrategy {
    override def getWait: String = "10s"
  }

  def submit(): Unit = {
    try {
      val evaluation = jobController.startDriver()
      logInfo(s"Nomad job with driver task submitted")
      reportOutcome(evaluation)
    } catch {
      case e: Throwable =>
        logError("Driver failure", e)
        launcherBackend.setState(SparkAppHandle.State.FAILED)
        throw e
    }
  }

  def reportOutcome(initialEvaluation: Evaluation): Unit = {
    launcherBackend.setState(SparkAppHandle.State.SUBMITTED)

    val monitorUntil = clusterModeConf.monitorUntil.getOrElse {
      if (launcherBackend.isConnected()) Complete
      else if (clusterModeConf.expectImmediateScheduling) Scheduled
      else Submitted
    }

    if (monitorUntil == Scheduled || monitorUntil == Complete) {
      val evaluation = new EvaluationUtils(nomad).traceUntilFullyScheduled(
        initialEvaluation,
        clusterModeConf.expectImmediateScheduling,
        waitForever
      )
      val jobUtils = new JobUtils(nomad)
      monitorUntil match {
        case Submitted =>
        // done
        case Scheduled =>
          val (nodeId, sparkUiAddress) =
            jobUtils.pollTaskGroupAllocation(evaluation.getJobId, "driver", waitForever) {
              alloc =>
                JobUtils.extractPortAddress(alloc, "driver", "SparkUI")
                  .map(alloc.getNodeId -> _)
            }
          log.info(
            s"The driver's Spark UI will be served on node $nodeId at http://$sparkUiAddress/"
          )
        case Complete =>
          jobUtils.traceTask(evaluation.getJobId, "driver", "driver", waitForever) {
            launcherBackend.setState(SparkAppHandle.State.RUNNING)
          }
          logInfo("Driver completed successfully")
      }
      launcherBackend.setState(SparkAppHandle.State.FINISHED)
    }
  }
}


private[spark] object NomadClusterModeLauncher extends Logging {

  private def usageException(message: String): Nothing = {
    throw new IllegalArgumentException(
      s"""Error: $message
         |Usage: org.apache.spark.deploy.nomad.NomadClusterModeLauncher [option...]
         |Options:
         |  --jar JAR_PATH          Path to your application's JAR file (required in nomad-cluster
         |                          mode)
         |  --class CLASS_NAME      Name of your application's main class (required)
         |  --primary-py-file       A main Python file
         |  --primary-r-file        A main R file
         |  --arg ARG               Argument to be passed to your application's main class.
         |                          Multiple invocations are possible, each will be passed in order.
         |""".stripMargin
    )
  }

  sealed trait PrimaryResource {
    val url: String
  }

  case class PrimaryJar(url: String) extends PrimaryResource

  case class PrimaryPythonFile(url: String) extends PrimaryResource

  case class PrimaryRFile(url: String) extends PrimaryResource

  def parseArguments(args: Seq[String]): ApplicationRunCommand = {
    var mainClass: Option[String] = None
    var primaryResource: Option[PrimaryResource] = None
    var userArgs: ArrayBuffer[String] = new ArrayBuffer[String]()

    def setPrimaryResource(resource: PrimaryResource): Unit = primaryResource match {
      case None => primaryResource = Some(resource)
      case Some(_) =>
        usageException("Cannot specify multiple primary resources (jar, python or R file")
    }

    @tailrec
    def parseRemaining(args: Seq[String]): Unit = {
      args match {
        case "--jar" +: value +: tail =>
          setPrimaryResource(PrimaryJar(value))
          parseRemaining(tail)

        case "--primary-py-file" +: value +: tail =>
          setPrimaryResource(PrimaryPythonFile(value))
          parseRemaining(tail)

        case "--primary-r-file" +: value +: tail =>
          setPrimaryResource(PrimaryRFile(value))
          parseRemaining(tail)

        case "--class" +: value +: tail =>
          mainClass match {
            case None => mainClass = Some(value)
            case Some(_) => usageException("--class cannot be specified multiple times")
          }
          parseRemaining(tail)

        case ("--arg") +: value +: tail =>
          userArgs += value
          parseRemaining(tail)

        case Nil =>

        case _ =>
          usageException(s"Unknown/unsupported param $args")
      }
    }

    parseRemaining(args)

    ApplicationRunCommand(
      primaryResource = primaryResource
        .getOrElse(usageException("Missing primary resource (jar, python or R file)")),
      mainClass = mainClass.getOrElse(usageException("Missing class")),
      arguments = userArgs
    )
  }

  sealed trait Milestone

  case object Submitted extends Milestone

  case object Scheduled extends Milestone

  case object Complete extends Milestone

  private val milestonesByName =
    Seq(Submitted, Scheduled, Complete)
      .map(m => m.toString.toLowerCase() -> m)
      .toMap

  def main(argStrings: Array[String]) {
    logInfo(s"Running ${getClass.getName.stripSuffix("$")}")
    val appCommand = parseArguments(argStrings)
    val conf = new SparkConf
    val clusterModeConf = NomadClusterModeConf(conf, appCommand)

    val result = new NomadClusterModeLauncher(conf, clusterModeConf).submit()
  }

}
