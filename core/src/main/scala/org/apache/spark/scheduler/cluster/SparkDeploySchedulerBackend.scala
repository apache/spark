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

package org.apache.spark.scheduler.cluster

import org.apache.spark.{Logging, SparkConf, SparkContext, SparkEnv}
import org.apache.spark.deploy.{ApplicationDescription, Command}
import org.apache.spark.deploy.client.{AppClient, AppClientListener}
import org.apache.spark.scheduler.{ExecutorExited, ExecutorLossReason, SlaveLost, TaskSchedulerImpl}
import org.apache.spark.util.Utils

private[spark] class SparkDeploySchedulerBackend(
    scheduler: TaskSchedulerImpl,
    sc: SparkContext,
    masters: Array[String])
  extends CoarseGrainedSchedulerBackend(scheduler, sc.env.actorSystem)
  with AppClientListener
  with Logging {

  var client: AppClient = null
  var stopping = false
  var shutdownCallback : (SparkDeploySchedulerBackend) => Unit = _
  @volatile var appId: String = _

  val registrationLock = new Object()
  var registrationDone = false

  val maxCores = conf.getOption("spark.cores.max").map(_.toInt)
  val totalExpectedCores = maxCores.getOrElse(0)

  override def start() {
    super.start()

    // The endpoint for executors to talk to us
    val driverUrl = "akka.tcp://%s@%s:%s/user/%s".format(
      SparkEnv.driverActorSystemName,
      conf.get("spark.driver.host"),
      conf.get("spark.driver.port"),
      CoarseGrainedSchedulerBackend.ACTOR_NAME)
    val args = Seq(driverUrl, "{{EXECUTOR_ID}}", "{{HOSTNAME}}", "{{CORES}}", "{{WORKER_URL}}")
    val extraJavaOpts = sc.conf.getOption("spark.executor.extraJavaOptions")
      .map(Utils.splitCommandString).getOrElse(Seq.empty)
    val classPathEntries = sc.conf.getOption("spark.executor.extraClassPath").toSeq.flatMap { cp =>
      cp.split(java.io.File.pathSeparator)
    }
    val libraryPathEntries =
      sc.conf.getOption("spark.executor.extraLibraryPath").toSeq.flatMap { cp =>
        cp.split(java.io.File.pathSeparator)
      }

    // Start executors with a few necessary configs for registering with the scheduler
    val sparkJavaOpts = Utils.sparkJavaOpts(conf, SparkConf.isExecutorStartupConf)
    val javaOpts = sparkJavaOpts ++ extraJavaOpts
    val command = Command("org.apache.spark.executor.CoarseGrainedExecutorBackend",
      args, sc.executorEnvs, classPathEntries, libraryPathEntries, javaOpts)
    val appUIAddress = sc.ui.map(_.appUIAddress).getOrElse("")
    val eventLogDir = sc.eventLogger.map(_.logDir)
    val appDesc = new ApplicationDescription(sc.appName, maxCores, sc.executorMemory, command,
      appUIAddress, eventLogDir)

    client = new AppClient(sc.env.actorSystem, masters, appDesc, this, conf)
    client.start()

    waitForRegistration()
  }

  override def stop() {
    stopping = true
    super.stop()
    client.stop()
    if (shutdownCallback != null) {
      shutdownCallback(this)
    }
  }

  override def connected(appId: String) {
    logInfo("Connected to Spark cluster with app ID " + appId)
    this.appId = appId
    notifyContext()
  }

  override def disconnected() {
    notifyContext()
    if (!stopping) {
      logWarning("Disconnected from Spark cluster! Waiting for reconnection...")
    }
  }

  override def dead(reason: String) {
    notifyContext()
    if (!stopping) {
      logError("Application has been killed. Reason: " + reason)
      scheduler.error(reason)
      // Ensure the application terminates, as we can no longer run jobs.
      sc.stop()
    }
  }

  override def executorAdded(fullId: String, workerId: String, hostPort: String, cores: Int,
    memory: Int) {
    logInfo("Granted executor ID %s on hostPort %s with %d cores, %s RAM".format(
      fullId, hostPort, cores, Utils.megabytesToString(memory)))
  }

  override def executorRemoved(fullId: String, message: String, exitStatus: Option[Int]) {
    val reason: ExecutorLossReason = exitStatus match {
      case Some(code) => ExecutorExited(code)
      case None => SlaveLost(message)
    }
    logInfo("Executor %s removed: %s".format(fullId, message))
    removeExecutor(fullId.split("/")(1), reason.toString)
  }

  override def sufficientResourcesRegistered(): Boolean = {
    totalCoreCount.get() >= totalExpectedCores * minRegisteredRatio
  }

  override def applicationId(): Option[String] = Option(appId)

  private def waitForRegistration() = {
    registrationLock.synchronized {
      while (!registrationDone) {
        registrationLock.wait()
      }
    }
  }

  private def notifyContext() = {
    registrationLock.synchronized {
      registrationDone = true
      registrationLock.notifyAll()
    }
  }

}
