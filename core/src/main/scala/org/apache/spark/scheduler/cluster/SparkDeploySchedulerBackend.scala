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

import org.apache.spark.{Logging, SparkContext}
import org.apache.spark.deploy.client.{Client, ClientListener}
import org.apache.spark.deploy.{Command, ApplicationDescription}
import scala.collection.mutable.HashMap
import org.apache.spark.util.Utils

private[spark] class SparkDeploySchedulerBackend(
    scheduler: ClusterScheduler,
    sc: SparkContext,
    masters: Array[String],
    appName: String)
  extends CoarseGrainedSchedulerBackend(scheduler, sc.env.actorSystem)
  with ClientListener
  with Logging {

  var client: Client = null
  var stopping = false
  var shutdownCallback : (SparkDeploySchedulerBackend) => Unit = _

  val maxCores = System.getProperty("spark.cores.max", Int.MaxValue.toString).toInt

  override def start() {
    super.start()

    // The endpoint for executors to talk to us
    val driverUrl = "akka.tcp://spark@%s:%s/user/%s".format(
      System.getProperty("spark.driver.host"), System.getProperty("spark.driver.port"),
      CoarseGrainedSchedulerBackend.ACTOR_NAME)
    val args = Seq(driverUrl, "{{EXECUTOR_ID}}", "{{HOSTNAME}}", "{{CORES}}")
    val command = Command(
      "org.apache.spark.executor.CoarseGrainedExecutorBackend", args, sc.executorEnvs)
    val sparkHome = sc.getSparkHome().getOrElse(null)
    val appDesc = new ApplicationDescription(appName, maxCores, executorMemory, command, sparkHome,
        "http://" + sc.ui.appUIAddress)

    client = new Client(sc.env.actorSystem, masters, appDesc, this)
    client.start()
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
  }

  override def disconnected() {
    if (!stopping) {
      logWarning("Disconnected from Spark cluster! Waiting for reconnection...")
    }
  }

  override def dead() {
    if (!stopping) {
      logError("Spark cluster looks dead, giving up.")
      scheduler.error("Spark cluster looks down")
    }
  }

  override def executorAdded(fullId: String, workerId: String, hostPort: String, cores: Int, memory: Int) {
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
}
