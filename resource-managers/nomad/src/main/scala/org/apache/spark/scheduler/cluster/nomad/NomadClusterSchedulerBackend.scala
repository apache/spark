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

package org.apache.spark.scheduler.cluster.nomad

import scala.concurrent.Future

import org.apache.spark.SparkContext
import org.apache.spark.internal.config._
import org.apache.spark.launcher.{LauncherBackend, SparkAppHandle}
import org.apache.spark.rpc.{RpcCallContext, RpcEndpointAddress, RpcEnv}
import org.apache.spark.scheduler.TaskSchedulerImpl
import org.apache.spark.scheduler.cluster._
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages.{RegisterExecutor, RequestExecutors}
import org.apache.spark.util.{ShutdownHookManager, Utils}

/**
 * A scheduler backend that runs Spark executors on a Nomad cluster.
 *
 * The executors will each run in their own task group within a single Nomad job.
 * If the Spark application was submitted in cluster mode, the executors will be launched in the
 * same Nomad job as the application driver.
 *
 * When dynamic resource allocation is enabled, each executor task group will have a shuffle-server
 * task in addition to the executor task.
 */
private[spark] class NomadClusterSchedulerBackend(
    scheduler: TaskSchedulerImpl,
    sc: SparkContext,
    jobController: SparkNomadJobController,
    staticExecutorInstances: Option[Int]
  )
  extends CoarseGrainedSchedulerBackend(scheduler, sc.env.rpcEnv) {

  private class NomadLauncherBackend extends LauncherBackend {
    connect()
    setAppId(applicationId())
    setState(SparkAppHandle.State.RUNNING)

    private val failureReportingShutdownHook =
      ShutdownHookManager.addShutdownHook { () =>
        setState(SparkAppHandle.State.FAILED)
      }

    def setFinalState(finalState: SparkAppHandle.State): Unit =
      try setState(finalState)
      finally {
        ShutdownHookManager.removeShutdownHook(failureReportingShutdownHook)
        close()
      }

    override protected def onStopRequest(): Unit =
      stop(SparkAppHandle.State.KILLED)
  }

  private val launcherBackend = new NomadLauncherBackend

  override val minRegisteredRatio: Double =
    if (conf.getOption("spark.scheduler.minRegisteredResourcesRatio").isEmpty) 0.8
    else super.minRegisteredRatio

  private lazy val staticExecutorsToRequest: Option[Int] =
    if (sc.executorAllocationManager.isDefined) None
    else Some(staticExecutorInstances.getOrElse {
      logWarning(s"Defaulting to ${NomadClusterManagerConf.DEFAULT_EXECUTOR_INSTANCES} executors." +
        s" Consider setting ${EXECUTOR_INSTANCES.key} or enabling dynamic allocation.")
      NomadClusterManagerConf.DEFAULT_EXECUTOR_INSTANCES
    })

  override def sufficientResourcesRegistered(): Boolean = {
    val initiallyExpectedExecutors = staticExecutorsToRequest
      .getOrElse(Utils.getDynamicAllocationInitialExecutors(conf))

    totalRegisteredExecutors.get() >= initiallyExpectedExecutors * minRegisteredRatio
  }

  override def applicationId(): String = jobController.jobId

  override def getDriverLogUrls: Option[Map[String, String]] = {
    jobController.fetchDriverLogUrls(conf)
  }

  override def stopExecutors(): Unit = {
    jobController.setExecutorCount(0)
  }

  override def stop(): Unit = {
    stop(SparkAppHandle.State.FINISHED)
  }

  def stop(finalState: SparkAppHandle.State): Unit = {
    try {
      try super.stop()
      finally {
        try jobController.setExecutorCount(0)
        finally jobController.close()
      }
    } finally {
      launcherBackend.setFinalState(finalState)
    }
  }

  /**
   * Extend DriverEndpoint with Nomad-specific functionality.
   */
  protected class NomadDriverEndpoint(rpcEnv: RpcEnv, sparkProperties: Seq[(String, String)])
    extends DriverEndpoint(rpcEnv, sparkProperties) {

    override def onStart(): Unit = {

      val driverUrl = RpcEndpointAddress(
        conf.get("spark.driver.host"),
        conf.get("spark.driver.port").toInt,
        CoarseGrainedSchedulerBackend.ENDPOINT_NAME).toString

      jobController.initialiseExecutors(conf, driverUrl, staticExecutorsToRequest.getOrElse(0))
    }

    override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = ({

      /**
       * Request additional executors as required.
       *
       * Doing this here instead of directly in createDriverEndpoint allows us to serialise all
       * manipulations of the Nomad job.
       */
      case RequestExecutors(total, _, _, _) =>
        jobController.setExecutorCount(total)
        context.reply(true)

      /**
       * Intercept registration messages from executors, resolve their log URLs,
       * then relay a copy of the registration message with resolved log URLs to the parent class.
       */
      case message@RegisterExecutor(executorId, _, _, _, logUrls) =>
        super.receiveAndReply(context)(
          try message.copy(logUrls = jobController.resolveExecutorLogUrls(logUrls))
          catch { case e: Exception =>
            logWarning(s"Failed to lookup log URLs for executor $executorId", e)
            message
          }
        )

    }: PartialFunction[Any, Unit])
      .orElse(super.receiveAndReply(context))
  }

  /**
   * Create a NomadDriverEndpoint.
   */
  override protected def createDriverEndpoint(properties: Seq[(String, String)]): DriverEndpoint = {
    new NomadDriverEndpoint(rpcEnv, properties)
  }

  /**
   * Add executor task groups as needed to meet the desired number.
   *
   * If the request total is lower, we don't need to do anything,
   * as extra executors will eventually result in calls to [[doKillExecutors()]].
   */
  override protected def doRequestTotalExecutors(requestedTotal: Int): Future[Boolean] =
    driverEndpoint.ask[Boolean](RequestExecutors(requestedTotal, 0, Map.empty, Set.empty))

  override def doKillExecutors(executorIds: Seq[String]): Future[Boolean] = {
    logWarning(
      executorIds.mkString(
        s"Ignoring request to kill ${executorIds.size} executor(s): ",
        ",",
        " (not yet implemented for Nomad)"
      )
    )
    super.doKillExecutors(executorIds)
  }

}
