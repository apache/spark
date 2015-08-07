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

import scala.collection.mutable.{ArrayBuffer, HashSet}
import scala.concurrent.{Future, ExecutionContext}

import org.apache.spark.{Logging, SparkContext}
import org.apache.spark.rpc._
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages._
import org.apache.spark.scheduler._
import org.apache.spark.ui.JettyUtils
import org.apache.spark.util.{ThreadUtils, RpcUtils}

import scala.util.control.NonFatal

/**
 * Abstract Yarn scheduler backend that contains common logic
 * between the client and cluster Yarn scheduler backends.
 */
private[spark] abstract class YarnSchedulerBackend(
    scheduler: TaskSchedulerImpl,
    sc: SparkContext)
  extends CoarseGrainedSchedulerBackend(scheduler, sc.env.rpcEnv) {

  if (conf.getOption("spark.scheduler.minRegisteredResourcesRatio").isEmpty) {
    minRegisteredRatio = 0.8
  }

  protected var totalExpectedExecutors = 0

  private val yarnSchedulerEndpoint = rpcEnv.setupEndpoint(
    YarnSchedulerBackend.ENDPOINT_NAME, new YarnSchedulerEndpoint(rpcEnv))

  private implicit val askTimeout = RpcUtils.askRpcTimeout(sc.conf)

  /**
   * Request executors from the ApplicationMaster by specifying the total number desired.
   * This includes executors already pending or running.
   */
  override def doRequestTotalExecutors(requestedTotal: Int): Boolean = {
    yarnSchedulerEndpoint.askWithRetry[Boolean](
      RequestExecutors(requestedTotal, localityAwareTasks, hostToLocalTaskCount))
  }

  /**
   * Request that the ApplicationMaster kill the specified executors.
   */
  override def doKillExecutors(executorIds: Seq[String]): Boolean = {
    yarnSchedulerEndpoint.askWithRetry[Boolean](KillExecutors(executorIds))
  }

  override def sufficientResourcesRegistered(): Boolean = {
    totalRegisteredExecutors.get() >= totalExpectedExecutors * minRegisteredRatio
  }

  /**
   * Add filters to the SparkUI.
   */
  private def addWebUIFilter(
      filterName: String,
      filterParams: Map[String, String],
      proxyBase: String): Unit = {
    if (proxyBase != null && proxyBase.nonEmpty) {
      System.setProperty("spark.ui.proxyBase", proxyBase)
    }

    val hasFilter =
      filterName != null && filterName.nonEmpty &&
      filterParams != null && filterParams.nonEmpty
    if (hasFilter) {
      logInfo(s"Add WebUI Filter. $filterName, $filterParams, $proxyBase")
      conf.set("spark.ui.filters", filterName)
      filterParams.foreach { case (k, v) => conf.set(s"spark.$filterName.param.$k", v) }
      scheduler.sc.ui.foreach { ui => JettyUtils.addFilters(ui.getHandlers, conf) }
    }
  }

  /**
   * Override the DriverEndpoint to add extra logic for the case when an executor is disconnected.
   * We should check the cluster manager and find if the loss of the executor was caused by YARN
   * force killing it due to preemption.
   */
  private class YarnDriverEndpoint(rpcEnv: RpcEnv, sparkProperties: ArrayBuffer[(String, String)])
      extends DriverEndpoint(rpcEnv, sparkProperties) {

    private val pendingDisconnectedExecutors = new HashSet[String]
    private val handleDisconnectedExecutorThreadPool =
      ThreadUtils.newDaemonCachedThreadPool("yarn-driver-handle-lost-executor-thread-pool")

    override def onDisconnected(rpcAddress: RpcAddress): Unit = {
      addressToExecutorId.get(rpcAddress).foreach({ executorId =>
        pendingDisconnectedExecutors.synchronized {
          if (!pendingDisconnectedExecutors.contains(executorId)) {
            pendingDisconnectedExecutors.add(executorId)
            handleDisconnectedExecutorThreadPool.submit(new Runnable() {
              override def run(): Unit = {
                val executorLossReason =
                  yarnSchedulerEndpoint.askWithRetry[Option[ExecutorLossReason]](
                      GetExecutorLossReason(executorId))
                executorLossReason match {
                  case Some(reason) =>
                    driverEndpoint.askWithRetry[Boolean](RemoveExecutor(executorId, reason))
                  case None =>
                    logWarning(s"Attempted to get executor loss reason" +
                      s" for $rpcAddress but got no response. Marking as slave lost.")
                    driverEndpoint.askWithRetry[Boolean](RemoveExecutor(executorId, SlaveLost()))
                }
                pendingDisconnectedExecutors.synchronized {
                  pendingDisconnectedExecutors.remove(executorId)
                }
              }
            })
          }
        }
      })
    }
  }

  override def createDriverEndpoint(properties: ArrayBuffer[(String, String)]): DriverEndpoint = {
    new YarnDriverEndpoint(rpcEnv, properties)
  }

  /**
   * An [[RpcEndpoint]] that communicates with the ApplicationMaster.
   */
  private class YarnSchedulerEndpoint(override val rpcEnv: RpcEnv)
    extends ThreadSafeRpcEndpoint with Logging {
    private var amEndpoint: Option[RpcEndpointRef] = None

    private val askAmThreadPool =
      ThreadUtils.newDaemonCachedThreadPool("yarn-scheduler-ask-am-thread-pool")
    implicit val askAmExecutor = ExecutionContext.fromExecutor(askAmThreadPool)

    override def receive: PartialFunction[Any, Unit] = {
      case RegisterClusterManager(am) =>
        logInfo(s"ApplicationMaster registered as $am")
        amEndpoint = Some(am)

      case AddWebUIFilter(filterName, filterParams, proxyBase) =>
        addWebUIFilter(filterName, filterParams, proxyBase)

      case RemoveExecutor(executorId, reason) =>
        removeExecutor(executorId, reason)
    }

    override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
      case r: RequestExecutors =>
        amEndpoint match {
          case Some(am) =>
            Future {
              context.reply(am.askWithRetry[Boolean](r))
            } onFailure {
              case NonFatal(e) =>
                logError(s"Sending $r to AM was unsuccessful", e)
                context.sendFailure(e)
            }
          case None =>
            logWarning("Attempted to request executors before the AM has registered!")
            context.reply(false)
        }

      case k: KillExecutors =>
        amEndpoint match {
          case Some(am) =>
            Future {
              context.reply(am.askWithRetry[Boolean](k))
            } onFailure {
              case NonFatal(e) =>
                logError(s"Sending $k to AM was unsuccessful", e)
                context.sendFailure(e)
            }
          case None =>
            logWarning("Attempted to kill executors before the AM has registered!")
            context.reply(false)
        }

      case c: GetExecutorLossReason =>
        amEndpoint match {
          case Some(am) =>
            Future {
              context.reply(am.askWithRetry[Option[ExecutorLossReason]](c))
            } onFailure {
              case NonFatal(e) =>
                logError(s"Finding the executor loss reason was unsuccessful", e)
                context.sendFailure(e)
            }
          case None =>
            logWarning("Attempted to check if an executor exited normally" +
              " before the AM has registered!")
            context.reply(None)
        }
    }

    override def onDisconnected(remoteAddress: RpcAddress): Unit = {
      if (amEndpoint.exists(_.address == remoteAddress)) {
        logWarning(s"ApplicationMaster has disassociated: $remoteAddress")
      }
    }

    override def onStop(): Unit = {
      askAmThreadPool.shutdownNow()
    }
  }
}

private[spark] object YarnSchedulerBackend {
  val ENDPOINT_NAME = "YarnScheduler"
}
