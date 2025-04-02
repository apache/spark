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

package org.apache.spark.deploy.client

import java.util.concurrent._
import java.util.concurrent.{Future => JFuture, ScheduledFuture => JScheduledFuture}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}

import scala.concurrent.Future
import scala.util.{Failure, Success}
import scala.util.control.NonFatal

import org.apache.spark.SparkConf
import org.apache.spark.deploy.{ApplicationDescription, ExecutorState}
import org.apache.spark.deploy.DeployMessages._
import org.apache.spark.deploy.master.Master
import org.apache.spark.internal.{Logging, LogKeys, MDC}
import org.apache.spark.internal.LogKeys._
import org.apache.spark.resource.ResourceProfile
import org.apache.spark.rpc._
import org.apache.spark.scheduler.ExecutorDecommissionInfo
import org.apache.spark.util.{RpcUtils, ThreadUtils}

/**
 * Interface allowing applications to speak with a Spark standalone cluster manager.
 *
 * Takes a master URL, an app description, and a listener for cluster events, and calls
 * back the listener when various events occur.
 *
 * @param masterUrls Each url should look like spark://host:port.
 */
private[spark] class StandaloneAppClient(
    rpcEnv: RpcEnv,
    masterUrls: Array[String],
    appDescription: ApplicationDescription,
    listener: StandaloneAppClientListener,
    conf: SparkConf)
  extends Logging {

  private val masterRpcAddresses = masterUrls.map(RpcAddress.fromSparkURL(_))

  private val REGISTRATION_TIMEOUT_SECONDS = 20
  private val REGISTRATION_RETRIES = 3

  private val endpoint = new AtomicReference[RpcEndpointRef]
  private val appId = new AtomicReference[String]
  private val registered = new AtomicBoolean(false)

  private class ClientEndpoint(override val rpcEnv: RpcEnv) extends ThreadSafeRpcEndpoint
    with Logging {

    private var master: Option[RpcEndpointRef] = None
    // To avoid calling listener.disconnected() multiple times
    private var alreadyDisconnected = false
    // To avoid calling listener.dead() multiple times
    private val alreadyDead = new AtomicBoolean(false)
    private val registerMasterFutures = new AtomicReference[Array[JFuture[_]]]
    private val registrationRetryTimer = new AtomicReference[JScheduledFuture[_]]

    // A thread pool for registering with masters. Because registering with a master is a blocking
    // action, this thread pool must be able to create "masterRpcAddresses.size" threads at the same
    // time so that we can register with all masters.
    private val registerMasterThreadPool = ThreadUtils.newDaemonCachedThreadPool(
      "appclient-register-master-threadpool",
      masterRpcAddresses.length // Make sure we can register with all masters at the same time
    )

    // A scheduled executor for scheduling the registration actions
    private val registrationRetryThread =
      ThreadUtils.newDaemonSingleThreadScheduledExecutor("appclient-registration-retry-thread")

    override def onStart(): Unit = {
      try {
        registerWithMaster(1)
      } catch {
        case e: Exception =>
          logWarning("Failed to connect to master", e)
          markDisconnected()
          this.stop()
      }
    }

    /**
     *  Register with all masters asynchronously and returns an array `Future`s for cancellation.
     */
    private def tryRegisterAllMasters(): Array[JFuture[_]] = {
      for (masterAddress <- masterRpcAddresses) yield {
        registerMasterThreadPool.submit(new Runnable {
          override def run(): Unit = try {
            if (registered.get) {
              return
            }
            logInfo(
              log"Connecting to master ${MDC(LogKeys.MASTER_URL, masterAddress.toSparkURL)}...")
            val masterRef = rpcEnv.setupEndpointRef(masterAddress, Master.ENDPOINT_NAME)
            masterRef.send(RegisterApplication(appDescription, self))
          } catch {
            case ie: InterruptedException => // Cancelled
            case NonFatal(e) => logWarning(log"Failed to connect to master " +
              log"${MDC(MASTER_URL, masterAddress)}", e)
          }
        })
      }
    }

    /**
     * Register with all masters asynchronously. It will call `registerWithMaster` every
     * REGISTRATION_TIMEOUT_SECONDS seconds until exceeding REGISTRATION_RETRIES times.
     * Once we connect to a master successfully, all scheduling work and Futures will be cancelled.
     *
     * nthRetry means this is the nth attempt to register with master.
     */
    private def registerWithMaster(nthRetry: Int): Unit = {
      registerMasterFutures.set(tryRegisterAllMasters())
      registrationRetryTimer.set(registrationRetryThread.schedule(new Runnable {
        override def run(): Unit = {
          if (registered.get) {
            registerMasterFutures.get.foreach(_.cancel(true))
            registerMasterThreadPool.shutdownNow()
          } else if (nthRetry >= REGISTRATION_RETRIES) {
            markDead("All masters are unresponsive! Giving up.")
          } else {
            registerMasterFutures.get.foreach(_.cancel(true))
            registerWithMaster(nthRetry + 1)
          }
        }
      }, REGISTRATION_TIMEOUT_SECONDS, TimeUnit.SECONDS))
    }

    /**
     * Send a message to the current master. If we have not yet registered successfully with any
     * master, the message will be dropped.
     */
    private def sendToMaster(message: Any): Unit = {
      master match {
        case Some(masterRef) => masterRef.send(message)
        case None => logWarning(
          log"Drop ${MDC(MESSAGE, message)} because has not yet connected to master")
      }
    }

    private def isPossibleMaster(remoteAddress: RpcAddress): Boolean = {
      masterRpcAddresses.contains(remoteAddress)
    }

    override def receive: PartialFunction[Any, Unit] = {
      case RegisteredApplication(appId_, masterRef) =>
        // FIXME How to handle the following cases?
        // 1. A master receives multiple registrations and sends back multiple
        // RegisteredApplications due to an unstable network.
        // 2. Receive multiple RegisteredApplication from different masters because the master is
        // changing.
        appId.set(appId_)
        registered.set(true)
        master = Some(masterRef)
        listener.connected(appId.get)

      case ApplicationRemoved(message) =>
        markDead("Master removed our application: %s".format(message))
        this.stop()

      case ExecutorAdded(id: Int, workerId: String, hostPort: String, cores: Int, memory: Int) =>
        val fullId = s"$appId/$id"
        logInfo(log"Executor added: ${MDC(LogKeys.EXECUTOR_ID, fullId)} on " +
          log"${MDC(LogKeys.WORKER_ID, workerId)} (${MDC(LogKeys.HOST_PORT, hostPort)}) " +
          log"with ${MDC(LogKeys.NUM_CORES, cores)} core(s)")
        listener.executorAdded(fullId, workerId, hostPort, cores, memory)

      case ExecutorUpdated(id, state, message, exitStatus, workerHost) =>
        val fullId = s"$appId/$id"
        val messageText = message.map(s => " (" + s + ")").getOrElse("")
        logInfo(log"Executor updated: ${MDC(LogKeys.EXECUTOR_ID, fullId)} is now " +
          log"${MDC(LogKeys.EXECUTOR_STATE, state)}${MDC(LogKeys.MESSAGE, messageText)}")
        if (ExecutorState.isFinished(state)) {
          listener.executorRemoved(fullId, message.getOrElse(""), exitStatus, workerHost)
        } else if (state == ExecutorState.DECOMMISSIONED) {
          listener.executorDecommissioned(fullId,
            ExecutorDecommissionInfo(message.getOrElse(""), workerHost))
        }

      case WorkerRemoved(id, host, message) =>
        logInfo(log"Master removed worker ${MDC(LogKeys.WORKER_ID, id)}: " +
          log"${MDC(LogKeys.MESSAGE, message)}")
        listener.workerRemoved(id, host, message)

      case MasterChanged(masterRef, masterWebUiUrl) =>
        logInfo(log"Master has changed, new master is at " +
          log"${MDC(LogKeys.MASTER_URL, masterRef.address.toSparkURL)}")
        master = Some(masterRef)
        alreadyDisconnected = false
        masterRef.send(MasterChangeAcknowledged(appId.get))
    }

    override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
      case StopAppClient =>
        markDead("Application has been stopped.")
        sendToMaster(UnregisterApplication(appId.get))
        context.reply(true)
        this.stop()

      case r: RequestExecutors =>
        master match {
          case Some(m) => askAndReplyAsync(m, context, r)
          case None =>
            logWarning("Attempted to request executors before registering with Master.")
            context.reply(false)
        }

      case k: KillExecutors =>
        master match {
          case Some(m) => askAndReplyAsync(m, context, k)
          case None =>
            logWarning("Attempted to kill executors before registering with Master.")
            context.reply(false)
        }
    }

    private def askAndReplyAsync[T](
        endpointRef: RpcEndpointRef,
        context: RpcCallContext,
        msg: T): Unit = {
      // Ask a message and create a thread to reply with the result.  Allow thread to be
      // interrupted during shutdown, otherwise context must be notified of NonFatal errors.
      endpointRef.ask[Boolean](msg).andThen {
        case Success(b) => context.reply(b)
        case Failure(ie: InterruptedException) => // Cancelled
        case Failure(NonFatal(t)) => context.sendFailure(t)
      }(ThreadUtils.sameThread)
    }

    override def onDisconnected(address: RpcAddress): Unit = {
      if (master.exists(_.address == address)) {
        logWarning(
          log"Connection to ${MDC(MASTER_URL, address)} failed; waiting for master to reconnect...")
        markDisconnected()
      }
    }

    override def onNetworkError(cause: Throwable, address: RpcAddress): Unit = {
      if (isPossibleMaster(address)) {
        logWarning(log"Could not connect to ${MDC(MASTER_URL, address)}: " +
          log"${MDC(ERROR, cause)}")
      }
    }

    /**
     * Notify the listener that we disconnected, if we hadn't already done so before.
     */
    def markDisconnected(): Unit = {
      if (!alreadyDisconnected) {
        listener.disconnected()
        alreadyDisconnected = true
      }
    }

    def markDead(reason: String): Unit = {
      if (!alreadyDead.get) {
        listener.dead(reason)
        alreadyDead.set(true)
      }
    }

    override def onStop(): Unit = {
      if (registrationRetryTimer.get != null) {
        registrationRetryTimer.get.cancel(true)
      }
      registrationRetryThread.shutdownNow()
      registerMasterFutures.get.foreach(_.cancel(true))
      registerMasterThreadPool.shutdownNow()
    }

  }

  def start(): Unit = {
    // Just launch an rpcEndpoint; it will call back into the listener.
    endpoint.set(rpcEnv.setupEndpoint("AppClient", new ClientEndpoint(rpcEnv)))
  }

  def stop(): Unit = {
    if (endpoint.get != null) {
      try {
        val timeout = RpcUtils.askRpcTimeout(conf)
        timeout.awaitResult(endpoint.get.ask[Boolean](StopAppClient))
      } catch {
        case e: TimeoutException =>
          logInfo("Stop request to Master timed out; it may already be shut down.")
      }
      endpoint.set(null)
    }
  }

  /**
   * Request executors for default resource profile from the Master by specifying the
   * total number desired, including existing pending and running executors.
   *
   * @return whether the request is acknowledged.
   */
  def requestTotalExecutors(requestedTotal: Int): Future[Boolean] = {
    requestTotalExecutors(Map(appDescription.defaultProfile -> requestedTotal))
  }

  /**
   * Request executors from the Master by specifying the total number desired for each
   * resource profile, including existing pending and running executors.
   *
   * @return whether the request is acknowledged.
   */
  def requestTotalExecutors(
      resourceProfileToTotalExecs: Map[ResourceProfile, Int]): Future[Boolean] = {
    if (endpoint.get != null && appId.get != null) {
      endpoint.get.ask[Boolean](RequestExecutors(appId.get, resourceProfileToTotalExecs))
    } else {
      logWarning("Attempted to request executors before driver fully initialized.")
      Future.successful(false)
    }
  }

  /**
   * Kill the given list of executors through the Master.
   * @return whether the kill request is acknowledged.
   */
  def killExecutors(executorIds: Seq[String]): Future[Boolean] = {
    if (endpoint.get != null && appId.get != null) {
      endpoint.get.ask[Boolean](KillExecutors(appId.get, executorIds))
    } else {
      logWarning("Attempted to kill executors before driver fully initialized.")
      Future.successful(false)
    }
  }

}
