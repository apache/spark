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

import java.util.concurrent.{ScheduledFuture, TimeUnit, Executors, TimeoutException}

import scala.concurrent.duration._

import org.apache.spark.{Logging, SparkConf}
import org.apache.spark.deploy.{ApplicationDescription, ExecutorState}
import org.apache.spark.deploy.DeployMessages._
import org.apache.spark.deploy.master.Master
import org.apache.spark.rpc._
import org.apache.spark.util.Utils

/**
 * Interface allowing applications to speak with a Spark deploy cluster. Takes a master URL,
 * an app description, and a listener for cluster events, and calls back the listener when various
 * events occur.
 */
private[spark] class AppClient(
    rpcEnv: RpcEnv,
    masterAddresses: Set[RpcAddress],
    appDescription: ApplicationDescription,
    listener: AppClientListener,
    conf: SparkConf)
  extends Logging {

  val REGISTRATION_TIMEOUT = 20.seconds.toMillis

  val REGISTRATION_RETRIES = 3

  var masterAddress: RpcAddress = null
  var actor: RpcEndpointRef = null
  var appId: String = null
  var registered = false

  class ClientActor(override val rpcEnv: RpcEnv) extends NetworkRpcEndpoint with Logging {
    var master: RpcEndpointRef = null
    var alreadyDisconnected = false  // To avoid calling listener.disconnected() multiple times
    var alreadyDead = false  // To avoid calling listener.dead() multiple times
    var registrationRetryTimer: Option[ScheduledFuture[_]] = None

    private val scheduler =
      Executors.newScheduledThreadPool(1, Utils.namedThreadFactory("client-actor"))

    override def onStart() {
      try {
        registerWithMaster()
      } catch {
        case e: Exception =>
          logWarning("Failed to connect to master", e)
          markDisconnected()
          stop()
      }
    }

    def tryRegisterAllMasters() {
      for (masterAddress <- masterAddresses) {
        logInfo("Connecting to master " + masterAddress + "...")
        val actor = Master.toEndpointRef(rpcEnv, masterAddress)
        actor.send(RegisterApplication(appDescription))
      }
    }

    def registerWithMaster() {
      tryRegisterAllMasters()
      var retries = 0
      registrationRetryTimer = Some {
        scheduler.scheduleAtFixedRate(new Runnable {
          override def run(): Unit = {
            Utils.tryOrExit {
              retries += 1
              if (registered) {
                registrationRetryTimer.foreach(_.cancel(true))
              } else if (retries >= REGISTRATION_RETRIES) {
                markDead("All masters are unresponsive! Giving up.")
              } else {
                tryRegisterAllMasters()
              }
            }
          }
        }, REGISTRATION_TIMEOUT, REGISTRATION_TIMEOUT, TimeUnit.MILLISECONDS)
      }
    }

    def changeMaster(url: String) {
      // url is a valid Spark url since we receive it from master.
      master = Master.toEndpointRef(rpcEnv, url)
      masterAddress = master.address
    }

    private def isPossibleMaster(remoteUrl: RpcAddress) = {
      masterAddresses.contains(remoteUrl)
    }

    override def receive(sender: RpcEndpointRef) = {
      case RegisteredApplication(appId_, masterUrl) =>
        appId = appId_
        registered = true
        changeMaster(masterUrl)
        listener.connected(appId)

      case ApplicationRemoved(message) =>
        markDead("Master removed our application: %s".format(message))
        stop()

      case ExecutorAdded(id: Int, workerId: String, hostPort: String, cores: Int, memory: Int) =>
        val fullId = appId + "/" + id
        logInfo("Executor added: %s on %s (%s) with %d cores".format(fullId, workerId, hostPort,
          cores))
        master.send(ExecutorStateChanged(appId, id, ExecutorState.RUNNING, None, None))
        listener.executorAdded(fullId, workerId, hostPort, cores, memory)

      case ExecutorUpdated(id, state, message, exitStatus) =>
        val fullId = appId + "/" + id
        val messageText = message.map(s => " (" + s + ")").getOrElse("")
        logInfo("Executor updated: %s is now %s%s".format(fullId, state, messageText))
        if (ExecutorState.isFinished(state)) {
          listener.executorRemoved(fullId, message.getOrElse(""), exitStatus)
        }

      case MasterChanged(masterUrl, masterWebUiUrl) =>
        logInfo("Master has changed, new master is at " + masterUrl)
        changeMaster(masterUrl)
        alreadyDisconnected = false
        sender.send(MasterChangeAcknowledged(appId))

      case StopAppClient =>
        markDead("Application has been stopped.")
        sender.send(true)
        stop()
    }

    override def onDisconnected(address: RpcAddress): Unit = {
      if (address == masterAddress) {
        logWarning(s"Connection to $address failed; waiting for master to reconnect...")
        markDisconnected()
      }
    }

    override def onNetworkError(cause: Throwable, remoteAddress: RpcAddress): Unit = {
      if (isPossibleMaster(remoteAddress)) {
        logWarning(s"Could not connect to $remoteAddress: $cause")
      }
    }

    /**
     * Notify the listener that we disconnected, if we hadn't already done so before.
     */
    def markDisconnected() {
      if (!alreadyDisconnected) {
        listener.disconnected()
        alreadyDisconnected = true
      }
    }

    def markDead(reason: String) {
      if (!alreadyDead) {
        listener.dead(reason)
        alreadyDead = true
      }
    }

    override def onStop() {
      registrationRetryTimer.foreach(_.cancel(true))
    }

  }

  def start() {
    // Just launch an actor; it will call back into the listener.
    actor = rpcEnv.setupEndpoint("client-actor", new ClientActor(rpcEnv))
  }

  def stop() {
    if (actor != null) {
      try {
        actor.askWithReply(StopAppClient)
      } catch {
        case e: TimeoutException =>
          logInfo("Stop request to Master timed out; it may already be shut down.")
      }
      actor = null
    }
  }
}
