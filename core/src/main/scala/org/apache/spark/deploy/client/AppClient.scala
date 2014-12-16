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

import java.util.concurrent.TimeoutException

import scala.concurrent.Await
import scala.concurrent.duration._

import akka.actor._
import akka.pattern.ask
import akka.remote.{AssociationErrorEvent, DisassociatedEvent, RemotingLifecycleEvent}

import org.apache.spark.{Logging, SparkConf, SparkException}
import org.apache.spark.deploy.{ApplicationDescription, ExecutorState}
import org.apache.spark.deploy.DeployMessages._
import org.apache.spark.deploy.master.Master
import org.apache.spark.util.{ActorLogReceive, Utils, AkkaUtils}

/**
 * Interface allowing applications to speak with a Spark deploy cluster. Takes a master URL,
 * an app description, and a listener for cluster events, and calls back the listener when various
 * events occur.
 *
 * @param masterUrls Each url should look like spark://host:port.
 */
private[spark] class AppClient(
    actorSystem: ActorSystem,
    masterUrls: Array[String],
    appDescription: ApplicationDescription,
    listener: AppClientListener,
    conf: SparkConf)
  extends Logging {

  val REGISTRATION_TIMEOUT = 20.seconds
  val REGISTRATION_RETRIES = 3

  var masterAddress: Address = null
  var actor: ActorRef = null
  var appId: String = null
  var registered = false
  var activeMasterUrl: String = null

  class ClientActor extends Actor with ActorLogReceive with Logging {
    var master: ActorSelection = null
    var alreadyDisconnected = false  // To avoid calling listener.disconnected() multiple times
    var alreadyDead = false  // To avoid calling listener.dead() multiple times
    var registrationRetryTimer: Option[Cancellable] = None

    override def preStart() {
      context.system.eventStream.subscribe(self, classOf[RemotingLifecycleEvent])
      try {
        registerWithMaster()
      } catch {
        case e: Exception =>
          logWarning("Failed to connect to master", e)
          markDisconnected()
          context.stop(self)
      }
    }

    def tryRegisterAllMasters() {
      for (masterUrl <- masterUrls) {
        logInfo("Connecting to master " + masterUrl + "...")
        val actor = context.actorSelection(Master.toAkkaUrl(masterUrl))
        actor ! RegisterApplication(appDescription)
      }
    }

    def registerWithMaster() {
      tryRegisterAllMasters()
      import context.dispatcher
      var retries = 0
      registrationRetryTimer = Some {
        context.system.scheduler.schedule(REGISTRATION_TIMEOUT, REGISTRATION_TIMEOUT) {
          Utils.tryOrExit {
            retries += 1
            if (registered) {
              registrationRetryTimer.foreach(_.cancel())
            } else if (retries >= REGISTRATION_RETRIES) {
              markDead("All masters are unresponsive! Giving up.")
            } else {
              tryRegisterAllMasters()
            }
          }
        }
      }
    }

    def changeMaster(url: String) {
      activeMasterUrl = url
      master = context.actorSelection(Master.toAkkaUrl(activeMasterUrl))
      masterAddress = activeMasterUrl match {
        case Master.sparkUrlRegex(host, port) =>
          Address("akka.tcp", Master.systemName, host, port.toInt)
        case x =>
          throw new SparkException("Invalid spark URL: " + x)
      }
    }

    private def isPossibleMaster(remoteUrl: Address) = {
      masterUrls.map(s => Master.toAkkaUrl(s))
        .map(u => AddressFromURIString(u).hostPort)
        .contains(remoteUrl.hostPort)
    }

    override def receiveWithLogging = {
      case RegisteredApplication(appId_, masterUrl) =>
        appId = appId_
        registered = true
        changeMaster(masterUrl)
        listener.connected(appId)

      case ApplicationRemoved(message) =>
        markDead("Master removed our application: %s".format(message))
        context.stop(self)

      case ExecutorAdded(id: Int, workerId: String, hostPort: String, cores: Int, memory: Int) =>
        val fullId = appId + "/" + id
        logInfo("Executor added: %s on %s (%s) with %d cores".format(fullId, workerId, hostPort,
          cores))
        master ! ExecutorStateChanged(appId, id, ExecutorState.RUNNING, None, None)
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
        sender ! MasterChangeAcknowledged(appId)

      case DisassociatedEvent(_, address, _) if address == masterAddress =>
        logWarning(s"Connection to $address failed; waiting for master to reconnect...")
        markDisconnected()

      case AssociationErrorEvent(cause, _, address, _, _) if isPossibleMaster(address) =>
        logWarning(s"Could not connect to $address: $cause")

      case StopAppClient =>
        markDead("Application has been stopped.")
        sender ! true
        context.stop(self)
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

    override def postStop() {
      registrationRetryTimer.foreach(_.cancel())
    }

  }

  def start() {
    // Just launch an actor; it will call back into the listener.
    actor = actorSystem.actorOf(Props(new ClientActor))
  }

  def stop() {
    if (actor != null) {
      try {
        val timeout = AkkaUtils.askTimeout(conf)
        val future = actor.ask(StopAppClient)(timeout)
        Await.result(future, timeout)
      } catch {
        case e: TimeoutException =>
          logInfo("Stop request to Master timed out; it may already be shut down.")
      }
      actor = null
    }
  }
}
