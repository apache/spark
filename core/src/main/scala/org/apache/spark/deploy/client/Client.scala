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

import akka.actor._
import akka.actor.Terminated
import akka.pattern.ask
import akka.util.Duration
import akka.remote.RemoteClientDisconnected
import akka.remote.RemoteClientLifeCycleEvent
import akka.remote.RemoteClientShutdown
import akka.dispatch.Await

import org.apache.spark.Logging
import org.apache.spark.deploy.{ApplicationDescription, ExecutorState}
import org.apache.spark.deploy.DeployMessages._
import org.apache.spark.deploy.master.Master


/**
 * The main class used to talk to a Spark deploy cluster. Takes a master URL, an app description,
 * and a listener for cluster events, and calls back the listener when various events occur.
 */
private[spark] class Client(
    actorSystem: ActorSystem,
    masterUrl: String,
    appDescription: ApplicationDescription,
    listener: ClientListener)
  extends Logging {

  var actor: ActorRef = null
  var appId: String = null

  class ClientActor extends Actor with Logging {
    var master: ActorRef = null
    var masterAddress: Address = null
    var alreadyDisconnected = false  // To avoid calling listener.disconnected() multiple times

    override def preStart() {
      logInfo("Connecting to master " + masterUrl)
      try {
        master = context.actorFor(Master.toAkkaUrl(masterUrl))
        masterAddress = master.path.address
        master ! RegisterApplication(appDescription)
        context.system.eventStream.subscribe(self, classOf[RemoteClientLifeCycleEvent])
        context.watch(master)  // Doesn't work with remote actors, but useful for testing
      } catch {
        case e: Exception =>
          logError("Failed to connect to master", e)
          markDisconnected()
          context.stop(self)
      }
    }

    override def receive = {
      case RegisteredApplication(appId_) =>
        appId = appId_
        listener.connected(appId)

      case ApplicationRemoved(message) =>
        logError("Master removed our application: %s; stopping client".format(message))
        markDisconnected()
        context.stop(self)

      case ExecutorAdded(id: Int, workerId: String, hostPort: String, cores: Int, memory: Int) =>
        val fullId = appId + "/" + id
        logInfo("Executor added: %s on %s (%s) with %d cores".format(fullId, workerId, hostPort, cores))
        listener.executorAdded(fullId, workerId, hostPort, cores, memory)

      case ExecutorUpdated(id, state, message, exitStatus) =>
        val fullId = appId + "/" + id
        val messageText = message.map(s => " (" + s + ")").getOrElse("")
        logInfo("Executor updated: %s is now %s%s".format(fullId, state, messageText))
        if (ExecutorState.isFinished(state)) {
          listener.executorRemoved(fullId, message.getOrElse(""), exitStatus)
        }

      case Terminated(actor_) if actor_ == master =>
        logError("Connection to master failed; stopping client")
        markDisconnected()
        context.stop(self)

      case RemoteClientDisconnected(transport, address) if address == masterAddress =>
        logError("Connection to master failed; stopping client")
        markDisconnected()
        context.stop(self)

      case RemoteClientShutdown(transport, address) if address == masterAddress =>
        logError("Connection to master failed; stopping client")
        markDisconnected()
        context.stop(self)

      case StopClient =>
        markDisconnected()
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
  }

  def start() {
    // Just launch an actor; it will call back into the listener.
    actor = actorSystem.actorOf(Props(new ClientActor))
  }

  def stop() {
    if (actor != null) {
      try {
        val timeout = Duration.create(System.getProperty("spark.akka.askTimeout", "10").toLong, "seconds")
        val future = actor.ask(StopClient)(timeout)
        Await.result(future, timeout)
      } catch {
        case e: TimeoutException =>
          logInfo("Stop request to Master timed out; it may already be shut down.")
      }
      actor = null
    }
  }
}
