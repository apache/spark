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

import java.util.concurrent.TimeUnit

import scala.concurrent.Await
import scala.concurrent.duration.{Duration, FiniteDuration}

import akka.actor._
import akka.actor.Actor.emptyBehavior
import akka.pattern.ask
import akka.remote.RemotingLifecycleEvent

import org.apache.spark.Logging
import org.apache.spark.deploy.{DeployMessage, DriverDescription}
import org.apache.spark.deploy.DeployMessages._
import org.apache.spark.deploy.master.Master
import org.apache.spark.util.{AkkaUtils, Utils}

/**
 * Actor that sends a single message to the standalone master and then shuts down.
 */
private[spark] abstract class SingleMessageClient(
    actorSystem: ActorSystem, master: String, message: DeployMessage)
  extends Logging {

  // Concrete child classes must implement
  def handleResponse(response: Any)

  var actor: ActorRef = actorSystem.actorOf(Props(new DriverActor()))

  class DriverActor extends Actor with Logging {
    override def preStart() {
      context.system.eventStream.subscribe(self, classOf[RemotingLifecycleEvent])
      logInfo("Sending message to master " + master + "...")
      val masterActor = context.actorSelection(Master.toAkkaUrl(master))
      val timeoutDuration: FiniteDuration = Duration.create(
        System.getProperty("spark.akka.askTimeout", "10").toLong, TimeUnit.SECONDS)
      val submitFuture = masterActor.ask(message)(timeoutDuration)
      handleResponse(Await.result(submitFuture, timeoutDuration))
      actorSystem.stop(actor)
      actorSystem.shutdown()
    }

    override def receive = emptyBehavior
  }
}

/**
 * Submits a driver to the master.
 */
private[spark] class SubmissionClient(actorSystem: ActorSystem, master: String,
    driverDescription: DriverDescription)
    extends SingleMessageClient(actorSystem, master, RequestSubmitDriver(driverDescription)) {

  override def handleResponse(response: Any) {
    val resp = response.asInstanceOf[SubmitDriverResponse]
    if (!resp.success) {
      logError(s"Error submitting driver to $master")
      logError(resp.message)
    }
  }
}

/**
 * Terminates a client at the master.
 */
private[spark] class TerminationClient(actorSystem: ActorSystem, master: String, driverId: String)
    extends SingleMessageClient(actorSystem, master, RequestKillDriver(driverId)) {

  override def handleResponse(response: Any) {
    val resp = response.asInstanceOf[KillDriverResponse]
    if (!resp.success) {
      logError(s"Error terminating $driverId at $master")
      logError(resp.message)
    }
  }
}

/**
 * Executable utility for starting and terminating drivers inside of a standalone cluster.
 */
object DriverClient {

  def main(args: Array[String]) {
    val driverArgs = new DriverClientArguments(args)

    // TODO: See if we can initialize akka so return messages are sent back using the same TCP
    //       flow. Else, this (sadly) requires the DriverClient be routable from the Master.
    val (actorSystem, boundPort) = AkkaUtils.createActorSystem(
      "driverClient", Utils.localHostName(), 0)

    driverArgs.cmd match {
      case "launch" =>
        val driverDescription = new DriverDescription(
          driverArgs.jarUrl,
          driverArgs.mainClass,
          driverArgs.memory,
          driverArgs.cores,
          driverArgs.driverOptions,
          driverArgs.driverJavaOptions,
          driverArgs.driverEnvVars)
        val client = new SubmissionClient(actorSystem, driverArgs.master, driverDescription)

      case "kill" =>
        val master = driverArgs.master
        val driverId = driverArgs.driverId
        val client = new TerminationClient(actorSystem, master, driverId)
    }
    actorSystem.awaitTermination()
  }
}
