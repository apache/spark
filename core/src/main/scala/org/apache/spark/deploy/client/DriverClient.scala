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

import akka.actor._
import akka.remote.{RemotingLifecycleEvent}

import org.apache.spark.{SparkException, Logging}
import org.apache.spark.deploy.{DeployMessage, DriverDescription}
import org.apache.spark.deploy.DeployMessages._
import org.apache.spark.deploy.master.{MasterArguments, Master}
import akka.pattern.ask

import org.apache.spark.util.{Utils, AkkaUtils}
import scala.concurrent.duration.{FiniteDuration, Duration}
import java.util.concurrent.TimeUnit
import akka.util.Timeout
import scala.concurrent.Await
import akka.actor.Actor.emptyBehavior

/**
 * Parent class for actors that to send a single message to the standalone master and then die.
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
 * Callable utility for starting and terminating drivers inside of the standalone scheduler.
 */
object DriverClient {

  def main(args: Array[String]) {
    if (args.size < 3) {
      println("usage: DriverClient launch <active-master> <jar-url> <main-class>")
      println("usage: DriverClient kill <active-master> <driver-id>")
      System.exit(-1)
    }

    val (actorSystem, boundPort) = AkkaUtils.createActorSystem(
      "driverSubmission", Utils.localHostName(), 0)

    // TODO Should be configurable
    val mem = 512

    args(0) match {
      case "launch" =>
        val master = args(1)
        val jarUrl = args(2)
        val mainClass = args(3)
        val driverDescription = new DriverDescription(jarUrl, mainClass, mem)
        val client = new SubmissionClient(actorSystem, master, driverDescription)

      case "kill" =>
        val master = args(1)
        val driverId = args(2)
        val client = new TerminationClient(actorSystem, master, driverId)
    }
    actorSystem.awaitTermination()
  }
}
