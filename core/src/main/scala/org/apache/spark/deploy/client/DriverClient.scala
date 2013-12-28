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

import scala.collection.JavaConversions._
import scala.collection.mutable.Map
import scala.concurrent._

import akka.actor._

import org.apache.spark.Logging
import org.apache.spark.deploy.{Command, DriverDescription}
import org.apache.spark.deploy.DeployMessages._
import org.apache.spark.deploy.master.Master
import org.apache.spark.util.{AkkaUtils, Utils}

/**
 * Actor that sends a single message to the standalone master and returns the response in the
 * given promise.
 */
class DriverActor(master: String, response: Promise[(Boolean, String)]) extends Actor with Logging {
  override def receive = {
    case SubmitDriverResponse(success, message) => {
      response.success((success, message))
    }

    case KillDriverResponse(success, message) => {
      response.success((success, message))
    }

    // Relay all other messages to the server.
    case message => {
      logInfo(s"Sending message to master $master...")
      val masterActor = context.actorSelection(Master.toAkkaUrl(master))
      masterActor ! message
    }
  }
}

/**
 * Executable utility for starting and terminating drivers inside of a standalone cluster.
 */
object DriverClient extends Logging {

  def main(args: Array[String]) {
    val driverArgs = new DriverClientArguments(args)

    // TODO: See if we can initialize akka so return messages are sent back using the same TCP
    //       flow. Else, this (sadly) requires the DriverClient be routable from the Master.
    val (actorSystem, boundPort) = AkkaUtils.createActorSystem(
      "driverClient", Utils.localHostName(), 0)
    val master = driverArgs.master
    val response = promise[(Boolean, String)]
    val driver: ActorRef = actorSystem.actorOf(Props(new DriverActor(driverArgs.master, response)))

    driverArgs.cmd match {
      case "launch" =>
        // TODO: Could modify env here to pass a flag indicating Spark is in deploy-driver mode
        //       then use that to load jars locally
        val env = Map[String, String]()
        System.getenv().foreach{case (k, v) => env(k) = v}

        val mainClass = "org.apache.spark.deploy.worker.DriverWrapper"
        val command = new Command(mainClass, Seq("{{WORKER_URL}}", driverArgs.mainClass) ++
          driverArgs.driverOptions, env)

        val driverDescription = new DriverDescription(
          driverArgs.jarUrl,
          driverArgs.memory,
          driverArgs.cores,
          command)
        driver ! RequestSubmitDriver(driverDescription)

      case "kill" =>
        val driverId = driverArgs.driverId
        driver ! RequestKillDriver(driverId)
    }

    val (success, message) =
      try {
        Await.result(response.future, AkkaUtils.askTimeout)
      } catch {
        case e: TimeoutException => (false, s"Master $master failed to respond in time")
      }
    if (success) logInfo(message) else logError(message)
    actorSystem.shutdown()
    actorSystem.awaitTermination()
  }
}
