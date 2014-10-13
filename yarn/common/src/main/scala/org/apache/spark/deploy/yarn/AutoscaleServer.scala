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

package org.apache.spark.deploy.yarn

import org.apache.spark.SparkEnv
import org.apache.spark.SparkConf
import akka.actor.ActorSystem
import akka.actor.ActorSelection
import akka.actor.Props
import org.apache.spark.Logging
import akka.actor.Actor
import akka.remote.RemotingLifecycleEvent
import akka.remote.DisassociatedEvent
import org.apache.spark.autoscale.AutoscaleMessages._

private[spark] class AutoscaleServer(allocator: YarnAllocator, sparkConf: SparkConf, actorSystem: ActorSystem, isClientRemote : Boolean = true) extends Logging {
  
  def start() {
    var driverUrl = if (isClientRemote) {
      "akka.tcp://%s@%s:%s/user/%s".format(
      SparkEnv.driverActorSystemName,
      sparkConf.get("spark.driver.host"),
      sparkConf.get("spark.driver.port"),
      "AutoscaleClient")
    } else {
        "akka://%s/user/%s".format(
        SparkEnv.driverActorSystemName,
        "AutoscaleClient")
    }
    actorSystem.actorOf(Props(new AutoscaleServerActor(driverUrl)), name = "AutoscaleServerActor")
  }
  
  private class AutoscaleServerActor(driverUrl: String) extends Actor {

    var driver: ActorSelection = _

    override def preStart() = {
      logInfo("Listen to driver: " + driverUrl)
      driver = context.actorSelection(driverUrl)
      // Send a hello message to establish the connection, after which
      // we can monitor Lifecycle Events.
      driver ! "Hello"
      driver ! RegisterAutoscaleServer
      context.system.eventStream.subscribe(self, classOf[RemotingLifecycleEvent])
    }

    override def receive = {
      case x: DisassociatedEvent =>
        logInfo(s"Driver terminated or disconnected! Shutting down. $x")
        //finish(FinalApplicationStatus.SUCCEEDED)
      case RegisteredAutoscaleServer =>
        logInfo("Autoscale Server registered successfully")
      case AddExecutors(count: Int) =>
        logInfo("Request to add " + count + "executors")
        allocator.addExecutors(count)
      case DeleteExecutors(execIds: List[String]) =>
        logInfo("Request to delete executors : " + execIds)
        allocator.deleteExecutors(execIds)
    }

  }

}