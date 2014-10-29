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

package org.apache.spark.autoscale

import org.apache.spark.SparkEnv
import akka.actor.ActorRef
import akka.actor.Actor
import akka.actor.Props
import akka.actor.ActorSelection
import akka.actor.ActorSystem
import org.apache.spark.Logging
import org.apache.spark.autoscale.AutoscaleMessages._

class AutoscaleClient(actorSystem: ActorSystem) extends Logging {
  var server : ActorRef = _
  actorSystem.actorOf(Props(new AutoscaleClientActor()), "AutoscaleClient")
  
  private class AutoscaleClientActor extends Actor {
    override def preStart = {
      logInfo("Starting Autoscale Client Actor")
    }
    override def receive = {
      case RegisterAutoscaleServer =>
        logInfo("Registering Autoscale Server Actor")
         server = sender
         sender ! true
    }
  }
  def addExecutors(count: Int) {
    if (server != null) server ! AddExecutors(count)
    else logInfo("Autoscale Server not registered")
  }
  def deleteExecutors(execIds: List[String]) {
    if (server!= null) server ! DeleteExecutors(execIds)
    else logInfo("Autoscale Server not registered")
  }
}

