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

package org.apache.spark.executor

import java.nio.ByteBuffer

import akka.actor.{ActorRef, Actor, Props, Terminated}
import akka.remote.{RemoteClientLifeCycleEvent, RemoteClientShutdown, RemoteClientDisconnected}

import org.apache.spark.{Logging, SparkEnv}
import org.apache.spark.TaskState.TaskState
import org.apache.spark.scheduler.cluster.StandaloneClusterMessages._
import org.apache.spark.util.{Utils, AkkaUtils}


private[spark] class StandaloneExecutorBackend(
    driverUrl: String,
    executorId: String,
    hostPort: String,
    cores: Int)
  extends Actor
  with ExecutorBackend
  with Logging {

  Utils.checkHostPort(hostPort, "Expected hostport")

  var executor: Executor = null
  var driver: ActorRef = null

  override def preStart() {
    logInfo("Connecting to driver: " + driverUrl)
    driver = context.actorFor(driverUrl)
    driver ! RegisterExecutor(executorId, hostPort, cores)
    context.system.eventStream.subscribe(self, classOf[RemoteClientLifeCycleEvent])
    context.watch(driver) // Doesn't work with remote actors, but useful for testing
  }

  override def receive = {
    case RegisteredExecutor(sparkProperties) =>
      logInfo("Successfully registered with driver")
      // Make this host instead of hostPort ?
      executor = new Executor(executorId, Utils.parseHostPort(hostPort)._1, sparkProperties)

    case RegisterExecutorFailed(message) =>
      logError("Slave registration failed: " + message)
      System.exit(1)

    case LaunchTask(taskDesc) =>
      logInfo("Got assigned task " + taskDesc.taskId)
      if (executor == null) {
        logError("Received launchTask but executor was null")
        System.exit(1)
      } else {
        executor.launchTask(this, taskDesc.taskId, taskDesc.serializedTask)
      }

    case Terminated(_) | RemoteClientDisconnected(_, _) | RemoteClientShutdown(_, _) =>
      logError("Driver terminated or disconnected! Shutting down.")
      System.exit(1)
  }

  override def statusUpdate(taskId: Long, state: TaskState, data: ByteBuffer) {
    driver ! StatusUpdate(executorId, taskId, state, data)
  }
}

private[spark] object StandaloneExecutorBackend {
  def run(driverUrl: String, executorId: String, hostname: String, cores: Int) {
    // Debug code
    Utils.checkHost(hostname)

    // Create a new ActorSystem to run the backend, because we can't create a SparkEnv / Executor
    // before getting started with all our system properties, etc
    val (actorSystem, boundPort) = AkkaUtils.createActorSystem("sparkExecutor", hostname, 0)
    // set it
    val sparkHostPort = hostname + ":" + boundPort
    System.setProperty("spark.hostPort", sparkHostPort)
    val actor = actorSystem.actorOf(
      Props(new StandaloneExecutorBackend(driverUrl, executorId, sparkHostPort, cores)),
      name = "Executor")
    actorSystem.awaitTermination()
  }

  def main(args: Array[String]) {
    if (args.length < 4) {
      //the reason we allow the last frameworkId argument is to make it easy to kill rogue executors
      System.err.println("Usage: StandaloneExecutorBackend <driverUrl> <executorId> <hostname> <cores> [<appid>]")
      System.exit(1)
    }
    run(args(0), args(1), args(2), args(3).toInt)
  }
}
