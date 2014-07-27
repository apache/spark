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

package org.apache.spark.deploy.worker

import akka.actor.{Actor, Address, AddressFromURIString}
import akka.remote.{AssociatedEvent, AssociationErrorEvent, AssociationEvent, DisassociatedEvent, RemotingLifecycleEvent}

import org.apache.spark.Logging
import org.apache.spark.deploy.DeployMessages.SendHeartbeat

/**
 * Actor which connects to a worker process and terminates the JVM if the connection is severed.
 * Provides fate sharing between a worker and its associated child processes.
 */
private[spark] class WorkerWatcher(workerUrl: String) extends Actor
    with Logging {
  override def preStart() {
    context.system.eventStream.subscribe(self, classOf[RemotingLifecycleEvent])

    logInfo(s"Connecting to worker $workerUrl")
    val worker = context.actorSelection(workerUrl)
    worker ! SendHeartbeat // need to send a message here to initiate connection
  }

  // Used to avoid shutting down JVM during tests
  private[deploy] var isShutDown = false
  private[deploy] def setTesting(testing: Boolean) = isTesting = testing
  private var isTesting = false

  // Lets us filter events only from the worker's actor system
  private val expectedHostPort = AddressFromURIString(workerUrl).hostPort
  private def isWorker(address: Address) = address.hostPort == expectedHostPort

  def exitNonZero() = if (isTesting) isShutDown = true else System.exit(-1)

  override def receive = {
    case AssociatedEvent(localAddress, remoteAddress, inbound) if isWorker(remoteAddress) =>
      logInfo(s"Successfully connected to $workerUrl")

    case AssociationErrorEvent(cause, localAddress, remoteAddress, inbound, _)
        if isWorker(remoteAddress) =>
      // These logs may not be seen if the worker (and associated pipe) has died
      logError(s"Could not initialize connection to worker $workerUrl. Exiting.")
      logError(s"Error was: $cause")
      exitNonZero()

    case DisassociatedEvent(localAddress, remoteAddress, inbound) if isWorker(remoteAddress) =>
      // This log message will never be seen
      logError(s"Lost connection to worker actor $workerUrl. Exiting.")
      exitNonZero()

    case e: AssociationEvent =>
      // pass through association events relating to other remote actor systems

    case e => logWarning(s"Received unexpected actor system event: $e")
  }
}
