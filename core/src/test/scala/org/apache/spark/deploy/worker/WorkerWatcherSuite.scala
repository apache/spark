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


import akka.testkit.TestActorRef
import org.scalatest.FunSuite
import akka.remote.DisassociatedEvent
import akka.actor.{ActorSystem, AddressFromURIString, Props}

class WorkerWatcherSuite extends FunSuite {
  test("WorkerWatcher shuts down on valid disassociation") {
    val actorSystem = ActorSystem("test")
    val targetWorkerUrl = "akka://1.2.3.4/user/Worker"
    val targetWorkerAddress = AddressFromURIString(targetWorkerUrl)
    val actorRef = TestActorRef[WorkerWatcher](Props(classOf[WorkerWatcher], targetWorkerUrl))(actorSystem)
    val workerWatcher = actorRef.underlyingActor
    workerWatcher.setTesting(testing = true)
    actorRef.underlyingActor.receive(new DisassociatedEvent(null, targetWorkerAddress, false))
    assert(actorRef.underlyingActor.isShutDown)
  }

  test("WorkerWatcher stays alive on invalid disassociation") {
    val actorSystem = ActorSystem("test")
    val targetWorkerUrl = "akka://1.2.3.4/user/Worker"
    val otherAkkaURL = "akka://4.3.2.1/user/OtherActor"
    val otherAkkaAddress = AddressFromURIString(otherAkkaURL)
    val actorRef = TestActorRef[WorkerWatcher](Props(classOf[WorkerWatcher], targetWorkerUrl))(actorSystem)
    val workerWatcher = actorRef.underlyingActor
    workerWatcher.setTesting(testing = true)
    actorRef.underlyingActor.receive(new DisassociatedEvent(null, otherAkkaAddress, false))
    assert(!actorRef.underlyingActor.isShutDown)
  }
}