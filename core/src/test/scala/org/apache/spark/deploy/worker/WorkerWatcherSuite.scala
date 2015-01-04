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

import akka.actor.{ActorSystem, AddressFromURIString}
import org.apache.spark.SparkConf
import org.apache.spark.rpc.akka.AkkaRpcEnv
import org.scalatest.FunSuite

class WorkerWatcherSuite extends FunSuite {
  test("WorkerWatcher shuts down on valid disassociation") {
    val actorSystem = ActorSystem("test")
    val rpcEnv = new AkkaRpcEnv(actorSystem, new SparkConf())
    val targetWorkerUrl = "akka://1.2.3.4/user/Worker"
    val targetWorkerAddress = AddressFromURIString(targetWorkerUrl)
    val workerWatcher = new WorkerWatcher(rpcEnv, targetWorkerUrl)
    workerWatcher.setTesting(testing = true)
    rpcEnv.setupEndpoint("worker-watcher", workerWatcher)
    workerWatcher.remoteConnectionTerminated(targetWorkerAddress.toString)
    assert(workerWatcher.isShutDown)
    rpcEnv.stopAll()
    actorSystem.shutdown()
  }

  test("WorkerWatcher stays alive on invalid disassociation") {
    val actorSystem = ActorSystem("test")
    val rpcEnv = new AkkaRpcEnv(actorSystem, new SparkConf())
    val targetWorkerUrl = "akka://1.2.3.4/user/Worker"
    val otherAkkaURL = "akka://4.3.2.1/user/OtherActor"
    val otherAkkaAddress = AddressFromURIString(otherAkkaURL)
    val workerWatcher = new WorkerWatcher(rpcEnv, targetWorkerUrl)
    workerWatcher.setTesting(testing = true)
    rpcEnv.setupEndpoint("worker-watcher", workerWatcher)
    workerWatcher.remoteConnectionTerminated(otherAkkaAddress.toString)
    assert(!workerWatcher.isShutDown)
    rpcEnv.stopAll()
    actorSystem.shutdown()
  }
}
