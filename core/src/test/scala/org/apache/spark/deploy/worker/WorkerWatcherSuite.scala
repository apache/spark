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

import akka.actor.AddressFromURIString
import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.SecurityManager
import org.apache.spark.rpc.{RpcAddress, RpcEnv}

class WorkerWatcherSuite extends SparkFunSuite {
  test("WorkerWatcher shuts down on valid disassociation") {
    val conf = new SparkConf()
    val rpcEnv = RpcEnv.create("test", "localhost", 12345, conf, new SecurityManager(conf))
    val targetWorkerUrl = "akka://test@1.2.3.4:1234/user/Worker"
    val targetWorkerAddress = AddressFromURIString(targetWorkerUrl)
    val workerWatcher = new WorkerWatcher(rpcEnv, targetWorkerUrl)
    workerWatcher.setTesting(testing = true)
    rpcEnv.setupEndpoint("worker-watcher", workerWatcher)
    workerWatcher.onDisconnected(
      RpcAddress(targetWorkerAddress.host.get, targetWorkerAddress.port.get))
    assert(workerWatcher.isShutDown)
    rpcEnv.shutdown()
  }

  test("WorkerWatcher stays alive on invalid disassociation") {
    val conf = new SparkConf()
    val rpcEnv = RpcEnv.create("test", "localhost", 12345, conf, new SecurityManager(conf))
    val targetWorkerUrl = "akka://test@1.2.3.4:1234/user/Worker"
    val otherAkkaURL = "akka://test@4.3.2.1:1234/user/OtherActor"
    val otherAkkaAddress = AddressFromURIString(otherAkkaURL)
    val workerWatcher = new WorkerWatcher(rpcEnv, targetWorkerUrl)
    workerWatcher.setTesting(testing = true)
    rpcEnv.setupEndpoint("worker-watcher", workerWatcher)
    workerWatcher.onDisconnected(RpcAddress(otherAkkaAddress.host.get, otherAkkaAddress.port.get))
    assert(!workerWatcher.isShutDown)
    rpcEnv.shutdown()
  }
}
