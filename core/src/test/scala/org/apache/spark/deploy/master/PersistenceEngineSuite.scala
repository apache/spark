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


package org.apache.spark.deploy.master

import java.net.ServerSocket

import org.apache.commons.lang3.RandomUtils
import org.apache.curator.test.TestingServer

import org.apache.spark.{SecurityManager, SparkConf, SparkFunSuite}
import org.apache.spark.rpc.{RpcEndpoint, RpcEnv}
import org.apache.spark.serializer.{Serializer, JavaSerializer}
import org.apache.spark.util.Utils

class PersistenceEngineSuite extends SparkFunSuite {

  test("FileSystemPersistenceEngine") {
    val dir = Utils.createTempDir()
    try {
      val conf = new SparkConf()
      testPersistenceEngine(conf, serializer =>
        new FileSystemPersistenceEngine(dir.getAbsolutePath, serializer)
      )
    } finally {
      Utils.deleteRecursively(dir)
    }
  }

  test("ZooKeeperPersistenceEngine") {
    val conf = new SparkConf()
    // TestingServer logs the port conflict exception rather than throwing an exception.
    // So we have to find a free port by ourselves. This approach cannot guarantee always starting
    // zkTestServer successfully because there is a time gap between finding a free port and
    // starting zkTestServer. But the failure possibility should be very low.
    val zkTestServer = new TestingServer(findFreePort(conf))
    try {
      testPersistenceEngine(conf, serializer => {
        conf.set("spark.deploy.zookeeper.url", zkTestServer.getConnectString)
        new ZooKeeperPersistenceEngine(conf, serializer)
      })
    } finally {
      zkTestServer.stop()
    }
  }

  private def testPersistenceEngine(
      conf: SparkConf, persistenceEngineCreator: Serializer => PersistenceEngine): Unit = {
    val serializer = new JavaSerializer(conf)
    val persistenceEngine = persistenceEngineCreator(serializer)
    persistenceEngine.persist("test_1", "test_1_value")
    assert(Seq("test_1_value") === persistenceEngine.read[String]("test_"))
    persistenceEngine.persist("test_2", "test_2_value")
    assert(Set("test_1_value", "test_2_value") === persistenceEngine.read[String]("test_").toSet)
    persistenceEngine.unpersist("test_1")
    assert(Seq("test_2_value") === persistenceEngine.read[String]("test_"))
    persistenceEngine.unpersist("test_2")
    assert(persistenceEngine.read[String]("test_").isEmpty)

    // Test deserializing objects that contain RpcEndpointRef
    val testRpcEnv = RpcEnv.create("test", "localhost", 12345, conf, new SecurityManager(conf))
    try {
      // Create a real endpoint so that we can test RpcEndpointRef deserialization
      val workerEndpoint = testRpcEnv.setupEndpoint("worker", new RpcEndpoint {
        override val rpcEnv: RpcEnv = testRpcEnv
      })

      val workerToPersist = new WorkerInfo(
        id = "test_worker",
        host = "127.0.0.1",
        port = 10000,
        cores = 0,
        memory = 0,
        endpoint = workerEndpoint,
        webUiPort = 0,
        publicAddress = ""
      )

      persistenceEngine.addWorker(workerToPersist)

      val (storedApps, storedDrivers, storedWorkers) =
        persistenceEngine.readPersistedData(testRpcEnv)

      assert(storedApps.isEmpty)
      assert(storedDrivers.isEmpty)

      // Check deserializing WorkerInfo
      assert(storedWorkers.size == 1)
      val recoveryWorkerInfo = storedWorkers.head
      assert(workerToPersist.id === recoveryWorkerInfo.id)
      assert(workerToPersist.host === recoveryWorkerInfo.host)
      assert(workerToPersist.port === recoveryWorkerInfo.port)
      assert(workerToPersist.cores === recoveryWorkerInfo.cores)
      assert(workerToPersist.memory === recoveryWorkerInfo.memory)
      assert(workerToPersist.endpoint === recoveryWorkerInfo.endpoint)
      assert(workerToPersist.webUiPort === recoveryWorkerInfo.webUiPort)
      assert(workerToPersist.publicAddress === recoveryWorkerInfo.publicAddress)
    } finally {
      testRpcEnv.shutdown()
      testRpcEnv.awaitTermination()
    }
  }

  private def findFreePort(conf: SparkConf): Int = {
    val candidatePort = RandomUtils.nextInt(1024, 65536)
    Utils.startServiceOnPort(candidatePort, (trialPort: Int) => {
      val socket = new ServerSocket(trialPort)
      socket.close()
      (null, trialPort)
    }, conf)._2
  }
}
