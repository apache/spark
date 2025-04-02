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
import java.nio.file.{Files, Paths}
import java.util.concurrent.ThreadLocalRandom

import org.apache.curator.test.TestingServer

import org.apache.spark.{SecurityManager, SparkConf, SparkFunSuite}
import org.apache.spark.internal.config.Deploy.ZOOKEEPER_URL
import org.apache.spark.io.CompressionCodec
import org.apache.spark.rpc.{RpcEndpoint, RpcEnv}
import org.apache.spark.serializer.{JavaSerializer, Serializer}
import org.apache.spark.util.Utils

class PersistenceEngineSuite extends SparkFunSuite {

  test("FileSystemPersistenceEngine") {
    withTempDir { dir =>
      val conf = new SparkConf()
      testPersistenceEngine(conf, serializer =>
        new FileSystemPersistenceEngine(dir.getAbsolutePath, serializer)
      )
    }
  }

  test("SPARK-46258: RocksDBPersistenceEngine") {
    withTempDir { dir =>
      val conf = new SparkConf()
      testPersistenceEngine(conf, serializer =>
        new RocksDBPersistenceEngine(dir.getAbsolutePath, serializer)
      )
    }
  }

  test("SPARK-46191: FileSystemPersistenceEngine.persist error message for the existing file") {
    withTempDir { dir =>
      val conf = new SparkConf()
      val serializer = new JavaSerializer(conf)
      val engine = new FileSystemPersistenceEngine(dir.getAbsolutePath, serializer)
      engine.persist("test_1", "test_1_value")
      val m = intercept[IllegalStateException] {
        engine.persist("test_1", "test_1_value")
      }.getMessage
      assert(m.contains("File already exists"))
    }
  }

  test("SPARK-46215: FileSystemPersistenceEngine with a non-existent parent dir") {
    withTempDir { dir =>
      val conf = new SparkConf()
      testPersistenceEngine(conf, serializer =>
        new FileSystemPersistenceEngine(dir.getAbsolutePath + "/a/b/c/dir", serializer)
      )
    }
  }

  test("SPARK-46215: FileSystemPersistenceEngine with a symbolic link") {
    withTempDir { dir =>
      val target = Paths.get(dir.getAbsolutePath(), "target")
      val link = Paths.get(dir.getAbsolutePath(), "symbolic_link");

      Files.createDirectories(target)
      Files.createSymbolicLink(link, target);

      val conf = new SparkConf()
      testPersistenceEngine(conf, serializer =>
        new FileSystemPersistenceEngine(link.toAbsolutePath.toString, serializer)
      )
    }
  }

  test("SPARK-46827: RocksDBPersistenceEngine with a symbolic link") {
    withTempDir { dir =>
      val target = Paths.get(dir.getAbsolutePath(), "target")
      val link = Paths.get(dir.getAbsolutePath(), "symbolic_link");

      Files.createDirectories(target)
      Files.createSymbolicLink(link, target);

      val conf = new SparkConf()
      testPersistenceEngine(conf, serializer =>
        new RocksDBPersistenceEngine(link.toAbsolutePath.toString, serializer)
      )
    }
  }

  test("SPARK-46216: FileSystemPersistenceEngine with compression") {
    val conf = new SparkConf()
    CompressionCodec.ALL_COMPRESSION_CODECS.foreach { c =>
      val codec = CompressionCodec.createCodec(conf, c)
      withTempDir { dir =>
        testPersistenceEngine(conf, serializer =>
          new FileSystemPersistenceEngine(dir.getAbsolutePath, serializer, Some(codec))
        )
      }
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
        conf.set(ZOOKEEPER_URL, zkTestServer.getConnectString)
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
    try {
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
          webUiAddress = "http://localhost:80",
          Map.empty)

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
        assert(workerToPersist.webUiAddress === recoveryWorkerInfo.webUiAddress)
      } finally {
        testRpcEnv.shutdown()
        testRpcEnv.awaitTermination()
      }
    } finally {
      persistenceEngine.close()
    }
  }

  private def findFreePort(conf: SparkConf): Int = {
    val candidatePort = ThreadLocalRandom.current().nextInt(1024, 65536)
    Utils.startServiceOnPort(candidatePort, (trialPort: Int) => {
      val socket = new ServerSocket(trialPort)
      socket.close()
      (null, trialPort)
    }, conf)._2
  }
}
