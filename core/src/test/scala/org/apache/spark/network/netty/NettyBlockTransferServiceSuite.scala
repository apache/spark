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

package org.apache.spark.network.netty

import java.io.IOException
import java.util.concurrent.{CompletableFuture, ExecutionException, TimeUnit}

import scala.concurrent.Future
import scala.jdk.CollectionConverters._
import scala.reflect.ClassTag
import scala.util.Random

import org.mockito.ArgumentMatchers.{any, anyInt}
import org.mockito.Mockito.{mock, when}
import org.scalatest.matchers.must.Matchers
import org.scalatest.matchers.should.Matchers._

import org.apache.spark.{ExecutorDeadException, SecurityManager, SparkConf, SparkFunSuite}
import org.apache.spark.network.BlockDataManager
import org.apache.spark.network.client.{RpcResponseCallback, TransportClient, TransportClientFactory}
import org.apache.spark.network.shuffle.{BlockFetchingListener, DownloadFileManager}
import org.apache.spark.network.shuffle.protocol.LocalDirsForExecutors
import org.apache.spark.rpc.{RpcAddress, RpcEndpointRef, RpcTimeout}
import org.apache.spark.serializer.{JavaSerializer, SerializerManager}

class NettyBlockTransferServiceSuite
  extends SparkFunSuite
  with Matchers {

  private var service0: NettyBlockTransferService = _
  private var service1: NettyBlockTransferService = _

  override def afterEach(): Unit = {
    try {
      if (service0 != null) {
        service0.close()
        service0 = null
      }

      if (service1 != null) {
        service1.close()
        service1 = null
      }
    } finally {
      super.afterEach()
    }
  }

  test("can bind to a random port") {
    service0 = createService(port = 0)
    service0.port should not be 0
  }

  test("can bind to two random ports") {
    service0 = createService(port = 0)
    service1 = createService(port = 0)
    service0.port should not be service1.port
  }

  test("can bind to a specific port") {
    val port = 17634 + Random.nextInt(10000)
    logInfo("random port for test: " + port)
    service0 = createService(port)
    verifyServicePort(expectedPort = port, actualPort = service0.port)
  }

  test("can bind to a specific port twice and the second increments") {
    val port = 17634 + Random.nextInt(10000)
    logInfo("random port for test: " + port)
    service0 = createService(port)
    verifyServicePort(expectedPort = port, actualPort = service0.port)
    service1 = createService(service0.port)
    // `service0.port` is occupied, so `service1.port` should not be `service0.port`
    verifyServicePort(expectedPort = service0.port + 1, actualPort = service1.port)
  }

  test("SPARK-27637: test fetch block with executor dead") {
    val port = 17634 + Random.nextInt(10000)
    logInfo("random port for test: " + port)

    val driverEndpointRef = createDriverEndpointRef(alive = false)

    val clientFactory = mock(classOf[TransportClientFactory])
    val client = mock(classOf[TransportClient])
    // This is used to touch an IOException during fetching block.
    when(client.sendRpc(any(), any())).thenAnswer(_ => {throw new IOException()})
    var createClientCount = 0
    when(clientFactory.createClient(any(), any(), any())).thenAnswer(_ => {
      createClientCount += 1
      client
    })

    val listener = mock(classOf[BlockFetchingListener])
    var hitExecutorDeadException = false
    when(listener.onBlockTransferFailure(any(), any(classOf[ExecutorDeadException])))
      .thenAnswer(_ => {hitExecutorDeadException = true})

    service0 = createService(port, driverEndpointRef)
    injectClientFactory(service0, clientFactory)

    service0.fetchBlocks("localhost", port, "exec1",
      Array("block1"), listener, mock(classOf[DownloadFileManager]))
    assert(createClientCount === 1)
    assert(hitExecutorDeadException)
  }

  test("SPARK-62324: get host local dirs does not retry a dead executor") {
    val port = 17634 + Random.nextInt(10000)
    logInfo("random port for test: " + port)

    val driverEndpointRef = createDriverEndpointRef(alive = false)

    val clientFactory = mock(classOf[TransportClientFactory])
    var createClientCount = 0
    when(clientFactory.createClient(any[String](), anyInt())).thenAnswer(_ => {
      createClientCount += 1
      throw new IOException()
    })

    service0 = createService(port, driverEndpointRef, retryWait = "0s")
    injectClientFactory(service0, clientFactory)

    val future = new CompletableFuture[java.util.Map[String, Array[String]]]()
    service0.getHostLocalDirs("localhost", port, Array("exec1"), future)
    val error = intercept[ExecutionException] {
      future.get(30, TimeUnit.SECONDS)
    }
    assert(error.getCause.isInstanceOf[ExecutorDeadException])
    assert(createClientCount === 1)
  }

  test("SPARK-62324: get host local dirs retries when the executor is alive") {
    val port = 17634 + Random.nextInt(10000)
    logInfo("random port for test: " + port)

    val response = new LocalDirsForExecutors(
      Map("exec1" -> Array("/tmp/dir0")).asJava).toByteBuffer
    val client = mock(classOf[TransportClient])
    when(client.sendRpc(any(), any())).thenAnswer(invocation => {
      invocation.getArgument(1).asInstanceOf[RpcResponseCallback].onSuccess(response)
      0L
    })
    val clientFactory = mock(classOf[TransportClientFactory])
    var createClientCount = 0
    when(clientFactory.createClient(any[String](), anyInt())).thenAnswer(_ => {
      createClientCount += 1
      if (createClientCount == 1) {
        throw new IOException()
      }
      client
    })

    service0 = createService(port, createDriverEndpointRef(alive = true), retryWait = "0s")
    injectClientFactory(service0, clientFactory)

    val future = new CompletableFuture[java.util.Map[String, Array[String]]]()
    service0.getHostLocalDirs("localhost", port, Array("exec1"), future)
    val result = future.get(30, TimeUnit.SECONDS)
    assert(result.get("exec1").toSeq === Seq("/tmp/dir0"))
    assert(createClientCount === 2)
  }

  test("SPARK-62324: get host local dirs skips the aliveness check when retries are disabled") {
    val port = 17634 + Random.nextInt(10000)
    logInfo("random port for test: " + port)

    val clientFactory = mock(classOf[TransportClientFactory])
    var createClientCount = 0
    when(clientFactory.createClient(any[String](), anyInt())).thenAnswer(_ => {
      createClientCount += 1
      throw new IOException()
    })

    service0 = createService(port, createDriverEndpointRef(alive = false),
      retryWait = "0s", maxRetries = "0")
    injectClientFactory(service0, clientFactory)

    val future = new CompletableFuture[java.util.Map[String, Array[String]]]()
    service0.getHostLocalDirs("localhost", port, Array("exec1"), future)
    val error = intercept[ExecutionException] {
      future.get(30, TimeUnit.SECONDS)
    }
    assert(error.getCause.isInstanceOf[IOException])
    assert(createClientCount === 1)
  }

  private def createDriverEndpointRef(alive: Boolean): RpcEndpointRef = {
    new RpcEndpointRef(new SparkConf()) {
      override def address: RpcAddress = null
      override def name: String = "test"
      override def send(message: Any): Unit = {}
      override def ask[T: ClassTag](message: Any, timeout: RpcTimeout): Future[T] = {
        Future.successful(alive.asInstanceOf[T])
      }
    }
  }

  private def injectClientFactory(
      service: NettyBlockTransferService,
      clientFactory: TransportClientFactory): Unit = {
    val clientFactoryField = service.getClass
      .getSuperclass.getSuperclass.getDeclaredField("clientFactory")
    clientFactoryField.setAccessible(true)
    // Close the factory created by init() so its event loop group does not leak.
    Option(clientFactoryField.get(service).asInstanceOf[TransportClientFactory])
      .foreach(_.close())
    clientFactoryField.set(service, clientFactory)
  }

  private def verifyServicePort(expectedPort: Int, actualPort: Int): Unit = {
    actualPort should be >= expectedPort
    // avoid testing equality in case of simultaneous tests
    // if `spark.testing` is true,
    // the default value for `spark.port.maxRetries` is 100 under test
    actualPort should be <= (expectedPort + 100)
  }

  private def createService(
      port: Int,
      rpcEndpointRef: RpcEndpointRef = null,
      retryWait: String = "5s",
      maxRetries: String = "3"): NettyBlockTransferService = {
    val conf = new SparkConf()
      .set("spark.app.id", s"test-${getClass.getName}")
      .set("spark.shuffle.io.retryWait", retryWait)
      .set("spark.shuffle.io.maxRetries", maxRetries)
    val serializerManager = new SerializerManager(new JavaSerializer(conf), conf)
    val securityManager = new SecurityManager(conf)
    val blockDataManager = mock(classOf[BlockDataManager])
    val service = new NettyBlockTransferService(
      conf, securityManager, serializerManager, "localhost", "localhost", port, 1, rpcEndpointRef)
    service.init(blockDataManager)
    service
  }
}
