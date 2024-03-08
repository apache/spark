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

import scala.concurrent.{ExecutionContext, Future}
import scala.reflect.ClassTag
import scala.util.Random

import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{mock, when}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.matchers.must.Matchers
import org.scalatest.matchers.should.Matchers._

import org.apache.spark.{ExecutorDeadException, SecurityManager, SparkConf, SparkFunSuite}
import org.apache.spark.network.BlockDataManager
import org.apache.spark.network.client.{TransportClient, TransportClientFactory}
import org.apache.spark.network.shuffle.{BlockFetchingListener, DownloadFileManager}
import org.apache.spark.rpc.{RpcAddress, RpcEndpointRef, RpcTimeout}
import org.apache.spark.serializer.{JavaSerializer, SerializerManager}

class NettyBlockTransferServiceSuite
  extends SparkFunSuite
  with BeforeAndAfterEach
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
    implicit val executionContext = ExecutionContext.global
    val port = 17634 + Random.nextInt(10000)
    logInfo("random port for test: " + port)

    val driverEndpointRef = new RpcEndpointRef(new SparkConf()) {
      override def address: RpcAddress = null
      override def name: String = "test"
      override def send(message: Any): Unit = {}
      // This rpcEndPointRef always return false for unit test to touch ExecutorDeadException.
      override def ask[T: ClassTag](message: Any, timeout: RpcTimeout): Future[T] = {
        Future{false.asInstanceOf[T]}
      }
    }

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
    val clientFactoryField = service0.getClass
      .getSuperclass.getSuperclass.getDeclaredField("clientFactory")
    clientFactoryField.setAccessible(true)
    clientFactoryField.set(service0, clientFactory)

    service0.fetchBlocks("localhost", port, "exec1",
      Array("block1"), listener, mock(classOf[DownloadFileManager]))
    assert(createClientCount === 1)
    assert(hitExecutorDeadException)
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
      rpcEndpointRef: RpcEndpointRef = null): NettyBlockTransferService = {
    val conf = new SparkConf()
      .set("spark.app.id", s"test-${getClass.getName}")
    val serializerManager = new SerializerManager(new JavaSerializer(conf), conf)
    val securityManager = new SecurityManager(conf)
    val blockDataManager = mock(classOf[BlockDataManager])
    val service = new NettyBlockTransferService(
      conf, securityManager, serializerManager, "localhost", "localhost", port, 1, rpcEndpointRef)
    service.init(blockDataManager)
    service
  }
}
