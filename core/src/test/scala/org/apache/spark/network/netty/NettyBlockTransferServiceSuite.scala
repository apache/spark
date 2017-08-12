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

import scala.util.Random

import org.mockito.Mockito.mock
import org.scalatest._

import org.apache.spark.{SecurityManager, SparkConf, SparkFunSuite}
import org.apache.spark.network.BlockDataManager

class NettyBlockTransferServiceSuite
  extends SparkFunSuite
  with BeforeAndAfterEach
  with ShouldMatchers {

  private var service0: NettyBlockTransferService = _
  private var service1: NettyBlockTransferService = _

  override def afterEach() {
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

  private def verifyServicePort(expectedPort: Int, actualPort: Int): Unit = {
    actualPort should be >= expectedPort
    // avoid testing equality in case of simultaneous tests
    // the default value for `spark.port.maxRetries` is 100 under test
    actualPort should be <= (expectedPort + 100)
  }

  private def createService(port: Int): NettyBlockTransferService = {
    val conf = new SparkConf()
      .set("spark.app.id", s"test-${getClass.getName}")
    val securityManager = new SecurityManager(conf)
    val blockDataManager = mock(classOf[BlockDataManager])
    val service = new NettyBlockTransferService(conf, securityManager, "localhost", "localhost",
      port, 1)
    service.init(blockDataManager)
    service
  }
}
