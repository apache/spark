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

import org.apache.spark.network.BlockDataManager
import org.apache.spark.{SecurityManager, SparkConf, SparkFunSuite}
import org.mockito.Mockito.mock
import org.scalatest._

class NettyBlockTransferServiceSuite
  extends SparkFunSuite
  with BeforeAndAfterEach
  with ShouldMatchers {

  private var service0: NettyBlockTransferService = _
  private var service1: NettyBlockTransferService = _

  override def afterEach() {
    if (service0 != null) {
      service0.close()
      service0 = null
    }

    if (service1 != null) {
      service1.close()
      service1 = null
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
    val port = 17634
    service0 = createService(port)
    service0.port should be >= port
    service0.port should be <= (port + 10) // avoid testing equality in case of simultaneous tests
  }

  test("can bind to a specific port twice and the second increments") {
    val port = 17634
    service0 = createService(port)
    service1 = createService(port)
    service0.port should be >= port
    service0.port should be <= (port + 10)
    service1.port should be (service0.port + 1)
  }

  private def createService(port: Int): NettyBlockTransferService = {
    val conf = new SparkConf()
      .set("spark.app.id", s"test-${getClass.getName}")
      .set("spark.blockManager.port", port.toString)
    val securityManager = new SecurityManager(conf)
    val blockDataManager = mock(classOf[BlockDataManager])
    val service = new NettyBlockTransferService(conf, securityManager, numCores = 1)
    service.init(blockDataManager)
    service
  }
}
