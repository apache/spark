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

import scala.concurrent.{Await, future}
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

import org.scalatest.{BeforeAndAfterAll, FunSuite}

import org.apache.spark.SparkConf


class BlockClientFactorySuite extends FunSuite with BeforeAndAfterAll {

  private val conf = new SparkConf
  private var server1: BlockServer = _
  private var server2: BlockServer = _

  override def beforeAll() {
    server1 = new BlockServer(new NettyConfig(conf), null)
    server2 = new BlockServer(new NettyConfig(conf), null)
  }

  override def afterAll() {
    if (server1 != null) {
      server1.close()
    }
    if (server2 != null) {
      server2.close()
    }
  }

  test("BlockClients created are active and reused") {
    val factory = new BlockClientFactory(conf)
    val c1 = factory.createClient(server1.hostName, server1.port)
    val c2 = factory.createClient(server1.hostName, server1.port)
    val c3 = factory.createClient(server2.hostName, server2.port)
    assert(c1.isActive)
    assert(c3.isActive)
    assert(c1 === c2)
    assert(c1 !== c3)
    factory.close()
  }

  test("never return inactive clients") {
    val factory = new BlockClientFactory(conf)
    val c1 = factory.createClient(server1.hostName, server1.port)
    c1.close()

    // Block until c1 is no longer active
    val f = future {
      while (c1.isActive) {
        Thread.sleep(10)
      }
    }
    Await.result(f, 3 seconds)
    assert(!c1.isActive)

    // Create c2, which should be different from c1
    val c2 = factory.createClient(server1.hostName, server1.port)
    assert(c1 !== c2)
    factory.close()
  }

  test("BlockClients are close when BlockClientFactory is stopped") {
    val factory = new BlockClientFactory(conf)
    val c1 = factory.createClient(server1.hostName, server1.port)
    val c2 = factory.createClient(server2.hostName, server2.port)
    assert(c1.isActive)
    assert(c2.isActive)
    factory.close()
    assert(!c1.isActive)
    assert(!c2.isActive)
  }
}
