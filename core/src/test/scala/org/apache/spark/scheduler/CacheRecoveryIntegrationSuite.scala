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

package org.apache.spark.scheduler

import scala.util.Try

import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Matchers}
import org.scalatest.concurrent.Eventually
import org.scalatest.time.{Seconds, Span}

import org.apache.spark.{SparkConf, SparkContext, SparkException, SparkFunSuite, TestUtils}
import org.apache.spark.internal.config._
import org.apache.spark.network.TransportContext
import org.apache.spark.network.netty.SparkTransportConf
import org.apache.spark.network.shuffle.ExternalShuffleBlockHandler
import org.apache.spark.rdd.RDD
import org.apache.spark.storage._

/**
 * This is an integration test for the cache recovery feature using a local spark cluster. It
 * extends the unit tests in CacheRecoveryManagerSuite which mocks a lot of cluster infrastructure.
 */
class CacheRecoveryIntegrationSuite extends SparkFunSuite
    with Matchers
    with BeforeAndAfterEach
    with BeforeAndAfterAll
    with Eventually {

  private var conf: SparkConf = makeBaseConf()
  private val transportConf = SparkTransportConf.fromSparkConf(conf, "shuffle", numUsableCores = 4)
  private val rpcHandler = new ExternalShuffleBlockHandler(transportConf, null)
  private val transportContext = new TransportContext(transportConf, rpcHandler)
  private val shuffleService = transportContext.createServer()
  private var sc: SparkContext = _

  private def makeBaseConf() = new SparkConf()
    .setAppName("test")
    .setMaster("local-cluster[4, 1, 512]")
    .set("spark.dynamicAllocation.enabled", "true")
    .set("spark.dynamicAllocation.executorIdleTimeout", "1s") // always
    .set("spark.dynamicAllocation.cachedExecutorIdleTimeout", "1s")
    .set(EXECUTOR_MEMORY.key, "512m")
    .set(SHUFFLE_SERVICE_ENABLED.key, "true")
    .set(DYN_ALLOCATION_CACHE_RECOVERY.key, "true")
    .set(DYN_ALLOCATION_CACHE_RECOVERY_TIMEOUT.key, "500s")
    .set(EXECUTOR_INSTANCES.key, "1")
    .set(DYN_ALLOCATION_INITIAL_EXECUTORS.key, "4")
    .set(DYN_ALLOCATION_MIN_EXECUTORS.key, "3")

  override def beforeEach(): Unit = {
    conf = makeBaseConf()
    conf.set("spark.shuffle.service.port", shuffleService.getPort.toString)
  }

  override def afterEach(): Unit = {
    sc.stop()
    conf = null
  }

  override def afterAll(): Unit = {
    shuffleService.close()
  }

  private def getLocations(
      sc: SparkContext,
      rdd: RDD[_]): Map[BlockId, Map[BlockManagerId, BlockStatus]] = {
    import scala.collection.breakOut
    val blockIds: Array[BlockId] = rdd.partitions.map(p => RDDBlockId(rdd.id, p.index))
    blockIds.map { id =>
      id -> Try(sc.env.blockManager.master.getBlockStatus(id)).getOrElse(Map.empty)
    }(breakOut)
  }

  test("cached data is replicated before dynamic de-allocation") {
    sc = new SparkContext(conf)
    TestUtils.waitUntilExecutorsUp(sc, 4, 60000)

    val rdd = sc.parallelize(1 to 1000, 4).map(_ * 4).cache()
    rdd.reduce(_ + _) shouldBe 2002000
    sc.getExecutorIds().size shouldBe 4
    getLocations(sc, rdd).forall { case (_, map) => map.nonEmpty } shouldBe true

    eventually(timeout(Span(5, Seconds)), interval(Span(1, Seconds))) {
      sc.getExecutorIds().size shouldBe 3
      getLocations(sc, rdd).forall { case (_, map) => map.nonEmpty } shouldBe true
    }
  }

  test("dont fail if a bunch of executors are shut down at once") {
    conf.set("spark.dynamicAllocation.minExecutors", "1")
    sc = new SparkContext(conf)
    TestUtils.waitUntilExecutorsUp(sc, 2, 60000)

    val rdd = sc.parallelize(1 to 1000, 4).map(_ * 4).cache()
    rdd.reduce(_ + _) shouldBe 2002000
    sc.getExecutorIds().size shouldBe 4
    getLocations(sc, rdd).forall { case (_, map) => map.nonEmpty } shouldBe true

    eventually(timeout(Span(5, Seconds)), interval(Span(1, Seconds))) {
      sc.getExecutorIds().size shouldBe 1
      getLocations(sc, rdd).forall { case (_, map) => map.nonEmpty } shouldBe true
    }
  }

  test("executors should not accept new work while replicating away data before deallocation") {
    conf.set("spark.dynamicAllocation.minExecutors", "1")

    sc = new SparkContext(conf)
    TestUtils.waitUntilExecutorsUp(sc, 4, 60000)

    val rdd = sc.parallelize(1 to 100000, 4).map(_ * 4L).cache() // cache on all 4 executors
    rdd.reduce(_ + _) shouldBe 20000200000L // realize the cache

    // wait until executors are de-allocated
    eventually(timeout(Span(4, Seconds)), interval(Span(1, Seconds))) {
      sc.getExecutorIds().size shouldBe 1
    }

    val rdd2 = sc.parallelize(1 to 100000, 4).map(_ * 4L).cache() // should be created on 1 exe
    rdd2.reduce(_ + _) shouldBe 20000200000L

    val executorIds = for {
      maps <- getLocations(sc, rdd2).values
      blockManagerId <- maps.keys
    } yield blockManagerId.executorId

    executorIds.toSet.size shouldBe 1
  }

  test("throw error if cache recovery is enabled and cachedExecutor timeout is not set") {
    conf.remove("spark.dynamicAllocation.cachedExecutorIdleTimeout")
    a [SparkException] should be thrownBy new SparkContext(conf)
  }
}
