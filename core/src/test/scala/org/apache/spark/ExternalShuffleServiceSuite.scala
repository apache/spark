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

package org.apache.spark

import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.Eventually
import org.scalatest.matchers.should.Matchers._
import org.scalatest.time.SpanSugar._

import org.apache.spark.internal.config
import org.apache.spark.network.TransportContext
import org.apache.spark.network.netty.SparkTransportConf
import org.apache.spark.network.server.TransportServer
import org.apache.spark.network.shuffle.{ExternalBlockHandler, ExternalBlockStoreClient}
import org.apache.spark.storage.{RDDBlockId, StorageLevel}
import org.apache.spark.util.Utils

/**
 * This suite creates an external shuffle server and routes all shuffle fetches through it.
 * Note that failures in this suite may arise due to changes in Spark that invalidate expectations
 * set up in `ExternalBlockHandler`, such as changing the format of shuffle files or how
 * we hash files into folders.
 */
class ExternalShuffleServiceSuite extends ShuffleSuite with BeforeAndAfterAll with Eventually {
  var server: TransportServer = _
  var transportContext: TransportContext = _
  var rpcHandler: ExternalBlockHandler = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    val transportConf = SparkTransportConf.fromSparkConf(conf, "shuffle", numUsableCores = 2)
    rpcHandler = new ExternalBlockHandler(transportConf, null)
    transportContext = new TransportContext(transportConf, rpcHandler)
    server = transportContext.createServer()

    conf.set(config.SHUFFLE_MANAGER, "sort")
    conf.set(config.SHUFFLE_SERVICE_ENABLED, true)
    conf.set(config.SHUFFLE_SERVICE_PORT, server.getPort)
  }

  override def afterAll(): Unit = {
    Utils.tryLogNonFatalError{
      server.close()
    }
    Utils.tryLogNonFatalError{
      rpcHandler.close()
    }
    Utils.tryLogNonFatalError{
      transportContext.close()
    }
    super.afterAll()
  }

  // This test ensures that the external shuffle service is actually in use for the other tests.
  test("using external shuffle service") {
    sc = new SparkContext("local-cluster[2,1,1024]", "test", conf)
    sc.getConf.get(config.SHUFFLE_HOST_LOCAL_DISK_READING_ENABLED) should equal(false)
    sc.env.blockManager.externalShuffleServiceEnabled should equal(true)
    sc.env.blockManager.blockStoreClient.getClass should equal(classOf[ExternalBlockStoreClient])

    // In a slow machine, one executor may register hundreds of milliseconds ahead of the other one.
    // If we don't wait for all executors, it's possible that only one executor runs all jobs. Then
    // all shuffle blocks will be in this executor, ShuffleBlockFetcherIterator will directly fetch
    // local blocks from the local BlockManager and won't send requests to ExternalShuffleService.
    // In this case, we won't receive FetchFailed. And it will make this test fail.
    // Therefore, we should wait until all executors are up
    TestUtils.waitUntilExecutorsUp(sc, 2, 60000)

    val rdd = sc.parallelize(0 until 1000, 10)
      .map { i => (i, 1) }
      .reduceByKey(_ + _)

    rdd.count()
    rdd.count()

    // Invalidate the registered executors, disallowing access to their shuffle blocks (without
    // deleting the actual shuffle files, so we could access them without the shuffle service).
    rpcHandler.applicationRemoved(sc.conf.getAppId, false /* cleanupLocalDirs */)

    // Now Spark will receive FetchFailed, and not retry the stage due to "spark.test.noStageRetry"
    // being set.
    val e = intercept[SparkException] {
      rdd.count()
    }
    e.getMessage should include ("Fetch failure will not retry stage due to testing config")
  }

  test("SPARK-25888: using external shuffle service fetching disk persisted blocks") {
    val confWithRddFetchEnabled = conf.clone.set(config.SHUFFLE_SERVICE_FETCH_RDD_ENABLED, true)
    sc = new SparkContext("local-cluster[1,1,1024]", "test", confWithRddFetchEnabled)
    sc.env.blockManager.externalShuffleServiceEnabled should equal(true)
    sc.env.blockManager.blockStoreClient.getClass should equal(classOf[ExternalBlockStoreClient])
    try {
      val rdd = sc.parallelize(0 until 100, 2)
        .map { i => (i, 1) }
        .persist(StorageLevel.DISK_ONLY)

      rdd.count()

      val blockId = RDDBlockId(rdd.id, 0)
      eventually(timeout(2.seconds), interval(100.milliseconds)) {
        val locations = sc.env.blockManager.master.getLocations(blockId)
        assert(locations.size === 2)
        assert(locations.map(_.port).contains(server.getPort),
          "external shuffle service port should be contained")
      }

      sc.killExecutors(sc.getExecutorIds())

      eventually(timeout(2.seconds), interval(100.milliseconds)) {
        val locations = sc.env.blockManager.master.getLocations(blockId)
        assert(locations.size === 1)
        assert(locations.map(_.port).contains(server.getPort),
          "external shuffle service port should be contained")
      }

      assert(sc.env.blockManager.getRemoteValues(blockId).isDefined)

      // test unpersist: as executors are killed the blocks will be removed via the shuffle service
      rdd.unpersist(true)
      assert(sc.env.blockManager.getRemoteValues(blockId).isEmpty)
    } finally {
      rpcHandler.applicationRemoved(sc.conf.getAppId, true)
    }
  }
}
