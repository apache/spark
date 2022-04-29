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

package org.apache.spark.shuffle

import org.scalatest.matchers.must.Matchers
import org.scalatest.matchers.should.Matchers._

import org.apache.spark._
import org.apache.spark.internal.config._
import org.apache.spark.network.TransportContext
import org.apache.spark.network.netty.{NettyBlockTransferService, SparkTransportConf}
import org.apache.spark.network.server.TransportServer
import org.apache.spark.network.shuffle.{ExternalBlockHandler, ExternalBlockStoreClient}
import org.apache.spark.util.Utils

/**
 * This's an end to end test suite used to test the host local shuffle reading.
 */
class HostLocalShuffleReadingSuite extends SparkFunSuite with Matchers with LocalSparkContext {
  var rpcHandler: ExternalBlockHandler = _
  var server: TransportServer = _
  var transportContext: TransportContext = _

  override def afterEach(): Unit = {
    Option(rpcHandler).foreach { handler =>
      Utils.tryLogNonFatalError{
        server.close()
      }
      Utils.tryLogNonFatalError{
        handler.close()
      }
      Utils.tryLogNonFatalError{
        transportContext.close()
      }
      server = null
      rpcHandler = null
      transportContext = null
    }
    super.afterEach()
  }

  Seq(true, false).foreach { isESSEnabled => /* ESS: external shuffle service */
    val conf = new SparkConf()
      .set(SHUFFLE_HOST_LOCAL_DISK_READING_ENABLED, true)

    import scala.language.existentials
    val (essStatus, blockStoreClientClass) = if (isESSEnabled) {
      // LocalSparkCluster will disable the ExternalShuffleService by default. Therefore,
      // we have to manually setup an server which embedded with ExternalBlockHandler to
      // mimic a ExternalShuffleService. Then, executors on the Worker can successfully
      // find a ExternalShuffleService to connect.
      val transportConf = SparkTransportConf.fromSparkConf(conf, "shuffle", numUsableCores = 2)
      rpcHandler = new ExternalBlockHandler(transportConf, null)
      transportContext = new TransportContext(transportConf, rpcHandler)
      server = transportContext.createServer()
      conf.set(SHUFFLE_SERVICE_PORT, server.getPort)

      ("enabled (SPARK-27651)", classOf[ExternalBlockStoreClient])
    } else {
      ("disabled (SPARK-32077)", classOf[NettyBlockTransferService])
    }

    test(s"host local shuffle reading with external shuffle service $essStatus") {
      conf.set(SHUFFLE_SERVICE_ENABLED, isESSEnabled)
        .set(STORAGE_LOCAL_DISK_BY_EXECUTORS_CACHE_SIZE, 5)
      sc = new SparkContext("local-cluster[2,1,1024]", "test-host-local-shuffle-reading", conf)
      // In a slow machine, one executor may register hundreds of milliseconds ahead of the other
      // one. If we don't wait for all executors, it's possible that only one executor runs all
      // jobs. Then all shuffle blocks will be in this executor, ShuffleBlockFetcherIterator will
      // directly fetch local blocks from the local BlockManager and won't send requests to
      // BlockStoreClient. In this case, we won't receive FetchFailed. And it will make this
      // test fail. Therefore, we should wait until all executors are up
      TestUtils.waitUntilExecutorsUp(sc, 2, 60000)

      sc.getConf.get(SHUFFLE_HOST_LOCAL_DISK_READING_ENABLED) should equal(true)
      sc.env.blockManager.externalShuffleServiceEnabled should equal(isESSEnabled)
      sc.env.blockManager.hostLocalDirManager.isDefined should equal(true)
      sc.env.blockManager.blockStoreClient.getClass should equal(blockStoreClientClass)

      val rdd = sc.parallelize(0 until 1000, 10)
        .map { i =>
          SparkEnv.get.blockManager.hostLocalDirManager.map { localDirManager =>
            // No shuffle fetch yet. So the cache must be empty
            assert(localDirManager.getCachedHostLocalDirs.isEmpty)
          }
         (i, 1)
        }.reduceByKey(_ + _)

      // raise a job and trigger the shuffle fetching during the job
      assert(rdd.count() === 1000)

      val cachedExecutors = rdd.mapPartitions { _ =>
        SparkEnv.get.blockManager.hostLocalDirManager.map { localDirManager =>
          localDirManager.getCachedHostLocalDirs.keySet.iterator
        }.getOrElse(Iterator.empty)
      }.collect().toSet

      // both executors are caching the dirs of the other one
      cachedExecutors should equal(sc.getExecutorIds().toSet)

      Option(rpcHandler).foreach { handler =>
        // Invalidate the registered executors, disallowing access to their shuffle blocks (without
        // deleting the actual shuffle files, so we could access them without the shuffle service).
        // As directories are already cached there is no request to external shuffle service.
        handler.applicationRemoved(sc.conf.getAppId, false /* cleanupLocalDirs */)
      }

      val (local, remote) = rdd.map { case (_, _) =>
        val shuffleReadMetrics = TaskContext.get().taskMetrics().shuffleReadMetrics
        ((shuffleReadMetrics.localBytesRead, shuffleReadMetrics.localBlocksFetched),
        (shuffleReadMetrics.remoteBytesRead, shuffleReadMetrics.remoteBlocksFetched))
      }.collect().unzip
      // Spark should read the shuffle data locally from the cached directories on the same host,
      // so there's no remote fetching at all.
      val (localBytesRead, localBlocksFetched) = local.unzip
      val (remoteBytesRead, remoteBlocksFetched) = remote.unzip
      assert(localBytesRead.sum > 0 && localBlocksFetched.sum > 0)
      assert(remoteBytesRead.sum === 0 && remoteBlocksFetched.sum === 0)
    }
  }

  test("Enable host local shuffle reading when push based shuffle is enabled") {
    val conf = new SparkConf()
      .set(SHUFFLE_SERVICE_ENABLED, true)
      .set("spark.yarn.maxAttempts", "1")
      .set(PUSH_BASED_SHUFFLE_ENABLED, true)
      .set(SERIALIZER, "org.apache.spark.serializer.KryoSerializer")
    sc = new SparkContext("local-cluster[2, 1, 1024]", "test-host-local-shuffle-reading", conf)
    sc.env.blockManager.hostLocalDirManager.isDefined should equal(true)
  }
}
