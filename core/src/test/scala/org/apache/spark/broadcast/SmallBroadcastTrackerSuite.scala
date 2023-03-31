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

package org.apache.spark.broadcast

import org.mockito.Mockito.{spy, times, verify}

import org.apache.spark.LocalSparkContext
import org.apache.spark.SecurityManager
import org.apache.spark.SparkConf
import org.apache.spark.SparkFunSuite
import org.apache.spark.internal.config
import org.apache.spark.rpc.RpcEnv
import org.apache.spark.util.Utils

class SmallBroadcastTrackerMasterTester(conf: SparkConf)
  extends SmallBroadcastTrackerMaster(conf) {

  var getSerializedSmallBroadcastCnt = 0
  override  def getSerialized(id: Long): Option[Array[Byte]] = {
    getSerializedSmallBroadcastCnt += 1
    super.getSerialized(id)
  }
}

class SmallBroadcastTrackerSuite extends SparkFunSuite with LocalSparkContext {

  private val cache_expire_seconds = 5

  private val conf = new SparkConf()
    .set(config.SMALL_BROADCAST_EXECUTOR_CACHE_EXPIRE_SECONDS, cache_expire_seconds)

  private val hostname = "localhost"

  def createRpcEnv(name: String, host: String = "localhost", port: Int = 0,
    securityManager: SecurityManager = new SecurityManager(conf)): RpcEnv = {
    RpcEnv.create(name, host, port, conf, securityManager)
  }

  def withTrackerMaster(f: (RpcEnv, SmallBroadcastTrackerMaster) => Unit): Unit = {
    val masterRpcEnv = createRpcEnv("spark-master", hostname, 0, new SecurityManager(conf))
    val masterTracker = spy(new SmallBroadcastTrackerMaster(conf))
    masterRpcEnv.setupEndpoint(SmallBroadcastTracker.ENDPOINT_NAME,
      new SmallBroadcastTrackerMasterEndpoint(masterRpcEnv, masterTracker))
    Utils.tryWithSafeFinally {
      f(masterRpcEnv, masterTracker)
    } {
      masterTracker.stop()
      masterRpcEnv.shutdown()
    }
  }

  def withTrackerWorker(masterRpcEnv: RpcEnv)
    (f: (RpcEnv, SmallBroadcastTrackerWorker) => Unit): Unit = {
    val workerRpcEnv = createRpcEnv("spark-worker", hostname, 0, new SecurityManager(conf))
    val workerTracker = new SmallBroadcastTrackerWorker(conf)
    workerTracker.trackerEndpoint = workerRpcEnv.setupEndpointRef(
      masterRpcEnv.address, SmallBroadcastTracker.ENDPOINT_NAME)
    Utils.tryWithSafeFinally {
      f(workerRpcEnv, workerTracker)
    } {
      workerTracker.stop()
      workerRpcEnv.shutdown()
    }
  }

  test("master register and unregister") {
    withTrackerMaster((_, tracker) => {
      val bid = 0
      val obj = List(1, 2, 3)
      tracker.register(bid, obj)
      assert(tracker.get(bid).isDefined)
      tracker.unregister(bid)
      assert(tracker.cache.isEmpty)
    })
  }

  test("remote fetch and cache") {
    withTrackerMaster((masterEnv, masterTracker) => {
      val bid = 0
      val obj = List(1, 2, 3)
      masterTracker.register(bid, obj)
      withTrackerWorker(masterEnv)((_, workerTracker) => {
        assert(workerTracker.get(bid).isDefined)
        verify(masterTracker, times(1)).getSerialized(bid)
        // cached in local, should not fetch from remote
        assert(workerTracker.get(bid).isDefined)
        verify(masterTracker, times(1)).getSerialized(bid)
        // wait for cache expire
        Thread.sleep(cache_expire_seconds * 1000)
        workerTracker.cache.cleanUp() // force cleanup expired entries
        assert(workerTracker.cache.size() == 0)
      })
    })
  }
}
