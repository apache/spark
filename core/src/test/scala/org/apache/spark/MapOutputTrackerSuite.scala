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

import scala.collection.mutable.ArrayBuffer

import org.mockito.Matchers.any
import org.mockito.Mockito._

import org.apache.spark.broadcast.BroadcastManager
import org.apache.spark.rpc.{RpcAddress, RpcCallContext, RpcEnv}
import org.apache.spark.scheduler.{CompressedMapStatus, MapStatus}
import org.apache.spark.shuffle.FetchFailedException
import org.apache.spark.storage.{BlockManagerId, ShuffleBlockId}

class MapOutputTrackerSuite extends SparkFunSuite {
  private val conf = new SparkConf

  private def newTrackerMaster(sparkConf: SparkConf = conf) = {
    val broadcastManager = new BroadcastManager(true, sparkConf,
      new SecurityManager(sparkConf))
    new MapOutputTrackerMaster(sparkConf, broadcastManager, true)
  }

  def createRpcEnv(name: String, host: String = "localhost", port: Int = 0,
      securityManager: SecurityManager = new SecurityManager(conf)): RpcEnv = {
    RpcEnv.create(name, host, port, conf, securityManager)
  }

  test("master start and stop") {
    val rpcEnv = createRpcEnv("test")
    val tracker = newTrackerMaster()
    tracker.trackerEndpoint = rpcEnv.setupEndpoint(MapOutputTracker.ENDPOINT_NAME,
      new MapOutputTrackerMasterEndpoint(rpcEnv, tracker, conf))
    tracker.stop()
    rpcEnv.shutdown()
  }

  test("master register shuffle and fetch") {
    val rpcEnv = createRpcEnv("test")
    val tracker = newTrackerMaster()
    tracker.trackerEndpoint = rpcEnv.setupEndpoint(MapOutputTracker.ENDPOINT_NAME,
      new MapOutputTrackerMasterEndpoint(rpcEnv, tracker, conf))
    tracker.registerShuffle(10, 2)
    assert(tracker.containsShuffle(10))
    val size1000 = MapStatus.decompressSize(MapStatus.compressSize(1000L))
    val size10000 = MapStatus.decompressSize(MapStatus.compressSize(10000L))
    tracker.registerMapOutput(10, 0, MapStatus(BlockManagerId("a", "hostA", 1000),
        Array(1000L, 10000L)))
    tracker.registerMapOutput(10, 1, MapStatus(BlockManagerId("b", "hostB", 1000),
        Array(10000L, 1000L)))
    val statuses = tracker.getMapSizesByExecutorId(10, 0)
    assert(statuses.toSet ===
      Seq((BlockManagerId("a", "hostA", 1000), ArrayBuffer((ShuffleBlockId(10, 0, 0), size1000))),
          (BlockManagerId("b", "hostB", 1000), ArrayBuffer((ShuffleBlockId(10, 1, 0), size10000))))
        .toSet)
    assert(0 == tracker.getNumCachedSerializedBroadcast)
    tracker.stop()
    rpcEnv.shutdown()
  }

  test("master register and unregister shuffle") {
    val rpcEnv = createRpcEnv("test")
    val tracker = newTrackerMaster()
    tracker.trackerEndpoint = rpcEnv.setupEndpoint(MapOutputTracker.ENDPOINT_NAME,
      new MapOutputTrackerMasterEndpoint(rpcEnv, tracker, conf))
    tracker.registerShuffle(10, 2)
    val compressedSize1000 = MapStatus.compressSize(1000L)
    val compressedSize10000 = MapStatus.compressSize(10000L)
    tracker.registerMapOutput(10, 0, MapStatus(BlockManagerId("a", "hostA", 1000),
      Array(compressedSize1000, compressedSize10000)))
    tracker.registerMapOutput(10, 1, MapStatus(BlockManagerId("b", "hostB", 1000),
      Array(compressedSize10000, compressedSize1000)))
    assert(tracker.containsShuffle(10))
    assert(tracker.getMapSizesByExecutorId(10, 0).nonEmpty)
    assert(0 == tracker.getNumCachedSerializedBroadcast)
    tracker.unregisterShuffle(10)
    assert(!tracker.containsShuffle(10))
    assert(tracker.getMapSizesByExecutorId(10, 0).isEmpty)

    tracker.stop()
    rpcEnv.shutdown()
  }

  test("master register shuffle and unregister map output and fetch") {
    val rpcEnv = createRpcEnv("test")
    val tracker = newTrackerMaster()
    tracker.trackerEndpoint = rpcEnv.setupEndpoint(MapOutputTracker.ENDPOINT_NAME,
      new MapOutputTrackerMasterEndpoint(rpcEnv, tracker, conf))
    tracker.registerShuffle(10, 2)
    val compressedSize1000 = MapStatus.compressSize(1000L)
    val compressedSize10000 = MapStatus.compressSize(10000L)
    tracker.registerMapOutput(10, 0, MapStatus(BlockManagerId("a", "hostA", 1000),
        Array(compressedSize1000, compressedSize1000, compressedSize1000)))
    tracker.registerMapOutput(10, 1, MapStatus(BlockManagerId("b", "hostB", 1000),
        Array(compressedSize10000, compressedSize1000, compressedSize1000)))

    assert(0 == tracker.getNumCachedSerializedBroadcast)
    // As if we had two simultaneous fetch failures
    tracker.unregisterMapOutput(10, 0, BlockManagerId("a", "hostA", 1000))
    tracker.unregisterMapOutput(10, 0, BlockManagerId("a", "hostA", 1000))

    // The remaining reduce task might try to grab the output despite the shuffle failure;
    // this should cause it to fail, and the scheduler will ignore the failure due to the
    // stage already being aborted.
    intercept[FetchFailedException] { tracker.getMapSizesByExecutorId(10, 1) }

    tracker.stop()
    rpcEnv.shutdown()
  }

  test("remote fetch") {
    val hostname = "localhost"
    val rpcEnv = createRpcEnv("spark", hostname, 0, new SecurityManager(conf))

    val masterTracker = newTrackerMaster()
    masterTracker.trackerEndpoint = rpcEnv.setupEndpoint(MapOutputTracker.ENDPOINT_NAME,
      new MapOutputTrackerMasterEndpoint(rpcEnv, masterTracker, conf))

    val slaveRpcEnv = createRpcEnv("spark-slave", hostname, 0, new SecurityManager(conf))
    val slaveTracker = new MapOutputTrackerWorker(conf)
    slaveTracker.trackerEndpoint =
      slaveRpcEnv.setupEndpointRef(rpcEnv.address, MapOutputTracker.ENDPOINT_NAME)

    masterTracker.registerShuffle(10, 1)
    masterTracker.incrementEpoch()
    slaveTracker.updateEpoch(masterTracker.getEpoch)
    intercept[FetchFailedException] { slaveTracker.getMapSizesByExecutorId(10, 0) }

    val size1000 = MapStatus.decompressSize(MapStatus.compressSize(1000L))
    masterTracker.registerMapOutput(10, 0, MapStatus(
      BlockManagerId("a", "hostA", 1000), Array(1000L)))
    masterTracker.incrementEpoch()
    slaveTracker.updateEpoch(masterTracker.getEpoch)
    assert(slaveTracker.getMapSizesByExecutorId(10, 0) ===
      Seq((BlockManagerId("a", "hostA", 1000), ArrayBuffer((ShuffleBlockId(10, 0, 0), size1000)))))
    assert(0 == masterTracker.getNumCachedSerializedBroadcast)

    masterTracker.unregisterMapOutput(10, 0, BlockManagerId("a", "hostA", 1000))
    masterTracker.incrementEpoch()
    slaveTracker.updateEpoch(masterTracker.getEpoch)
    intercept[FetchFailedException] { slaveTracker.getMapSizesByExecutorId(10, 0) }

    // failure should be cached
    intercept[FetchFailedException] { slaveTracker.getMapSizesByExecutorId(10, 0) }
    assert(0 == masterTracker.getNumCachedSerializedBroadcast)

    masterTracker.stop()
    slaveTracker.stop()
    rpcEnv.shutdown()
    slaveRpcEnv.shutdown()
  }

  test("remote fetch below max RPC message size") {
    val newConf = new SparkConf
    newConf.set("spark.rpc.message.maxSize", "1")
    newConf.set("spark.rpc.askTimeout", "1") // Fail fast
    newConf.set("spark.shuffle.mapOutput.minSizeForBroadcast", "1048576")

    val masterTracker = newTrackerMaster(newConf)
    val rpcEnv = createRpcEnv("spark")
    val masterEndpoint = new MapOutputTrackerMasterEndpoint(rpcEnv, masterTracker, newConf)
    rpcEnv.setupEndpoint(MapOutputTracker.ENDPOINT_NAME, masterEndpoint)

    // Message size should be ~123B, and no exception should be thrown
    masterTracker.registerShuffle(10, 1)
    masterTracker.registerMapOutput(10, 0, MapStatus(
      BlockManagerId("88", "mph", 1000), Array.fill[Long](10)(0)))
    val senderAddress = RpcAddress("localhost", 12345)
    val rpcCallContext = mock(classOf[RpcCallContext])
    when(rpcCallContext.senderAddress).thenReturn(senderAddress)
    masterEndpoint.receiveAndReply(rpcCallContext)(GetMapOutputStatuses(10))
    // Default size for broadcast in this testsuite is set to -1 so should not cause broadcast
    // to be used.
    verify(rpcCallContext, timeout(30000)).reply(any())
    assert(0 == masterTracker.getNumCachedSerializedBroadcast)

//    masterTracker.stop() // this throws an exception
    rpcEnv.shutdown()
  }

  test("min broadcast size exceeds max RPC message size") {
    val newConf = new SparkConf
    newConf.set("spark.rpc.message.maxSize", "1")
    newConf.set("spark.rpc.askTimeout", "1") // Fail fast
    newConf.set("spark.shuffle.mapOutput.minSizeForBroadcast", Int.MaxValue.toString)

    intercept[IllegalArgumentException] { newTrackerMaster(newConf) }
  }

  test("getLocationsWithLargestOutputs with multiple outputs in same machine") {
    val rpcEnv = createRpcEnv("test")
    val tracker = newTrackerMaster()
    tracker.trackerEndpoint = rpcEnv.setupEndpoint(MapOutputTracker.ENDPOINT_NAME,
      new MapOutputTrackerMasterEndpoint(rpcEnv, tracker, conf))
    // Setup 3 map tasks
    // on hostA with output size 2
    // on hostA with output size 2
    // on hostB with output size 3
    tracker.registerShuffle(10, 3)
    tracker.registerMapOutput(10, 0, MapStatus(BlockManagerId("a", "hostA", 1000),
        Array(2L)))
    tracker.registerMapOutput(10, 1, MapStatus(BlockManagerId("a", "hostA", 1000),
        Array(2L)))
    tracker.registerMapOutput(10, 2, MapStatus(BlockManagerId("b", "hostB", 1000),
        Array(3L)))

    // When the threshold is 50%, only host A should be returned as a preferred location
    // as it has 4 out of 7 bytes of output.
    val topLocs50 = tracker.getLocationsWithLargestOutputs(10, 0, 1, 0.5)
    assert(topLocs50.nonEmpty)
    assert(topLocs50.get.size === 1)
    assert(topLocs50.get.head === BlockManagerId("a", "hostA", 1000))

    // When the threshold is 20%, both hosts should be returned as preferred locations.
    val topLocs20 = tracker.getLocationsWithLargestOutputs(10, 0, 1, 0.2)
    assert(topLocs20.nonEmpty)
    assert(topLocs20.get.size === 2)
    assert(topLocs20.get.toSet ===
           Seq(BlockManagerId("a", "hostA", 1000), BlockManagerId("b", "hostB", 1000)).toSet)

    tracker.stop()
    rpcEnv.shutdown()
  }

  test("remote fetch using broadcast") {
    val newConf = new SparkConf
    newConf.set("spark.rpc.message.maxSize", "1")
    newConf.set("spark.rpc.askTimeout", "1") // Fail fast
    newConf.set("spark.shuffle.mapOutput.minSizeForBroadcast", "10240") // 10 KB << 1MB framesize

    // needs TorrentBroadcast so need a SparkContext
    val sc = new SparkContext("local", "MapOutputTrackerSuite", newConf)
    try {
      val masterTracker = sc.env.mapOutputTracker.asInstanceOf[MapOutputTrackerMaster]
      val rpcEnv = sc.env.rpcEnv
      val masterEndpoint = new MapOutputTrackerMasterEndpoint(rpcEnv, masterTracker, newConf)
      rpcEnv.stop(masterTracker.trackerEndpoint)
      rpcEnv.setupEndpoint(MapOutputTracker.ENDPOINT_NAME, masterEndpoint)

      // Frame size should be ~1.1MB, and MapOutputTrackerMasterEndpoint should throw exception.
      // Note that the size is hand-selected here because map output statuses are compressed before
      // being sent.
      masterTracker.registerShuffle(20, 100)
      (0 until 100).foreach { i =>
        masterTracker.registerMapOutput(20, i, new CompressedMapStatus(
          BlockManagerId("999", "mps", 1000), Array.fill[Long](4000000)(0)))
      }
      val senderAddress = RpcAddress("localhost", 12345)
      val rpcCallContext = mock(classOf[RpcCallContext])
      when(rpcCallContext.senderAddress).thenReturn(senderAddress)
      masterEndpoint.receiveAndReply(rpcCallContext)(GetMapOutputStatuses(20))
      // should succeed since majority of data is broadcast and actual serialized
      // message size is small
      verify(rpcCallContext, timeout(30000)).reply(any())
      assert(1 == masterTracker.getNumCachedSerializedBroadcast)
      masterTracker.unregisterShuffle(20)
      assert(0 == masterTracker.getNumCachedSerializedBroadcast)

    } finally {
      LocalSparkContext.stop(sc)
    }
  }

}
