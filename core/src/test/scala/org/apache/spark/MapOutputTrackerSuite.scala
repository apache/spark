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

import org.mockito.Mockito._
import org.mockito.Matchers.{any, isA}
import org.scalatest.FunSuite

import org.apache.spark.rpc.{RpcAddress, RpcEndpointRef, RpcCallContext, RpcEnv}
import org.apache.spark.scheduler.{CompressedMapStatus, MapStatus}
import org.apache.spark.shuffle.FetchFailedException
import org.apache.spark.storage.BlockManagerId

class MapOutputTrackerSuite extends FunSuite {
  private val conf = new SparkConf

  def createRpcEnv(name: String, host: String = "localhost", port: Int = 0,
      securityManager: SecurityManager = new SecurityManager(conf)): RpcEnv = {
    RpcEnv.create(name, host, port, conf, securityManager)
  }

  test("master start and stop") {
    val rpcEnv = createRpcEnv("test")
    val tracker = new MapOutputTrackerMaster(conf)
    tracker.trackerEndpoint = rpcEnv.setupEndpoint(MapOutputTracker.ENDPOINT_NAME,
      new MapOutputTrackerMasterEndpoint(rpcEnv, tracker, conf))
    tracker.stop()
    rpcEnv.shutdown()
  }

  test("master register shuffle and fetch") {
    val rpcEnv = createRpcEnv("test")
    val tracker = new MapOutputTrackerMaster(conf)
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
    val statuses = tracker.getServerStatuses(10, 0)
    assert(statuses.toSeq === Seq((BlockManagerId("a", "hostA", 1000), size1000),
                                  (BlockManagerId("b", "hostB", 1000), size10000)))
    tracker.stop()
    rpcEnv.shutdown()
  }

  test("master register and unregister shuffle") {
    val rpcEnv = createRpcEnv("test")
    val tracker = new MapOutputTrackerMaster(conf)
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
    assert(tracker.getServerStatuses(10, 0).nonEmpty)
    tracker.unregisterShuffle(10)
    assert(!tracker.containsShuffle(10))
    assert(tracker.getServerStatuses(10, 0).isEmpty)

    tracker.stop()
    rpcEnv.shutdown()
  }

  test("master register shuffle and unregister map output and fetch") {
    val rpcEnv = createRpcEnv("test")
    val tracker = new MapOutputTrackerMaster(conf)
    tracker.trackerEndpoint = rpcEnv.setupEndpoint(MapOutputTracker.ENDPOINT_NAME,
      new MapOutputTrackerMasterEndpoint(rpcEnv, tracker, conf))
    tracker.registerShuffle(10, 2)
    val compressedSize1000 = MapStatus.compressSize(1000L)
    val compressedSize10000 = MapStatus.compressSize(10000L)
    tracker.registerMapOutput(10, 0, MapStatus(BlockManagerId("a", "hostA", 1000),
        Array(compressedSize1000, compressedSize1000, compressedSize1000)))
    tracker.registerMapOutput(10, 1, MapStatus(BlockManagerId("b", "hostB", 1000),
        Array(compressedSize10000, compressedSize1000, compressedSize1000)))

    // As if we had two simultaneous fetch failures
    tracker.unregisterMapOutput(10, 0, BlockManagerId("a", "hostA", 1000))
    tracker.unregisterMapOutput(10, 0, BlockManagerId("a", "hostA", 1000))

    // The remaining reduce task might try to grab the output despite the shuffle failure;
    // this should cause it to fail, and the scheduler will ignore the failure due to the
    // stage already being aborted.
    intercept[FetchFailedException] { tracker.getServerStatuses(10, 1) }

    tracker.stop()
    rpcEnv.shutdown()
  }

  test("remote fetch") {
    val hostname = "localhost"
    val rpcEnv = createRpcEnv("spark", hostname, 0, new SecurityManager(conf))

    val masterTracker = new MapOutputTrackerMaster(conf)
    masterTracker.trackerEndpoint = rpcEnv.setupEndpoint(MapOutputTracker.ENDPOINT_NAME,
      new MapOutputTrackerMasterEndpoint(rpcEnv, masterTracker, conf))

    val slaveRpcEnv = createRpcEnv("spark-slave", hostname, 0, new SecurityManager(conf))
    val slaveTracker = new MapOutputTrackerWorker(conf)
    slaveTracker.trackerEndpoint =
      slaveRpcEnv.setupEndpointRef("spark", rpcEnv.address, MapOutputTracker.ENDPOINT_NAME)

    masterTracker.registerShuffle(10, 1)
    masterTracker.incrementEpoch()
    slaveTracker.updateEpoch(masterTracker.getEpoch)
    intercept[FetchFailedException] { slaveTracker.getServerStatuses(10, 0) }

    val size1000 = MapStatus.decompressSize(MapStatus.compressSize(1000L))
    masterTracker.registerMapOutput(10, 0, MapStatus(
      BlockManagerId("a", "hostA", 1000), Array(1000L)))
    masterTracker.incrementEpoch()
    slaveTracker.updateEpoch(masterTracker.getEpoch)
    assert(slaveTracker.getServerStatuses(10, 0).toSeq ===
      Seq((BlockManagerId("a", "hostA", 1000), size1000)))

    masterTracker.unregisterMapOutput(10, 0, BlockManagerId("a", "hostA", 1000))
    masterTracker.incrementEpoch()
    slaveTracker.updateEpoch(masterTracker.getEpoch)
    intercept[FetchFailedException] { slaveTracker.getServerStatuses(10, 0) }

    // failure should be cached
    intercept[FetchFailedException] { slaveTracker.getServerStatuses(10, 0) }

    masterTracker.stop()
    slaveTracker.stop()
    rpcEnv.shutdown()
    slaveRpcEnv.shutdown()
  }

  test("remote fetch below akka frame size") {
    val newConf = new SparkConf
    newConf.set("spark.akka.frameSize", "1")
    newConf.set("spark.akka.askTimeout", "1") // Fail fast

    val masterTracker = new MapOutputTrackerMaster(conf)
    val rpcEnv = createRpcEnv("spark")
    val masterEndpoint = new MapOutputTrackerMasterEndpoint(rpcEnv, masterTracker, newConf)
    rpcEnv.setupEndpoint(MapOutputTracker.ENDPOINT_NAME, masterEndpoint)

    // Frame size should be ~123B, and no exception should be thrown
    masterTracker.registerShuffle(10, 1)
    masterTracker.registerMapOutput(10, 0, MapStatus(
      BlockManagerId("88", "mph", 1000), Array.fill[Long](10)(0)))
    val sender = mock(classOf[RpcEndpointRef])
    when(sender.address).thenReturn(RpcAddress("localhost", 12345))
    val rpcCallContext = mock(classOf[RpcCallContext])
    when(rpcCallContext.sender).thenReturn(sender)
    masterEndpoint.receiveAndReply(rpcCallContext)(GetMapOutputStatuses(10))
    verify(rpcCallContext).reply(any())
    verify(rpcCallContext, never()).sendFailure(any())

//    masterTracker.stop() // this throws an exception
    rpcEnv.shutdown()
  }

  test("remote fetch exceeds akka frame size") {
    val newConf = new SparkConf
    newConf.set("spark.akka.frameSize", "1")
    newConf.set("spark.akka.askTimeout", "1") // Fail fast

    val masterTracker = new MapOutputTrackerMaster(conf)
    val rpcEnv = createRpcEnv("test")
    val masterEndpoint = new MapOutputTrackerMasterEndpoint(rpcEnv, masterTracker, newConf)
    rpcEnv.setupEndpoint(MapOutputTracker.ENDPOINT_NAME, masterEndpoint)

    // Frame size should be ~1.1MB, and MapOutputTrackerMasterEndpoint should throw exception.
    // Note that the size is hand-selected here because map output statuses are compressed before
    // being sent.
    masterTracker.registerShuffle(20, 100)
    (0 until 100).foreach { i =>
      masterTracker.registerMapOutput(20, i, new CompressedMapStatus(
        BlockManagerId("999", "mps", 1000), Array.fill[Long](4000000)(0)))
    }
    val sender = mock(classOf[RpcEndpointRef])
    when(sender.address).thenReturn(RpcAddress("localhost", 12345))
    val rpcCallContext = mock(classOf[RpcCallContext])
    when(rpcCallContext.sender).thenReturn(sender)
    masterEndpoint.receiveAndReply(rpcCallContext)(GetMapOutputStatuses(20))
    verify(rpcCallContext, never()).reply(any())
    verify(rpcCallContext).sendFailure(isA(classOf[SparkException]))

//    masterTracker.stop() // this throws an exception
    rpcEnv.shutdown()
  }
}
