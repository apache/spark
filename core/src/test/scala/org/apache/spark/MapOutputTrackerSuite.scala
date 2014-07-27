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

import scala.concurrent.Await

import akka.actor._
import akka.testkit.TestActorRef
import org.scalatest.FunSuite

import org.apache.spark.scheduler.MapStatus
import org.apache.spark.shuffle.FetchFailedException
import org.apache.spark.storage.BlockManagerId
import org.apache.spark.util.AkkaUtils

class MapOutputTrackerSuite extends FunSuite with LocalSparkContext {
  private val conf = new SparkConf
  test("compressSize") {
    assert(MapOutputTracker.compressSize(0L) === 0)
    assert(MapOutputTracker.compressSize(1L) === 1)
    assert(MapOutputTracker.compressSize(2L) === 8)
    assert(MapOutputTracker.compressSize(10L) === 25)
    assert((MapOutputTracker.compressSize(1000000L) & 0xFF) === 145)
    assert((MapOutputTracker.compressSize(1000000000L) & 0xFF) === 218)
    // This last size is bigger than we can encode in a byte, so check that we just return 255
    assert((MapOutputTracker.compressSize(1000000000000000000L) & 0xFF) === 255)
  }

  test("decompressSize") {
    assert(MapOutputTracker.decompressSize(0) === 0)
    for (size <- Seq(2L, 10L, 100L, 50000L, 1000000L, 1000000000L)) {
      val size2 = MapOutputTracker.decompressSize(MapOutputTracker.compressSize(size))
      assert(size2 >= 0.99 * size && size2 <= 1.11 * size,
        "size " + size + " decompressed to " + size2 + ", which is out of range")
    }
  }

  test("master start and stop") {
    val actorSystem = ActorSystem("test")
    val tracker = new MapOutputTrackerMaster(conf)
    tracker.trackerActor =
      actorSystem.actorOf(Props(new MapOutputTrackerMasterActor(tracker, conf)))
    tracker.stop()
  }

  test("master register shuffle and fetch") {
    val actorSystem = ActorSystem("test")
    val tracker = new MapOutputTrackerMaster(conf)
    tracker.trackerActor =
      actorSystem.actorOf(Props(new MapOutputTrackerMasterActor(tracker, conf)))
    tracker.registerShuffle(10, 2)
    assert(tracker.containsShuffle(10))
    val compressedSize1000 = MapOutputTracker.compressSize(1000L)
    val compressedSize10000 = MapOutputTracker.compressSize(10000L)
    val size1000 = MapOutputTracker.decompressSize(compressedSize1000)
    val size10000 = MapOutputTracker.decompressSize(compressedSize10000)
    tracker.registerMapOutput(10, 0, new MapStatus(BlockManagerId("a", "hostA", 1000, 0),
        Array(compressedSize1000, compressedSize10000)))
    tracker.registerMapOutput(10, 1, new MapStatus(BlockManagerId("b", "hostB", 1000, 0),
        Array(compressedSize10000, compressedSize1000)))
    val statuses = tracker.getServerStatuses(10, 0)
    assert(statuses.toSeq === Seq((BlockManagerId("a", "hostA", 1000, 0), size1000),
                                  (BlockManagerId("b", "hostB", 1000, 0), size10000)))
    tracker.stop()
  }

  test("master register and unregister shuffle") {
    val actorSystem = ActorSystem("test")
    val tracker = new MapOutputTrackerMaster(conf)
    tracker.trackerActor = actorSystem.actorOf(Props(new MapOutputTrackerMasterActor(tracker, conf)))
    tracker.registerShuffle(10, 2)
    val compressedSize1000 = MapOutputTracker.compressSize(1000L)
    val compressedSize10000 = MapOutputTracker.compressSize(10000L)
    tracker.registerMapOutput(10, 0, new MapStatus(BlockManagerId("a", "hostA", 1000, 0),
      Array(compressedSize1000, compressedSize10000)))
    tracker.registerMapOutput(10, 1, new MapStatus(BlockManagerId("b", "hostB", 1000, 0),
      Array(compressedSize10000, compressedSize1000)))
    assert(tracker.containsShuffle(10))
    assert(tracker.getServerStatuses(10, 0).nonEmpty)
    tracker.unregisterShuffle(10)
    assert(!tracker.containsShuffle(10))
    assert(tracker.getServerStatuses(10, 0).isEmpty)
  }

  test("master register shuffle and unregister map output and fetch") {
    val actorSystem = ActorSystem("test")
    val tracker = new MapOutputTrackerMaster(conf)
    tracker.trackerActor =
      actorSystem.actorOf(Props(new MapOutputTrackerMasterActor(tracker, conf)))
    tracker.registerShuffle(10, 2)
    val compressedSize1000 = MapOutputTracker.compressSize(1000L)
    val compressedSize10000 = MapOutputTracker.compressSize(10000L)
    tracker.registerMapOutput(10, 0, new MapStatus(BlockManagerId("a", "hostA", 1000, 0),
        Array(compressedSize1000, compressedSize1000, compressedSize1000)))
    tracker.registerMapOutput(10, 1, new MapStatus(BlockManagerId("b", "hostB", 1000, 0),
        Array(compressedSize10000, compressedSize1000, compressedSize1000)))

    // As if we had two simultaneous fetch failures
    tracker.unregisterMapOutput(10, 0, BlockManagerId("a", "hostA", 1000, 0))
    tracker.unregisterMapOutput(10, 0, BlockManagerId("a", "hostA", 1000, 0))

    // The remaining reduce task might try to grab the output despite the shuffle failure;
    // this should cause it to fail, and the scheduler will ignore the failure due to the
    // stage already being aborted.
    intercept[FetchFailedException] { tracker.getServerStatuses(10, 1) }
  }

  test("remote fetch") {
    val hostname = "localhost"
    val (actorSystem, boundPort) = AkkaUtils.createActorSystem("spark", hostname, 0, conf = conf,
      securityManager = new SecurityManager(conf))

    val masterTracker = new MapOutputTrackerMaster(conf)
    masterTracker.trackerActor = actorSystem.actorOf(
      Props(new MapOutputTrackerMasterActor(masterTracker, conf)), "MapOutputTracker")

    val (slaveSystem, _) = AkkaUtils.createActorSystem("spark-slave", hostname, 0, conf = conf,
      securityManager = new SecurityManager(conf))
    val slaveTracker = new MapOutputTrackerWorker(conf)
    val selection = slaveSystem.actorSelection(
      s"akka.tcp://spark@localhost:$boundPort/user/MapOutputTracker")
    val timeout = AkkaUtils.lookupTimeout(conf)
    slaveTracker.trackerActor = Await.result(selection.resolveOne(timeout), timeout)

    masterTracker.registerShuffle(10, 1)
    masterTracker.incrementEpoch()
    slaveTracker.updateEpoch(masterTracker.getEpoch)
    intercept[FetchFailedException] { slaveTracker.getServerStatuses(10, 0) }

    val compressedSize1000 = MapOutputTracker.compressSize(1000L)
    val size1000 = MapOutputTracker.decompressSize(compressedSize1000)
    masterTracker.registerMapOutput(10, 0, new MapStatus(
      BlockManagerId("a", "hostA", 1000, 0), Array(compressedSize1000)))
    masterTracker.incrementEpoch()
    slaveTracker.updateEpoch(masterTracker.getEpoch)
    assert(slaveTracker.getServerStatuses(10, 0).toSeq ===
      Seq((BlockManagerId("a", "hostA", 1000, 0), size1000)))

    masterTracker.unregisterMapOutput(10, 0, BlockManagerId("a", "hostA", 1000, 0))
    masterTracker.incrementEpoch()
    slaveTracker.updateEpoch(masterTracker.getEpoch)
    intercept[FetchFailedException] { slaveTracker.getServerStatuses(10, 0) }

    // failure should be cached
    intercept[FetchFailedException] { slaveTracker.getServerStatuses(10, 0) }
  }

  test("remote fetch below akka frame size") {
    val newConf = new SparkConf
    newConf.set("spark.akka.frameSize", "1")
    newConf.set("spark.akka.askTimeout", "1") // Fail fast

    val masterTracker = new MapOutputTrackerMaster(conf)
    val actorSystem = ActorSystem("test")
    val actorRef = TestActorRef[MapOutputTrackerMasterActor](
      Props(new MapOutputTrackerMasterActor(masterTracker, newConf)))(actorSystem)
    val masterActor = actorRef.underlyingActor

    // Frame size should be ~123B, and no exception should be thrown
    masterTracker.registerShuffle(10, 1)
    masterTracker.registerMapOutput(10, 0, new MapStatus(
      BlockManagerId("88", "mph", 1000, 0), Array.fill[Byte](10)(0)))
    masterActor.receive(GetMapOutputStatuses(10))
  }

  test("remote fetch exceeds akka frame size") {
    val newConf = new SparkConf
    newConf.set("spark.akka.frameSize", "1")
    newConf.set("spark.akka.askTimeout", "1") // Fail fast

    val masterTracker = new MapOutputTrackerMaster(conf)
    val actorSystem = ActorSystem("test")
    val actorRef = TestActorRef[MapOutputTrackerMasterActor](
      Props(new MapOutputTrackerMasterActor(masterTracker, newConf)))(actorSystem)
    val masterActor = actorRef.underlyingActor

    // Frame size should be ~1.1MB, and MapOutputTrackerMasterActor should throw exception.
    // Note that the size is hand-selected here because map output statuses are compressed before
    // being sent.
    masterTracker.registerShuffle(20, 100)
    (0 until 100).foreach { i =>
      masterTracker.registerMapOutput(20, i, new MapStatus(
        BlockManagerId("999", "mps", 1000, 0), Array.fill[Byte](4000000)(0)))
    }
    intercept[SparkException] { masterActor.receive(GetMapOutputStatuses(20)) }
  }
}
