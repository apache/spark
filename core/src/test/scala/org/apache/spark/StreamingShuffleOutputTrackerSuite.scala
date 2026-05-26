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
import scala.util.control.NonFatal

import org.scalatest.BeforeAndAfter
import org.scalatest.matchers.should.Matchers

import org.apache.spark.internal.config.SHUFFLE_MAPOUTPUT_DISPATCHER_NUM_THREADS
import org.apache.spark.rpc.RpcEnv

class StreamingShuffleOutputTrackerSuite
  extends SparkFunSuite
  with LocalSparkContext
  with ThreadAudit
  with BeforeAndAfter
  with Matchers {

  before {
    doThreadPreAudit()
  }

  after {
    trackersToStop.foreach { t =>
      try t.stop() catch { case NonFatal(_) => }
    }
    trackersToStop.clear()
    rpcEnvsToShutdown.foreach { e =>
      try e.shutdown() catch { case NonFatal(_) => }
    }
    rpcEnvsToShutdown.clear()
    doThreadPostAudit()
  }

  protected val conf = new SparkConf

  private val trackersToStop = ArrayBuffer.empty[StreamingShuffleOutputTracker]
  private val rpcEnvsToShutdown = ArrayBuffer.empty[RpcEnv]

  protected def newTrackerMaster(sparkConf: SparkConf = conf) = {
    val tracker = new StreamingShuffleOutputTrackerMaster(sparkConf)
    trackersToStop += tracker
    tracker
  }

  protected def newTrackerWorker(
      sparkConf: SparkConf = conf): StreamingShuffleOutputTrackerWorker = {
    val tracker = new StreamingShuffleOutputTrackerWorker(sparkConf)
    trackersToStop += tracker
    tracker
  }

  def createRpcEnv(
      name: String,
      host: String = "localhost",
      port: Int = 0,
      securityManager: SecurityManager = new SecurityManager(conf)): RpcEnv = {
    val rpcEnv = RpcEnv.create(name, host, port, conf, securityManager)
    rpcEnvsToShutdown += rpcEnv
    rpcEnv
  }

  test("master start and stop") {
    val rpcEnv = createRpcEnv("test")
    val tracker = newTrackerMaster()
    tracker.trackerEndpoint = rpcEnv.setupEndpoint(
      StreamingShuffleOutputTracker.ENDPOINT_NAME,
      new StreamingShuffleOutputTrackerMasterEndpoint(rpcEnv, tracker, conf))
  }

  test("test tracker workflow") {
    val master = newTrackerMaster()
    val worker = newTrackerWorker()

    val rpcEnv = createRpcEnv("test")
    val rpcEndpoint = rpcEnv.setupEndpoint(
      StreamingShuffleOutputTracker.ENDPOINT_NAME,
      new StreamingShuffleOutputTrackerMasterEndpoint(rpcEnv, master, conf))

    master.trackerEndpoint = rpcEndpoint
    worker.trackerEndpoint = rpcEndpoint

    val shuffleId = 0
    val numMaps = 2
    val numReduces = 2
    val jobId = 0
    val taskMap =
      Map(
        0 -> StreamingShuffleTaskLocation("executor-1", "host-1", 0),
        1 -> StreamingShuffleTaskLocation("executor-2", "host-2", 0))
    master.registerShuffle(shuffleId, numMaps, numReduces, jobId)
    // register one shuffle write task
    worker.registerShuffleWriterTask(shuffleId, 0, taskMap(0))
    // Get all shuffle write task location information.  Should return None since
    // only one of the shuffle writers have registered
    worker.getAllShuffleWriterTaskLocations(shuffleId) should be(None)
    // register the next shuffle write task
    worker.registerShuffleWriterTask(shuffleId, 1, taskMap(1))
    // should get all the shuffle task location information now
    worker.getAllShuffleWriterTaskLocations(shuffleId) should be(Some(taskMap))

    // should be a no-op for the worker
    worker.unregisterShuffle(shuffleId)
    master.unregisterShuffle(shuffleId)

    master.getShuffleInfo(shuffleId) should be(None)
  }

  test("register task for shuffle that doesn't exist") {
    val master = newTrackerMaster()
    val worker = newTrackerWorker()

    val rpcEnv = createRpcEnv("test")
    val rpcEndpoint = rpcEnv.setupEndpoint(
      StreamingShuffleOutputTracker.ENDPOINT_NAME,
      new StreamingShuffleOutputTrackerMasterEndpoint(rpcEnv, master, conf))

    master.trackerEndpoint = rpcEndpoint
    worker.trackerEndpoint = rpcEndpoint

    worker.registerShuffleWriterTask(
      0,
      0,
      StreamingShuffleTaskLocation("executor-1", "host-1", 0)) should be(false)
  }

  test("get task location for shuffle that doesn't exist") {
    val master = newTrackerMaster()
    val worker = newTrackerWorker()

    val rpcEnv = createRpcEnv("test")
    val rpcEndpoint = rpcEnv.setupEndpoint(
      StreamingShuffleOutputTracker.ENDPOINT_NAME,
      new StreamingShuffleOutputTrackerMasterEndpoint(rpcEnv, master, conf))

    master.trackerEndpoint = rpcEndpoint
    worker.trackerEndpoint = rpcEndpoint

    worker.getAllShuffleWriterTaskLocations(0) should be(None)
  }

  test("StreamingShuffleOutputTrackerMaster - register shuffle") {
    val conf = new SparkConf(false)
    val tracker = newTrackerMaster(conf)

    // Register a shuffle with 3 mappers and 2 reducers
    tracker.registerShuffle(shuffleId = 0, numMaps = 3, numReduces = 2, jobId = 1)

    // Verify shuffle was registered
    val shuffleInfo = tracker.getShuffleInfo(0)
    assert(shuffleInfo.isDefined)
    assert(shuffleInfo.get.numMaps === 3)
    assert(shuffleInfo.get.numReduces === 2)
    assert(shuffleInfo.get.jobId === 1)
  }

  test("StreamingShuffleOutputTrackerMaster - registering same shuffle twice fails") {
    val conf = new SparkConf(false)
    val tracker = newTrackerMaster(conf)

    tracker.registerShuffle(shuffleId = 0, numMaps = 2, numReduces = 2, jobId = 1)

    intercept[IllegalArgumentException] {
      tracker.registerShuffle(shuffleId = 0, numMaps = 2, numReduces = 2, jobId = 1)
    }
  }

  test("StreamingShuffleOutputTrackerMaster - register and get writer task locations") {
    val conf = new SparkConf(false)
    val tracker = newTrackerMaster(conf)

    // Register a shuffle
    tracker.registerShuffle(shuffleId = 0, numMaps = 2, numReduces = 2, jobId = 1)

    // Register first writer task
    val location1 = StreamingShuffleTaskLocation("executor1", "host1", 8000)
    val success1 = tracker.registerShuffleWriterTask(shuffleId = 0, mapId = 0, location1)
    assert(success1)

    // getAllShuffleWriterTaskLocations should return None until all writers registered
    assert(tracker.getAllShuffleWriterTaskLocations(0).isEmpty)

    // Register second writer task
    val location2 = StreamingShuffleTaskLocation("executor2", "host2", 8001)
    val success2 = tracker.registerShuffleWriterTask(shuffleId = 0, mapId = 1, location2)
    assert(success2)

    // Now getAllShuffleWriterTaskLocations should return all locations
    val allLocations = tracker.getAllShuffleWriterTaskLocations(0)
    assert(allLocations.isDefined)
    assert(allLocations.get.size === 2)
    assert(allLocations.get(0L) === location1)
    assert(allLocations.get(1L) === location2)
  }

  test("StreamingShuffleOutputTrackerMaster - get available writer task locations") {
    val conf = new SparkConf(false)
    val tracker = newTrackerMaster(conf)

    // Register a shuffle
    tracker.registerShuffle(shuffleId = 0, numMaps = 3, numReduces = 2, jobId = 1)

    // Initially should return empty map with total count
    val response1 = tracker.getAvailableShuffleWriterTaskLocations(0)
    assert(response1.isDefined)
    assert(response1.get.shuffleTaskLocations.isEmpty)
    assert(response1.get.numShuffleWriterTasks === 3)

    // Register one writer task
    val location1 = StreamingShuffleTaskLocation("executor1", "host1", 8000)
    tracker.registerShuffleWriterTask(shuffleId = 0, mapId = 0, location1)

    // Should return partial locations
    val response2 = tracker.getAvailableShuffleWriterTaskLocations(0)
    assert(response2.isDefined)
    assert(response2.get.shuffleTaskLocations.size === 1)
    assert(response2.get.numShuffleWriterTasks === 3)
    assert(response2.get.shuffleTaskLocations(0L) === location1)

    // Register remaining writer tasks
    val location2 = StreamingShuffleTaskLocation("executor2", "host2", 8001)
    val location3 = StreamingShuffleTaskLocation("executor3", "host3", 8002)
    tracker.registerShuffleWriterTask(shuffleId = 0, mapId = 1, location2)
    tracker.registerShuffleWriterTask(shuffleId = 0, mapId = 2, location3)

    // Should return all locations
    val response3 = tracker.getAvailableShuffleWriterTaskLocations(0)
    assert(response3.isDefined)
    assert(response3.get.shuffleTaskLocations.size === 3)
    assert(response3.get.numShuffleWriterTasks === 3)
  }

  test("StreamingShuffleOutputTrackerMaster - unregister shuffle") {
    val conf = new SparkConf(false)
    val tracker = newTrackerMaster(conf)

    // Register a shuffle and a writer task
    tracker.registerShuffle(shuffleId = 0, numMaps = 1, numReduces = 1, jobId = 1)
    val location = StreamingShuffleTaskLocation("executor1", "host1", 8000)
    tracker.registerShuffleWriterTask(shuffleId = 0, mapId = 0, location)

    // Verify it exists
    assert(tracker.getShuffleInfo(0).isDefined)
    assert(tracker.getAllShuffleWriterTaskLocations(0).isDefined)

    // Unregister shuffle
    tracker.unregisterShuffle(0)

    // Verify it's removed
    assert(tracker.getShuffleInfo(0).isEmpty)
    assert(tracker.getAllShuffleWriterTaskLocations(0).isEmpty)
  }

  test("StreamingShuffleOutputTrackerMaster - register writer before shuffle fails") {
    val conf = new SparkConf(false)
    val tracker = newTrackerMaster(conf)

    // Try to register writer task without registering shuffle first
    val location = StreamingShuffleTaskLocation("executor1", "host1", 8000)
    val success = tracker.registerShuffleWriterTask(shuffleId = 0, mapId = 0, location)

    // Should fail
    assert(!success)
  }

  test("StreamingShuffleOutputTrackerMaster - concurrent registration") {
    val conf = new SparkConf(false)
      .set(SHUFFLE_MAPOUTPUT_DISPATCHER_NUM_THREADS, 4)
    val tracker = newTrackerMaster(conf)

    // Register a shuffle with many mappers
    val numMaps = 100
    tracker.registerShuffle(shuffleId = 0, numMaps = numMaps, numReduces = 10, jobId = 1)

    // Register all writer tasks concurrently
    val threads = (0 until numMaps).map { mapId =>
      new Thread {
        override def run(): Unit = {
          val location = StreamingShuffleTaskLocation(
            s"executor$mapId",
            s"host$mapId",
            8000 + mapId)
          tracker.registerShuffleWriterTask(shuffleId = 0, mapId = mapId, location)
        }
      }
    }

    threads.foreach(_.start())
    threads.foreach(_.join())

    // Verify all were registered
    val allLocations = tracker.getAllShuffleWriterTaskLocations(0)
    assert(allLocations.isDefined)
    assert(allLocations.get.size === numMaps)
  }

  test("StreamingShuffleOutputTrackerMaster - multiple shuffles") {
    val conf = new SparkConf(false)
    val tracker = newTrackerMaster(conf)

    // Register multiple shuffles
    tracker.registerShuffle(shuffleId = 0, numMaps = 2, numReduces = 2, jobId = 1)
    tracker.registerShuffle(shuffleId = 1, numMaps = 3, numReduces = 3, jobId = 1)

    // Register writer tasks for both shuffles
    val location00 = StreamingShuffleTaskLocation("executor1", "host1", 8000)
    val location01 = StreamingShuffleTaskLocation("executor2", "host2", 8001)
    tracker.registerShuffleWriterTask(shuffleId = 0, mapId = 0, location00)
    tracker.registerShuffleWriterTask(shuffleId = 0, mapId = 1, location01)

    val location10 = StreamingShuffleTaskLocation("executor3", "host3", 8002)
    val location11 = StreamingShuffleTaskLocation("executor4", "host4", 8003)
    val location12 = StreamingShuffleTaskLocation("executor5", "host5", 8004)
    tracker.registerShuffleWriterTask(shuffleId = 1, mapId = 0, location10)
    tracker.registerShuffleWriterTask(shuffleId = 1, mapId = 1, location11)
    tracker.registerShuffleWriterTask(shuffleId = 1, mapId = 2, location12)

    // Verify both shuffles have correct locations
    val shuffle0Locations = tracker.getAllShuffleWriterTaskLocations(0)
    assert(shuffle0Locations.isDefined)
    assert(shuffle0Locations.get.size === 2)

    val shuffle1Locations = tracker.getAllShuffleWriterTaskLocations(1)
    assert(shuffle1Locations.isDefined)
    assert(shuffle1Locations.get.size === 3)
  }
}
