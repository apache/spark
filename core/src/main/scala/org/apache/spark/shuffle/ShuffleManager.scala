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

import scala.concurrent.Await

import akka.actor.{Props, ActorSystem}

import org.apache.spark._
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.storage.BlockManagerId
import org.apache.spark.util.{AkkaUtils, Utils}


/**
 * Pluggable interface for shuffle systems. A ShuffleManager is created in SparkEnv on both the
 * driver and executors, based on the spark.shuffle.manager setting. The driver registers shuffles
 * with it, and executors (or tasks running locally in the driver) can ask to read and write data.
 */
private[spark] trait ShuffleManager {

  protected var mapOutputTracker: MapOutputTracker = _

  /**
   * initialize the mapOutputTracker
   *
   * Basic implementation here, the users can implement customized MapOutputTracker and override
   * this method for customized approach to keep track the map output
   */
  def initMapOutputTracker(conf: SparkConf, isDriver: Boolean, actorSystem: ActorSystem) {
    if (isDriver) {
      mapOutputTracker = new MapOutputTrackerMaster(conf)
      mapOutputTracker.trackerActor = actorSystem.actorOf(
        Props(new MapOutputTrackerMasterActor(mapOutputTracker.asInstanceOf[MapOutputTrackerMaster],
          conf)), "MapOutputTracker")
    } else {
      mapOutputTracker = new MapOutputTrackerWorker(conf)
      val driverHost: String = conf.get("spark.driver.host", "localhost")
      val driverPort: Int = conf.getInt("spark.driver.port", 7077)
      Utils.checkHost(driverHost, "Expected hostname")
      val url = s"akka.tcp://spark@$driverHost:$driverPort/user/MapOutputTracker"
      val timeout = AkkaUtils.lookupTimeout(conf)
      Await.result(actorSystem.actorSelection(url).resolveOne(timeout), timeout)
    }
  }

  /**
   * Register a shuffle with the manager and obtain a handle for it to pass to tasks.
   */
  def registerShuffle[K, V, C](
      shuffleId: Int,
      numMaps: Int,
      dependency: ShuffleDependency[K, V, C]): ShuffleHandle = {
    if (mapOutputTracker.isInstanceOf[MapOutputTrackerMaster]) {
      mapOutputTracker.asInstanceOf[MapOutputTrackerMaster].registerShuffle(shuffleId, numMaps)
    }
    new BaseShuffleHandle(shuffleId, numMaps, dependency)
  }

  /** Remove a shuffle's metadata from the ShuffleManager. */
  def unregisterShuffle(shuffleId: Int) {
    mapOutputTracker.unregisterShuffle(shuffleId)
  }

  def registerMapOutput(shuffleId: Int, mapId: Int, status: MapStatus) {
    if (mapOutputTracker.isInstanceOf[MapOutputTrackerMaster]) {
      mapOutputTracker.asInstanceOf[MapOutputTrackerMaster].registerMapOutput(shuffleId, mapId,
        status)
    }
  }

  /** Register multiple map output information for the given shuffle */
  def registerMapOutputs(shuffleId: Int, statuses: Array[MapStatus], changeEpoch: Boolean = false) {
    if (mapOutputTracker.isInstanceOf[MapOutputTrackerMaster]) {
      mapOutputTracker.asInstanceOf[MapOutputTrackerMaster].registerMapOutputs(
        shuffleId, statuses, changeEpoch)
    }
  }

  def unregisterMapOutput(shuffleId: Int, mapId: Int, bmAddress: BlockManagerId) {
    if (mapOutputTracker.isInstanceOf[MapOutputTrackerMaster]) {
      mapOutputTracker.asInstanceOf[MapOutputTrackerMaster].unregisterMapOutput(shuffleId, mapId,
        bmAddress)
    }
  }

  def containsShuffle(shuffleId: Int): Boolean =  {
    if (mapOutputTracker.isInstanceOf[MapOutputTrackerMaster]) {
      mapOutputTracker.asInstanceOf[MapOutputTrackerMaster].containsShuffle(shuffleId)
    } else {
      false
    }
  }

  // TODO: MapStatus should be customizable
  def getShuffleMetadata(shuffleId: Int): Array[MapStatus] = {
    if (mapOutputTracker.isInstanceOf[MapOutputTrackerMaster]) {
      val serLocs = mapOutputTracker.asInstanceOf[MapOutputTrackerMaster].
        getSerializedMapOutputStatuses(shuffleId)
      MapOutputTracker.deserializeMapStatuses(serLocs)
    } else {
      null
    }
  }


  /** Get a writer for a given partition. Called on executors by map tasks. */
  def getWriter[K, V](handle: ShuffleHandle, mapId: Int, context: TaskContext): ShuffleWriter[K, V]

  /**
   * Get a reader for a range of reduce partitions (startPartition to endPartition-1, inclusive).
   * Called on executors by reduce tasks.
   */
  def getReader[K, C](
      handle: ShuffleHandle,
      startPartition: Int,
      endPartition: Int,
      context: TaskContext): ShuffleReader[K, C]

  def getEpoch = mapOutputTracker.getEpoch

  // TODO: disassociate Epoch with ShuffleManager?
  def incrementEpoch() {
    if (mapOutputTracker.isInstanceOf[MapOutputTrackerMaster]) {
      mapOutputTracker.asInstanceOf[MapOutputTrackerMaster].incrementEpoch()
    }
  }

  def updateEpoch(newEpoch: Long) {
    mapOutputTracker.updateEpoch(newEpoch)
  }

  def getServerStatuses(shuffleId: Int, reduceId: Int) = {
    mapOutputTracker.getServerStatuses(shuffleId, reduceId)
  }

  /** Shut down this ShuffleManager. */
  def stop() {
    mapOutputTracker.stop()
  }
}
