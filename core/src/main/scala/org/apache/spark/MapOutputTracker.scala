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

import java.io._
import java.util.zip.{GZIPInputStream, GZIPOutputStream}

import scala.collection.mutable.HashSet
import scala.concurrent.Await
import scala.concurrent.duration._

import akka.actor._
import akka.pattern.ask

import org.apache.spark.scheduler.MapStatus
import org.apache.spark.storage.BlockManagerId
import org.apache.spark.util.{AkkaUtils, MetadataCleaner, MetadataCleanerType, TimeStampedHashMap, Utils}

private[spark] sealed trait MapOutputTrackerMessage
private[spark] case class GetMapOutputStatuses(shuffleId: Int)
  extends MapOutputTrackerMessage
private[spark] case object StopMapOutputTracker extends MapOutputTrackerMessage

private[spark] class MapOutputTrackerMasterActor(tracker: MapOutputTrackerMaster)
  extends Actor with Logging {
  def receive = {
    case GetMapOutputStatuses(shuffleId: Int) =>
      val hostPort = sender.path.address.hostPort
      logInfo("Asked to send map output locations for shuffle " + shuffleId + " to " + hostPort)
      sender ! tracker.getSerializedMapOutputStatuses(shuffleId)

    case StopMapOutputTracker =>
      logInfo("MapOutputTrackerActor stopped!")
      sender ! true
      context.stop(self)
  }
}

private[spark] class MapOutputTracker(conf: SparkConf) extends Logging {

  private val timeout = AkkaUtils.askTimeout(conf)

  // Set to the MapOutputTrackerActor living on the driver
  var trackerActor: ActorRef = _

  protected val mapStatuses = new TimeStampedHashMap[Int, Array[MapStatus]]

  // Incremented every time a fetch fails so that client nodes know to clear
  // their cache of map output locations if this happens.
  protected var epoch: Long = 0
  protected val epochLock = new java.lang.Object

  private val metadataCleaner =
    new MetadataCleaner(MetadataCleanerType.MAP_OUTPUT_TRACKER, this.cleanup, conf)

  // Send a message to the trackerActor and get its result within a default timeout, or
  // throw a SparkException if this fails.
  private def askTracker(message: Any): Any = {
    try {
      val future = trackerActor.ask(message)(timeout)
      Await.result(future, timeout)
    } catch {
      case e: Exception =>
        throw new SparkException("Error communicating with MapOutputTracker", e)
    }
  }

  // Send a one-way message to the trackerActor, to which we expect it to reply with true.
  private def communicate(message: Any) {
    if (askTracker(message) != true) {
      throw new SparkException("Error reply received from MapOutputTracker")
    }
  }

  // Remembers which map output locations are currently being fetched on a worker
  private val fetching = new HashSet[Int]

  // Called on possibly remote nodes to get the server URIs and output sizes for a given shuffle
  def getServerStatuses(shuffleId: Int, reduceId: Int): Array[(BlockManagerId, Long)] = {
    val statuses = mapStatuses.get(shuffleId).orNull
    if (statuses == null) {
      logInfo("Don't have map outputs for shuffle " + shuffleId + ", fetching them")
      var fetchedStatuses: Array[MapStatus] = null
      fetching.synchronized {
        if (fetching.contains(shuffleId)) {
          // Someone else is fetching it; wait for them to be done
          while (fetching.contains(shuffleId)) {
            try {
              fetching.wait()
            } catch {
              case e: InterruptedException =>
            }
          }
        }

        // Either while we waited the fetch happened successfully, or
        // someone fetched it in between the get and the fetching.synchronized.
        fetchedStatuses = mapStatuses.get(shuffleId).orNull
        if (fetchedStatuses == null) {
          // We have to do the fetch, get others to wait for us.
          fetching += shuffleId
        }
      }

      if (fetchedStatuses == null) {
        // We won the race to fetch the output locs; do so
        logInfo("Doing the fetch; tracker actor = " + trackerActor)
        // This try-finally prevents hangs due to timeouts:
        try {
          val fetchedBytes =
            askTracker(GetMapOutputStatuses(shuffleId)).asInstanceOf[Array[Byte]]
          fetchedStatuses = MapOutputTracker.deserializeMapStatuses(fetchedBytes)
          logInfo("Got the output locations")
          mapStatuses.put(shuffleId, fetchedStatuses)
        } finally {
          fetching.synchronized {
            fetching -= shuffleId
            fetching.notifyAll()
          }
        }
      }
      if (fetchedStatuses != null) {
        fetchedStatuses.synchronized {
          return MapOutputTracker.convertMapStatuses(shuffleId, reduceId, fetchedStatuses)
        }
      }
      else {
        throw new FetchFailedException(null, shuffleId, -1, reduceId,
          new Exception("Missing all output locations for shuffle " + shuffleId))
      }
    } else {
      statuses.synchronized {
        return MapOutputTracker.convertMapStatuses(shuffleId, reduceId, statuses)
      }
    }
  }

  protected def cleanup(cleanupTime: Long) {
    mapStatuses.clearOldValues(cleanupTime)
  }

  def stop() {
    communicate(StopMapOutputTracker)
    mapStatuses.clear()
    metadataCleaner.cancel()
    trackerActor = null
  }

  // Called to get current epoch number
  def getEpoch: Long = {
    epochLock.synchronized {
      return epoch
    }
  }

  // Called on workers to update the epoch number, potentially clearing old outputs
  // because of a fetch failure. (Each worker task calls this with the latest epoch
  // number on the master at the time it was created.)
  def updateEpoch(newEpoch: Long) {
    epochLock.synchronized {
      if (newEpoch > epoch) {
        logInfo("Updating epoch to " + newEpoch + " and clearing cache")
        epoch = newEpoch
        mapStatuses.clear()
      }
    }
  }
}

private[spark] class MapOutputTrackerMaster(conf: SparkConf)
  extends MapOutputTracker(conf) {

  // Cache a serialized version of the output statuses for each shuffle to send them out faster
  private var cacheEpoch = epoch
  private val cachedSerializedStatuses = new TimeStampedHashMap[Int, Array[Byte]]

  def registerShuffle(shuffleId: Int, numMaps: Int) {
    if (mapStatuses.putIfAbsent(shuffleId, new Array[MapStatus](numMaps)).isDefined) {
      throw new IllegalArgumentException("Shuffle ID " + shuffleId + " registered twice")
    }
  }

  def registerMapOutput(shuffleId: Int, mapId: Int, status: MapStatus) {
    val array = mapStatuses(shuffleId)
    array.synchronized {
      array(mapId) = status
    }
  }

  def registerMapOutputs(shuffleId: Int, statuses: Array[MapStatus], changeEpoch: Boolean = false) {
    mapStatuses.put(shuffleId, Array[MapStatus]() ++ statuses)
    if (changeEpoch) {
      incrementEpoch()
    }
  }

  def unregisterMapOutput(shuffleId: Int, mapId: Int, bmAddress: BlockManagerId) {
    val arrayOpt = mapStatuses.get(shuffleId)
    if (arrayOpt.isDefined && arrayOpt.get != null) {
      val array = arrayOpt.get
      array.synchronized {
        if (array(mapId) != null && array(mapId).location == bmAddress) {
          array(mapId) = null
        }
      }
      incrementEpoch()
    } else {
      throw new SparkException("unregisterMapOutput called for nonexistent shuffle ID")
    }
  }

  def incrementEpoch() {
    epochLock.synchronized {
      epoch += 1
      logDebug("Increasing epoch to " + epoch)
    }
  }

  def getSerializedMapOutputStatuses(shuffleId: Int): Array[Byte] = {
    var statuses: Array[MapStatus] = null
    var epochGotten: Long = -1
    epochLock.synchronized {
      if (epoch > cacheEpoch) {
        cachedSerializedStatuses.clear()
        cacheEpoch = epoch
      }
      cachedSerializedStatuses.get(shuffleId) match {
        case Some(bytes) =>
          return bytes
        case None =>
          statuses = mapStatuses.getOrElse(shuffleId, Array[MapStatus]())
          epochGotten = epoch
      }
    }
    // If we got here, we failed to find the serialized locations in the cache, so we pulled
    // out a snapshot of the locations as "statuses"; let's serialize and return that
    val bytes = MapOutputTracker.serializeMapStatuses(statuses)
    logInfo("Size of output statuses for shuffle %d is %d bytes".format(shuffleId, bytes.length))
    // Add them into the table only if the epoch hasn't changed while we were working
    epochLock.synchronized {
      if (epoch == epochGotten) {
        cachedSerializedStatuses(shuffleId) = bytes
      }
    }
    bytes
  }

  protected override def cleanup(cleanupTime: Long) {
    super.cleanup(cleanupTime)
    cachedSerializedStatuses.clearOldValues(cleanupTime)
  }

  override def stop() {
    super.stop()
    cachedSerializedStatuses.clear()
  }

  override def updateEpoch(newEpoch: Long) {
    // This might be called on the MapOutputTrackerMaster if we're running in local mode.
  }

  def has(shuffleId: Int): Boolean = {
    cachedSerializedStatuses.get(shuffleId).isDefined || mapStatuses.contains(shuffleId)
  }
}

private[spark] object MapOutputTracker {
  private val LOG_BASE = 1.1

  // Serialize an array of map output locations into an efficient byte format so that we can send
  // it to reduce tasks. We do this by compressing the serialized bytes using GZIP. They will
  // generally be pretty compressible because many map outputs will be on the same hostname.
  def serializeMapStatuses(statuses: Array[MapStatus]): Array[Byte] = {
    val out = new ByteArrayOutputStream
    val objOut = new ObjectOutputStream(new GZIPOutputStream(out))
    // Since statuses can be modified in parallel, sync on it
    statuses.synchronized {
      objOut.writeObject(statuses)
    }
    objOut.close()
    out.toByteArray
  }

  // Opposite of serializeMapStatuses.
  def deserializeMapStatuses(bytes: Array[Byte]): Array[MapStatus] = {
    val objIn = new ObjectInputStream(new GZIPInputStream(new ByteArrayInputStream(bytes)))
    objIn.readObject().asInstanceOf[Array[MapStatus]]
  }

  // Convert an array of MapStatuses to locations and sizes for a given reduce ID. If
  // any of the statuses is null (indicating a missing location due to a failed mapper),
  // throw a FetchFailedException.
  private def convertMapStatuses(
        shuffleId: Int,
        reduceId: Int,
        statuses: Array[MapStatus]): Array[(BlockManagerId, Long)] = {
    assert (statuses != null)
    statuses.map {
      status =>
        if (status == null) {
          throw new FetchFailedException(null, shuffleId, -1, reduceId,
            new Exception("Missing an output location for shuffle " + shuffleId))
        } else {
          (status.location, decompressSize(status.compressedSizes(reduceId)))
        }
    }
  }

  /**
   * Compress a size in bytes to 8 bits for efficient reporting of map output sizes.
   * We do this by encoding the log base 1.1 of the size as an integer, which can support
   * sizes up to 35 GB with at most 10% error.
   */
  def compressSize(size: Long): Byte = {
    if (size == 0) {
      0
    } else if (size <= 1L) {
      1
    } else {
      math.min(255, math.ceil(math.log(size) / math.log(LOG_BASE)).toInt).toByte
    }
  }

  /**
   * Decompress an 8-bit encoded block size, using the reverse operation of compressSize.
   */
  def decompressSize(compressedSize: Byte): Long = {
    if (compressedSize == 0) {
      0
    } else {
      math.pow(LOG_BASE, (compressedSize & 0xFF)).toLong
    }
  }
}
