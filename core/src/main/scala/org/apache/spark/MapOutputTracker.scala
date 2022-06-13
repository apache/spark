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

import java.io.{ByteArrayInputStream, InputStream, IOException, ObjectInputStream, ObjectOutputStream}
import java.nio.ByteBuffer
import java.util.concurrent.{ConcurrentHashMap, LinkedBlockingQueue, ThreadPoolExecutor, TimeUnit}
import java.util.concurrent.locks.ReentrantReadWriteLock

import scala.collection.JavaConverters._
import scala.collection.mutable.{HashMap, ListBuffer, Map}
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.Duration
import scala.reflect.ClassTag
import scala.util.control.NonFatal

import org.apache.commons.io.output.{ByteArrayOutputStream => ApacheByteArrayOutputStream}
import org.roaringbitmap.RoaringBitmap

import org.apache.spark.broadcast.{Broadcast, BroadcastManager}
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config._
import org.apache.spark.io.CompressionCodec
import org.apache.spark.rpc.{RpcCallContext, RpcEndpoint, RpcEndpointRef, RpcEnv}
import org.apache.spark.scheduler.{MapStatus, MergeStatus, ShuffleOutputStatus}
import org.apache.spark.shuffle.MetadataFetchFailedException
import org.apache.spark.storage.{BlockId, BlockManagerId, ShuffleBlockId, ShuffleMergedBlockId}
import org.apache.spark.util._
import org.apache.spark.util.io.{ChunkedByteBuffer, ChunkedByteBufferOutputStream}

/**
 * Helper class used by the [[MapOutputTrackerMaster]] to perform bookkeeping for a single
 * ShuffleMapStage.
 *
 * This class maintains a mapping from map index to `MapStatus`. It also maintains a cache of
 * serialized map statuses in order to speed up tasks' requests for map output statuses.
 *
 * All public methods of this class are thread-safe.
 */
private class ShuffleStatus(
    numPartitions: Int,
    numReducers: Int = -1) extends Logging {

  private val (readLock, writeLock) = {
    val lock = new ReentrantReadWriteLock()
    (lock.readLock(), lock.writeLock())
  }

  // All accesses to the following state must be guarded with `withReadLock` or `withWriteLock`.
  private def withReadLock[B](fn: => B): B = {
    readLock.lock()
    try {
      fn
    } finally {
      readLock.unlock()
    }
  }

  private def withWriteLock[B](fn: => B): B = {
    writeLock.lock()
    try {
      fn
    } finally {
      writeLock.unlock()
    }
  }

  /**
   * MapStatus for each partition. The index of the array is the map partition id.
   * Each value in the array is the MapStatus for a partition, or null if the partition
   * is not available. Even though in theory a task may run multiple times (due to speculation,
   * stage retries, etc.), in practice the likelihood of a map output being available at multiple
   * locations is so small that we choose to ignore that case and store only a single location
   * for each output.
   */
  // Exposed for testing
  val mapStatuses = new Array[MapStatus](numPartitions)

  /**
   * Keep the previous deleted MapStatus for recovery.
   */
  val mapStatusesDeleted = new Array[MapStatus](numPartitions)

  /**
   * MergeStatus for each shuffle partition when push-based shuffle is enabled. The index of the
   * array is the shuffle partition id (reduce id). Each value in the array is the MergeStatus for
   * a shuffle partition, or null if not available. When push-based shuffle is enabled, this array
   * provides a reducer oriented view of the shuffle status specifically for the results of
   * merging shuffle partition blocks into per-partition merged shuffle files.
   */
  val mergeStatuses = if (numReducers > 0) {
    new Array[MergeStatus](numReducers)
  } else {
    Array.empty[MergeStatus]
  }

  /**
   * The cached result of serializing the map statuses array. This cache is lazily populated when
   * [[serializedMapStatus]] is called. The cache is invalidated when map outputs are removed.
   */
  private[this] var cachedSerializedMapStatus: Array[Byte] = _

  /**
   * Broadcast variable holding serialized map output statuses array. When [[serializedMapStatus]]
   * serializes the map statuses array it may detect that the result is too large to send in a
   * single RPC, in which case it places the serialized array into a broadcast variable and then
   * sends a serialized broadcast variable instead. This variable holds a reference to that
   * broadcast variable in order to keep it from being garbage collected and to allow for it to be
   * explicitly destroyed later on when the ShuffleMapStage is garbage-collected.
   */
  private[spark] var cachedSerializedBroadcast: Broadcast[Array[Array[Byte]]] = _

  /**
   * Similar to cachedSerializedMapStatus and cachedSerializedBroadcast, but for MergeStatus.
   */
  private[this] var cachedSerializedMergeStatus: Array[Byte] = _

  private[this] var cachedSerializedBroadcastMergeStatus: Broadcast[Array[Array[Byte]]] = _

  /**
   * Counter tracking the number of partitions that have output. This is a performance optimization
   * to avoid having to count the number of non-null entries in the `mapStatuses` array and should
   * be equivalent to`mapStatuses.count(_ ne null)`.
   */
  private[this] var _numAvailableMapOutputs: Int = 0

  /**
   * Counter tracking the number of MergeStatus results received so far from the shuffle services.
   */
  private[this] var _numAvailableMergeResults: Int = 0

  private[this] var shufflePushMergerLocations: Seq[BlockManagerId] = Seq.empty

  /**
   * Register a map output. If there is already a registered location for the map output then it
   * will be replaced by the new location.
   */
  def addMapOutput(mapIndex: Int, status: MapStatus): Unit = withWriteLock {
    if (mapStatuses(mapIndex) == null) {
      _numAvailableMapOutputs += 1
      invalidateSerializedMapOutputStatusCache()
    }
    mapStatuses(mapIndex) = status
  }

  /**
   * Update the map output location (e.g. during migration).
   */
  def updateMapOutput(mapId: Long, bmAddress: BlockManagerId): Unit = withWriteLock {
    try {
      val mapStatusOpt = mapStatuses.find(x => x != null && x.mapId == mapId)
      mapStatusOpt match {
        case Some(mapStatus) =>
          logInfo(s"Updating map output for ${mapId} to ${bmAddress}")
          mapStatus.updateLocation(bmAddress)
          invalidateSerializedMapOutputStatusCache()
        case None =>
          val index = mapStatusesDeleted.indexWhere(x => x != null && x.mapId == mapId)
          if (index >= 0 && mapStatuses(index) == null) {
            val mapStatus = mapStatusesDeleted(index)
            mapStatus.updateLocation(bmAddress)
            mapStatuses(index) = mapStatus
            _numAvailableMapOutputs += 1
            invalidateSerializedMapOutputStatusCache()
            mapStatusesDeleted(index) = null
            logInfo(s"Recover ${mapStatus.mapId} ${mapStatus.location}")
          } else {
            logWarning(s"Asked to update map output ${mapId} for untracked map status.")
          }
      }
    } catch {
      case e: java.lang.NullPointerException =>
        logWarning(s"Unable to update map output for ${mapId}, status removed in-flight")
    }
  }

  /**
   * Remove the map output which was served by the specified block manager.
   * This is a no-op if there is no registered map output or if the registered output is from a
   * different block manager.
   */
  def removeMapOutput(mapIndex: Int, bmAddress: BlockManagerId): Unit = withWriteLock {
    logDebug(s"Removing existing map output ${mapIndex} ${bmAddress}")
    if (mapStatuses(mapIndex) != null && mapStatuses(mapIndex).location == bmAddress) {
      _numAvailableMapOutputs -= 1
      mapStatusesDeleted(mapIndex) = mapStatuses(mapIndex)
      mapStatuses(mapIndex) = null
      invalidateSerializedMapOutputStatusCache()
    }
  }

  /**
   * Register a merge result.
   */
  def addMergeResult(reduceId: Int, status: MergeStatus): Unit = withWriteLock {
    if (mergeStatuses(reduceId) != status) {
      _numAvailableMergeResults += 1
      invalidateSerializedMergeOutputStatusCache()
    }
    mergeStatuses(reduceId) = status
  }

  def registerShuffleMergerLocations(shuffleMergers: Seq[BlockManagerId]): Unit = withWriteLock {
    if (shufflePushMergerLocations.isEmpty) {
      shufflePushMergerLocations = shuffleMergers
    }
  }

  def removeShuffleMergerLocations(): Unit = withWriteLock {
    shufflePushMergerLocations = Nil
  }

  // TODO support updateMergeResult for similar use cases as updateMapOutput

  /**
   * Remove the merge result which was served by the specified block manager.
   */
  def removeMergeResult(reduceId: Int, bmAddress: BlockManagerId): Unit = withWriteLock {
    if (mergeStatuses(reduceId) != null && mergeStatuses(reduceId).location == bmAddress) {
      _numAvailableMergeResults -= 1
      mergeStatuses(reduceId) = null
      invalidateSerializedMergeOutputStatusCache()
    }
  }

  /**
   * Removes all shuffle outputs associated with this host. Note that this will also remove
   * outputs which are served by an external shuffle server (if one exists).
   */
  def removeOutputsOnHost(host: String): Unit = withWriteLock {
    logDebug(s"Removing outputs for host ${host}")
    removeOutputsByFilter(x => x.host == host)
    removeMergeResultsByFilter(x => x.host == host)
  }

  /**
   * Removes all map outputs associated with the specified executor. Note that this will also
   * remove outputs which are served by an external shuffle server (if one exists), as they are
   * still registered with that execId.
   */
  def removeOutputsOnExecutor(execId: String): Unit = withWriteLock {
    logDebug(s"Removing outputs for execId ${execId}")
    removeOutputsByFilter(x => x.executorId == execId)
  }

  /**
   * Removes all shuffle outputs which satisfies the filter. Note that this will also
   * remove outputs which are served by an external shuffle server (if one exists).
   */
  def removeOutputsByFilter(f: BlockManagerId => Boolean): Unit = withWriteLock {
    for (mapIndex <- mapStatuses.indices) {
      if (mapStatuses(mapIndex) != null && f(mapStatuses(mapIndex).location)) {
        _numAvailableMapOutputs -= 1
        mapStatusesDeleted(mapIndex) = mapStatuses(mapIndex)
        mapStatuses(mapIndex) = null
        invalidateSerializedMapOutputStatusCache()
      }
    }
  }

  /**
   * Removes all shuffle merge result which satisfies the filter.
   */
  def removeMergeResultsByFilter(f: BlockManagerId => Boolean): Unit = withWriteLock {
    for (reduceId <- mergeStatuses.indices) {
      if (mergeStatuses(reduceId) != null && f(mergeStatuses(reduceId).location)) {
        _numAvailableMergeResults -= 1
        mergeStatuses(reduceId) = null
        invalidateSerializedMergeOutputStatusCache()
      }
    }
  }

  /**
   * Number of partitions that have shuffle map outputs.
   */
  def numAvailableMapOutputs: Int = withReadLock {
    _numAvailableMapOutputs
  }

  /**
   * Number of shuffle partitions that have already been merge finalized when push-based
   * is enabled.
   */
  def numAvailableMergeResults: Int = withReadLock {
    _numAvailableMergeResults
  }

  /**
   * Returns the sequence of partition ids that are missing (i.e. needs to be computed).
   */
  def findMissingPartitions(): Seq[Int] = withReadLock {
    val missing = (0 until numPartitions).filter(id => mapStatuses(id) == null)
    assert(missing.size == numPartitions - _numAvailableMapOutputs,
      s"${missing.size} missing, expected ${numPartitions - _numAvailableMapOutputs}")
    missing
  }

  /**
   * Serializes the mapStatuses array into an efficient compressed format. See the comments on
   * `MapOutputTracker.serializeOutputStatuses()` for more details on the serialization format.
   *
   * This method is designed to be called multiple times and implements caching in order to speed
   * up subsequent requests. If the cache is empty and multiple threads concurrently attempt to
   * serialize the map statuses then serialization will only be performed in a single thread and
   * all other threads will block until the cache is populated.
   */
  def serializedMapStatus(
      broadcastManager: BroadcastManager,
      isLocal: Boolean,
      minBroadcastSize: Int,
      conf: SparkConf): Array[Byte] = {
    var result: Array[Byte] = null
    withReadLock {
      if (cachedSerializedMapStatus != null) {
        result = cachedSerializedMapStatus
      }
    }

    if (result == null) withWriteLock {
      if (cachedSerializedMapStatus == null) {
        val serResult = MapOutputTracker.serializeOutputStatuses[MapStatus](
          mapStatuses, broadcastManager, isLocal, minBroadcastSize, conf)
        cachedSerializedMapStatus = serResult._1
        cachedSerializedBroadcast = serResult._2
      }
      // The following line has to be outside if statement since it's possible that another thread
      // initializes cachedSerializedMapStatus in-between `withReadLock` and `withWriteLock`.
      result = cachedSerializedMapStatus
    }
    result
  }

  /**
   * Serializes the mapStatuses and mergeStatuses array into an efficient compressed format.
   * See the comments on `MapOutputTracker.serializeOutputStatuses()` for more details
   * on the serialization format.
   *
   * This method is designed to be called multiple times and implements caching in order to speed
   * up subsequent requests. If the cache is empty and multiple threads concurrently attempt to
   * serialize the statuses array then serialization will only be performed in a single thread and
   * all other threads will block until the cache is populated.
   */
  def serializedMapAndMergeStatus(
      broadcastManager: BroadcastManager,
      isLocal: Boolean,
      minBroadcastSize: Int,
      conf: SparkConf): (Array[Byte], Array[Byte]) = {
    val mapStatusesBytes: Array[Byte] =
      serializedMapStatus(broadcastManager, isLocal, minBroadcastSize, conf)
    var mergeStatusesBytes: Array[Byte] = null

    withReadLock {
      if (cachedSerializedMergeStatus != null) {
        mergeStatusesBytes = cachedSerializedMergeStatus
      }
    }

    if (mergeStatusesBytes == null) withWriteLock {
      if (cachedSerializedMergeStatus == null) {
        val serResult = MapOutputTracker.serializeOutputStatuses[MergeStatus](
          mergeStatuses, broadcastManager, isLocal, minBroadcastSize, conf)
        cachedSerializedMergeStatus = serResult._1
        cachedSerializedBroadcastMergeStatus = serResult._2
      }

      // The following line has to be outside if statement since it's possible that another
      // thread initializes cachedSerializedMergeStatus in-between `withReadLock` and
      // `withWriteLock`.
      mergeStatusesBytes = cachedSerializedMergeStatus
    }
    (mapStatusesBytes, mergeStatusesBytes)
  }

  // Used in testing.
  def hasCachedSerializedBroadcast: Boolean = withReadLock {
    cachedSerializedBroadcast != null
  }

  /**
   * Helper function which provides thread-safe access to the mapStatuses array.
   * The function should NOT mutate the array.
   */
  def withMapStatuses[T](f: Array[MapStatus] => T): T = withReadLock {
    f(mapStatuses)
  }

  def withMergeStatuses[T](f: Array[MergeStatus] => T): T = withReadLock {
    f(mergeStatuses)
  }

  def getShufflePushMergerLocations: Seq[BlockManagerId] = withReadLock {
    shufflePushMergerLocations
  }

  /**
   * Clears the cached serialized map output statuses.
   */
  def invalidateSerializedMapOutputStatusCache(): Unit = withWriteLock {
    if (cachedSerializedBroadcast != null) {
      // Prevent errors during broadcast cleanup from crashing the DAGScheduler (see SPARK-21444)
      Utils.tryLogNonFatalError {
        // Use `blocking = false` so that this operation doesn't hang while trying to send cleanup
        // RPCs to dead executors.
        cachedSerializedBroadcast.destroy()
      }
      cachedSerializedBroadcast = null
    }
    cachedSerializedMapStatus = null
  }

  /**
   * Clears the cached serialized merge result statuses.
   */
  def invalidateSerializedMergeOutputStatusCache(): Unit = withWriteLock {
    if (cachedSerializedBroadcastMergeStatus != null) {
      Utils.tryLogNonFatalError {
        // Use `blocking = false` so that this operation doesn't hang while trying to send cleanup
        // RPCs to dead executors.
        cachedSerializedBroadcastMergeStatus.destroy()
      }
      cachedSerializedBroadcastMergeStatus = null
    }
    cachedSerializedMergeStatus = null
  }
}

private[spark] sealed trait MapOutputTrackerMessage
private[spark] case class GetMapOutputStatuses(shuffleId: Int)
  extends MapOutputTrackerMessage
private[spark] case class GetMapAndMergeResultStatuses(shuffleId: Int)
  extends MapOutputTrackerMessage
private[spark] case class GetShufflePushMergerLocations(shuffleId: Int)
  extends MapOutputTrackerMessage
private[spark] case object StopMapOutputTracker extends MapOutputTrackerMessage

private[spark] sealed trait MapOutputTrackerMasterMessage
private[spark] case class GetMapOutputMessage(shuffleId: Int,
  context: RpcCallContext) extends MapOutputTrackerMasterMessage
private[spark] case class GetMapAndMergeOutputMessage(shuffleId: Int,
  context: RpcCallContext) extends MapOutputTrackerMasterMessage
private[spark] case class GetShufflePushMergersMessage(shuffleId: Int,
  context: RpcCallContext) extends MapOutputTrackerMasterMessage
private[spark] case class MapSizesByExecutorId(
  iter: Iterator[(BlockManagerId, Seq[(BlockId, Long, Int)])], enableBatchFetch: Boolean)

/** RpcEndpoint class for MapOutputTrackerMaster */
private[spark] class MapOutputTrackerMasterEndpoint(
    override val rpcEnv: RpcEnv, tracker: MapOutputTrackerMaster, conf: SparkConf)
  extends RpcEndpoint with Logging {

  logDebug("init") // force eager creation of logger

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case GetMapOutputStatuses(shuffleId: Int) =>
      val hostPort = context.senderAddress.hostPort
      logInfo(s"Asked to send map output locations for shuffle $shuffleId to $hostPort")
      tracker.post(GetMapOutputMessage(shuffleId, context))

    case GetMapAndMergeResultStatuses(shuffleId: Int) =>
      val hostPort = context.senderAddress.hostPort
      logInfo(s"Asked to send map/merge result locations for shuffle $shuffleId to $hostPort")
      tracker.post(GetMapAndMergeOutputMessage(shuffleId, context))

    case GetShufflePushMergerLocations(shuffleId: Int) =>
      logInfo(s"Asked to send shuffle push merger locations for shuffle" +
        s" $shuffleId to ${context.senderAddress.hostPort}")
      tracker.post(GetShufflePushMergersMessage(shuffleId, context))

    case StopMapOutputTracker =>
      logInfo("MapOutputTrackerMasterEndpoint stopped!")
      context.reply(true)
      stop()
  }
}

/**
 * Class that keeps track of the location of the map output of a stage. This is abstract because the
 * driver and executor have different versions of the MapOutputTracker. In principle the driver-
 * and executor-side classes don't need to share a common base class; the current shared base class
 * is maintained primarily for backwards-compatibility in order to avoid having to update existing
 * test code.
*/
private[spark] abstract class MapOutputTracker(conf: SparkConf) extends Logging {
  /** Set to the MapOutputTrackerMasterEndpoint living on the driver. */
  var trackerEndpoint: RpcEndpointRef = _

  /**
   * The driver-side counter is incremented every time that a map output is lost. This value is sent
   * to executors as part of tasks, where executors compare the new epoch number to the highest
   * epoch number that they received in the past. If the new epoch number is higher then executors
   * will clear their local caches of map output statuses and will re-fetch (possibly updated)
   * statuses from the driver.
   */
  protected var epoch: Long = 0
  protected val epochLock = new AnyRef

  /**
   * Send a message to the trackerEndpoint and get its result within a default timeout, or
   * throw a SparkException if this fails.
   */
  protected def askTracker[T: ClassTag](message: Any): T = {
    try {
      trackerEndpoint.askSync[T](message)
    } catch {
      case e: Exception =>
        logError("Error communicating with MapOutputTracker", e)
        throw new SparkException("Error communicating with MapOutputTracker", e)
    }
  }

  /** Send a one-way message to the trackerEndpoint, to which we expect it to reply with true. */
  protected def sendTracker(message: Any): Unit = {
    val response = askTracker[Boolean](message)
    if (response != true) {
      throw new SparkException(
        "Error reply received from MapOutputTracker. Expecting true, got " + response.toString)
    }
  }

  // For testing
  def getMapSizesByExecutorId(shuffleId: Int, reduceId: Int)
      : Iterator[(BlockManagerId, Seq[(BlockId, Long, Int)])] = {
    getMapSizesByExecutorId(shuffleId, 0, Int.MaxValue, reduceId, reduceId + 1)
  }

  // For testing
  def getPushBasedShuffleMapSizesByExecutorId(shuffleId: Int, reduceId: Int)
      : MapSizesByExecutorId = {
    getPushBasedShuffleMapSizesByExecutorId(shuffleId, 0, Int.MaxValue, reduceId, reduceId + 1)
  }

  /**
   * Called from executors to get the server URIs and output sizes for each shuffle block that
   * needs to be read from a given range of map output partitions (startPartition is included but
   * endPartition is excluded from the range) within a range of mappers (startMapIndex is included
   * but endMapIndex is excluded) when push based shuffle is not enabled for the specific shuffle
   * dependency. If endMapIndex=Int.MaxValue, the actual endMapIndex will be changed to the length
   * of total map outputs.
   *
   * @return A sequence of 2-item tuples, where the first item in the tuple is a BlockManagerId,
   *         and the second item is a sequence of (shuffle block id, shuffle block size, map index)
   *         tuples describing the shuffle blocks that are stored at that block manager.
   *         Note that zero-sized blocks are excluded in the result.
   */
  def getMapSizesByExecutorId(
      shuffleId: Int,
      startMapIndex: Int,
      endMapIndex: Int,
      startPartition: Int,
      endPartition: Int): Iterator[(BlockManagerId, Seq[(BlockId, Long, Int)])]

  /**
   * Called from executors to get the server URIs and output sizes for each shuffle block that
   * needs to be read from a given range of map output partitions (startPartition is included but
   * endPartition is excluded from the range) within a range of mappers (startMapIndex is included
   * but endMapIndex is excluded) when push based shuffle is enabled for the specific shuffle
   * dependency. If endMapIndex=Int.MaxValue, the actual endMapIndex will be changed to the length
   * of total map outputs.
   *
   * @return A case class object which includes two attributes. The first attribute is a sequence
   *         of 2-item tuples, where the first item in the tuple is a BlockManagerId, and the
   *         second item is a sequence of (shuffle block id, shuffle block size, map index) tuples
   *         tuples describing the shuffle blocks that are stored at that block manager. Note that
   *         zero-sized blocks are excluded in the result. The second attribute is a boolean flag,
   *         indicating whether batch fetch can be enabled.
   */
  def getPushBasedShuffleMapSizesByExecutorId(
      shuffleId: Int,
      startMapIndex: Int,
      endMapIndex: Int,
      startPartition: Int,
      endPartition: Int): MapSizesByExecutorId

  /**
   * Called from executors upon fetch failure on an entire merged shuffle reduce partition.
   * Such failures can happen if the shuffle client fails to fetch the metadata for the given
   * merged shuffle partition. This method is to get the server URIs and output sizes for each
   * shuffle block that is merged in the specified merged shuffle block so fetch failure on a
   * merged shuffle block can fall back to fetching the unmerged blocks.
   *
   * @return A sequence of 2-item tuples, where the first item in the tuple is a BlockManagerId,
   *         and the second item is a sequence of (shuffle block ID, shuffle block size, map index)
   *         tuples describing the shuffle blocks that are stored at that block manager.
   */
  def getMapSizesForMergeResult(
      shuffleId: Int,
      partitionId: Int): Iterator[(BlockManagerId, Seq[(BlockId, Long, Int)])]

  /**
   * Called from executors upon fetch failure on a merged shuffle reduce partition chunk. This is
   * to get the server URIs and output sizes for each shuffle block that is merged in the specified
   * merged shuffle partition chunk so fetch failure on a merged shuffle block chunk can fall back
   * to fetching the unmerged blocks.
   *
   * chunkBitMap tracks the mapIds which are part of the current merged chunk, this way if there is
   * a fetch failure on the merged chunk, it can fallback to fetching the corresponding original
   * blocks part of this merged chunk.
   *
   * @return A sequence of 2-item tuples, where the first item in the tuple is a BlockManagerId,
   *         and the second item is a sequence of (shuffle block ID, shuffle block size, map index)
   *         tuples describing the shuffle blocks that are stored at that block manager.
   */
  def getMapSizesForMergeResult(
      shuffleId: Int,
      partitionId: Int,
      chunkBitmap: RoaringBitmap): Iterator[(BlockManagerId, Seq[(BlockId, Long, Int)])]

  /**
   * Called from executors whenever a task with push based shuffle is enabled doesn't have shuffle
   * mergers available. This typically happens when the initial stages doesn't have enough shuffle
   * mergers available since very few executors got registered. This is on a best effort basis,
   * if there is not enough shuffle mergers available for this stage then an empty sequence would
   * be returned indicating the task to avoid shuffle push.
   * @param shuffleId
   */
  def getShufflePushMergerLocations(shuffleId: Int): Seq[BlockManagerId]

  /**
   * Deletes map output status information for the specified shuffle stage.
   */
  def unregisterShuffle(shuffleId: Int): Unit

  def stop(): Unit = {}
}

/**
 * Driver-side class that keeps track of the location of the map output of a stage.
 *
 * The DAGScheduler uses this class to (de)register map output statuses and to look up statistics
 * for performing locality-aware reduce task scheduling.
 *
 * ShuffleMapStage uses this class for tracking available / missing outputs in order to determine
 * which tasks need to be run.
 */
private[spark] class MapOutputTrackerMaster(
    conf: SparkConf,
    private[spark] val broadcastManager: BroadcastManager,
    private[spark] val isLocal: Boolean)
  extends MapOutputTracker(conf) {

  // The size at which we use Broadcast to send the map output statuses to the executors
  private val minSizeForBroadcast = conf.get(SHUFFLE_MAPOUTPUT_MIN_SIZE_FOR_BROADCAST).toInt

  /** Whether to compute locality preferences for reduce tasks */
  private val shuffleLocalityEnabled = conf.get(SHUFFLE_REDUCE_LOCALITY_ENABLE)

  // Number of map and reduce tasks above which we do not assign preferred locations based on map
  // output sizes. We limit the size of jobs for which assign preferred locations as computing the
  // top locations by size becomes expensive.
  private val SHUFFLE_PREF_MAP_THRESHOLD = 1000
  // NOTE: This should be less than 2000 as we use HighlyCompressedMapStatus beyond that
  private val SHUFFLE_PREF_REDUCE_THRESHOLD = 1000

  // Fraction of total map output that must be at a location for it to considered as a preferred
  // location for a reduce task. Making this larger will focus on fewer locations where most data
  // can be read locally, but may lead to more delay in scheduling if those locations are busy.
  private val REDUCER_PREF_LOCS_FRACTION = 0.2

  // HashMap for storing shuffleStatuses in the driver.
  // Statuses are dropped only by explicit de-registering.
  // Exposed for testing
  val shuffleStatuses = new ConcurrentHashMap[Int, ShuffleStatus]().asScala

  private val maxRpcMessageSize = RpcUtils.maxMessageSizeBytes(conf)

  // requests for MapOutputTrackerMasterMessages
  private val mapOutputTrackerMasterMessages =
    new LinkedBlockingQueue[MapOutputTrackerMasterMessage]

  private val pushBasedShuffleEnabled = Utils.isPushBasedShuffleEnabled(conf, isDriver = true)

  // Thread pool used for handling map output status requests. This is a separate thread pool
  // to ensure we don't block the normal dispatcher threads.
  private val threadpool: ThreadPoolExecutor = {
    val numThreads = conf.get(SHUFFLE_MAPOUTPUT_DISPATCHER_NUM_THREADS)
    val pool = ThreadUtils.newDaemonFixedThreadPool(numThreads, "map-output-dispatcher")
    for (i <- 0 until numThreads) {
      pool.execute(new MessageLoop)
    }
    pool
  }

  // Make sure that we aren't going to exceed the max RPC message size by making sure
  // we use broadcast to send large map output statuses.
  if (minSizeForBroadcast > maxRpcMessageSize) {
    val msg = s"${SHUFFLE_MAPOUTPUT_MIN_SIZE_FOR_BROADCAST.key} ($minSizeForBroadcast bytes) " +
      s"must be <= spark.rpc.message.maxSize ($maxRpcMessageSize bytes) to prevent sending an " +
      "rpc message that is too large."
    logError(msg)
    throw new IllegalArgumentException(msg)
  }

  def post(message: MapOutputTrackerMasterMessage): Unit = {
    mapOutputTrackerMasterMessages.offer(message)
  }

  /** Message loop used for dispatching messages. */
  private class MessageLoop extends Runnable {
    private def handleStatusMessage(
        shuffleId: Int,
        context: RpcCallContext,
        needMergeOutput: Boolean): Unit = {
      val hostPort = context.senderAddress.hostPort
      val shuffleStatus = shuffleStatuses.get(shuffleId).head
      logDebug(s"Handling request to send ${if (needMergeOutput) "map" else "map/merge"}" +
        s" output locations for shuffle $shuffleId to $hostPort")
      if (needMergeOutput) {
        context.reply(
          shuffleStatus.
            serializedMapAndMergeStatus(broadcastManager, isLocal, minSizeForBroadcast, conf))
      } else {
        context.reply(
          shuffleStatus.serializedMapStatus(broadcastManager, isLocal, minSizeForBroadcast, conf))
      }
    }

    override def run(): Unit = {
      try {
        while (true) {
          try {
            val data = mapOutputTrackerMasterMessages.take()
            if (data == PoisonPill) {
              // Put PoisonPill back so that other MessageLoops can see it.
              mapOutputTrackerMasterMessages.offer(PoisonPill)
              return
            }

            data match {
              case GetMapOutputMessage(shuffleId, context) =>
                handleStatusMessage(shuffleId, context, false)
              case GetMapAndMergeOutputMessage(shuffleId, context) =>
                handleStatusMessage(shuffleId, context, true)
              case GetShufflePushMergersMessage(shuffleId, context) =>
                logDebug(s"Handling request to send shuffle push merger locations for shuffle" +
                  s" $shuffleId to ${context.senderAddress.hostPort}")
                context.reply(shuffleStatuses.get(shuffleId).map(_.getShufflePushMergerLocations)
                  .getOrElse(Seq.empty[BlockManagerId]))
            }
          } catch {
            case NonFatal(e) => logError(e.getMessage, e)
          }
        }
      } catch {
        case ie: InterruptedException => // exit
      }
    }
  }

  /** A poison endpoint that indicates MessageLoop should exit its message loop. */
  private val PoisonPill = GetMapOutputMessage(-99, null)

  // Used only in unit tests.
  private[spark] def getNumCachedSerializedBroadcast: Int = {
    shuffleStatuses.valuesIterator.count(_.hasCachedSerializedBroadcast)
  }

  def registerShuffle(shuffleId: Int, numMaps: Int, numReduces: Int): Unit = {
    if (pushBasedShuffleEnabled) {
      if (shuffleStatuses.put(shuffleId, new ShuffleStatus(numMaps, numReduces)).isDefined) {
        throw new IllegalArgumentException("Shuffle ID " + shuffleId + " registered twice")
      }
    } else {
      if (shuffleStatuses.put(shuffleId, new ShuffleStatus(numMaps)).isDefined) {
        throw new IllegalArgumentException("Shuffle ID " + shuffleId + " registered twice")
      }
    }
  }

  def updateMapOutput(shuffleId: Int, mapId: Long, bmAddress: BlockManagerId): Unit = {
    shuffleStatuses.get(shuffleId) match {
      case Some(shuffleStatus) =>
        shuffleStatus.updateMapOutput(mapId, bmAddress)
      case None =>
        logError(s"Asked to update map output for unknown shuffle ${shuffleId}")
    }
  }

  def registerMapOutput(shuffleId: Int, mapIndex: Int, status: MapStatus): Unit = {
    shuffleStatuses(shuffleId).addMapOutput(mapIndex, status)
  }

  /** Unregister map output information of the given shuffle, mapper and block manager */
  def unregisterMapOutput(shuffleId: Int, mapIndex: Int, bmAddress: BlockManagerId): Unit = {
    shuffleStatuses.get(shuffleId) match {
      case Some(shuffleStatus) =>
        shuffleStatus.removeMapOutput(mapIndex, bmAddress)
        incrementEpoch()
      case None =>
        throw new SparkException("unregisterMapOutput called for nonexistent shuffle ID")
    }
  }

  /** Unregister all map and merge output information of the given shuffle. */
  def unregisterAllMapAndMergeOutput(shuffleId: Int): Unit = {
    shuffleStatuses.get(shuffleId) match {
      case Some(shuffleStatus) =>
        shuffleStatus.removeOutputsByFilter(x => true)
        shuffleStatus.removeMergeResultsByFilter(x => true)
        shuffleStatus.removeShuffleMergerLocations()
        incrementEpoch()
      case None =>
        throw new SparkException(
          s"unregisterAllMapAndMergeOutput called for nonexistent shuffle ID $shuffleId.")
    }
  }

  def registerMergeResult(shuffleId: Int, reduceId: Int, status: MergeStatus): Unit = {
    shuffleStatuses(shuffleId).addMergeResult(reduceId, status)
  }

  def registerMergeResults(shuffleId: Int, statuses: Seq[(Int, MergeStatus)]): Unit = {
    statuses.foreach {
      case (reduceId, status) => registerMergeResult(shuffleId, reduceId, status)
    }
  }

  def registerShufflePushMergerLocations(
      shuffleId: Int,
      shuffleMergers: Seq[BlockManagerId]): Unit = {
    shuffleStatuses(shuffleId).registerShuffleMergerLocations(shuffleMergers)
  }

  /**
   * Unregisters a merge result corresponding to the reduceId if present. If the optional mapIndex
   * is specified, it will only unregister the merge result if the mapIndex is part of that merge
   * result.
   *
   * @param shuffleId the shuffleId.
   * @param reduceId  the reduceId.
   * @param bmAddress block manager address.
   * @param mapIndex  the optional mapIndex which should be checked to see it was part of the
   *                  merge result.
   */
  def unregisterMergeResult(
      shuffleId: Int,
      reduceId: Int,
      bmAddress: BlockManagerId,
      mapIndex: Option[Int] = None): Unit = {
    shuffleStatuses.get(shuffleId) match {
      case Some(shuffleStatus) =>
        val mergeStatus = shuffleStatus.mergeStatuses(reduceId)
        if (mergeStatus != null &&
          (mapIndex.isEmpty || mergeStatus.tracker.contains(mapIndex.get))) {
          shuffleStatus.removeMergeResult(reduceId, bmAddress)
          incrementEpoch()
        }
      case None =>
        throw new SparkException("unregisterMergeResult called for nonexistent shuffle ID")
    }
  }

  def unregisterAllMergeResult(shuffleId: Int): Unit = {
    shuffleStatuses.get(shuffleId) match {
      case Some(shuffleStatus) =>
        shuffleStatus.removeMergeResultsByFilter(x => true)
        incrementEpoch()
      case None =>
        throw new SparkException(
          s"unregisterAllMergeResult called for nonexistent shuffle ID $shuffleId.")
    }
  }

  /** Unregister shuffle data */
  def unregisterShuffle(shuffleId: Int): Unit = {
    shuffleStatuses.remove(shuffleId).foreach { shuffleStatus =>
      shuffleStatus.invalidateSerializedMapOutputStatusCache()
      shuffleStatus.invalidateSerializedMergeOutputStatusCache()
    }
  }

  /**
   * Removes all shuffle outputs associated with this host. Note that this will also remove
   * outputs which are served by an external shuffle server (if one exists).
   */
  def removeOutputsOnHost(host: String): Unit = {
    shuffleStatuses.valuesIterator.foreach { _.removeOutputsOnHost(host) }
    incrementEpoch()
  }

  /**
   * Removes all shuffle outputs associated with this executor. Note that this will also remove
   * outputs which are served by an external shuffle server (if one exists), as they are still
   * registered with this execId.
   */
  def removeOutputsOnExecutor(execId: String): Unit = {
    shuffleStatuses.valuesIterator.foreach { _.removeOutputsOnExecutor(execId) }
    incrementEpoch()
  }

  /** Check if the given shuffle is being tracked */
  def containsShuffle(shuffleId: Int): Boolean = shuffleStatuses.contains(shuffleId)

  def getNumAvailableOutputs(shuffleId: Int): Int = {
    shuffleStatuses.get(shuffleId).map(_.numAvailableMapOutputs).getOrElse(0)
  }

  /** VisibleForTest. Invoked in test only. */
  private[spark] def getNumAvailableMergeResults(shuffleId: Int): Int = {
    shuffleStatuses.get(shuffleId).map(_.numAvailableMergeResults).getOrElse(0)
  }

  /**
   * Returns the sequence of partition ids that are missing (i.e. needs to be computed), or None
   * if the MapOutputTrackerMaster doesn't know about this shuffle.
   */
  def findMissingPartitions(shuffleId: Int): Option[Seq[Int]] = {
    shuffleStatuses.get(shuffleId).map(_.findMissingPartitions())
  }

  /**
   * Grouped function of Range, this is to avoid traverse of all elements of Range using
   * IterableLike's grouped function.
   */
  def rangeGrouped(range: Range, size: Int): Seq[Range] = {
    val start = range.start
    val step = range.step
    val end = range.end
    for (i <- start.until(end, size * step)) yield {
      i.until(i + size * step, step)
    }
  }

  /**
   * To equally divide n elements into m buckets, basically each bucket should have n/m elements,
   * for the remaining n%m elements, add one more element to the first n%m buckets each.
   */
  def equallyDivide(numElements: Int, numBuckets: Int): Seq[Seq[Int]] = {
    val elementsPerBucket = numElements / numBuckets
    val remaining = numElements % numBuckets
    val splitPoint = (elementsPerBucket + 1) * remaining
    if (elementsPerBucket == 0) {
      rangeGrouped(0.until(splitPoint), elementsPerBucket + 1)
    } else {
      rangeGrouped(0.until(splitPoint), elementsPerBucket + 1) ++
        rangeGrouped(splitPoint.until(numElements), elementsPerBucket)
    }
  }

  /**
   * Return statistics about all of the outputs for a given shuffle.
   */
  def getStatistics(dep: ShuffleDependency[_, _, _]): MapOutputStatistics = {
    shuffleStatuses(dep.shuffleId).withMapStatuses { statuses =>
      val totalSizes = new Array[Long](dep.partitioner.numPartitions)
      val parallelAggThreshold = conf.get(
        SHUFFLE_MAP_OUTPUT_PARALLEL_AGGREGATION_THRESHOLD)
      val parallelism = math.min(
        Runtime.getRuntime.availableProcessors(),
        statuses.length.toLong * totalSizes.length / parallelAggThreshold + 1).toInt
      if (parallelism <= 1) {
        statuses.filter(_ != null).foreach { s =>
          for (i <- totalSizes.indices) {
            totalSizes(i) += s.getSizeForBlock(i)
          }
        }
      } else {
        val threadPool = ThreadUtils.newDaemonFixedThreadPool(parallelism, "map-output-aggregate")
        try {
          implicit val executionContext = ExecutionContext.fromExecutor(threadPool)
          val mapStatusSubmitTasks = equallyDivide(totalSizes.length, parallelism).map {
            reduceIds => Future {
              statuses.filter(_ != null).foreach { s =>
                reduceIds.foreach(i => totalSizes(i) += s.getSizeForBlock(i))
              }
            }
          }
          ThreadUtils.awaitResult(Future.sequence(mapStatusSubmitTasks), Duration.Inf)
        } finally {
          threadPool.shutdown()
        }
      }
      new MapOutputStatistics(dep.shuffleId, totalSizes)
    }
  }

  /**
   * Return the preferred hosts on which to run the given map output partition in a given shuffle,
   * i.e. the nodes that the most outputs for that partition are on. If the map output is
   * pre-merged, then return the node where the merged block is located if the merge ratio is
   * above the threshold.
   *
   * @param dep shuffle dependency object
   * @param partitionId map output partition that we want to read
   * @return a sequence of host names
   */
  def getPreferredLocationsForShuffle(dep: ShuffleDependency[_, _, _], partitionId: Int)
      : Seq[String] = {
    val shuffleStatus = shuffleStatuses.get(dep.shuffleId).orNull
    if (shuffleStatus != null) {
      // Check if the map output is pre-merged and if the merge ratio is above the threshold.
      // If so, the location of the merged block is the preferred location.
      val preferredLoc = if (pushBasedShuffleEnabled) {
        shuffleStatus.withMergeStatuses { statuses =>
          val status = statuses(partitionId)
          val numMaps = dep.rdd.partitions.length
          if (status != null && status.getNumMissingMapOutputs(numMaps).toDouble / numMaps
            <= (1 - REDUCER_PREF_LOCS_FRACTION)) {
            Seq(status.location.host)
          } else {
            Nil
          }
        }
      } else {
        Nil
      }
      if (preferredLoc.nonEmpty) {
        preferredLoc
      } else {
        if (shuffleLocalityEnabled && dep.rdd.partitions.length < SHUFFLE_PREF_MAP_THRESHOLD &&
          dep.partitioner.numPartitions < SHUFFLE_PREF_REDUCE_THRESHOLD) {
          val blockManagerIds = getLocationsWithLargestOutputs(dep.shuffleId, partitionId,
            dep.partitioner.numPartitions, REDUCER_PREF_LOCS_FRACTION)
          if (blockManagerIds.nonEmpty) {
            blockManagerIds.get.map(_.host)
          } else {
            Nil
          }
        } else {
          Nil
        }
      }
    } else {
      Nil
    }
  }

  /**
   * Return a list of locations that each have fraction of map output greater than the specified
   * threshold.
   *
   * @param shuffleId id of the shuffle
   * @param reducerId id of the reduce task
   * @param numReducers total number of reducers in the shuffle
   * @param fractionThreshold fraction of total map output size that a location must have
   *                          for it to be considered large.
   */
  def getLocationsWithLargestOutputs(
      shuffleId: Int,
      reducerId: Int,
      numReducers: Int,
      fractionThreshold: Double)
    : Option[Array[BlockManagerId]] = {

    val shuffleStatus = shuffleStatuses.get(shuffleId).orNull
    if (shuffleStatus != null) {
      shuffleStatus.withMapStatuses { statuses =>
        if (statuses.nonEmpty) {
          // HashMap to add up sizes of all blocks at the same location
          val locs = new HashMap[BlockManagerId, Long]
          var totalOutputSize = 0L
          var mapIdx = 0
          while (mapIdx < statuses.length) {
            val status = statuses(mapIdx)
            // status may be null here if we are called between registerShuffle, which creates an
            // array with null entries for each output, and registerMapOutputs, which populates it
            // with valid status entries. This is possible if one thread schedules a job which
            // depends on an RDD which is currently being computed by another thread.
            if (status != null) {
              val blockSize = status.getSizeForBlock(reducerId)
              if (blockSize > 0) {
                locs(status.location) = locs.getOrElse(status.location, 0L) + blockSize
                totalOutputSize += blockSize
              }
            }
            mapIdx = mapIdx + 1
          }
          val topLocs = locs.filter { case (loc, size) =>
            size.toDouble / totalOutputSize >= fractionThreshold
          }
          // Return if we have any locations which satisfy the required threshold
          if (topLocs.nonEmpty) {
            return Some(topLocs.keys.toArray)
          }
        }
      }
    }
    None
  }

  /**
   * Return the locations where the Mappers ran. The locations each includes both a host and an
   * executor id on that host.
   *
   * @param dep shuffle dependency object
   * @param startMapIndex the start map index
   * @param endMapIndex the end map index (exclusive)
   * @return a sequence of locations where task runs.
   */
  def getMapLocation(
      dep: ShuffleDependency[_, _, _],
      startMapIndex: Int,
      endMapIndex: Int): Seq[String] =
  {
    val shuffleStatus = shuffleStatuses.get(dep.shuffleId).orNull
    if (shuffleStatus != null) {
      shuffleStatus.withMapStatuses { statuses =>
        if (startMapIndex < endMapIndex &&
          (startMapIndex >= 0 && endMapIndex <= statuses.length)) {
          val statusesPicked = statuses.slice(startMapIndex, endMapIndex).filter(_ != null)
          statusesPicked.map(_.location.host).toSeq
        } else {
          Nil
        }
      }
    } else {
      Nil
    }
  }

  def incrementEpoch(): Unit = {
    epochLock.synchronized {
      epoch += 1
      logDebug("Increasing epoch to " + epoch)
    }
  }

  /** Called to get current epoch number. */
  def getEpoch: Long = {
    epochLock.synchronized {
      return epoch
    }
  }

  // This method is only called in local-mode.
  override def getMapSizesByExecutorId(
      shuffleId: Int,
      startMapIndex: Int,
      endMapIndex: Int,
      startPartition: Int,
      endPartition: Int): Iterator[(BlockManagerId, Seq[(BlockId, Long, Int)])] = {
    val mapSizesByExecutorId = getPushBasedShuffleMapSizesByExecutorId(
      shuffleId, startMapIndex, endMapIndex, startPartition, endPartition)
    assert(mapSizesByExecutorId.enableBatchFetch == true)
    mapSizesByExecutorId.iter
  }

  // This method is only called in local-mode.
  override def getPushBasedShuffleMapSizesByExecutorId(
      shuffleId: Int,
      startMapIndex: Int,
      endMapIndex: Int,
      startPartition: Int,
      endPartition: Int): MapSizesByExecutorId = {
    logDebug(s"Fetching outputs for shuffle $shuffleId")
    shuffleStatuses.get(shuffleId) match {
      case Some(shuffleStatus) =>
        shuffleStatus.withMapStatuses { statuses =>
          val actualEndMapIndex = if (endMapIndex == Int.MaxValue) statuses.length else endMapIndex
          logDebug(s"Convert map statuses for shuffle $shuffleId, " +
            s"mappers $startMapIndex-$actualEndMapIndex, partitions $startPartition-$endPartition")
          MapOutputTracker.convertMapStatuses(
            shuffleId, startPartition, endPartition, statuses, startMapIndex, actualEndMapIndex)
        }
      case None =>
        MapSizesByExecutorId(Iterator.empty, true)
    }
  }

  // This method is only called in local-mode. Since push based shuffle won't be
  // enabled in local-mode, this method returns empty list.
  override def getMapSizesForMergeResult(
      shuffleId: Int,
      partitionId: Int): Iterator[(BlockManagerId, Seq[(BlockId, Long, Int)])] = {
    Seq.empty.iterator
  }

  // This method is only called in local-mode. Since push based shuffle won't be
  // enabled in local-mode, this method returns empty list.
  override def getMapSizesForMergeResult(
      shuffleId: Int,
      partitionId: Int,
      chunkTracker: RoaringBitmap): Iterator[(BlockManagerId, Seq[(BlockId, Long, Int)])] = {
    Seq.empty.iterator
  }

  // This method is only called in local-mode.
  override def getShufflePushMergerLocations(shuffleId: Int): Seq[BlockManagerId] = {
    shuffleStatuses(shuffleId).getShufflePushMergerLocations
  }

  override def stop(): Unit = {
    mapOutputTrackerMasterMessages.offer(PoisonPill)
    threadpool.shutdown()
    try {
      sendTracker(StopMapOutputTracker)
    } catch {
      case e: SparkException =>
        logError("Could not tell tracker we are stopping.", e)
    }
    trackerEndpoint = null
    shuffleStatuses.clear()
  }
}

/**
 * Executor-side client for fetching map output info from the driver's MapOutputTrackerMaster.
 * Note that this is not used in local-mode; instead, local-mode Executors access the
 * MapOutputTrackerMaster directly (which is possible because the master and worker share a common
 * superclass).
 */
private[spark] class MapOutputTrackerWorker(conf: SparkConf) extends MapOutputTracker(conf) {

  val mapStatuses: Map[Int, Array[MapStatus]] =
    new ConcurrentHashMap[Int, Array[MapStatus]]().asScala

  val mergeStatuses: Map[Int, Array[MergeStatus]] =
    new ConcurrentHashMap[Int, Array[MergeStatus]]().asScala

  // This must be lazy to ensure that it is initialized when the first task is run and not at
  // executor startup time. At startup time, user-added libraries may not have been
  // downloaded to the executor, causing `isPushBasedShuffleEnabled` to fail when it tries to
  // instantiate a serializer. See the followup to SPARK-36705 for more details.
  private lazy val fetchMergeResult = Utils.isPushBasedShuffleEnabled(conf, isDriver = false)

  /**
   * [[shufflePushMergerLocations]] tracks shuffle push merger locations for the latest
   * shuffle execution
   *
   * Exposed for testing
   */
  val shufflePushMergerLocations = new ConcurrentHashMap[Int, Seq[BlockManagerId]]().asScala

  /**
   * A [[KeyLock]] whose key is a shuffle id to ensure there is only one thread fetching
   * the same shuffle block.
   */
  private val fetchingLock = new KeyLock[Int]

  override def getMapSizesByExecutorId(
      shuffleId: Int,
      startMapIndex: Int,
      endMapIndex: Int,
      startPartition: Int,
      endPartition: Int): Iterator[(BlockManagerId, Seq[(BlockId, Long, Int)])] = {
    val mapSizesByExecutorId = getMapSizesByExecutorIdImpl(
      shuffleId, startMapIndex, endMapIndex, startPartition, endPartition, useMergeResult = false)
    assert(mapSizesByExecutorId.enableBatchFetch == true)
    mapSizesByExecutorId.iter
  }

  override def getPushBasedShuffleMapSizesByExecutorId(
      shuffleId: Int,
      startMapIndex: Int,
      endMapIndex: Int,
      startPartition: Int,
      endPartition: Int): MapSizesByExecutorId = {
    getMapSizesByExecutorIdImpl(
      shuffleId, startMapIndex, endMapIndex, startPartition, endPartition, useMergeResult = true)
  }

  private def getMapSizesByExecutorIdImpl(
      shuffleId: Int,
      startMapIndex: Int,
      endMapIndex: Int,
      startPartition: Int,
      endPartition: Int,
      useMergeResult: Boolean): MapSizesByExecutorId = {
    logDebug(s"Fetching outputs for shuffle $shuffleId")
    val (mapOutputStatuses, mergedOutputStatuses) = getStatuses(shuffleId, conf,
      // enableBatchFetch can be set to false during stage retry when the
      // shuffleDependency.isShuffleMergeFinalizedMarked is set to false, and Driver
      // has already collected the mergedStatus for its shuffle dependency.
      // In this case, boolean check helps to ensure that the unnecessary
      // mergeStatus won't be fetched, thus mergedOutputStatuses won't be
      // passed to convertMapStatuses. See details in [SPARK-37023].
      if (useMergeResult) fetchMergeResult else false)
    try {
      val actualEndMapIndex =
        if (endMapIndex == Int.MaxValue) mapOutputStatuses.length else endMapIndex
      logDebug(s"Convert map statuses for shuffle $shuffleId, " +
        s"mappers $startMapIndex-$actualEndMapIndex, partitions $startPartition-$endPartition")
      MapOutputTracker.convertMapStatuses(
        shuffleId, startPartition, endPartition, mapOutputStatuses, startMapIndex,
          actualEndMapIndex, Option(mergedOutputStatuses))
    } catch {
      case e: MetadataFetchFailedException =>
        // We experienced a fetch failure so our mapStatuses cache is outdated; clear it:
        mapStatuses.clear()
        mergeStatuses.clear()
        throw e
    }
  }

  override def getMapSizesForMergeResult(
      shuffleId: Int,
      partitionId: Int): Iterator[(BlockManagerId, Seq[(BlockId, Long, Int)])] = {
    logDebug(s"Fetching backup outputs for shuffle $shuffleId, partition $partitionId")
    // Fetch the map statuses and merge statuses again since they might have already been
    // cleared by another task running in the same executor.
    val (mapOutputStatuses, mergeResultStatuses) = getStatuses(shuffleId, conf, fetchMergeResult)
    try {
      val mergeStatus = mergeResultStatuses(partitionId)
      // If the original MergeStatus is no longer available, we cannot identify the list of
      // unmerged blocks to fetch in this case. Throw MetadataFetchFailedException in this case.
      MapOutputTracker.validateStatus(mergeStatus, shuffleId, partitionId)
      // Use the MergeStatus's partition level bitmap since we are doing partition level fallback
      MapOutputTracker.getMapStatusesForMergeStatus(shuffleId, partitionId,
        mapOutputStatuses, mergeStatus.tracker)
    } catch {
      // We experienced a fetch failure so our mapStatuses cache is outdated; clear it
      case e: MetadataFetchFailedException =>
        mapStatuses.clear()
        mergeStatuses.clear()
        throw e
    }
  }

  override def getMapSizesForMergeResult(
      shuffleId: Int,
      partitionId: Int,
      chunkTracker: RoaringBitmap): Iterator[(BlockManagerId, Seq[(BlockId, Long, Int)])] = {
    logDebug(s"Fetching backup outputs for shuffle $shuffleId, partition $partitionId")
    // Fetch the map statuses and merge statuses again since they might have already been
    // cleared by another task running in the same executor.
    val (mapOutputStatuses, _) = getStatuses(shuffleId, conf, fetchMergeResult)
    try {
      MapOutputTracker.getMapStatusesForMergeStatus(shuffleId, partitionId, mapOutputStatuses,
        chunkTracker)
    } catch {
      // We experienced a fetch failure so our mapStatuses cache is outdated; clear it:
      case e: MetadataFetchFailedException =>
        mapStatuses.clear()
        mergeStatuses.clear()
        throw e
    }
  }

  override def getShufflePushMergerLocations(shuffleId: Int): Seq[BlockManagerId] = {
    shufflePushMergerLocations.getOrElse(shuffleId, getMergerLocations(shuffleId))
  }

  private def getMergerLocations(shuffleId: Int): Seq[BlockManagerId] = {
    fetchingLock.withLock(shuffleId) {
      var fetchedMergers = shufflePushMergerLocations.get(shuffleId).orNull
      if (null == fetchedMergers) {
        fetchedMergers =
          askTracker[Seq[BlockManagerId]](GetShufflePushMergerLocations(shuffleId))
        if (fetchedMergers.nonEmpty) {
          shufflePushMergerLocations(shuffleId) = fetchedMergers
        } else {
          fetchedMergers = Seq.empty[BlockManagerId]
        }
      }
      fetchedMergers
    }
  }

  /**
   * Get or fetch the array of MapStatuses and MergeStatuses if push based shuffle enabled
   * for a given shuffle ID. NOTE: clients MUST synchronize
   * on this array when reading it, because on the driver, we may be changing it in place.
   *
   * (It would be nice to remove this restriction in the future.)
   */
  private def getStatuses(
      shuffleId: Int,
      conf: SparkConf,
      canFetchMergeResult: Boolean): (Array[MapStatus], Array[MergeStatus]) = {
    if (canFetchMergeResult) {
      val mapOutputStatuses = mapStatuses.get(shuffleId).orNull
      val mergeOutputStatuses = mergeStatuses.get(shuffleId).orNull

      if (mapOutputStatuses == null || mergeOutputStatuses == null) {
        logInfo("Don't have map/merge outputs for shuffle " + shuffleId + ", fetching them")
        val startTimeNs = System.nanoTime()
        fetchingLock.withLock(shuffleId) {
          var fetchedMapStatuses = mapStatuses.get(shuffleId).orNull
          var fetchedMergeStatuses = mergeStatuses.get(shuffleId).orNull
          if (fetchedMapStatuses == null || fetchedMergeStatuses == null) {
            logInfo("Doing the fetch; tracker endpoint = " + trackerEndpoint)
            val fetchedBytes =
              askTracker[(Array[Byte], Array[Byte])](GetMapAndMergeResultStatuses(shuffleId))
            try {
              fetchedMapStatuses =
                MapOutputTracker.deserializeOutputStatuses[MapStatus](fetchedBytes._1, conf)
              fetchedMergeStatuses =
                MapOutputTracker.deserializeOutputStatuses[MergeStatus](fetchedBytes._2, conf)
            } catch {
              case e: SparkException =>
                throw new MetadataFetchFailedException(shuffleId, -1,
                  s"Unable to deserialize broadcasted map/merge statuses" +
                    s" for shuffle $shuffleId: " + e.getCause)
            }
            logInfo("Got the map/merge output locations")
            mapStatuses.put(shuffleId, fetchedMapStatuses)
            mergeStatuses.put(shuffleId, fetchedMergeStatuses)
          }
          logDebug(s"Fetching map/merge output statuses for shuffle $shuffleId took " +
            s"${TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTimeNs)} ms")
          (fetchedMapStatuses, fetchedMergeStatuses)
        }
      } else {
        (mapOutputStatuses, mergeOutputStatuses)
      }
    } else {
      val statuses = mapStatuses.get(shuffleId).orNull
      if (statuses == null) {
        logInfo("Don't have map outputs for shuffle " + shuffleId + ", fetching them")
        val startTimeNs = System.nanoTime()
        fetchingLock.withLock(shuffleId) {
          var fetchedStatuses = mapStatuses.get(shuffleId).orNull
          if (fetchedStatuses == null) {
            logInfo("Doing the fetch; tracker endpoint = " + trackerEndpoint)
            val fetchedBytes = askTracker[Array[Byte]](GetMapOutputStatuses(shuffleId))
            try {
              fetchedStatuses =
                MapOutputTracker.deserializeOutputStatuses[MapStatus](fetchedBytes, conf)
            } catch {
              case e: SparkException =>
                throw new MetadataFetchFailedException(shuffleId, -1,
                  s"Unable to deserialize broadcasted map statuses for shuffle $shuffleId: " +
                    e.getCause)
            }
            logInfo("Got the map output locations")
            mapStatuses.put(shuffleId, fetchedStatuses)
          }
          logDebug(s"Fetching map output statuses for shuffle $shuffleId took " +
            s"${TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTimeNs)} ms")
          (fetchedStatuses, null)
        }
      } else {
        (statuses, null)
      }
    }
  }

  /** Unregister shuffle data. */
  def unregisterShuffle(shuffleId: Int): Unit = {
    mapStatuses.remove(shuffleId)
    mergeStatuses.remove(shuffleId)
    shufflePushMergerLocations.remove(shuffleId)
  }

  /**
   * Called from executors to update the epoch number, potentially clearing old outputs
   * because of a fetch failure. Each executor task calls this with the latest epoch
   * number on the driver at the time it was created.
   */
  def updateEpoch(newEpoch: Long): Unit = {
    epochLock.synchronized {
      if (newEpoch > epoch) {
        logInfo("Updating epoch to " + newEpoch + " and clearing cache")
        epoch = newEpoch
        mapStatuses.clear()
        mergeStatuses.clear()
        shufflePushMergerLocations.clear()
      }
    }
  }
}

private[spark] object MapOutputTracker extends Logging {

  val ENDPOINT_NAME = "MapOutputTracker"
  private val DIRECT = 0
  private val BROADCAST = 1

  val SHUFFLE_PUSH_MAP_ID = -1

  // Serialize an array of map/merge output locations into an efficient byte format so that we can
  // send it to reduce tasks. We do this by compressing the serialized bytes using Zstd. They will
  // generally be pretty compressible because many outputs will be on the same hostname.
  def serializeOutputStatuses[T <: ShuffleOutputStatus](
      statuses: Array[T],
      broadcastManager: BroadcastManager,
      isLocal: Boolean,
      minBroadcastSize: Int,
      conf: SparkConf): (Array[Byte], Broadcast[Array[Array[Byte]]]) = {
    // ByteArrayOutputStream has the 2GB limit so use ChunkedByteBufferOutputStream instead
    val out = new ChunkedByteBufferOutputStream(1024 * 1024, ByteBuffer.allocate)
    out.write(DIRECT)
    val codec = CompressionCodec.createCodec(conf, conf.get(MAP_STATUS_COMPRESSION_CODEC))
    val objOut = new ObjectOutputStream(codec.compressedOutputStream(out))
    Utils.tryWithSafeFinally {
      // Since statuses can be modified in parallel, sync on it
      statuses.synchronized {
        objOut.writeObject(statuses)
      }
    } {
      objOut.close()
    }
    val chunkedByteBuf = out.toChunkedByteBuffer
    val arrSize = out.size
    if (arrSize >= minBroadcastSize) {
      // Use broadcast instead.
      // Important arr(0) is the tag == DIRECT, ignore that while deserializing !
      // arr is a nested Array so that it can handle over 2GB serialized data
      val arr = chunkedByteBuf.getChunks().map(_.array())
      val bcast = broadcastManager.newBroadcast(arr, isLocal)
      // Using `org.apache.commons.io.output.ByteArrayOutputStream` instead of the standard one
      // This implementation doesn't reallocate the whole memory block but allocates
      // additional buffers. This way no buffers need to be garbage collected and
      // the contents don't have to be copied to the new buffer.
      val out = new ApacheByteArrayOutputStream()
      out.write(BROADCAST)
      val oos = new ObjectOutputStream(codec.compressedOutputStream(out))
      Utils.tryWithSafeFinally {
        oos.writeObject(bcast)
      } {
        oos.close()
      }
      val outArr = out.toByteArray
      logInfo("Broadcast outputstatuses size = " + outArr.length + ", actual size = " + arrSize)
      (outArr, bcast)
    } else {
      (chunkedByteBuf.toArray, null)
    }
  }

  // Opposite of serializeOutputStatuses.
  def deserializeOutputStatuses[T <: ShuffleOutputStatus](
      bytes: Array[Byte], conf: SparkConf): Array[T] = {
    assert (bytes.length > 0)

    def deserializeObject(in: InputStream): AnyRef = {
      val codec = CompressionCodec.createCodec(conf, conf.get(MAP_STATUS_COMPRESSION_CODEC))
      // The ZStd codec is wrapped in a `BufferedInputStream` which avoids overhead excessive
      // of JNI call while trying to decompress small amount of data for each element
      // of `MapStatuses`
      val objIn = new ObjectInputStream(codec.compressedInputStream(in))
      Utils.tryWithSafeFinally {
        objIn.readObject()
      } {
        objIn.close()
      }
    }

    val in = new ByteArrayInputStream(bytes, 1, bytes.length - 1)
    bytes(0) match {
      case DIRECT =>
        deserializeObject(in).asInstanceOf[Array[T]]
      case BROADCAST =>
        try {
          // deserialize the Broadcast, pull .value array out of it, and then deserialize that
          val bcast = deserializeObject(in).asInstanceOf[Broadcast[Array[Array[Byte]]]]
          logInfo("Broadcast outputstatuses size = " + bytes.length +
            ", actual size = " + bcast.value.foldLeft(0L)(_ + _.length))
          val bcastIn = new ChunkedByteBuffer(bcast.value.map(ByteBuffer.wrap)).toInputStream()
          // Important - ignore the DIRECT tag ! Start from offset 1
          bcastIn.skip(1)
          deserializeObject(bcastIn).asInstanceOf[Array[T]]
        } catch {
          case e: IOException =>
            logWarning("Exception encountered during deserializing broadcasted" +
              " output statuses: ", e)
            throw new SparkException("Unable to deserialize broadcasted" +
              " output statuses", e)
        }
      case _ => throw new IllegalArgumentException("Unexpected byte tag = " + bytes(0))
    }
  }

  /**
   * Given an array of map statuses and a range of map output partitions, returns a sequence that,
   * for each block manager ID, lists the shuffle block IDs and corresponding shuffle block sizes
   * stored at that block manager.
   * Note that empty blocks are filtered in the result.
   *
   * If push-based shuffle is enabled and an array of merge statuses is available, prioritize
   * the locations of the merged shuffle partitions over unmerged shuffle blocks.
   *
   * If any of the statuses is null (indicating a missing location due to a failed mapper),
   * throws a FetchFailedException.
   *
   * @param shuffleId Identifier for the shuffle
   * @param startPartition Start of map output partition ID range (included in range)
   * @param endPartition End of map output partition ID range (excluded from range)
   * @param mapStatuses List of map statuses, indexed by map partition index.
   * @param startMapIndex Start Map index.
   * @param endMapIndex End Map index.
   * @param mergeStatuses List of merge statuses, index by reduce ID.
   * @return A sequence of 2-item tuples, where the first item in the tuple is a BlockManagerId,
   *         and the second item is a sequence of (shuffle block id, shuffle block size, map index)
   *         tuples describing the shuffle blocks that are stored at that block manager.
   */
  def convertMapStatuses(
      shuffleId: Int,
      startPartition: Int,
      endPartition: Int,
      mapStatuses: Array[MapStatus],
      startMapIndex : Int,
      endMapIndex: Int,
      mergeStatusesOpt: Option[Array[MergeStatus]] = None): MapSizesByExecutorId = {
    assert (mapStatuses != null)
    val splitsByAddress = new HashMap[BlockManagerId, ListBuffer[(BlockId, Long, Int)]]
    var enableBatchFetch = true
    // Only use MergeStatus for reduce tasks that fetch all map outputs. Since a merged shuffle
    // partition consists of blocks merged in random order, we are unable to serve map index
    // subrange requests. However, when a reduce task needs to fetch blocks from a subrange of
    // map outputs, it usually indicates skewed partitions which push-based shuffle delegates
    // to AQE to handle.
    // TODO: SPARK-35036: Instead of reading map blocks in case of AQE with Push based shuffle,
    // TODO: improve push based shuffle to read partial merged blocks satisfying the start/end
    // TODO: map indexes
    if (mergeStatusesOpt.exists(_.exists(_ != null)) && startMapIndex == 0
      && endMapIndex == mapStatuses.length) {
      enableBatchFetch = false
      logDebug(s"Disable shuffle batch fetch as Push based shuffle is enabled for $shuffleId.")
      val mergeStatuses = mergeStatusesOpt.get
      for (partId <- startPartition until endPartition) {
        val mergeStatus = mergeStatuses(partId)
        if (mergeStatus != null && mergeStatus.totalSize > 0) {
          // If MergeStatus is available for the given partition, add location of the
          // pre-merged shuffle partition for this partition ID. Here we create a
          // ShuffleMergedBlockId to indicate this is a merged shuffle block.
          splitsByAddress.getOrElseUpdate(mergeStatus.location, ListBuffer()) +=
            ((ShuffleMergedBlockId(shuffleId, mergeStatus.shuffleMergeId, partId),
              mergeStatus.totalSize, SHUFFLE_PUSH_MAP_ID))
        }
      }

      // Add location for the mapper shuffle partition blocks
      for ((mapStatus, mapIndex) <- mapStatuses.iterator.zipWithIndex) {
        validateStatus(mapStatus, shuffleId, startPartition)
        for (partId <- startPartition until endPartition) {
          // For the "holes" in this pre-merged shuffle partition, i.e., unmerged mapper
          // shuffle partition blocks, fetch the original map produced shuffle partition blocks
          val mergeStatus = mergeStatuses(partId)
          if (mergeStatus == null || mergeStatus.totalSize == 0 ||
            !mergeStatus.tracker.contains(mapIndex)) {
            val size = mapStatus.getSizeForBlock(partId)
            if (size != 0) {
              splitsByAddress.getOrElseUpdate(mapStatus.location, ListBuffer()) +=
                ((ShuffleBlockId(shuffleId, mapStatus.mapId, partId), size, mapIndex))
            }
          }
        }
      }
    } else {
      val iter = mapStatuses.iterator.zipWithIndex
      for ((status, mapIndex) <- iter.slice(startMapIndex, endMapIndex)) {
        validateStatus(status, shuffleId, startPartition)
        for (part <- startPartition until endPartition) {
          val size = status.getSizeForBlock(part)
          if (size != 0) {
            splitsByAddress.getOrElseUpdate(status.location, ListBuffer()) +=
              ((ShuffleBlockId(shuffleId, status.mapId, part), size, mapIndex))
          }
        }
      }
    }

    MapSizesByExecutorId(splitsByAddress.mapValues(_.toSeq).iterator, enableBatchFetch)
  }

  /**
   * Given a shuffle ID, a partition ID, an array of map statuses, and bitmap corresponding
   * to either a merged shuffle partition or a merged shuffle partition chunk, identify
   * the metadata about the shuffle partition blocks that are merged into the merged shuffle
   * partition or partition chunk represented by the bitmap.
   *
   * @param shuffleId Identifier for the shuffle
   * @param partitionId The partition ID of the MergeStatus for which we look for the metadata
   *                    of the merged shuffle partition blocks
   * @param mapStatuses List of map statuses, indexed by map ID
   * @param tracker     bitmap containing mapIndexes that belong to the merged block or merged
   *                    block chunk.
   * @return A sequence of 2-item tuples, where the first item in the tuple is a BlockManagerId,
   *         and the second item is a sequence of (shuffle block ID, shuffle block size) tuples
   *         describing the shuffle blocks that are stored at that block manager.
   */
  def getMapStatusesForMergeStatus(
      shuffleId: Int,
      partitionId: Int,
      mapStatuses: Array[MapStatus],
      tracker: RoaringBitmap): Iterator[(BlockManagerId, Seq[(BlockId, Long, Int)])] = {
    assert (mapStatuses != null && tracker != null)
    val splitsByAddress = new HashMap[BlockManagerId, ListBuffer[(BlockId, Long, Int)]]
    for ((status, mapIndex) <- mapStatuses.zipWithIndex) {
      // Only add blocks that are merged
      if (tracker.contains(mapIndex)) {
        MapOutputTracker.validateStatus(status, shuffleId, partitionId)
        splitsByAddress.getOrElseUpdate(status.location, ListBuffer()) +=
          ((ShuffleBlockId(shuffleId, status.mapId, partitionId),
            status.getSizeForBlock(partitionId), mapIndex))
      }
    }
    splitsByAddress.mapValues(_.toSeq).iterator
  }

  def validateStatus(status: ShuffleOutputStatus, shuffleId: Int, partition: Int) : Unit = {
    if (status == null) {
      val errorMessage = s"Missing an output location for shuffle $shuffleId partition $partition"
      logError(errorMessage)
      throw new MetadataFetchFailedException(shuffleId, partition, errorMessage)
    }
  }
}
