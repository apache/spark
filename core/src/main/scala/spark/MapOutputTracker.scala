package spark

import java.io._
import java.util.zip.{GZIPInputStream, GZIPOutputStream}

import scala.collection.mutable.HashMap
import scala.collection.mutable.HashSet

import akka.actor._
import akka.dispatch._
import akka.pattern.ask
import akka.remote._
import akka.util.Duration


import spark.scheduler.MapStatus
import spark.storage.BlockManagerId
import spark.util.{MetadataCleaner, TimeStampedHashMap}


private[spark] sealed trait MapOutputTrackerMessage
private[spark] case class GetMapOutputStatuses(shuffleId: Int, requester: String)
  extends MapOutputTrackerMessage
private[spark] case object StopMapOutputTracker extends MapOutputTrackerMessage

private[spark] class MapOutputTrackerActor(tracker: MapOutputTracker) extends Actor with Logging {
  def receive = {
    case GetMapOutputStatuses(shuffleId: Int, requester: String) =>
      logInfo("Asked to send map output locations for shuffle " + shuffleId + " to " + requester)
      sender ! tracker.getSerializedLocations(shuffleId)

    case StopMapOutputTracker =>
      logInfo("MapOutputTrackerActor stopped!")
      sender ! true
      context.stop(self)
  }
}

private[spark] class MapOutputTracker extends Logging {

  private val timeout = Duration.create(System.getProperty("spark.akka.askTimeout", "10").toLong, "seconds")
  
  // Set to the MapOutputTrackerActor living on the driver
  var trackerActor: ActorRef = _

  private var mapStatuses = new TimeStampedHashMap[Int, Array[MapStatus]]

  // Incremented every time a fetch fails so that client nodes know to clear
  // their cache of map output locations if this happens.
  private var generation: Long = 0
  private val generationLock = new java.lang.Object

  // Cache a serialized version of the output statuses for each shuffle to send them out faster
  var cacheGeneration = generation
  private val cachedSerializedStatuses = new TimeStampedHashMap[Int, Array[Byte]]

  val metadataCleaner = new MetadataCleaner("MapOutputTracker", this.cleanup)

  // Send a message to the trackerActor and get its result within a default timeout, or
  // throw a SparkException if this fails.
  def askTracker(message: Any): Any = {
    try {
      val future = trackerActor.ask(message)(timeout)
      return Await.result(future, timeout)
    } catch {
      case e: Exception =>
        throw new SparkException("Error communicating with MapOutputTracker", e)
    }
  }

  // Send a one-way message to the trackerActor, to which we expect it to reply with true.
  def communicate(message: Any) {
    if (askTracker(message) != true) {
      throw new SparkException("Error reply received from MapOutputTracker")
    }
  }

  def registerShuffle(shuffleId: Int, numMaps: Int) {
    if (mapStatuses.putIfAbsent(shuffleId, new Array[MapStatus](numMaps)).isDefined) {
      throw new IllegalArgumentException("Shuffle ID " + shuffleId + " registered twice")
    }
  }

  def registerMapOutput(shuffleId: Int, mapId: Int, status: MapStatus) {
    var array = mapStatuses(shuffleId)
    array.synchronized {
      array(mapId) = status
    }
  }

  def registerMapOutputs(
      shuffleId: Int,
      statuses: Array[MapStatus],
      changeGeneration: Boolean = false) {
    mapStatuses.put(shuffleId, Array[MapStatus]() ++ statuses)
    if (changeGeneration) {
      incrementGeneration()
    }
  }

  def unregisterMapOutput(shuffleId: Int, mapId: Int, bmAddress: BlockManagerId) {
    var arrayOpt = mapStatuses.get(shuffleId)
    if (arrayOpt.isDefined && arrayOpt.get != null) {
      var array = arrayOpt.get
      array.synchronized {
        if (array(mapId) != null && array(mapId).location == bmAddress) {
          array(mapId) = null
        }
      }
      incrementGeneration()
    } else {
      throw new SparkException("unregisterMapOutput called for nonexistent shuffle ID")
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
        val hostPort = Utils.localHostPort()
        // This try-finally prevents hangs due to timeouts:
        try {
          val fetchedBytes =
            askTracker(GetMapOutputStatuses(shuffleId, hostPort)).asInstanceOf[Array[Byte]]
          fetchedStatuses = deserializeStatuses(fetchedBytes)
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
      else{
        throw new FetchFailedException(null, shuffleId, -1, reduceId,
          new Exception("Missing all output locations for shuffle " + shuffleId))
      }      
    } else {
      statuses.synchronized {
        return MapOutputTracker.convertMapStatuses(shuffleId, reduceId, statuses)
      }
    }
  }

  private def cleanup(cleanupTime: Long) {
    mapStatuses.clearOldValues(cleanupTime)
    cachedSerializedStatuses.clearOldValues(cleanupTime)
  }

  def stop() {
    communicate(StopMapOutputTracker)
    mapStatuses.clear()
    metadataCleaner.cancel()
    trackerActor = null
  }

  // Called on master to increment the generation number
  def incrementGeneration() {
    generationLock.synchronized {
      generation += 1
      logDebug("Increasing generation to " + generation)
    }
  }

  // Called on master or workers to get current generation number
  def getGeneration: Long = {
    generationLock.synchronized {
      return generation
    }
  }

  // Called on workers to update the generation number, potentially clearing old outputs
  // because of a fetch failure. (Each Mesos task calls this with the latest generation
  // number on the master at the time it was created.)
  def updateGeneration(newGen: Long) {
    generationLock.synchronized {
      if (newGen > generation) {
        logInfo("Updating generation to " + newGen + " and clearing cache")
        // mapStatuses = new TimeStampedHashMap[Int, Array[MapStatus]]
        mapStatuses.clear()
        generation = newGen
      }
    }
  }

  def getSerializedLocations(shuffleId: Int): Array[Byte] = {
    var statuses: Array[MapStatus] = null
    var generationGotten: Long = -1
    generationLock.synchronized {
      if (generation > cacheGeneration) {
        cachedSerializedStatuses.clear()
        cacheGeneration = generation
      }
      cachedSerializedStatuses.get(shuffleId) match {
        case Some(bytes) =>
          return bytes
        case None =>
          statuses = mapStatuses(shuffleId)
          generationGotten = generation
      }
    }
    // If we got here, we failed to find the serialized locations in the cache, so we pulled
    // out a snapshot of the locations as "locs"; let's serialize and return that
    val bytes = serializeStatuses(statuses)
    logInfo("Size of output statuses for shuffle %d is %d bytes".format(shuffleId, bytes.length))
    // Add them into the table only if the generation hasn't changed while we were working
    generationLock.synchronized {
      if (generation == generationGotten) {
        cachedSerializedStatuses(shuffleId) = bytes
      }
    }
    return bytes
  }

  // Serialize an array of map output locations into an efficient byte format so that we can send
  // it to reduce tasks. We do this by compressing the serialized bytes using GZIP. They will
  // generally be pretty compressible because many map outputs will be on the same hostname.
  private def serializeStatuses(statuses: Array[MapStatus]): Array[Byte] = {
    val out = new ByteArrayOutputStream
    val objOut = new ObjectOutputStream(new GZIPOutputStream(out))
    // Since statuses can be modified in parallel, sync on it
    statuses.synchronized {
      objOut.writeObject(statuses)
    }
    objOut.close()
    out.toByteArray
  }

  // Opposite of serializeStatuses.
  def deserializeStatuses(bytes: Array[Byte]): Array[MapStatus] = {
    val objIn = new ObjectInputStream(new GZIPInputStream(new ByteArrayInputStream(bytes)))
    objIn.readObject().
      // // drop all null's from status - not sure why they are occuring though. Causes NPE downstream in slave if present
      // comment this out - nulls could be due to missing location ? 
      asInstanceOf[Array[MapStatus]] // .filter( _ != null )
  }
}

private[spark] object MapOutputTracker {
  private val LOG_BASE = 1.1

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
