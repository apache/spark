package spark

import java.io._
import java.util.concurrent.ConcurrentHashMap

import akka.actor._
import akka.dispatch._
import akka.pattern.ask
import akka.remote._
import akka.util.Duration
import akka.util.Timeout
import akka.util.duration._

import scala.collection.mutable.HashMap
import scala.collection.mutable.HashSet

import scheduler.MapStatus
import spark.storage.BlockManagerId
import java.util.zip.{GZIPInputStream, GZIPOutputStream}

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

private[spark] class MapOutputTracker(actorSystem: ActorSystem, isMaster: Boolean) extends Logging {
  val ip: String = System.getProperty("spark.master.host", "localhost")
  val port: Int = System.getProperty("spark.master.port", "7077").toInt
  val actorName: String = "MapOutputTracker"

  val timeout = 10.seconds

  var mapStatuses = new ConcurrentHashMap[Int, Array[MapStatus]]

  // Incremented every time a fetch fails so that client nodes know to clear
  // their cache of map output locations if this happens.
  private var generation: Long = 0
  private val generationLock = new java.lang.Object

  // Cache a serialized version of the output statuses for each shuffle to send them out faster
  var cacheGeneration = generation
  val cachedSerializedStatuses = new HashMap[Int, Array[Byte]]

  var trackerActor: ActorRef = if (isMaster) {
    val actor = actorSystem.actorOf(Props(new MapOutputTrackerActor(this)), name = actorName)
    logInfo("Registered MapOutputTrackerActor actor")
    actor
  } else {
    val url = "akka://spark@%s:%s/user/%s".format(ip, port, actorName)
    actorSystem.actorFor(url)
  }

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
    if (mapStatuses.get(shuffleId) != null) {
      throw new IllegalArgumentException("Shuffle ID " + shuffleId + " registered twice")
    }
    mapStatuses.put(shuffleId, new Array[MapStatus](numMaps))
  }
  
  def registerMapOutput(shuffleId: Int, mapId: Int, status: MapStatus) {
    var array = mapStatuses.get(shuffleId)
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
    var array = mapStatuses.get(shuffleId)
    if (array != null) {
      array.synchronized {
        if (array(mapId).address == bmAddress) {
          array(mapId) = null
        }
      }
      incrementGeneration()
    } else {
      throw new SparkException("unregisterMapOutput called for nonexistent shuffle ID")
    }
  }
  
  // Remembers which map output locations are currently being fetched on a worker
  val fetching = new HashSet[Int]
  
  // Called on possibly remote nodes to get the server URIs and output sizes for a given shuffle
  def getServerStatuses(shuffleId: Int, reduceId: Int): Array[(BlockManagerId, Long)] = {
    val statuses = mapStatuses.get(shuffleId)
    if (statuses == null) {
      logInfo("Don't have map outputs for shuffle " + shuffleId + ", fetching them")
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
          return mapStatuses.get(shuffleId).map(status =>
            (status.address, MapOutputTracker.decompressSize(status.compressedSizes(reduceId))))
        } else {
          fetching += shuffleId
        }
      }
      // We won the race to fetch the output locs; do so
      logInfo("Doing the fetch; tracker actor = " + trackerActor)
      val host = System.getProperty("spark.hostname", Utils.localHostName)
      val fetchedBytes = askTracker(GetMapOutputStatuses(shuffleId, host)).asInstanceOf[Array[Byte]]
      val fetchedStatuses = deserializeStatuses(fetchedBytes)
      
      logInfo("Got the output locations")
      mapStatuses.put(shuffleId, fetchedStatuses)
      fetching.synchronized {
        fetching -= shuffleId
        fetching.notifyAll()
      }
      return fetchedStatuses.map(s =>
        (s.address, MapOutputTracker.decompressSize(s.compressedSizes(reduceId))))
    } else {
      return statuses.map(s =>
        (s.address, MapOutputTracker.decompressSize(s.compressedSizes(reduceId))))
    }
  }

  def stop() {
    communicate(StopMapOutputTracker)
    mapStatuses.clear()
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
        mapStatuses = new ConcurrentHashMap[Int, Array[MapStatus]]
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
          statuses = mapStatuses.get(shuffleId)
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
  def serializeStatuses(statuses: Array[MapStatus]): Array[Byte] = {
    val out = new ByteArrayOutputStream
    val objOut = new ObjectOutputStream(new GZIPOutputStream(out))
    objOut.writeObject(statuses)
    objOut.close()
    out.toByteArray
  }

  // Opposite of serializeStatuses.
  def deserializeStatuses(bytes: Array[Byte]): Array[MapStatus] = {
    val objIn = new ObjectInputStream(new GZIPInputStream(new ByteArrayInputStream(bytes)))
    objIn.readObject().asInstanceOf[Array[MapStatus]]
  }
}

private[spark] object MapOutputTracker {
  private val LOG_BASE = 1.1

  /**
   * Compress a size in bytes to 8 bits for efficient reporting of map output sizes.
   * We do this by encoding the log base 1.1 of the size as an integer, which can support
   * sizes up to 35 GB with at most 10% error.
   */
  def compressSize(size: Long): Byte = {
    if (size <= 1L) {
      0
    } else {
      math.min(255, math.ceil(math.log(size) / math.log(LOG_BASE)).toInt).toByte
    }
  }

  /**
   * Decompress an 8-bit encoded block size, using the reverse operation of compressSize.
   */
  def decompressSize(compressedSize: Byte): Long = {
    if (compressedSize == 0) {
      1
    } else {
      math.pow(LOG_BASE, (compressedSize & 0xFF)).toLong
    }
  }
}
