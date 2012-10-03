package spark

import java.io.{DataInputStream, DataOutputStream, ByteArrayOutputStream, ByteArrayInputStream}
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

import spark.storage.BlockManagerId

private[spark] sealed trait MapOutputTrackerMessage
private[spark] case class GetMapOutputLocations(shuffleId: Int) extends MapOutputTrackerMessage 
private[spark] case object StopMapOutputTracker extends MapOutputTrackerMessage

private[spark] class MapOutputTrackerActor(tracker: MapOutputTracker) extends Actor with Logging {
  def receive = {
    case GetMapOutputLocations(shuffleId: Int) =>
      logInfo("Asked to get map output locations for shuffle " + shuffleId)
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

  var bmAddresses = new ConcurrentHashMap[Int, Array[BlockManagerId]]

  // Incremented every time a fetch fails so that client nodes know to clear
  // their cache of map output locations if this happens.
  private var generation: Long = 0
  private var generationLock = new java.lang.Object

  // Cache a serialized version of the output locations for each shuffle to send them out faster
  var cacheGeneration = generation
  val cachedSerializedLocs = new HashMap[Int, Array[Byte]]

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
    if (bmAddresses.get(shuffleId) != null) {
      throw new IllegalArgumentException("Shuffle ID " + shuffleId + " registered twice")
    }
    bmAddresses.put(shuffleId, new Array[BlockManagerId](numMaps))
  }
  
  def registerMapOutput(shuffleId: Int, mapId: Int, bmAddress: BlockManagerId) {
    var array = bmAddresses.get(shuffleId)
    array.synchronized {
      array(mapId) = bmAddress
    }
  }
  
  def registerMapOutputs(shuffleId: Int, locs: Array[BlockManagerId], changeGeneration: Boolean = false) {
    bmAddresses.put(shuffleId, Array[BlockManagerId]() ++ locs)
    if (changeGeneration) {
      incrementGeneration()
    }
  }

  def unregisterMapOutput(shuffleId: Int, mapId: Int, bmAddress: BlockManagerId) {
    var array = bmAddresses.get(shuffleId)
    if (array != null) {
      array.synchronized {
        if (array(mapId) == bmAddress) {
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
  
  // Called on possibly remote nodes to get the server URIs for a given shuffle
  def getServerAddresses(shuffleId: Int): Array[BlockManagerId] = {
    val locs = bmAddresses.get(shuffleId)
    if (locs == null) {
      logInfo("Don't have map outputs for shuffle " + shuffleId + ", fetching them")
      fetching.synchronized {
        if (fetching.contains(shuffleId)) {
          // Someone else is fetching it; wait for them to be done
          while (fetching.contains(shuffleId)) {
            try {
              fetching.wait()
            } catch {
              case _ =>
            }
          }
          return bmAddresses.get(shuffleId)
        } else {
          fetching += shuffleId
        }
      }
      // We won the race to fetch the output locs; do so
      logInfo("Doing the fetch; tracker actor = " + trackerActor)
      val fetchedBytes = askTracker(GetMapOutputLocations(shuffleId)).asInstanceOf[Array[Byte]]
      val fetchedLocs = deserializeLocations(fetchedBytes)
      
      logInfo("Got the output locations")
      bmAddresses.put(shuffleId, fetchedLocs)
      fetching.synchronized {
        fetching -= shuffleId
        fetching.notifyAll()
      }
      return fetchedLocs
    } else {
      return locs
    }
  }

  def stop() {
    communicate(StopMapOutputTracker)
    bmAddresses.clear()
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
        bmAddresses = new ConcurrentHashMap[Int, Array[BlockManagerId]]
        generation = newGen
      }
    }
  }

  def getSerializedLocations(shuffleId: Int): Array[Byte] = {
    var locs: Array[BlockManagerId] = null
    var generationGotten: Long = -1
    generationLock.synchronized {
      if (generation > cacheGeneration) {
        cachedSerializedLocs.clear()
        cacheGeneration = generation
      }
      cachedSerializedLocs.get(shuffleId) match {
        case Some(bytes) =>
          return bytes
        case None =>
          locs = bmAddresses.get(shuffleId)
          generationGotten = generation
      }
    }
    // If we got here, we failed to find the serialized locations in the cache, so we pulled
    // out a snapshot of the locations as "locs"; let's serialize and return that
    val bytes = serializeLocations(locs)
    // Add them into the table only if the generation hasn't changed while we were working
    generationLock.synchronized {
      if (generation == generationGotten) {
        cachedSerializedLocs(shuffleId) = bytes
      }
    }
    return bytes
  }

  // Serialize an array of map output locations into an efficient byte format so that we can send
  // it to reduce tasks. We do this by grouping together the locations by block manager ID.
  def serializeLocations(locs: Array[BlockManagerId]): Array[Byte] = {
    val out = new ByteArrayOutputStream
    val dataOut = new DataOutputStream(out)
    dataOut.writeInt(locs.length)
    val grouped = locs.zipWithIndex.groupBy(_._1)
    dataOut.writeInt(grouped.size)
    for ((id, pairs) <- grouped if id != null) {
      dataOut.writeUTF(id.ip)
      dataOut.writeInt(id.port)
      dataOut.writeInt(pairs.length)
      for ((_, blockIndex) <- pairs) {
        dataOut.writeInt(blockIndex)
      }
    }
    dataOut.close()
    out.toByteArray
  }

  // Opposite of serializeLocations.
  def deserializeLocations(bytes: Array[Byte]): Array[BlockManagerId] = {
    val dataIn = new DataInputStream(new ByteArrayInputStream(bytes))
    val length = dataIn.readInt()
    val array = new Array[BlockManagerId](length)
    val numGroups = dataIn.readInt()
    for (i <- 0 until numGroups) {
      val ip = dataIn.readUTF()
      val port = dataIn.readInt()
      val id = new BlockManagerId(ip, port)
      val numBlocks = dataIn.readInt()
      for (j <- 0 until numBlocks) {
        array(dataIn.readInt()) = id
      }
    }
    array
  }
}
