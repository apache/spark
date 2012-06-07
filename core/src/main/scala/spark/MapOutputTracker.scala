package spark

import java.util.concurrent.ConcurrentHashMap

import akka.actor._
import akka.actor.Actor
import akka.actor.Actor._
import akka.util.duration._

import scala.collection.mutable.HashSet

import spark.storage.BlockManagerId

sealed trait MapOutputTrackerMessage
case class GetMapOutputLocations(shuffleId: Int) extends MapOutputTrackerMessage 
case object StopMapOutputTracker extends MapOutputTrackerMessage

class MapOutputTrackerActor(bmAddresses: ConcurrentHashMap[Int, Array[BlockManagerId]]) 
extends Actor with Logging {
  def receive = {
    case GetMapOutputLocations(shuffleId: Int) =>
      logInfo("Asked to get map output locations for shuffle " + shuffleId)
      self.reply(bmAddresses.get(shuffleId))

    case StopMapOutputTracker =>
      logInfo("MapOutputTrackerActor stopped!")
      self.reply(true)
      self.exit()
  }
}

class MapOutputTracker(isMaster: Boolean) extends Logging {
  val ip: String = System.getProperty("spark.master.host", "localhost")
  val port: Int = System.getProperty("spark.master.port", "7077").toInt
  val aName: String = "MapOutputTracker"

  private var bmAddresses = new ConcurrentHashMap[Int, Array[BlockManagerId]]

  // Incremented every time a fetch fails so that client nodes know to clear
  // their cache of map output locations if this happens.
  private var generation: Long = 0
  private var generationLock = new java.lang.Object

  var trackerActor: ActorRef = if (isMaster) {
    val actor = actorOf(new MapOutputTrackerActor(bmAddresses))
    remote.register(aName, actor)
    logInfo("Registered MapOutputTrackerActor actor @ " + ip + ":" + port)
    actor
  } else {
    remote.actorFor(aName, ip, port)
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
      logInfo("Don't have map outputs for shuffe " + shuffleId + ", fetching them")
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
      val fetched = (trackerActor ? GetMapOutputLocations(shuffleId)).as[Array[BlockManagerId]].get
      
      logInfo("Got the output locations")
      bmAddresses.put(shuffleId, fetched)
      fetching.synchronized {
        fetching -= shuffleId
        fetching.notifyAll()
      }
      return fetched
    } else {
      return locs
    }
  }

  def stop() {
    trackerActor !! StopMapOutputTracker
    bmAddresses.clear()
    trackerActor = null
  }

  // Called on master to increment the generation number
  def incrementGeneration() {
    generationLock.synchronized {
      generation += 1
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
}
