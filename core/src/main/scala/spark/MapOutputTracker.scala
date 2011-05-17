package spark

import java.util.concurrent.ConcurrentHashMap

import scala.actors._
import scala.actors.Actor._
import scala.actors.remote._
import scala.collection.mutable.HashSet

sealed trait MapOutputTrackerMessage
case class GetMapOutputLocations(shuffleId: Int) extends MapOutputTrackerMessage 
case object StopMapOutputTracker extends MapOutputTrackerMessage

class MapOutputTrackerActor(serverUris: ConcurrentHashMap[Int, Array[String]])
extends DaemonActor with Logging {
  def act() {
    val port = System.getProperty("spark.master.port").toInt
    RemoteActor.alive(port)
    RemoteActor.register('MapOutputTracker, self)
    logInfo("Registered actor on port " + port)
    
    loop {
      react {
        case GetMapOutputLocations(shuffleId: Int) =>
          logInfo("Asked to get map output locations for shuffle " + shuffleId)
          reply(serverUris.get(shuffleId))
        case StopMapOutputTracker =>
          reply('OK)
          exit()
      }
    }
  }
}

class MapOutputTracker(isMaster: Boolean) extends Logging {
  var trackerActor: AbstractActor = null
  
  if (isMaster) {
    val tracker = new MapOutputTrackerActor(serverUris)
    tracker.start
    trackerActor = tracker
  } else {
    val host = System.getProperty("spark.master.host")
    val port = System.getProperty("spark.master.port").toInt
    trackerActor = RemoteActor.select(Node(host, port), 'MapOutputTracker)
  }

  private val serverUris = new ConcurrentHashMap[Int, Array[String]]
  
  def registerMapOutput(shuffleId: Int, numMaps: Int, mapId: Int, serverUri: String) {
    var array = serverUris.get(shuffleId)
    if (array == null) {
      array = Array.fill[String](numMaps)(null)
      serverUris.put(shuffleId, array)
    }
    array(mapId) = serverUri
  }
  
  def registerMapOutputs(shuffleId: Int, locs: Array[String]) {
    serverUris.put(shuffleId, Array[String]() ++ locs)
  }
  
  // Remembers which map output locations are currently being fetched
  val fetching = new HashSet[Int]
  
  def getServerUris(shuffleId: Int): Array[String] = {
    // TODO: On remote node, fetch locations from master
    val locs = serverUris.get(shuffleId)
    if (locs == null) {
      logInfo("Don't have map outputs for " + shuffleId + ", fetching them")
      fetching.synchronized {
        if (fetching.contains(shuffleId)) {
          // Someone else is fetching it; wait for them to be done
          while (fetching.contains(shuffleId)) {
            try {fetching.wait()} catch {case _ =>}
          }
          return serverUris.get(shuffleId)
        } else {
          fetching += shuffleId
        }
      }
      // We won the race to fetch the output locs; do so
      logInfo("Doing the fetch; tracker actor = " + trackerActor)
      val fetched = (trackerActor !? GetMapOutputLocations(shuffleId)).asInstanceOf[Array[String]]
      serverUris.put(shuffleId, fetched)
      fetching.synchronized {
        fetching -= shuffleId
        fetching.notifyAll()
      }
      return fetched
    } else {
      return locs
    }
  }
  
  def getMapOutputUri(serverUri: String, shuffleId: Int, mapId: Int, reduceId: Int): String = {
    "%s/shuffle/%s/%s/%s".format(serverUri, shuffleId, mapId, reduceId)
  }

  def stop() {
    trackerActor !? StopMapOutputTracker
    serverUris.clear()
    trackerActor = null
  }
}
