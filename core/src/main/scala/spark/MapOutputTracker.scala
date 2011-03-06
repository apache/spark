package spark

import java.util.concurrent.ConcurrentHashMap

import scala.actors._
import scala.actors.Actor._
import scala.actors.remote._

class MapOutputTracker extends DaemonActor with Logging {
  def act() {
    val port = System.getProperty("spark.master.port", "50501").toInt
    RemoteActor.alive(port)
    RemoteActor.register('MapOutputTracker, self)
    logInfo("Registered actor on port " + port)
  }
}

object MapOutputTracker {
  var trackerActor: AbstractActor = null
  
  def initialize(isMaster: Boolean) {
    if (isMaster) {
      val tracker = new MapOutputTracker
      tracker.start
      trackerActor = tracker
    } else {
      val host = System.getProperty("spark.master.host")
      val port = System.getProperty("spark.master.port").toInt
      trackerActor = RemoteActor.select(Node(host, port), 'MapOutputTracker)
    }
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
  
  def getServerUris(shuffleId: Int): Array[String] = {
    // TODO: On remote node, fetch locations from master
    serverUris.get(shuffleId)
  }
  
  def getMapOutputUri(serverUri: String, shuffleId: Int, mapId: Int, reduceId: Int): String = {
    "%s/shuffle/%s/%s/%s".format(serverUri, shuffleId, mapId, reduceId)
  }
}