package spark

import java.util.concurrent.ConcurrentHashMap

object MapOutputTracker {
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