package spark

import scala.actors._
import scala.actors.Actor._
import scala.actors.remote._

sealed trait CacheMessage
case class CacheEntryAdded(rddId: Int, partition: Int, host: String)
case class CacheEntryRemoved(rddId: Int, partition: Int, host: String)

class RDDCacheTracker extends DaemonActor with Logging {
  def act() {
    val port = System.getProperty("spark.master.port", "50501").toInt
    RemoteActor.alive(port)
    RemoteActor.register('RDDCacheTracker, self)
    logInfo("Started on port " + port)
    
    loop {
      react {
        case CacheEntryAdded(rddId, partition, host) =>
          logInfo("Cache entry added: %s, %s, %s".format(rddId, partition, host))
          
        case CacheEntryRemoved(rddId, partition, host) =>
          logInfo("Cache entry removed: %s, %s, %s".format(rddId, partition, host))
      }
    }
  }
}

import scala.collection.mutable.HashSet
private object RDDCache extends Logging {
  // Stores map results for various splits locally
  val cache = Cache.newKeySpace()

  // Remembers which splits are currently being loaded
  val loading = new HashSet[(Int, Int)]
  
  // Tracker actor on the master, or remote reference to it on workers
  var trackerActor: AbstractActor = null
  
  def initialize(isMaster: Boolean) {
    if (isMaster) {
      val tracker = new RDDCacheTracker
      tracker.start
      trackerActor = tracker
    } else {
      val host = System.getProperty("spark.master.host")
      val port = System.getProperty("spark.master.port").toInt
      trackerActor = RemoteActor.select(Node(host, port), 'RDDCacheTracker)
    }
  }
  
  // Gets or computes an RDD split
  def getOrCompute[T](rdd: RDD[T], split: Split)(implicit m: ClassManifest[T])
      : Iterator[T] = {
    val key = (rdd.id, split.index)
    logInfo("CachedRDD split key is " + key)
    val cache = RDDCache.cache
    val loading = RDDCache.loading
    val cachedVal = cache.get(key)
    if (cachedVal != null) {
      // Split is in cache, so just return its values
      return Iterator.fromArray(cachedVal.asInstanceOf[Array[T]])
    } else {
      // Mark the split as loading (unless someone else marks it first)
      loading.synchronized {
        if (loading.contains(key)) {
          while (loading.contains(key)) {
            try {loading.wait()} catch {case _ =>}
          }
          return Iterator.fromArray(cache.get(key).asInstanceOf[Array[T]])
        } else {
          loading.add(key)
        }
      }
      val host = System.getProperty("spark.hostname", Utils.localHostName)
      trackerActor ! CacheEntryAdded(rdd.id, split.index, host)
      // If we got here, we have to load the split
      // TODO: fetch any remote copy of the split that may be available
      // TODO: also notify the master that we're loading it
      // TODO: also register a listener for when it unloads
      logInfo("Computing and caching " + split)
      val array = rdd.compute(split).toArray(m)
      cache.put(key, array)
      loading.synchronized {
        loading.remove(key)
        loading.notifyAll()
      }
      return Iterator.fromArray(array)
    }
  }
}