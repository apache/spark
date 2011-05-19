package spark

import scala.actors._
import scala.actors.Actor._
import scala.actors.remote._
import scala.collection.mutable.HashMap
import scala.collection.mutable.HashSet

sealed trait CacheTrackerMessage
case class AddedToCache(rddId: Int, partition: Int, host: String) extends CacheTrackerMessage
case class DroppedFromCache(rddId: Int, partition: Int, host: String) extends CacheTrackerMessage
case class MemoryCacheLost(host: String) extends CacheTrackerMessage
case class RegisterRDD(rddId: Int, numPartitions: Int) extends CacheTrackerMessage
case object GetCacheLocations extends CacheTrackerMessage
case object StopCacheTracker extends CacheTrackerMessage

class CacheTrackerActor extends DaemonActor with Logging {
  val locs = new HashMap[Int, Array[List[String]]]
  // TODO: Should probably store (String, CacheType) tuples
  
  def act() {
    val port = System.getProperty("spark.master.port").toInt
    RemoteActor.alive(port)
    RemoteActor.register('CacheTracker, self)
    logInfo("Registered actor on port " + port)
    
    loop {
      react {
        case RegisterRDD(rddId: Int, numPartitions: Int) =>
          logInfo("Registering RDD " + rddId + " with " + numPartitions + " partitions")
          locs(rddId) = Array.fill[List[String]](numPartitions)(Nil)
          reply('OK)
        
        case AddedToCache(rddId, partition, host) =>
          logInfo("Cache entry added: (%s, %s) on %s".format(rddId, partition, host))
          locs(rddId)(partition) = host :: locs(rddId)(partition)
          reply('OK)
          
        case DroppedFromCache(rddId, partition, host) =>
          logInfo("Cache entry removed: (%s, %s) on %s".format(rddId, partition, host))
          locs(rddId)(partition) -= host
        
        case MemoryCacheLost(host) =>
          logInfo("Memory cache lost on " + host)
          // TODO: Drop host from the memory locations list of all RDDs
        
        case GetCacheLocations =>
          logInfo("Asked for current cache locations")
          val locsCopy = new HashMap[Int, Array[List[String]]]
          for ((rddId, array) <- locs) {
            locsCopy(rddId) = array.clone()
          }
          reply(locsCopy)

        case StopCacheTracker =>
          reply('OK)
          exit()
      }
    }
  }
}

class CacheTracker(isMaster: Boolean, theCache: Cache) extends Logging {
  // Tracker actor on the master, or remote reference to it on workers
  var trackerActor: AbstractActor = null
  
  if (isMaster) {
    val tracker = new CacheTrackerActor
    tracker.start()
    trackerActor = tracker
  } else {
    val host = System.getProperty("spark.master.host")
    val port = System.getProperty("spark.master.port").toInt
    trackerActor = RemoteActor.select(Node(host, port), 'CacheTracker)
  }

  val registeredRddIds = new HashSet[Int]

  // Stores map results for various splits locally
  val cache = theCache.newKeySpace()

  // Remembers which splits are currently being loaded (on worker nodes)
  val loading = new HashSet[(Int, Int)]
  
  // Registers an RDD (on master only)
  def registerRDD(rddId: Int, numPartitions: Int) {
    registeredRddIds.synchronized {
      if (!registeredRddIds.contains(rddId)) {
        logInfo("Registering RDD ID " + rddId + " with cache")
        registeredRddIds += rddId
        trackerActor !? RegisterRDD(rddId, numPartitions)
      }
    }
  }
  
  // Get a snapshot of the currently known locations
  def getLocationsSnapshot(): HashMap[Int, Array[List[String]]] = {
    (trackerActor !? GetCacheLocations) match {
      case h: HashMap[Int, Array[List[String]]] => h
      case _ => throw new SparkException(
          "Internal error: CacheTrackerActor did not reply with a HashMap")
    }
  }
  
  // Gets or computes an RDD split
  def getOrCompute[T](rdd: RDD[T], split: Split)(implicit m: ClassManifest[T])
      : Iterator[T] = {
    val key = (rdd.id, split.index)
    logInfo("CachedRDD partition key is " + key)
    val cachedVal = cache.get(key)
    if (cachedVal != null) {
      // Split is in cache, so just return its values
      logInfo("Found partition in cache!")
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
      // If we got here, we have to load the split
      // Tell the master that we're doing so
      val host = System.getProperty("spark.hostname", Utils.localHostName)
      val future = trackerActor !! AddedToCache(rdd.id, split.index, host)
      // TODO: fetch any remote copy of the split that may be available
      // TODO: also register a listener for when it unloads
      logInfo("Computing partition " + split)
      val array = rdd.compute(split).toArray(m)
      cache.put(key, array)
      loading.synchronized {
        loading.remove(key)
        loading.notifyAll()
      }
      future.apply() // Wait for the reply from the cache tracker
      return Iterator.fromArray(array)
    }
  }

  def stop() {
    trackerActor !? StopCacheTracker
    registeredRddIds.clear()
    trackerActor = null
  }
}
