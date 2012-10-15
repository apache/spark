package spark

import akka.actor._
import akka.dispatch._
import akka.pattern.ask
import akka.remote._
import akka.util.Duration
import akka.util.Timeout
import akka.util.duration._

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap
import scala.collection.mutable.HashSet

import spark.storage.BlockManager
import spark.storage.StorageLevel

private[spark] sealed trait CacheTrackerMessage

private[spark] case class AddedToCache(rddId: Int, partition: Int, host: String, size: Long = 0L)
  extends CacheTrackerMessage
private[spark] case class DroppedFromCache(rddId: Int, partition: Int, host: String, size: Long = 0L)
  extends CacheTrackerMessage
private[spark] case class MemoryCacheLost(host: String) extends CacheTrackerMessage
private[spark] case class RegisterRDD(rddId: Int, numPartitions: Int) extends CacheTrackerMessage
private[spark] case class SlaveCacheStarted(host: String, size: Long) extends CacheTrackerMessage
private[spark] case object GetCacheStatus extends CacheTrackerMessage
private[spark] case object GetCacheLocations extends CacheTrackerMessage
private[spark] case object StopCacheTracker extends CacheTrackerMessage

private[spark] class CacheTrackerActor extends Actor with Logging {
  // TODO: Should probably store (String, CacheType) tuples
  private val locs = new HashMap[Int, Array[List[String]]]

  /**
   * A map from the slave's host name to its cache size.
   */
  private val slaveCapacity = new HashMap[String, Long]
  private val slaveUsage = new HashMap[String, Long]

  private def getCacheUsage(host: String): Long = slaveUsage.getOrElse(host, 0L)
  private def getCacheCapacity(host: String): Long = slaveCapacity.getOrElse(host, 0L)
  private def getCacheAvailable(host: String): Long = getCacheCapacity(host) - getCacheUsage(host)
  
  def receive = {
    case SlaveCacheStarted(host: String, size: Long) =>
      slaveCapacity.put(host, size)
      slaveUsage.put(host, 0)
      sender ! true

    case RegisterRDD(rddId: Int, numPartitions: Int) =>
      logInfo("Registering RDD " + rddId + " with " + numPartitions + " partitions")
      locs(rddId) = Array.fill[List[String]](numPartitions)(Nil)
      sender ! true

    case AddedToCache(rddId, partition, host, size) =>
      slaveUsage.put(host, getCacheUsage(host) + size)
      locs(rddId)(partition) = host :: locs(rddId)(partition)
      sender ! true

    case DroppedFromCache(rddId, partition, host, size) =>
      slaveUsage.put(host, getCacheUsage(host) - size)
      // Do a sanity check to make sure usage is greater than 0.
      locs(rddId)(partition) = locs(rddId)(partition).filterNot(_ == host)
      sender ! true

    case MemoryCacheLost(host) =>
      logInfo("Memory cache lost on " + host)
      for ((id, locations) <- locs) {
        for (i <- 0 until locations.length) {
          locations(i) = locations(i).filterNot(_ == host)
        }
      }
      sender ! true

    case GetCacheLocations =>
      logInfo("Asked for current cache locations")
      sender ! locs.map{case (rrdId, array) => (rrdId -> array.clone())}

    case GetCacheStatus =>
      val status = slaveCapacity.map { case (host, capacity) =>
        (host, capacity, getCacheUsage(host))
      }.toSeq
      sender ! status

    case StopCacheTracker =>
      logInfo("Stopping CacheTrackerActor")
      sender ! true
      context.stop(self)
  }
}

private[spark] class CacheTracker(actorSystem: ActorSystem, isMaster: Boolean, blockManager: BlockManager)
  extends Logging {
 
  // Tracker actor on the master, or remote reference to it on workers
  val ip: String = System.getProperty("spark.master.host", "localhost")
  val port: Int = System.getProperty("spark.master.port", "7077").toInt
  val actorName: String = "CacheTracker"

  val timeout = 10.seconds
  
  var trackerActor: ActorRef = if (isMaster) {
    val actor = actorSystem.actorOf(Props[CacheTrackerActor], name = actorName)
    logInfo("Registered CacheTrackerActor actor")
    actor
  } else {
    val url = "akka://spark@%s:%s/user/%s".format(ip, port, actorName)
    actorSystem.actorFor(url)
  }

  val registeredRddIds = new HashSet[Int]

  // Remembers which splits are currently being loaded (on worker nodes)
  val loading = new HashSet[String]

  // Send a message to the trackerActor and get its result within a default timeout, or
  // throw a SparkException if this fails.
  def askTracker(message: Any): Any = {
    try {
      val future = trackerActor.ask(message)(timeout)
      return Await.result(future, timeout)
    } catch {
      case e: Exception =>
        throw new SparkException("Error communicating with CacheTracker", e)
    }
  }

  // Send a one-way message to the trackerActor, to which we expect it to reply with true.
  def communicate(message: Any) {
    if (askTracker(message) != true) {
      throw new SparkException("Error reply received from CacheTracker")
    }
  }
  
  // Registers an RDD (on master only)
  def registerRDD(rddId: Int, numPartitions: Int) {
    registeredRddIds.synchronized {
      if (!registeredRddIds.contains(rddId)) {
        logInfo("Registering RDD ID " + rddId + " with cache")
        registeredRddIds += rddId
        communicate(RegisterRDD(rddId, numPartitions))
      }
    }
  }
  
  // For BlockManager.scala only
  def cacheLost(host: String) {
    communicate(MemoryCacheLost(host))
    logInfo("CacheTracker successfully removed entries on " + host)
  }

  // Get the usage status of slave caches. Each tuple in the returned sequence
  // is in the form of (host name, capacity, usage).
  def getCacheStatus(): Seq[(String, Long, Long)] = {
    askTracker(GetCacheStatus).asInstanceOf[Seq[(String, Long, Long)]]
  }
  
  // For BlockManager.scala only
  def notifyFromBlockManager(t: AddedToCache) {
    communicate(t)
  }
  
  // Get a snapshot of the currently known locations
  def getLocationsSnapshot(): HashMap[Int, Array[List[String]]] = {
    askTracker(GetCacheLocations).asInstanceOf[HashMap[Int, Array[List[String]]]]
  }
  
  // Gets or computes an RDD split
  def getOrCompute[T](rdd: RDD[T], split: Split, storageLevel: StorageLevel): Iterator[T] = {
    val key = "rdd_%d_%d".format(rdd.id, split.index)
    logInfo("Cache key is " + key)
    blockManager.get(key) match {
      case Some(cachedValues) =>
        // Split is in cache, so just return its values
        logInfo("Found partition in cache!")
        return cachedValues.asInstanceOf[Iterator[T]]

      case None =>
        // Mark the split as loading (unless someone else marks it first)
        loading.synchronized {
          if (loading.contains(key)) {
            logInfo("Loading contains " + key + ", waiting...")
            while (loading.contains(key)) {
              try {loading.wait()} catch {case _ =>}
            }
            logInfo("Loading no longer contains " + key + ", so returning cached result")
            // See whether someone else has successfully loaded it. The main way this would fail
            // is for the RDD-level cache eviction policy if someone else has loaded the same RDD
            // partition but we didn't want to make space for it. However, that case is unlikely
            // because it's unlikely that two threads would work on the same RDD partition. One
            // downside of the current code is that threads wait serially if this does happen.
            blockManager.get(key) match {
              case Some(values) =>
                return values.asInstanceOf[Iterator[T]]
              case None =>
                logInfo("Whoever was loading " + key + " failed; we'll try it ourselves")
                loading.add(key)
            }
          } else {
            loading.add(key)
          }
        }
        // If we got here, we have to load the split
        // Tell the master that we're doing so
        //val host = System.getProperty("spark.hostname", Utils.localHostName)
        //val future = trackerActor !! AddedToCache(rdd.id, split.index, host)
        // TODO: fetch any remote copy of the split that may be available
        // TODO: also register a listener for when it unloads
        logInfo("Computing partition " + split)
        val elements = new ArrayBuffer[Any]
        elements ++= rdd.compute(split)
        try {
          // Try to put this block in the blockManager
          blockManager.put(key, elements, storageLevel, true)
          //future.apply() // Wait for the reply from the cache tracker
        } finally {
          loading.synchronized {
            loading.remove(key)
            loading.notifyAll()
          }
        }
        return elements.iterator.asInstanceOf[Iterator[T]]
    }
  }

  // Called by the Cache to report that an entry has been dropped from it
  def dropEntry(rddId: Int, partition: Int) {
    communicate(DroppedFromCache(rddId, partition, Utils.localHostName()))
  }

  def stop() {
    communicate(StopCacheTracker)
    registeredRddIds.clear()
    trackerActor = null
  }
}
