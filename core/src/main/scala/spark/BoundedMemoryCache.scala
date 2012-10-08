package spark

import java.util.LinkedHashMap

/**
 * An implementation of Cache that estimates the sizes of its entries and attempts to limit its
 * total memory usage to a fraction of the JVM heap. Objects' sizes are estimated using
 * SizeEstimator, which has limitations; most notably, we will overestimate total memory used if
 * some cache entries have pointers to a shared object. Nonetheless, this Cache should work well
 * when most of the space is used by arrays of primitives or of simple classes.
 */
private[spark] class BoundedMemoryCache(maxBytes: Long) extends Cache with Logging {
  logInfo("BoundedMemoryCache.maxBytes = " + maxBytes)

  def this() {
    this(BoundedMemoryCache.getMaxBytes)
  }

  private var currentBytes = 0L
  private val map = new LinkedHashMap[(Any, Int), Entry](32, 0.75f, true)

  override def get(datasetId: Any, partition: Int): Any = {
    synchronized {
      val entry = map.get((datasetId, partition))
      if (entry != null) {
        entry.value
      } else {
        null
      }
    }
  }

  override def put(datasetId: Any, partition: Int, value: Any): CachePutResponse = {
    val key = (datasetId, partition)
    logInfo("Asked to add key " + key)
    val size = estimateValueSize(key, value)
    synchronized {
      if (size > getCapacity) {
        return CachePutFailure()
      } else if (ensureFreeSpace(datasetId, size)) {
        logInfo("Adding key " + key)
        map.put(key, new Entry(value, size))
        currentBytes += size
        logInfo("Number of entries is now " + map.size)
        return CachePutSuccess(size)
      } else {
        logInfo("Didn't add key " + key + " because we would have evicted part of same dataset")
        return CachePutFailure()
      }
    }
  }

  override def getCapacity: Long = maxBytes

  /**
   * Estimate sizeOf 'value'
   */
  private def estimateValueSize(key: (Any, Int), value: Any) = {
    val startTime = System.currentTimeMillis
    val size = SizeEstimator.estimate(value.asInstanceOf[AnyRef])
    val timeTaken = System.currentTimeMillis - startTime
    logInfo("Estimated size for key %s is %d".format(key, size))
    logInfo("Size estimation for key %s took %d ms".format(key, timeTaken))
    size
  }

  /**
   * Remove least recently used entries from the map until at least space bytes are free, in order
   * to make space for a partition from the given dataset ID. If this cannot be done without
   * evicting other data from the same dataset, returns false; otherwise, returns true. Assumes
   * that a lock is held on the BoundedMemoryCache.
   */
  private def ensureFreeSpace(datasetId: Any, space: Long): Boolean = {
    logInfo("ensureFreeSpace(%s, %d) called with curBytes=%d, maxBytes=%d".format(
      datasetId, space, currentBytes, maxBytes))
    val iter = map.entrySet.iterator   // Will give entries in LRU order
    while (maxBytes - currentBytes < space && iter.hasNext) {
      val mapEntry = iter.next()
      val (entryDatasetId, entryPartition) = mapEntry.getKey
      if (entryDatasetId == datasetId) {
        // Cannot make space without removing part of the same dataset, or a more recently used one
        return false
      }
      reportEntryDropped(entryDatasetId, entryPartition, mapEntry.getValue)
      currentBytes -= mapEntry.getValue.size
      iter.remove()
    }
    return true
  }

  protected def reportEntryDropped(datasetId: Any, partition: Int, entry: Entry) {
    logInfo("Dropping key (%s, %d) of size %d to make space".format(datasetId, partition, entry.size))
    // TODO: remove BoundedMemoryCache
    
    val (keySpaceId, innerDatasetId) = datasetId.asInstanceOf[(Any, Any)] 
    innerDatasetId match {
      case rddId: Int =>
        SparkEnv.get.cacheTracker.dropEntry(rddId, partition)
      case broadcastUUID: java.util.UUID =>
        // TODO: Maybe something should be done if the broadcasted variable falls out of cache  
      case _ => 
    }    
  }
}

// An entry in our map; stores a cached object and its size in bytes
private[spark] case class Entry(value: Any, size: Long)

private[spark] object BoundedMemoryCache {
  /**
   * Get maximum cache capacity from system configuration
   */
   def getMaxBytes: Long = {
    val memoryFractionToUse = System.getProperty("spark.boundedMemoryCache.memoryFraction", "0.66").toDouble
    (Runtime.getRuntime.maxMemory * memoryFractionToUse).toLong
  }
}

