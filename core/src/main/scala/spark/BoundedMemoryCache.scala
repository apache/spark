package spark

import java.util.LinkedHashMap

/**
 * An implementation of Cache that estimates the sizes of its entries and attempts to limit its
 * total memory usage to a fraction of the JVM heap. Objects' sizes are estimated using
 * SizeEstimator, which has limitations; most notably, we will overestimate total memory used if
 * some cache entries have pointers to a shared object. Nonetheless, this Cache should work well
 * when most of the space is used by arrays of primitives or of simple classes.
 */
class BoundedMemoryCache extends Cache with Logging {
  private val maxBytes: Long = getMaxBytes()
  logInfo("BoundedMemoryCache.maxBytes = " + maxBytes)

  private var currentBytes = 0L
  private val map = new LinkedHashMap[(Any, Int), Entry](32, 0.75f, true)

  // An entry in our map; stores a cached object and its size in bytes
  class Entry(val value: Any, val size: Long) {}

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

  override def put(datasetId: Any, partition: Int, value: Any): Long = {
    val key = (datasetId, partition)
    logInfo("Asked to add key " + key)
    val startTime = System.currentTimeMillis
    val size = SizeEstimator.estimate(value.asInstanceOf[AnyRef])
    val timeTaken = System.currentTimeMillis - startTime
    logInfo("Estimated size for key %s is %d".format(key, size))
    logInfo("Size estimation for key %s took %d ms".format(key, timeTaken))
    synchronized {
      if (ensureFreeSpace(datasetId, size)) {
        logInfo("Adding key " + key)
        map.put(key, new Entry(value, size))
        currentBytes += size
        logInfo("Number of entries is now " + map.size)
        return size
      } else {
        logInfo("Didn't add key " + key + " because we would have evicted part of same dataset")
        return -1L
      }
    }
  }

  override def getCapacity: Long = maxBytes

  private def getMaxBytes(): Long = {
    val memoryFractionToUse = System.getProperty(
      "spark.boundedMemoryCache.memoryFraction", "0.66").toDouble
    (Runtime.getRuntime.maxMemory * memoryFractionToUse).toLong
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
    logInfo("Dropping key (%s, %d) of size %d to make space".format(
      datasetId, partition, entry.size))
    SparkEnv.get.cacheTracker.dropEntry(datasetId, partition)
  }
}
