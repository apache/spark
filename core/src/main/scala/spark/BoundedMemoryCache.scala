package spark

import java.util.LinkedHashMap

/**
 * An implementation of Cache that estimates the sizes of its entries and
 * attempts to limit its total memory usage to a fraction of the JVM heap.
 * Objects' sizes are estimated using SizeEstimator, which has limitations;
 * most notably, we will overestimate total memory used if some cache
 * entries have pointers to a shared object. Nonetheless, this Cache should
 * work well when most of the space is used by arrays of primitives or of
 * simple classes.
 */
class BoundedMemoryCache extends Cache with Logging {
  private val maxBytes: Long = getMaxBytes()
  logInfo("BoundedMemoryCache.maxBytes = " + maxBytes)

  private var currentBytes = 0L
  private val map = new LinkedHashMap[Any, Entry](32, 0.75f, true)

  // An entry in our map; stores a cached object and its size in bytes
  class Entry(val value: Any, val size: Long) {}

  override def get(key: Any): Any = {
    synchronized {
      val entry = map.get(key)
      if (entry != null) entry.value else null
    }
  }

  override def put(key: Any, value: Any) {
    logInfo("Asked to add key " + key)
    val startTime = System.currentTimeMillis
    val size = SizeEstimator.estimate(value.asInstanceOf[AnyRef])
    val timeTaken = System.currentTimeMillis - startTime
    logInfo("Estimated size for key %s is %d".format(key, size))
    logInfo("Size estimation for key %s took %d ms".format(key, timeTaken))
    synchronized {
      ensureFreeSpace(size)
      logInfo("Adding key " + key)
      map.put(key, new Entry(value, size))
      currentBytes += size
      logInfo("Number of entries is now " + map.size)
    }
  }

  private def getMaxBytes(): Long = {
    val memoryFractionToUse = System.getProperty(
      "spark.boundedMemoryCache.memoryFraction", "0.66").toDouble
    (Runtime.getRuntime.totalMemory * memoryFractionToUse).toLong
  }

  /**
   * Remove least recently used entries from the map until at least space
   * bytes are free. Assumes that a lock is held on the BoundedMemoryCache.
   */
  private def ensureFreeSpace(space: Long) {
    logInfo("ensureFreeSpace(%d) called with curBytes=%d, maxBytes=%d".format(
      space, currentBytes, maxBytes))
    val iter = map.entrySet.iterator
    while (maxBytes - currentBytes < space && iter.hasNext) {
      val mapEntry = iter.next()
      dropEntry(mapEntry.getKey, mapEntry.getValue)
      currentBytes -= mapEntry.getValue.size
      iter.remove()
    }
  }

  protected def dropEntry(key: Any, entry: Entry) {
    logInfo("Dropping key %s of size %d to make space".format(
      key, entry.size))
  }
}
