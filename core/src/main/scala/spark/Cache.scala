package spark

import java.util.concurrent.atomic.AtomicInteger

/**
 * An interface for caches in Spark, to allow for multiple implementations. Caches are used to store
 * both partitions of cached RDDs and broadcast variables on Spark executors. Caches are also aware
 * of which entries are part of the same dataset (for example, partitions in the same RDD). The key
 * for each value in a cache is a (datasetID, partition) pair.
 *
 * A single Cache instance gets created on each machine and is shared by all caches (i.e. both the 
 * RDD split cache and the broadcast variable cache), to enable global replacement policies. 
 * However, because these several independent modules all perform caching, it is important to give
 * them separate key namespaces, so that an RDD and a broadcast variable (for example) do not use
 * the same key. For this purpose, Cache has the notion of KeySpaces. Each client module must first
 * ask for a KeySpace, and then call get() and put() on that space using its own keys.
 * 
 * This abstract class handles the creation of key spaces, so that subclasses need only deal with
 * keys that are unique across modules.
 */
abstract class Cache {
  private val nextKeySpaceId = new AtomicInteger(0)
  private def newKeySpaceId() = nextKeySpaceId.getAndIncrement()

  def newKeySpace() = new KeySpace(this, newKeySpaceId())

  /**
   * Get the value for a given (datasetId, partition), or null if it is not
   * found.
   */
  def get(datasetId: Any, partition: Int): Any

  /**
   * Attempt to put a value in the cache; returns a negative number if this was
   * not successful (e.g. because the cache replacement policy forbids it). If
   * size estimation is available, the cache implementation should return the
   * estimated size of the partition if the partition is successfully cached.
   */
  def put(datasetId: Any, partition: Int, value: Any): Long

  /**
   * Report the capacity of the cache partition. By default this just reports
   * zero. Specific implementations can choose to provide the capacity number.
   */
  def getCapacity: Long = 0L
}

/**
 * A key namespace in a Cache.
 */
class KeySpace(cache: Cache, val keySpaceId: Int) {
  def get(datasetId: Any, partition: Int): Any =
    cache.get((keySpaceId, datasetId), partition)

  def put(datasetId: Any, partition: Int, value: Any): Long =
    cache.put((keySpaceId, datasetId), partition, value)

  def getCapacity: Long = cache.getCapacity
}
