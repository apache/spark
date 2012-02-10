package spark

import java.util.concurrent.atomic.AtomicLong

/**
 * An interface for caches in Spark, to allow for multiple implementations. Caches are used to store
 * both partitions of cached RDDs and broadcast variables on Spark executors.
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
  private val nextKeySpaceId = new AtomicLong(0)
  private def newKeySpaceId() = nextKeySpaceId.getAndIncrement()

  def newKeySpace() = new KeySpace(this, newKeySpaceId())

  def get(key: Any): Any
  def put(key: Any, value: Any): Unit
}

/**
 * A key namespace in a Cache.
 */
class KeySpace(cache: Cache, id: Long) {
  def get(key: Any): Any = cache.get((id, key))
  def put(key: Any, value: Any): Unit = cache.put((id, key), value)
}
