package org.apache.spark.shuffle.hash

import org.apache.spark.{Aggregator, Partitioner, SparkConf}
import org.apache.spark.shuffle.{ShuffleReader, ShuffleWriter, ShuffleHandle, ShuffleManager}
import org.apache.spark.serializer.Serializer

/**
 * A ShuffleManager using the hash-based implementation available up to and including Spark 1.0.
 */
class HashShuffleManager(conf: SparkConf) extends ShuffleManager {
  /* Register a shuffle with the manager and obtain a handle for it to pass to tasks. */
  override def registerShuffle[K, V, C](
      shuffleId: Int, numMaps: Int,
      partitioner: Partitioner,
      serializer: Option[Serializer],
      keyOrdering: Option[Ordering[K]],
      aggregator: Option[Aggregator[K, V, C]]): ShuffleHandle = ???

  /**
   * Get a reader for a range of reduce partitions (startPartition to endPartition-1, inclusive).
   * Called on executors by reduce tasks.
   */
  override def getReader[K, C](handle: ShuffleHandle, startPartition: Int, endPartition: Int)
      : ShuffleReader[K, C] = ???

  /** Get a writer for a given partition. Called on executors by map tasks. */
  override def getWriter[K, V](handle: ShuffleHandle, mapId: Int): ShuffleWriter[K, V] = ???

  /** Remove a shuffle's metadata from the ShuffleManager. */
  override def unregisterShuffle(shuffleId: Int): Unit = ???

  /** Shut down this ShuffleManager. */
  override def stop(): Unit = ???
}
