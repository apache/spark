package spark

import spark.executor.TaskMetrics
import spark.serializer.Serializer


private[spark] abstract class ShuffleFetcher {
  /**
   * Fetch the shuffle outputs for a given ShuffleDependency.
   * @return An iterator over the elements of the fetched shuffle outputs.
   */
  def fetch[K, V](shuffleId: Int, reduceId: Int, metrics: TaskMetrics,
    serializer: Serializer = SparkEnv.get.serializerManager.default): Iterator[(K,V)]

  /** Stop the fetcher */
  def stop() {}
}
