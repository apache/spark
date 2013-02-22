package spark

import executor.TaskMetrics

private[spark] abstract class ShuffleFetcher {
  /**
   * Fetch the shuffle outputs for a given ShuffleDependency.
   * @return An iterator over the elements of the fetched shuffle outputs.
   */
  def fetch[K, V](shuffleId: Int, reduceId: Int, metrics: TaskMetrics) : Iterator[(K,V)]

  /** Stop the fetcher */
  def stop() {}
}
