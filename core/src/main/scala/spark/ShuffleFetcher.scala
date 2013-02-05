package spark

import util.TimedIterator

private[spark] abstract class ShuffleFetcher {
  /**
   * Fetch the shuffle outputs for a given ShuffleDependency.
   * @return An iterator over the elements of the fetched shuffle outputs.
   */
  def fetch[K, V](shuffleId: Int, reduceId: Int) : TimedIterator[(K, V)]

  /** Stop the fetcher */
  def stop() {}
}
