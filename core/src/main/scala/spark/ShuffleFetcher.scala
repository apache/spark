package spark

private[spark] abstract class ShuffleFetcher {
  // Fetch the shuffle outputs for a given ShuffleDependency, calling func exactly
  // once on each key-value pair obtained.
  def fetch[K, V](shuffleId: Int, reduceId: Int, func: (K, V) => Unit)

  // Stop the fetcher
  def stop() {}
}
