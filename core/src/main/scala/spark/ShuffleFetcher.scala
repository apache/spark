package spark

trait ShuffleFetcher {
  // Fetch the shuffle outputs for a given ShuffleDependency, calling func exactly
  // once on each key-value pair obtained.
  def fetch[K, V](shuffleId: Int, reduceId: Int, func: (K, V) => Unit)
}

class FetchFailedException(val serverUri: String, val shuffleId: Int,
  val mapId: Int, val reduceId: Int, cause: Throwable)
extends Exception {
  override def getMessage(): String = 
    "Fetch failed: %s %d %d %d".format(serverUri, shuffleId, mapId, reduceId)

  override def getCause(): Throwable = cause

  def toTaskEndReason: TaskEndReason =
    FetchFailed(serverUri, shuffleId, mapId, reduceId)
}
