package spark

import spark.storage.BlockManagerId

private[spark] class FetchFailedException(
    val bmAddress: BlockManagerId,
    val shuffleId: Int,
    val mapId: Int,
    val reduceId: Int,
    cause: Throwable)
  extends Exception {
  
  override def getMessage(): String = 
    "Fetch failed: %s %d %d %d".format(bmAddress, shuffleId, mapId, reduceId)

  override def getCause(): Throwable = cause

  def toTaskEndReason: TaskEndReason =
    FetchFailed(bmAddress, shuffleId, mapId, reduceId)
}
