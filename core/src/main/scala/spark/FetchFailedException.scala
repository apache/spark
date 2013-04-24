package spark

import spark.storage.BlockManagerId

private[spark] class FetchFailedException(
    taskEndReason: TaskEndReason,
    message: String,
    cause: Throwable)
  extends Exception {

  def this (bmAddress: BlockManagerId, shuffleId: Int, mapId: Int, reduceId: Int, cause: Throwable) =
    this(FetchFailed(bmAddress, shuffleId, mapId, reduceId),
      "Fetch failed: %s %d %d %d".format(bmAddress, shuffleId, mapId, reduceId),
      cause)

  def this (shuffleId: Int, reduceId: Int, cause: Throwable) =
    this(FetchFailed(null, shuffleId, -1, reduceId),
      "Unable to fetch locations from master: %d %d".format(shuffleId, reduceId), cause)

  override def getMessage(): String = message


  override def getCause(): Throwable = cause

  def toTaskEndReason: TaskEndReason = taskEndReason

}
