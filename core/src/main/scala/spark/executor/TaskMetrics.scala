package spark.executor

case class TaskMetrics(
  var shuffleReadMillis: Option[Long],
  var totalBlocksFetched : Option[Int],
  var remoteBlocksFetched: Option[Int],
  var localBlocksFetched: Option[Int],
  var remoteFetchWaitTime: Option[Long],
  var remoteFetchTime: Option[Long],
  var remoteBytesRead: Option[Long],
  var shuffleBytesWritten: Option[Long],
  var executorDeserializeTime: Int,
  var executorRunTime:Int,
  var resultSize: Long
)

object TaskMetrics {
  private[spark] def empty() : TaskMetrics = TaskMetrics(None,None,None,None,None,None,None,None, -1, -1, -1)
}