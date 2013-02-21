package spark.executor

/**
 *
 * @param totalBlocksFetched total number of blocks fetched in a shuffle (remote or local)
 * @param remoteBlocksFetched number of remote blocks fetched in a shuffle
 * @param localBlocksFetched local blocks fetched in a shuffle
 * @param shuffleReadMillis total time to read shuffle data
 * @param remoteFetchWaitTime total time that is spent blocked waiting for shuffle to fetch remote data
 * @param remoteFetchTime the total amount of time for all the shuffle fetches.  This adds up time from overlapping
 *                        shuffles, so can be longer than task time
 * @param remoteBytesRead total number of remote bytes read from a shuffle
 * @param shuffleBytesWritten number of bytes written for a shuffle
 * @param executorDeserializeTime time taken on the executor to deserialize this task
 * @param executorRunTime time the executor spends actually running the task (including fetching shuffle data)
 * @param resultSize the number of bytes this task transmitted back to the driver as the TaskResult
 */
case class TaskMetrics(
  var totalBlocksFetched : Option[Int],
  var remoteBlocksFetched: Option[Int],
  var localBlocksFetched: Option[Int],
  var shuffleReadMillis: Option[Long],
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