package spark.executor

class TaskMetrics extends Serializable {
  /**
   * Time taken on the executor to deserialize this task
   */
  var executorDeserializeTime: Int = _

  /**
   * Time the executor spends actually running the task (including fetching shuffle data)
   */
  var executorRunTime:Int = _

  /**
   * The number of bytes this task transmitted back to the driver as the TaskResult
   */
  var resultSize: Long = _

  /**
   * If this task reads from shuffle output, metrics on getting shuffle data will be collected here
   */
  var shuffleReadMetrics: Option[ShuffleReadMetrics] = None

  /**
   * If this task writes to shuffle output, metrics on the written shuffle data will be collected here
   */
  var shuffleWriteMetrics: Option[ShuffleWriteMetrics] = None
}

object TaskMetrics {
  private[spark] def empty(): TaskMetrics = new TaskMetrics
}


class ShuffleReadMetrics extends Serializable {
  /**
   * Total number of blocks fetched in a shuffle (remote or local)
   */
  var totalBlocksFetched : Int = _

  /**
   * Number of remote blocks fetched in a shuffle
   */
  var remoteBlocksFetched: Int = _

  /**
   * Local blocks fetched in a shuffle
   */
  var localBlocksFetched: Int = _

  /**
   * Total time that is spent blocked waiting for shuffle to fetch data
   */
  var fetchWaitTime: Long = _

  /**
   * The total amount of time for all the shuffle fetches.  This adds up time from overlapping
   *     shuffles, so can be longer than task time
   */
  var remoteFetchTime: Long = _

  /**
   * Total number of remote bytes read from a shuffle
   */
  var remoteBytesRead: Long = _
}

class ShuffleWriteMetrics extends Serializable {
  /**
   * Number of bytes written for a shuffle
   */
  var shuffleBytesWritten: Long = _
}
