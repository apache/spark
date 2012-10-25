package spark.scheduler

import spark.TaskContext

/**
 * Tracks information about an active job in the DAGScheduler.
 */
private[spark] class ActiveJob(
    val runId: Int,
    val finalStage: Stage,
    val func: (TaskContext, Iterator[_]) => _,
    val partitions: Array[Int],
    val callSite: String,
    val listener: JobListener) {

  val numPartitions = partitions.length
  val finished = Array.fill[Boolean](numPartitions)(false)
  var numFinished = 0
}
