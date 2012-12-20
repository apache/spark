package spark.scheduler

import spark._

private[spark] class ResultTask[T, U](
    stageId: Int,
    rdd: RDD[T],
    func: (TaskContext, Iterator[T]) => U,
    val partition: Int,
    @transient locs: Seq[String],
    val outputId: Int)
  extends Task[U](stageId) {

  val split = rdd.splits(partition)

  override def run(attemptId: Long): U = {
    val context = new TaskContext(stageId, partition, attemptId)
    val result = func(context, rdd.iterator(split, context))
    context.executeOnCompleteCallbacks()
    result
  }

  override def preferredLocations: Seq[String] = locs

  override def toString = "ResultTask(" + stageId + ", " + partition + ")"
}
