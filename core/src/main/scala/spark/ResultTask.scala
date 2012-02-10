package spark

class ResultTask[T, U](
    stageId: Int, 
    rdd: RDD[T], 
    func: (TaskContext, Iterator[T]) => U,
    val partition: Int, 
    locs: Seq[String],
    val outputId: Int)
    extends DAGTask[U](stageId) {
  
  val split = rdd.splits(partition)

  override def run(attemptId: Int): U = {
    val context = new TaskContext(stageId, partition, attemptId)
    func(context, rdd.iterator(split))
  }

  override def preferredLocations: Seq[String] = locs

  override def toString = "ResultTask(" + stageId + ", " + partition + ")"
}
