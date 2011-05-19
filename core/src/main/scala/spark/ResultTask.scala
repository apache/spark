package spark

class ResultTask[T, U](val stageId: Int, rdd: RDD[T], func: Iterator[T] => U,
                       val partition: Int, locs: Seq[String], val outputId: Int)
extends Task[U] {
  val split = rdd.splits(partition)

  override def run: U = {
    func(rdd.iterator(split))
  }

  override def preferredLocations: Seq[String] = locs

  override def toString = "ResultTask(" + stageId + ", " + partition + ")"
}
