package spark.streaming.dstream

import spark.{RDD, Partitioner}
import spark.rdd.CoGroupedRDD
import spark.streaming.{Time, DStream, Duration}

private[streaming]
class CoGroupedDStream[K : ClassManifest](
    parents: Seq[DStream[(K, _)]],
    partitioner: Partitioner
  ) extends DStream[(K, Seq[Seq[_]])](parents.head.ssc) {

  if (parents.length == 0) {
    throw new IllegalArgumentException("Empty array of parents")
  }

  if (parents.map(_.ssc).distinct.size > 1) {
    throw new IllegalArgumentException("Array of parents have different StreamingContexts")
  }

  if (parents.map(_.slideDuration).distinct.size > 1) {
    throw new IllegalArgumentException("Array of parents have different slide times")
  }

  override def dependencies = parents.toList

  override def slideDuration: Duration = parents.head.slideDuration

  override def compute(validTime: Time): Option[RDD[(K, Seq[Seq[_]])]] = {
    val part = partitioner
    val rdds = parents.flatMap(_.getOrCompute(validTime))
    if (rdds.size > 0) {
      val q = new CoGroupedRDD[K](rdds, part)
      Some(q)
    } else {
      None
    }
  }

}
