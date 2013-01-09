package spark.streaming.dstream

import spark.{RDD, Partitioner}
import spark.SparkContext._
import spark.streaming.{Duration, DStream, Time}

private[streaming]
class ShuffledDStream[K: ClassManifest, V: ClassManifest, C: ClassManifest](
    parent: DStream[(K,V)],
    createCombiner: V => C,
    mergeValue: (C, V) => C,
    mergeCombiner: (C, C) => C,
    partitioner: Partitioner
  ) extends DStream [(K,C)] (parent.ssc) {

  override def dependencies = List(parent)

  override def slideDuration: Duration = parent.slideDuration

  override def compute(validTime: Time): Option[RDD[(K,C)]] = {
    parent.getOrCompute(validTime) match {
      case Some(rdd) =>
        Some(rdd.combineByKey[C](createCombiner, mergeValue, mergeCombiner, partitioner))
      case None => None
    }
  }
}
