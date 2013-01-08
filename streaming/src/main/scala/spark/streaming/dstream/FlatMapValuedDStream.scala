package spark.streaming.dstream

import spark.streaming.{DStream, Time}
import spark.RDD
import spark.SparkContext._

private[streaming]
class FlatMapValuedDStream[K: ClassManifest, V: ClassManifest, U: ClassManifest](
    parent: DStream[(K, V)],
    flatMapValueFunc: V => TraversableOnce[U]
  ) extends DStream[(K, U)](parent.ssc) {

  override def dependencies = List(parent)

  override def slideTime: Time = parent.slideTime

  override def compute(validTime: Time): Option[RDD[(K, U)]] = {
    parent.getOrCompute(validTime).map(_.flatMapValues[U](flatMapValueFunc))
  }
}
