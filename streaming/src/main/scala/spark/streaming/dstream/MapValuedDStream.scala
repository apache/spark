package spark.streaming.dstream

import spark.streaming.{Duration, DStream, Time}
import spark.RDD
import spark.SparkContext._

private[streaming]
class MapValuedDStream[K: ClassManifest, V: ClassManifest, U: ClassManifest](
    parent: DStream[(K, V)],
    mapValueFunc: V => U
  ) extends DStream[(K, U)](parent.ssc) {

  override def dependencies = List(parent)

  override def slideDuration: Duration = parent.slideDuration

  override def compute(validTime: Time): Option[RDD[(K, U)]] = {
    parent.getOrCompute(validTime).map(_.mapValues[U](mapValueFunc))
  }
}

