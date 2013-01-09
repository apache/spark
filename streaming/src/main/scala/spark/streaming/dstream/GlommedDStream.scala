package spark.streaming.dstream

import spark.streaming.{Duration, DStream, Time}
import spark.RDD

private[streaming]
class GlommedDStream[T: ClassManifest](parent: DStream[T])
  extends DStream[Array[T]](parent.ssc) {

  override def dependencies = List(parent)

  override def slideDuration: Duration = parent.slideDuration

  override def compute(validTime: Time): Option[RDD[Array[T]]] = {
    parent.getOrCompute(validTime).map(_.glom())
  }
}
