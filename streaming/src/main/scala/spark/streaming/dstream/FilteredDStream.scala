package spark.streaming.dstream

import spark.streaming.{DStream, Time}
import spark.RDD

private[streaming]
class FilteredDStream[T: ClassManifest](
    parent: DStream[T],
    filterFunc: T => Boolean
  ) extends DStream[T](parent.ssc) {

  override def dependencies = List(parent)

  override def slideTime: Time = parent.slideTime

  override def compute(validTime: Time): Option[RDD[T]] = {
    parent.getOrCompute(validTime).map(_.filter(filterFunc))
  }
}


