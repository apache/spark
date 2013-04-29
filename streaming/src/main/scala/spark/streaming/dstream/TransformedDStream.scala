package spark.streaming.dstream

import spark.RDD
import spark.streaming.{Duration, DStream, Time}

import scala.reflect.ClassTag

private[streaming]
class TransformedDStream[T: ClassTag, U: ClassTag] (
    parent: DStream[T],
    transformFunc: (RDD[T], Time) => RDD[U]
  ) extends DStream[U](parent.ssc) {

  override def dependencies = List(parent)

  override def slideDuration: Duration = parent.slideDuration

  override def compute(validTime: Time): Option[RDD[U]] = {
    parent.getOrCompute(validTime).map(transformFunc(_, validTime))
  }
}
