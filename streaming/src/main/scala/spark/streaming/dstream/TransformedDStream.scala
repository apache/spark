package spark.streaming.dstream

import spark.RDD
import spark.streaming.{Duration, DStream, Time}

private[streaming]
class TransformedDStream[T: ClassManifest, U: ClassManifest] (
    parent: DStream[T],
    transformFunc: (RDD[T], Time) => RDD[U]
  ) extends DStream[U](parent.ssc) {

  override def dependencies = List(parent)

  override def slideDuration: Duration = parent.slideDuration

  override def compute(validTime: Time): Option[RDD[U]] = {
    parent.getOrCompute(validTime).map(transformFunc(_, validTime))
  }
}
