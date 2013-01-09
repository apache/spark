package spark.streaming.dstream

import spark.RDD
import spark.rdd.UnionRDD
import spark.storage.StorageLevel
import spark.streaming.{Duration, Interval, Time, DStream}

private[streaming]
class WindowedDStream[T: ClassManifest](
    parent: DStream[T],
    _windowDuration: Duration,
    _slideDuration: Duration)
  extends DStream[T](parent.ssc) {

  if (!_windowDuration.isMultipleOf(parent.slideDuration))
    throw new Exception("The window duration of WindowedDStream (" + _slideDuration + ") " +
    "must be multiple of the slide duration of parent DStream (" + parent.slideDuration + ")")

  if (!_slideDuration.isMultipleOf(parent.slideDuration))
    throw new Exception("The slide duration of WindowedDStream (" + _slideDuration + ") " +
    "must be multiple of the slide duration of parent DStream (" + parent.slideDuration + ")")

  parent.persist(StorageLevel.MEMORY_ONLY_SER)

  def windowDuration: Duration =  _windowDuration

  override def dependencies = List(parent)

  override def slideDuration: Duration = _slideDuration

  override def parentRememberDuration: Duration = rememberDuration + windowDuration

  override def compute(validTime: Time): Option[RDD[T]] = {
    val currentWindow = new Interval(validTime - windowDuration + parent.slideDuration, validTime)
    Some(new UnionRDD(ssc.sc, parent.slice(currentWindow)))
  }
}



