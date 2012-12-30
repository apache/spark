package spark.streaming.dstream

import spark.RDD
import spark.streaming.{Time, StreamingContext}

/**
 * An input stream that always returns the same RDD on each timestep. Useful for testing.
 */
class ConstantInputDStream[T: ClassManifest](ssc_ : StreamingContext, rdd: RDD[T])
  extends InputDStream[T](ssc_) {

  override def start() {}

  override def stop() {}

  override def compute(validTime: Time): Option[RDD[T]] = {
    Some(rdd)
  }
}