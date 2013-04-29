package spark.streaming.dstream

import spark.streaming.StreamingContext

import scala.reflect.ClassTag

private[streaming]
class PluggableInputDStream[T: ClassTag](
  @transient ssc_ : StreamingContext,
  receiver: NetworkReceiver[T]) extends NetworkInputDStream[T](ssc_) {

  def getReceiver(): NetworkReceiver[T] = {
    receiver
  }
}
