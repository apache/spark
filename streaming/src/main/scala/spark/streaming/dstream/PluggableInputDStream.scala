package spark.streaming.dstream

import spark.streaming.StreamingContext

private[streaming]
class PluggableInputDStream[T: ClassManifest](
  @transient ssc_ : StreamingContext,
  receiver: NetworkReceiver[T]) extends NetworkInputDStream[T](ssc_) {

  def getReceiver(): NetworkReceiver[T] = {
    receiver
  }
}
