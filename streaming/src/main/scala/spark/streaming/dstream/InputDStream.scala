package spark.streaming.dstream

import spark.streaming.{Duration, StreamingContext, DStream}

abstract class InputDStream[T: ClassManifest] (@transient ssc_ : StreamingContext)
  extends DStream[T](ssc_) {

  override def dependencies = List()

  override def slideDuration: Duration = {
    if (ssc == null) throw new Exception("ssc is null")
    if (ssc.graph.batchDuration == null) throw new Exception("batchDuration is null")
    ssc.graph.batchDuration
  }

  def start()

  def stop()
}
