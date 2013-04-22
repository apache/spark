package spark.streaming.dstream

import spark.RDD
import spark.rdd.UnionRDD

import scala.collection.mutable.Queue
import scala.collection.mutable.ArrayBuffer
import spark.streaming.{Time, StreamingContext}

private[streaming]
class QueueInputDStream[T: ClassManifest](
    @transient ssc: StreamingContext,
    val queue: Queue[RDD[T]],
    oneAtATime: Boolean,
    defaultRDD: RDD[T]
  ) extends InputDStream[T](ssc) {
  
  override def start() { }
  
  override def stop() { }
  
  override def compute(validTime: Time): Option[RDD[T]] = {
    val buffer = new ArrayBuffer[RDD[T]]()
    if (oneAtATime && queue.size > 0) {
      buffer += queue.dequeue()
    } else {
      buffer ++= queue
    }
    if (buffer.size > 0) {
      if (oneAtATime) {
        Some(buffer.head)
      } else {
        Some(new UnionRDD(ssc.sc, buffer.toSeq))
      }
    } else if (defaultRDD != null) {
      Some(defaultRDD)
    } else {
      None
    }
  }
  
}
