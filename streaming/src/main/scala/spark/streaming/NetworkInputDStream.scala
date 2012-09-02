package spark.streaming

import spark.RDD
import spark.BlockRDD

abstract class NetworkInputDStream[T: ClassManifest](@transient ssc: StreamingContext)
  extends InputDStream[T](ssc) {

  val id = ssc.getNewNetworkStreamId()  
  
  def start() {}

  def stop() {}

  override def compute(validTime: Time): Option[RDD[T]] = {
    val blockIds = ssc.networkInputTracker.getBlockIds(id, validTime)    
    Some(new BlockRDD[T](ssc.sc, blockIds))
  }

  /** Called on workers to run a receiver for the stream. */
  def runReceiver(): Unit
}

