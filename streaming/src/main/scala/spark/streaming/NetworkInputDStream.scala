package spark.streaming

import akka.actor._
import akka.pattern.ask
import akka.util.duration._
import akka.dispatch._

import spark.RDD
import spark.BlockRDD
import spark.Logging

import java.io.InputStream


class NetworkInputDStream[T: ClassManifest](
    @transient ssc: StreamingContext, 
    val host: String, 
    val port: Int, 
    val bytesToObjects: InputStream => Iterator[T]
  ) extends InputDStream[T](ssc) with Logging {
  
  val id = ssc.getNewNetworkStreamId()  
  
  def start() { }

  def stop() { }

  override def compute(validTime: Time): Option[RDD[T]] = {
    val blockIds = ssc.networkInputTracker.getBlockIds(id, validTime)    
    return Some(new BlockRDD[T](ssc.sc, blockIds))    
  }
  
  def createReceiver(): NetworkInputReceiver[T] = {
    new NetworkInputReceiver(id, host, port, bytesToObjects)
  } 
}