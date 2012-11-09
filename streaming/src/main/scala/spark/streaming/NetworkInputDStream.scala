package spark.streaming

import scala.collection.mutable.ArrayBuffer

import spark.{Logging, SparkEnv, RDD}
import spark.rdd.BlockRDD
import spark.streaming.util.{RecurringTimer, SystemClock}
import spark.storage.StorageLevel

import java.nio.ByteBuffer
import java.util.concurrent.ArrayBlockingQueue

import akka.actor.{Props, Actor}
import akka.pattern.ask
import akka.dispatch.Await
import akka.util.duration._

abstract class NetworkInputDStream[T: ClassManifest](@transient ssc_ : StreamingContext)
  extends InputDStream[T](ssc_) {

  // This is an unique identifier that is used to match the network receiver with the
  // corresponding network input stream.
  val id = ssc.getNewNetworkStreamId()

  /**
   * This method creates the receiver object that will be sent to the workers
   * to receive data. This method needs to defined by any specific implementation
   * of a NetworkInputDStream.
   */
  def createReceiver(): NetworkReceiver[T]

  // Nothing to start or stop as both taken care of by the NetworkInputTracker.
  def start() {}

  def stop() {}

  override def compute(validTime: Time): Option[RDD[T]] = {
    val blockIds = ssc.networkInputTracker.getBlockIds(id, validTime)    
    Some(new BlockRDD[T](ssc.sc, blockIds))
  }
}


sealed trait NetworkReceiverMessage
case class StopReceiver(msg: String) extends NetworkReceiverMessage
case class ReportBlock(blockId: String, metadata: Any) extends NetworkReceiverMessage
case class ReportError(msg: String) extends NetworkReceiverMessage

abstract class NetworkReceiver[T: ClassManifest](val streamId: Int) extends Serializable with Logging {

  initLogging()

  lazy protected val env = SparkEnv.get

  lazy protected val actor = env.actorSystem.actorOf(
    Props(new NetworkReceiverActor()), "NetworkReceiver-" + streamId)

  lazy protected val receivingThread = Thread.currentThread()

  /** This method will be called to start receiving data. */
  protected def onStart()

  /** This method will be called to stop receiving data. */
  protected def onStop()

  /**
   * This method starts the receiver. First is accesses all the lazy members to
   * materialize them. Then it calls the user-defined onStart() method to start
   * other threads, etc required to receiver the data.
   */
  def start() {
    try {
      // Access the lazy vals to materialize them
      env
      actor
      receivingThread

      // Call user-defined onStart()
      onStart()
    } catch {
      case ie: InterruptedException =>
        logInfo("Receiving thread interrupted")
        //println("Receiving thread interrupted")
      case e: Exception =>
        stopOnError(e)
    }
  }

  /**
   * This method stops the receiver. First it interrupts the main receiving thread,
   * that is, the thread that called receiver.start(). Then it calls the user-defined
   * onStop() method to stop other threads and/or do cleanup.
   */
  def stop() {
    receivingThread.interrupt()
    onStop()
    //TODO: terminate the actor
  }

  /**
   * This method stops the receiver and reports to exception to the tracker.
   * This should be called whenever an exception has happened on any thread
   * of the receiver.
   */
  protected def stopOnError(e: Exception) {
    logError("Error receiving data", e)
    stop()
    actor ! ReportError(e.toString)
  }


  /**
   * This method pushes a block (as iterator of values) into the block manager.
   */
  def pushBlock(blockId: String, iterator: Iterator[T], metadata: Any, level: StorageLevel) {
    val buffer = new ArrayBuffer[T] ++ iterator
    env.blockManager.put(blockId, buffer.asInstanceOf[ArrayBuffer[Any]], level)

    actor ! ReportBlock(blockId, metadata)
  }

  /**
   * This method pushes a block (as bytes) into the block manager.
   */
  def pushBlock(blockId: String, bytes: ByteBuffer, metadata: Any, level: StorageLevel) {
    env.blockManager.putBytes(blockId, bytes, level)
    actor ! ReportBlock(blockId, metadata)
  }

  /** A helper actor that communicates with the NetworkInputTracker */
  private class NetworkReceiverActor extends Actor {
    logInfo("Attempting to register with tracker")
    val ip = System.getProperty("spark.master.host", "localhost")
    val port = System.getProperty("spark.master.port", "7077").toInt
    val url = "akka://spark@%s:%s/user/NetworkInputTracker".format(ip, port)
    val tracker = env.actorSystem.actorFor(url)
    val timeout = 5.seconds

    override def preStart() {
      val future = tracker.ask(RegisterReceiver(streamId, self))(timeout)
      Await.result(future, timeout)
    }

    override def receive() = {
      case ReportBlock(blockId, metadata) =>
        tracker ! AddBlocks(streamId, Array(blockId), metadata)
      case ReportError(msg) =>
        tracker ! DeregisterReceiver(streamId, msg)
      case StopReceiver(msg) =>
        stop()
        tracker ! DeregisterReceiver(streamId, msg)
    }
  }

}

