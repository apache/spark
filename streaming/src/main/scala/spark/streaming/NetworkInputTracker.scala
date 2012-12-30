package spark.streaming

import spark.streaming.dstream.{NetworkInputDStream, NetworkReceiver}
import spark.streaming.dstream.{StopReceiver, ReportBlock, ReportError}
import spark.Logging
import spark.SparkEnv

import scala.collection.mutable.HashMap
import scala.collection.mutable.Queue

import akka.actor._
import akka.pattern.ask
import akka.util.duration._
import akka.dispatch._

private[streaming] sealed trait NetworkInputTrackerMessage
private[streaming] case class RegisterReceiver(streamId: Int, receiverActor: ActorRef) extends NetworkInputTrackerMessage
private[streaming] case class AddBlocks(streamId: Int, blockIds: Seq[String], metadata: Any) extends NetworkInputTrackerMessage
private[streaming] case class DeregisterReceiver(streamId: Int, msg: String) extends NetworkInputTrackerMessage


class NetworkInputTracker(
    @transient ssc: StreamingContext, 
    @transient networkInputStreams: Array[NetworkInputDStream[_]])
  extends Logging {

  val networkInputStreamMap = Map(networkInputStreams.map(x => (x.id, x)): _*)
  val receiverExecutor = new ReceiverExecutor()
  val receiverInfo = new HashMap[Int, ActorRef]
  val receivedBlockIds = new HashMap[Int, Queue[String]]
  val timeout = 5000.milliseconds

  var currentTime: Time = null

  def start() {
    ssc.env.actorSystem.actorOf(Props(new NetworkInputTrackerActor), "NetworkInputTracker")
    receiverExecutor.start()
  }

  def stop() {
    receiverExecutor.interrupt()
    receiverExecutor.stopReceivers()
  }

  def getBlockIds(receiverId: Int, time: Time): Array[String] = synchronized {
    val queue =  receivedBlockIds.synchronized {
      receivedBlockIds.getOrElse(receiverId, new Queue[String]())
    }
    val result = queue.synchronized {
      queue.dequeueAll(x => true)
    }
    logInfo("Stream " + receiverId + " received " + result.size + " blocks")
    result.toArray
  }

  private class NetworkInputTrackerActor extends Actor {
    def receive = {
      case RegisterReceiver(streamId, receiverActor) => {
        if (!networkInputStreamMap.contains(streamId)) {
          throw new Exception("Register received for unexpected id " + streamId)        
        }
        receiverInfo += ((streamId, receiverActor))
        logInfo("Registered receiver for network stream " + streamId + " from " + sender.path.address)
        sender ! true
      } 
      case AddBlocks(streamId, blockIds, metadata) => {
        val tmp = receivedBlockIds.synchronized {
          if (!receivedBlockIds.contains(streamId)) {
            receivedBlockIds += ((streamId, new Queue[String]))
          }
          receivedBlockIds(streamId)
        }
        tmp.synchronized {
          tmp ++= blockIds
        }
        networkInputStreamMap(streamId).addMetadata(metadata)
      }
      case DeregisterReceiver(streamId, msg) => {
        receiverInfo -= streamId
        logInfo("De-registered receiver for network stream " + streamId
          + " with message " + msg)
        //TODO: Do something about the corresponding NetworkInputDStream
      }
    }
  }
  
  class ReceiverExecutor extends Thread {
    val env = ssc.env
        
    override def run() {      
      try {
        SparkEnv.set(env)
        startReceivers()
      } catch {
        case ie: InterruptedException => logInfo("ReceiverExecutor interrupted")
      } finally {
        stopReceivers()
      }
    }
    
    def startReceivers() {
      val receivers = networkInputStreams.map(_.createReceiver())

      // Right now, we only honor preferences if all receivers have them
      val hasLocationPreferences = receivers.map(_.getLocationPreference().isDefined).reduce(_ && _)

      val tempRDD =
        if (hasLocationPreferences) {
          val receiversWithPreferences = receivers.map(r => (r, Seq(r.getLocationPreference().toString)))
          ssc.sc.makeRDD[NetworkReceiver[_]](receiversWithPreferences)
        }
        else {
          ssc.sc.makeRDD(receivers, receivers.size)
        }

      val startReceiver = (iterator: Iterator[NetworkReceiver[_]]) => {
        if (!iterator.hasNext) {
          throw new Exception("Could not start receiver as details not found.")
        }
        iterator.next().start()
      }
      ssc.sc.runJob(tempRDD, startReceiver)
    }
    
    def stopReceivers() {
      //implicit val ec = env.actorSystem.dispatcher
      receiverInfo.values.foreach(_ ! StopReceiver)
      //val listOfFutures = receiverInfo.values.map(_.ask(StopReceiver)(timeout)).toList
      //val futureOfList = Future.sequence(listOfFutures)
      //Await.result(futureOfList, timeout)
    }
  }
}
