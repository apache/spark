package spark.streaming

import spark.Logging
import spark.SparkEnv

import scala.collection.mutable.HashMap
import scala.collection.mutable.Queue

import akka.actor._
import akka.pattern.ask
import akka.util.duration._
import akka.dispatch._

trait NetworkInputTrackerMessage 
case class RegisterReceiver(streamId: Int, receiverActor: ActorRef) extends NetworkInputTrackerMessage

class NetworkInputTracker(
    @transient ssc: StreamingContext, 
    @transient networkInputStreams: Array[NetworkInputDStream[_]]) 
extends Logging {

  class TrackerActor extends Actor {
    def receive = {
      case RegisterReceiver(streamId, receiverActor) => {
        if (!networkInputStreamIds.contains(streamId)) {
          throw new Exception("Register received for unexpected id " + streamId)        
        }
        receiverInfo += ((streamId, receiverActor))
        logInfo("Registered receiver for network stream " + streamId)
        sender ! true
      } 
      case GotBlockIds(streamId, blockIds) => {
        val tmp = receivedBlockIds.synchronized {
          if (!receivedBlockIds.contains(streamId)) {
            receivedBlockIds += ((streamId, new Queue[String]))
          }
          receivedBlockIds(streamId)
        }
        tmp.synchronized {
          tmp ++= blockIds
        }
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
      val tempRDD = ssc.sc.makeRDD(networkInputStreams, networkInputStreams.size)
      
      val startReceiver = (iterator: Iterator[NetworkInputDStream[_]]) => {
        if (!iterator.hasNext) {
          throw new Exception("Could not start receiver as details not found.")
        }
        iterator.next().runReceiver()
      }
      
      ssc.sc.runJob(tempRDD, startReceiver)
    }
    
    def stopReceivers() {
      implicit val ec = env.actorSystem.dispatcher
      val listOfFutures = receiverInfo.values.map(_.ask(StopReceiver)(timeout)).toList
      val futureOfList = Future.sequence(listOfFutures)
      Await.result(futureOfList, timeout) 
    }
  }
  
  val networkInputStreamIds = networkInputStreams.map(_.id).toArray
  val receiverExecutor = new ReceiverExecutor()
  val receiverInfo = new HashMap[Int, ActorRef]
  val receivedBlockIds = new HashMap[Int, Queue[String]]
  val timeout = 5000.milliseconds
  
  
  var currentTime: Time = null 
  
  def start() {
    ssc.env.actorSystem.actorOf(Props(new TrackerActor), "NetworkInputTracker")
    receiverExecutor.start()
  }
  
  def stop() {
    // stop the actor
    receiverExecutor.interrupt()
  }
  
  def getBlockIds(receiverId: Int, time: Time): Array[String] = synchronized {
    val queue =  receivedBlockIds.synchronized {
      receivedBlockIds.getOrElse(receiverId, new Queue[String]())
    }
    val result = queue.synchronized {
      queue.dequeueAll(x => true)
    }
    result.toArray
  }  
}
