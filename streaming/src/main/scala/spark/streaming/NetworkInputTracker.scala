package spark.streaming

import spark.Logging
import spark.SparkEnv

import scala.collection.mutable.HashMap

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
        val stream = iterator.next
        val receiver = stream.createReceiver()
        receiver.run()
      }
      
      ssc.sc.runJob(tempRDD, startReceiver)
    }
    
    def stopReceivers() {
      implicit val ec = env.actorSystem.dispatcher
      val message = new StopReceiver() 
      val listOfFutures = receiverInfo.values.map(_.ask(message)(timeout)).toList
      val futureOfList = Future.sequence(listOfFutures)
      Await.result(futureOfList, timeout) 
    }
  }
  
  val networkInputStreamIds = networkInputStreams.map(_.id).toArray
  val receiverExecutor = new ReceiverExecutor()
  val receiverInfo = new HashMap[Int, ActorRef]
  val receivedBlockIds = new HashMap[Int, Array[String]]
  val timeout = 1000.milliseconds
  
  
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
    if (currentTime == null || time > currentTime) {
      logInfo("Getting block ids from receivers for " + time)
      implicit val ec = ssc.env.actorSystem.dispatcher
      receivedBlockIds.clear()
      val message = new GetBlockIds(time)
      val listOfFutures = receiverInfo.values.map(
          _.ask(message)(timeout).mapTo[GotBlockIds]
        ).toList
      val futureOfList = Future.sequence(listOfFutures)
      val allBlockIds = Await.result(futureOfList, timeout)
      receivedBlockIds ++= allBlockIds.map(x => (x.streamId, x.blocksIds))
      if (receivedBlockIds.size != receiverInfo.size) {
        throw new Exception("Unexpected number of the Block IDs received")
      }
      currentTime = time
    }
    receivedBlockIds.getOrElse(receiverId, Array[String]())    
  }  
}