package spark.stream

import spark._
import spark.storage._
import spark.util.AkkaUtils

import scala.math._
import scala.collection.mutable.{Queue, HashMap, ArrayBuffer, SynchronizedMap}

import akka.actor._
import akka.actor.Actor
import akka.dispatch._
import akka.pattern.ask
import akka.util.duration._

import java.io.DataInputStream
import java.io.BufferedInputStream
import java.net.Socket
import java.net.ServerSocket
import java.util.LinkedHashMap

import org.apache.hadoop.fs._
import org.apache.hadoop.conf._
import org.apache.hadoop.io._
import org.apache.hadoop.mapred._
import org.apache.hadoop.util._

import spark.Utils


class TestStreamReceiver3(actorSystem: ActorSystem, blockManager: BlockManager)
extends Thread with Logging {
    
  
  class DataHandler(
    inputName: String, 
    longIntervalDuration: LongTime, 
    shortIntervalDuration: LongTime,
    blockManager: BlockManager
  ) 
  extends Logging {

    class Block(var id: String, var shortInterval: Interval) {
      val data = ArrayBuffer[String]()
      var pushed = false
      def longInterval = getLongInterval(shortInterval)
      def empty() = (data.size == 0)
      def += (str: String) = (data += str) 
      override def toString() = "Block " + id 
    }

    class Bucket(val longInterval: Interval) {
      val blocks = new ArrayBuffer[Block]()
      var filled = false
      def += (block: Block) = blocks += block
      def empty() = (blocks.size == 0)
      def ready() = (filled && !blocks.exists(! _.pushed))
      def blockIds() = blocks.map(_.id).toArray
      override def toString() = "Bucket [" + longInterval + ", " + blocks.size + " blocks]"
    }

    initLogging()

    val shortIntervalDurationMillis = shortIntervalDuration.asInstanceOf[LongTime].milliseconds
    val longIntervalDurationMillis = longIntervalDuration.asInstanceOf[LongTime].milliseconds

    var currentBlock: Block = null
    var currentBucket: Bucket = null

    val blocksForPushing = new Queue[Block]()
    val buckets = new HashMap[Interval, Bucket]() with SynchronizedMap[Interval, Bucket]
 
    val blockUpdatingThread = new Thread() { override def run() { keepUpdatingCurrentBlock() } }
    val blockPushingThread = new Thread() { override def run() { keepPushingBlocks() } }

    def start() {
      blockUpdatingThread.start()
      blockPushingThread.start() 
    }
    
    def += (data: String) = addData(data)

    def addData(data: String) {
      if (currentBlock == null) {
        updateCurrentBlock()
      }
      currentBlock.synchronized {
        currentBlock += data
      }
    }

    def getShortInterval(time: Time): Interval = {
      val intervalBegin = time.floor(shortIntervalDuration) 
      Interval(intervalBegin, intervalBegin + shortIntervalDuration) 
    }

    def getLongInterval(shortInterval: Interval): Interval = {
      val intervalBegin = shortInterval.beginTime.floor(longIntervalDuration) 
      Interval(intervalBegin, intervalBegin + longIntervalDuration) 
    }

    def updateCurrentBlock() {
      /*logInfo("Updating current block")*/
      val currentTime: LongTime = LongTime(System.currentTimeMillis)
      val shortInterval = getShortInterval(currentTime)
      val longInterval = getLongInterval(shortInterval)

      def createBlock(reuseCurrentBlock: Boolean = false) {
        val newBlockId = inputName + "-" + longInterval.toFormattedString + "-" + currentBucket.blocks.size
        if (!reuseCurrentBlock) {
          val newBlock = new Block(newBlockId, shortInterval) 
          /*logInfo("Created " + currentBlock)*/
          currentBlock = newBlock
        } else {
          currentBlock.shortInterval = shortInterval
          currentBlock.id = newBlockId
        }
      }

      def createBucket() {
        val newBucket = new Bucket(longInterval)
        buckets += ((longInterval, newBucket))
        currentBucket = newBucket
        /*logInfo("Created " + currentBucket + ", " + buckets.size + " buckets")*/
      }
      
      if (currentBlock == null || currentBucket == null) {
        createBucket()
        currentBucket.synchronized {
          createBlock()
        }
        return
      }
      
      currentBlock.synchronized {
        var reuseCurrentBlock = false
        
        if (shortInterval != currentBlock.shortInterval) {
          if (!currentBlock.empty) {
            blocksForPushing.synchronized {
              blocksForPushing += currentBlock
              blocksForPushing.notifyAll()
            }
          }

          currentBucket.synchronized {
            if (currentBlock.empty) {
              reuseCurrentBlock = true
            } else {
              currentBucket += currentBlock
            }

            if (longInterval != currentBucket.longInterval) {
              currentBucket.filled = true
              if (currentBucket.ready) {
                currentBucket.notifyAll()
              }
              createBucket()
            }
          }

          createBlock(reuseCurrentBlock)
        }
      }
    }

    def pushBlock(block: Block) {
      try{
        if (blockManager != null) {
          logInfo("Pushing block")
          val startTime = System.currentTimeMillis

          val bytes = blockManager.dataSerialize(block.data.toIterator)
          val finishTime = System.currentTimeMillis
          logInfo(block + " serialization delay is " + (finishTime - startTime) / 1000.0 + " s")
          
          blockManager.putBytes(block.id.toString, bytes, StorageLevel.DISK_AND_MEMORY_2)
          /*blockManager.putBytes(block.id.toString, bytes, StorageLevel.DISK_AND_MEMORY_DESER_2)*/
          /*blockManager.put(block.id.toString, block.data.toIterator, StorageLevel.DISK_AND_MEMORY_DESER)*/
          /*blockManager.put(block.id.toString, block.data.toIterator, StorageLevel.DISK_AND_MEMORY)*/
          val finishTime1 = System.currentTimeMillis
          logInfo(block + " put delay is " + (finishTime1 - startTime) / 1000.0 + " s")
        } else {
          logWarning(block + " not put as block manager is null")
        }
      } catch {
        case e: Exception => logError("Exception writing " + block + " to blockmanager" , e)
      }
    }

    def getBucket(longInterval: Interval): Option[Bucket] = {
      buckets.get(longInterval)
    }

    def clearBucket(longInterval: Interval) {
      buckets.remove(longInterval)
    }
   
    def keepUpdatingCurrentBlock() {
      logInfo("Thread to update current block started")
      while(true) {
        updateCurrentBlock()
        val currentTimeMillis = System.currentTimeMillis
        val sleepTimeMillis = (currentTimeMillis / shortIntervalDurationMillis + 1) *
          shortIntervalDurationMillis - currentTimeMillis + 1
        Thread.sleep(sleepTimeMillis)
      }
    }

    def keepPushingBlocks() {
      var loop = true
      logInfo("Thread to push blocks started")
      while(loop) {
        val block = blocksForPushing.synchronized {
          if (blocksForPushing.size == 0) {
            blocksForPushing.wait()
          } 
          blocksForPushing.dequeue
        }
        pushBlock(block)
        block.pushed = true
        block.data.clear()

        val bucket = buckets(block.longInterval)
        bucket.synchronized {
          if (bucket.ready) {
            bucket.notifyAll()
          }
        }
      }
    }
  }

 
  class ConnectionListener(port: Int, dataHandler: DataHandler) 
  extends Thread with Logging { 
    initLogging()
    override def run {
      try {
        val listener = new ServerSocket(port)
        logInfo("Listening on port " + port)
        while (true) {
          new ConnectionHandler(listener.accept(), dataHandler).start();
        }
        listener.close()
      } catch {
        case e: Exception => logError("", e);
      }
    }
  }

  class ConnectionHandler(socket: Socket, dataHandler: DataHandler) extends Thread with Logging {
    initLogging()
    override def run {
      logInfo("New connection from " + socket.getInetAddress() + ":" + socket.getPort)
      val bytes = new Array[Byte](100 * 1024 * 1024)
      try {

        val inputStream = new DataInputStream(new BufferedInputStream(socket.getInputStream, 1024 * 1024))
        /*val inputStream = new DataInputStream(new BufferedInputStream(socket.getInputStream))*/
        var str: String = null
        str = inputStream.readUTF
        while(str != null) {
          dataHandler += str
          str = inputStream.readUTF()
        }
        
        /*
        var loop = true
        while(loop) {
          val numRead = inputStream.read(bytes)
          if (numRead < 0) {
            loop = false
          }
          inbox += ((LongTime(SystemTime.currentTimeMillis), "test"))
        }*/

        inputStream.close()
      } catch {
        case e => logError("Error receiving data", e)
      }
      socket.close()
    }
  }

  initLogging()

  val masterHost = System.getProperty("spark.master.host")
  val masterPort = System.getProperty("spark.master.port").toInt 
  
  val akkaPath = "akka://spark@%s:%s/user/".format(masterHost, masterPort)
  val sparkstreamScheduler = actorSystem.actorFor(akkaPath + "/SparkStreamScheduler")
  val testStreamCoordinator = actorSystem.actorFor(akkaPath + "/TestStreamCoordinator")

  logInfo("Getting stream details from master " + masterHost + ":" + masterPort)
  
  val timeout = 50 millis

  var started = false
  while (!started) {
    askActor[String](testStreamCoordinator, TestStarted) match {
      case Some(str) => { 
        started = true
        logInfo("TestStreamCoordinator started")
      }
      case None => {
        logInfo("TestStreamCoordinator not started yet")
        Thread.sleep(200)
      }
    } 
  }

  val streamDetails = askActor[GotStreamDetails](testStreamCoordinator, GetStreamDetails) match {
    case Some(details) => details
    case None => throw new Exception("Could not get stream details")
  }
  logInfo("Stream details received: " + streamDetails)
 
  val inputName = streamDetails.name
  val intervalDurationMillis = streamDetails.duration
  val intervalDuration = LongTime(intervalDurationMillis)

  val dataHandler = new DataHandler(
    inputName, 
    intervalDuration, 
    LongTime(TestStreamReceiver3.SHORT_INTERVAL_MILLIS), 
    blockManager)
  
  val connListener = new ConnectionListener(TestStreamReceiver3.PORT, dataHandler)

  // Send a message to an actor and return an option with its reply, or None if this times out
  def askActor[T](actor: ActorRef, message: Any): Option[T] = {
    try {
      val future = actor.ask(message)(timeout)
      return Some(Await.result(future, timeout).asInstanceOf[T])
    } catch {
      case e: Exception =>
        logInfo("Error communicating with " + actor, e)
        return None
    }
  }

  override def run() {
    connListener.start()
    dataHandler.start()    

    var interval = Interval.currentInterval(intervalDuration) 
    var dataStarted = false

    while(true) {
      waitFor(interval.endTime)
      logInfo("Woken up at " + System.currentTimeMillis + " for " + interval)
      dataHandler.getBucket(interval) match {
        case Some(bucket) => {
          logInfo("Found " + bucket + " for " + interval)
          bucket.synchronized {
            if (!bucket.ready) {
              logInfo("Waiting for " + bucket)
              bucket.wait()
              logInfo("Wait over for " + bucket)
            }
            if (dataStarted || !bucket.empty) {
              logInfo("Notifying " + bucket)
              notifyScheduler(interval, bucket.blockIds)
              dataStarted = true
            }
            bucket.blocks.clear()
            dataHandler.clearBucket(interval)
          }
        }
        case None => {
          logInfo("Found none for " + interval)
          if (dataStarted) {
            logInfo("Notifying none")
            notifyScheduler(interval, Array[String]())
          }
        }
      }
      interval = interval.next 
    }
  }

  def waitFor(time: Time) {
    val currentTimeMillis = System.currentTimeMillis
    val targetTimeMillis = time.asInstanceOf[LongTime].milliseconds
    if (currentTimeMillis < targetTimeMillis) {
      val sleepTime = (targetTimeMillis - currentTimeMillis)
      Thread.sleep(sleepTime + 1)
    }
  }

  def notifyScheduler(interval: Interval, blockIds: Array[String]) {
    try {  
      sparkstreamScheduler ! InputGenerated(inputName, interval, blockIds.toArray)
      val time = interval.endTime.asInstanceOf[LongTime]
      val delay = (System.currentTimeMillis - time.milliseconds) / 1000.0
      logInfo("Pushing delay for " + time + " is " + delay + " s")
    }  catch {
      case _ => logError("Exception notifying scheduler at interval " + interval) 
    }
  }
}

object TestStreamReceiver3 {
  
  val PORT = 9999
  val SHORT_INTERVAL_MILLIS = 100
  
  def main(args: Array[String]) {
    System.setProperty("spark.master.host", Utils.localHostName)
    System.setProperty("spark.master.port", "7078")
    val details = Array(("Sentences", 2000L))
    val (actorSystem, port) = AkkaUtils.createActorSystem("spark", Utils.localHostName, 7078)
    actorSystem.actorOf(Props(new TestStreamCoordinator(details)), name = "TestStreamCoordinator")
    new TestStreamReceiver3(actorSystem, null).start()
  }
}



