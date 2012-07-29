package spark.stream

import spark._
import spark.storage._
import spark.util.AkkaUtils

import scala.math._
import scala.collection.mutable.{Queue, HashMap, ArrayBuffer, SynchronizedMap}

import java.io._
import java.nio._
import java.nio.charset._
import java.nio.channels._
import java.util.concurrent.Executors

import akka.actor._
import akka.actor.Actor
import akka.dispatch._
import akka.pattern.ask
import akka.util.duration._

class TestStreamReceiver4(actorSystem: ActorSystem, blockManager: BlockManager) 
extends Thread with Logging {

  class DataHandler(
    inputName: String, 
    longIntervalDuration: LongTime, 
    shortIntervalDuration: LongTime,
    blockManager: BlockManager
  ) 
  extends Logging {

    class Block(val id: String, val shortInterval: Interval, val buffer: ByteBuffer) {
      var pushed = false
      def longInterval = getLongInterval(shortInterval)
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

    val syncOnLastShortInterval = true 

    val shortIntervalDurationMillis = shortIntervalDuration.asInstanceOf[LongTime].milliseconds
    val longIntervalDurationMillis = longIntervalDuration.asInstanceOf[LongTime].milliseconds

    val buffer = ByteBuffer.allocateDirect(100 * 1024 * 1024)
    var currentShortInterval = Interval.currentInterval(shortIntervalDuration)

    val blocksForPushing = new Queue[Block]()
    val buckets = new HashMap[Interval, Bucket]() with SynchronizedMap[Interval, Bucket]
 
    val bufferProcessingThread = new Thread() { override def run() { keepProcessingBuffers() } }
    val blockPushingExecutor = Executors.newFixedThreadPool(5) 


    def start() {
      buffer.clear()
      if (buffer.remaining == 0) {
        throw new Exception("Buffer initialization error")
      }
      bufferProcessingThread.start()
    }
  
    def readDataToBuffer(func: ByteBuffer => Int): Int = {
      buffer.synchronized {
        if (buffer.remaining == 0) {
          logInfo("Received first data for interval " + currentShortInterval)
        }
        func(buffer) 
      }
    }

    def getLongInterval(shortInterval: Interval): Interval = {
      val intervalBegin = shortInterval.beginTime.floor(longIntervalDuration) 
      Interval(intervalBegin, intervalBegin + longIntervalDuration) 
    }

    def processBuffer() {

      def readInt(buffer: ByteBuffer): Int = {
        var offset = 0
        var result = 0
        while (offset < 32) {
          val b = buffer.get()
          result |= ((b & 0x7F) << offset)
          if ((b & 0x80) == 0) {
            return result
          }
          offset += 7
        }
        throw new Exception("Malformed zigzag-encoded integer")                                                            
      }

      val currentLongInterval = getLongInterval(currentShortInterval)
      val startTime = System.currentTimeMillis
      val newBuffer: ByteBuffer = buffer.synchronized {
        buffer.flip()
        if (buffer.remaining == 0) {
          buffer.clear()
          null 
        } else {
          logDebug("Processing interval " + currentShortInterval + " with delay of " + (System.currentTimeMillis - startTime) + " ms")
          val startTime1 = System.currentTimeMillis
          var loop = true
          var count = 0
          while(loop) {
            buffer.mark()
            try {
              val len = readInt(buffer) 
              buffer.position(buffer.position + len)
              count += 1
            } catch {
              case e: Exception => {
                buffer.reset()
                loop = false
              }
            }
          }
          val bytesToCopy = buffer.position
          val newBuf = ByteBuffer.allocate(bytesToCopy)
          buffer.position(0)
          newBuf.put(buffer.slice().limit(bytesToCopy).asInstanceOf[ByteBuffer])
          newBuf.flip()
          buffer.position(bytesToCopy)
          buffer.compact()
          newBuf
        }
      }

      if (newBuffer != null) {
        val bucket = buckets.getOrElseUpdate(currentLongInterval, new Bucket(currentLongInterval))
        bucket.synchronized {
          val newBlockId = inputName + "-" + currentLongInterval.toFormattedString + "-" + currentShortInterval.toFormattedString 
          val newBlock = new Block(newBlockId, currentShortInterval, newBuffer)
          if (syncOnLastShortInterval) {
            bucket += newBlock
          }
          logDebug("Created " + newBlock + " with " + newBuffer.remaining + " bytes, creation delay is " + (System.currentTimeMillis - currentShortInterval.endTime.asInstanceOf[LongTime].milliseconds) / 1000.0 + " s" ) 
          blockPushingExecutor.execute(new Runnable() { def run() { pushAndNotifyBlock(newBlock) } })
        }
      }
     
      val newShortInterval = Interval.currentInterval(shortIntervalDuration)
      val newLongInterval = getLongInterval(newShortInterval)
     
      if (newLongInterval != currentLongInterval) {
        buckets.get(currentLongInterval) match {
          case Some(bucket) => {
            bucket.synchronized {
              bucket.filled = true
              if (bucket.ready) {
                bucket.notifyAll()
              }
            }
          }
          case None =>
        }
        buckets += ((newLongInterval, new Bucket(newLongInterval)))
      }
      
      currentShortInterval = newShortInterval
    }

    def pushBlock(block: Block) {
      try{
        if (blockManager != null) {
          val startTime = System.currentTimeMillis
          logInfo(block + " put start delay is " + (startTime - block.shortInterval.endTime.asInstanceOf[LongTime].milliseconds) + " ms")
          /*blockManager.putBytes(block.id.toString, block.buffer, StorageLevel.DISK_AND_MEMORY)*/
          /*blockManager.putBytes(block.id.toString, block.buffer, StorageLevel.DISK_AND_MEMORY_2)*/
          blockManager.putBytes(block.id.toString, block.buffer, StorageLevel.MEMORY_ONLY_2)
          /*blockManager.putBytes(block.id.toString, block.buffer, StorageLevel.MEMORY_ONLY)*/
          /*blockManager.putBytes(block.id.toString, block.buffer, StorageLevel.DISK_AND_MEMORY_DESER)*/
          /*blockManager.putBytes(block.id.toString, block.buffer, StorageLevel.DISK_AND_MEMORY_DESER_2)*/
          val finishTime = System.currentTimeMillis
          logInfo(block + " put delay is " + (finishTime - startTime) + " ms")
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
   
    def keepProcessingBuffers() {
      logInfo("Thread to process buffers started")
      while(true) {
        processBuffer()
        val currentTimeMillis = System.currentTimeMillis
        val sleepTimeMillis = (currentTimeMillis / shortIntervalDurationMillis + 1) *
          shortIntervalDurationMillis - currentTimeMillis + 1
        Thread.sleep(sleepTimeMillis)
      }
    }

    def pushAndNotifyBlock(block: Block) {
      pushBlock(block)
      block.pushed = true
      val bucket = if (syncOnLastShortInterval) {
        buckets(block.longInterval)
      } else {
        var longInterval = block.longInterval 
        while(!buckets.contains(longInterval)) {
          logWarning("Skipping bucket of " + longInterval + " for " + block)
          longInterval = longInterval.next
        }
        val chosenBucket = buckets(longInterval)
        logDebug("Choosing bucket of " + longInterval + " for " + block)
        chosenBucket += block
        chosenBucket
      }
      
      bucket.synchronized {
        if (bucket.ready) {
          bucket.notifyAll()
        }
      }

    }
  }


  class ReceivingConnectionHandler(host: String, port: Int, dataHandler: DataHandler)
  extends ConnectionHandler(host, port, false) {

    override def ready(key: SelectionKey) {
      changeInterest(key, SelectionKey.OP_READ)
    }

    override def read(key: SelectionKey) {
      try {
        val channel = key.channel.asInstanceOf[SocketChannel]
        val bytesRead = dataHandler.readDataToBuffer(channel.read)
        if (bytesRead < 0) {
          close(key)
        }
      } catch {
        case e: IOException => {
          logError("Error reading", e)
          close(key)
        }
      }
    }
  }

  initLogging()

  val masterHost = System.getProperty("spark.master.host", "localhost")
  val masterPort = System.getProperty("spark.master.port", "7078").toInt
  
  val akkaPath = "akka://spark@%s:%s/user/".format(masterHost, masterPort)
  val sparkstreamScheduler = actorSystem.actorFor(akkaPath + "/SparkStreamScheduler")
  val testStreamCoordinator = actorSystem.actorFor(akkaPath + "/TestStreamCoordinator")

  logInfo("Getting stream details from master " + masterHost + ":" + masterPort)

  val streamDetails = askActor[GotStreamDetails](testStreamCoordinator, GetStreamDetails) match {
    case Some(details) => details
    case None => throw new Exception("Could not get stream details")
  }
  logInfo("Stream details received: " + streamDetails)
 
  val inputName = streamDetails.name
  val intervalDurationMillis = streamDetails.duration
  val intervalDuration = Milliseconds(intervalDurationMillis)
  val shortIntervalDuration = Milliseconds(System.getProperty("spark.stream.shortinterval", "500").toInt)

  val dataHandler = new DataHandler(inputName, intervalDuration, shortIntervalDuration, blockManager)
  val connectionHandler = new ReceivingConnectionHandler("localhost", 9999, dataHandler)

  val timeout = 100 millis

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
    connectionHandler.start()
    dataHandler.start()    

    var interval = Interval.currentInterval(intervalDuration) 
    var dataStarted = false


    while(true) {
      waitFor(interval.endTime)
      /*logInfo("Woken up at " + System.currentTimeMillis + " for " + interval)*/
      dataHandler.getBucket(interval) match {
        case Some(bucket) => {
          logDebug("Found " + bucket + " for " + interval)
          bucket.synchronized {
            if (!bucket.ready) {
              logDebug("Waiting for " + bucket)
              bucket.wait()
              logDebug("Wait over for " + bucket)
            }
            if (dataStarted || !bucket.empty) {
              logDebug("Notifying " + bucket)
              notifyScheduler(interval, bucket.blockIds)
              dataStarted = true
            }
            bucket.blocks.clear()
            dataHandler.clearBucket(interval)
          }
        }
        case None => {
          logDebug("Found none for " + interval)
          if (dataStarted) {
            logDebug("Notifying none")
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
      val delay = (System.currentTimeMillis - time.milliseconds) 
      logInfo("Notification delay for " + time + " is " + delay + " ms")
    }  catch {
      case e: Exception => logError("Exception notifying scheduler at interval " + interval + ":  " + e) 
    }
  }
}


object TestStreamReceiver4 {
  def main(args: Array[String]) {
    val details = Array(("Sentences", 2000L))
    val (actorSystem, port) = AkkaUtils.createActorSystem("spark", Utils.localHostName, 7078)
    actorSystem.actorOf(Props(new TestStreamCoordinator(details)), name = "TestStreamCoordinator")
    new TestStreamReceiver4(actorSystem, null).start()
  }
}
