package spark.streaming

import spark.Logging
import spark.storage.BlockManager
import spark.storage.StorageLevel
import spark.SparkEnv
import spark.streaming.util.SystemClock
import spark.streaming.util.RecurringTimer

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Queue
import scala.collection.mutable.SynchronizedPriorityQueue
import scala.math.Ordering

import java.net.InetSocketAddress
import java.net.Socket
import java.io.InputStream
import java.io.BufferedInputStream
import java.io.DataInputStream
import java.io.EOFException
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.ArrayBlockingQueue

import akka.actor._
import akka.pattern.ask
import akka.util.duration._
import akka.dispatch._

trait NetworkInputReceiverMessage
case class GetBlockIds(time: Long) extends NetworkInputReceiverMessage
case class GotBlockIds(streamId: Int, blocksIds: Array[String]) extends NetworkInputReceiverMessage
case class StopReceiver() extends NetworkInputReceiverMessage

class NetworkInputReceiver[T: ClassManifest](streamId: Int, host: String, port: Int, bytesToObjects: InputStream => Iterator[T]) 
extends Logging {
 
  class ReceiverActor extends Actor {    
    override def preStart() = {
      logInfo("Attempting to register")
      val ip = System.getProperty("spark.master.host", "localhost")
      val port = System.getProperty("spark.master.port", "7077").toInt
      val actorName: String = "NetworkInputTracker"
      val url = "akka://spark@%s:%s/user/%s".format(ip, port, actorName)
      val trackerActor = env.actorSystem.actorFor(url)
      val timeout = 100.milliseconds
      val future = trackerActor.ask(RegisterReceiver(streamId, self))(timeout)
      Await.result(future, timeout)         
    }
    
    def receive = {
      case GetBlockIds(time) => {
        logInfo("Got request for block ids for " + time)
        sender ! GotBlockIds(streamId, dataHandler.getPushedBlocks())        
      }
      
      case StopReceiver() => {
        if (receivingThread != null) {
          receivingThread.interrupt()
        }
        sender ! true
      }
    }
  }
  
  class DataHandler {
    
    class Block(val time: Long, val iterator: Iterator[T]) {
      val blockId = "input-" + streamId + "-" + time
      var pushed = true
      override def toString() = "input block " + blockId
    }
    
    val clock = new SystemClock()
    val blockInterval = 200L 
    val blockIntervalTimer = new RecurringTimer(clock, blockInterval, updateCurrentBuffer)
    val blockOrdering = new Ordering[Block] {
      def compare(b1: Block, b2: Block) = (b1.time - b2.time).toInt
    }
    val blockStorageLevel = StorageLevel.DISK_AND_MEMORY
    val blocksForPushing = new ArrayBlockingQueue[Block](1000) 
    val blocksForReporting = new SynchronizedPriorityQueue[Block]()(blockOrdering)
    val blockPushingThread = new Thread() { override def run() { keepPushingBlocks() } }

    var currentBuffer = new ArrayBuffer[T]
    
    def start() {
      blockIntervalTimer.start()
      blockPushingThread.start()
      logInfo("Data handler started")
    }
    
    def stop() {
      blockIntervalTimer.stop()
      blockPushingThread.interrupt()
    }
    
    def += (obj: T) {
      currentBuffer += obj
    }
    
    def updateCurrentBuffer(time: Long) {      
      val newBlockBuffer = currentBuffer
      currentBuffer = new ArrayBuffer[T]      
      if (newBlockBuffer.size > 0) {
        val newBlock = new Block(time - blockInterval, newBlockBuffer.toIterator)
        blocksForPushing.add(newBlock)
        blocksForReporting.enqueue(newBlock)
      }
    } 
    
    def keepPushingBlocks() {
      logInfo("Block pushing thread started")
      try {
        while(true) {
          val block = blocksForPushing.take()
          if (blockManager != null) {
            blockManager.put(block.blockId, block.iterator, blockStorageLevel)
            block.pushed = true
          } else {
            logWarning(block + " not put as block manager is null")
          }
        }
      } catch {
        case ie: InterruptedException => println("Block pushing thread interrupted")
        case e: Exception => e.printStackTrace()
      }
    }
    
    def getPushedBlocks(): Array[String] = {
      val pushedBlocks = new ArrayBuffer[String]() 
      var loop = true
      while(loop && !blocksForReporting.isEmpty) {
        val block = blocksForReporting.dequeue()
        if (block == null) {
          loop = false
        } else if (!block.pushed) {
          blocksForReporting.enqueue(block)
        } else {
          pushedBlocks += block.blockId
        }
      }
      logInfo("Got " + pushedBlocks.size + " blocks")
      pushedBlocks.toArray
    }
  }
  
  val blockManager = if (SparkEnv.get != null) SparkEnv.get.blockManager else null  
  val dataHandler = new DataHandler()
  val env = SparkEnv.get
  
  var receiverActor: ActorRef = null
  var receivingThread: Thread = null

  def run() {
    initLogging()
    var socket: Socket = null
    try {
      if (SparkEnv.get != null) {
        receiverActor = SparkEnv.get.actorSystem.actorOf(Props(new ReceiverActor), "ReceiverActor-" + streamId)      
      }      
      dataHandler.start()            
      socket = connect()
      receivingThread = Thread.currentThread()
      receive(socket)            
    } catch {
      case ie: InterruptedException => logInfo("Receiver interrupted")
    } finally {
      receivingThread = null
      if (socket != null) socket.close()
      dataHandler.stop()
    }        
  } 
  
  def connect(): Socket = {
    logInfo("Connecting to " + host + ":" + port)
    val socket = new Socket(host, port)
    logInfo("Connected to " + host + ":" + port)
    socket        
  }
  
  def receive(socket: Socket) {
    val iterator = bytesToObjects(socket.getInputStream())
    while(iterator.hasNext) {
      val obj = iterator.next
      dataHandler += obj
    }
  }    
}


object NetworkInputReceiver {
  
  def bytesToLines(inputStream: InputStream): Iterator[String] = {
    val bufferedInputStream = new BufferedInputStream(inputStream)
    val dataInputStream = new DataInputStream(bufferedInputStream)   
    
    val iterator = new Iterator[String] {
      var gotNext = false
      var finished = false
      var nextValue: String = null
  
      private def getNext() {
        try {
          nextValue = dataInputStream.readLine()
          println("[" + nextValue + "]")
        } catch {
          case eof: EOFException =>
            finished = true
        }
        gotNext = true
      }
      
      override def hasNext: Boolean = {
        if (!gotNext) {
          getNext()
        }
        if (finished) {
          dataInputStream.close()
        }
        !finished
      }
  
      
      override def next(): String = {
        if (!gotNext) {
          getNext()
        }
        if (finished) {
          throw new NoSuchElementException("End of stream")
        }
        gotNext = false
        nextValue
      }
    }
    iterator
  }
  
  def main(args: Array[String]) {
    if (args.length < 2) {
      println("NetworkReceiver <hostname> <port>")
      System.exit(1)
    }
    val host = args(0)
    val port = args(1).toInt
    val receiver = new NetworkInputReceiver(0, host, port, bytesToLines)
    receiver.run()
  }
}
