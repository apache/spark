package spark

import java.net._
import java.util.{BitSet, UUID}
import java.util.concurrent.{Executors, ThreadFactory, ThreadPoolExecutor}

@serializable
trait Broadcast[T] {
  val uuid = UUID.randomUUID

  def value: T

  // We cannot have an abstract readObject here due to some weird issues with 
  // readObject having to be 'private' in sub-classes. Possibly a Scala bug!

  override def toString = "spark.Broadcast(" + uuid + ")"
}

trait BroadcastFactory {
  def initialize (isMaster: Boolean): Unit
  def newBroadcast[T] (value_ : T, isLocal: Boolean): Broadcast[T]
}

private object Broadcast
extends Logging {
  private var initialized = false 
  private var isMaster_ = false
  private var broadcastFactory: BroadcastFactory = null

  // Called by SparkContext or Executor before using Broadcast
  def initialize (isMaster__ : Boolean): Unit = synchronized {
    if (!initialized) {
      val broadcastFactoryClass = System.getProperty("spark.broadcast.factory",
        "spark.DfsBroadcastFactory")

      broadcastFactory = 
        Class.forName(broadcastFactoryClass).newInstance.asInstanceOf[BroadcastFactory]

      // Setup isMaster before using it
      isMaster_ = isMaster__

      // Initialize appropriate BroadcastFactory and BroadcastObject
      broadcastFactory.initialize(isMaster)
      
      initialized = true
    }
  }
  
  def getBroadcastFactory: BroadcastFactory = {
    if (broadcastFactory == null) {
      throw new SparkException ("Broadcast.getBroadcastFactory called before initialize")
    }
    broadcastFactory
  }
  
  // Load common broadcast-related config parameters
  private var MasterHostAddress_ = System.getProperty(
    "spark.broadcast.masterHostAddress", InetAddress.getLocalHost.getHostAddress)
  private var MasterTrackerPort_ = System.getProperty(
    "spark.broadcast.masterTrackerPort", "11111").toInt
  private var BlockSize_ = System.getProperty(
    "spark.broadcast.blockSize", "4096").toInt * 1024
  private var MaxRetryCount_ = System.getProperty(
    "spark.broadcast.maxRetryCount", "2").toInt

  private var TrackerSocketTimeout_ = System.getProperty(
    "spark.broadcast.trackerSocketTimeout", "50000").toInt
  private var ServerSocketTimeout_ = System.getProperty(
    "spark.broadcast.serverSocketTimeout", "10000").toInt

  private var MinKnockInterval_ = System.getProperty(
    "spark.broadcast.minKnockInterval", "500").toInt
  private var MaxKnockInterval_ = System.getProperty(
    "spark.broadcast.maxKnockInterval", "999").toInt

  def isMaster = isMaster_

  def MasterHostAddress = MasterHostAddress_
  def MasterTrackerPort = MasterTrackerPort_
  def BlockSize = BlockSize_
  def MaxRetryCount = MaxRetryCount_

  def TrackerSocketTimeout = TrackerSocketTimeout_
  def ServerSocketTimeout = ServerSocketTimeout_

  def MinKnockInterval = MinKnockInterval_
  def MaxKnockInterval = MaxKnockInterval_

  // Returns a standard ThreadFactory except all threads are daemons
  private def newDaemonThreadFactory: ThreadFactory = {
    new ThreadFactory {
      def newThread(r: Runnable): Thread = {
        var t = Executors.defaultThreadFactory.newThread (r)
        t.setDaemon (true)
        return t
      }
    }  
  }
  
  // Wrapper over newCachedThreadPool
  def newDaemonCachedThreadPool: ThreadPoolExecutor = {
    var threadPool = 
      Executors.newCachedThreadPool.asInstanceOf[ThreadPoolExecutor]
  
    threadPool.setThreadFactory (newDaemonThreadFactory)
    
    return threadPool
  }
  
  // Wrapper over newFixedThreadPool
  def newDaemonFixedThreadPool (nThreads: Int): ThreadPoolExecutor = {
    var threadPool = 
      Executors.newFixedThreadPool (nThreads).asInstanceOf[ThreadPoolExecutor]
  
    threadPool.setThreadFactory (newDaemonThreadFactory)
    
    return threadPool
  }  
}

// CHANGED: Keep track of the blockSize for THIS broadcast variable. 
// Broadcast.BlockSize is expected to be updated across different broadcasts
@serializable
case class SourceInfo (val hostAddress: String, 
                       val listenPort: Int, 
                       val totalBlocks: Int = SourceInfo.UnusedParam, 
                       val totalBytes: Int = SourceInfo.UnusedParam, 
                       val blockSize: Int = Broadcast.BlockSize)
extends Comparable[SourceInfo] with Logging {
  var currentLeechers = 0
  var receptionFailed = false  
  
  var hasBlocks = 0
  var hasBlocksBitVector: BitSet = new BitSet (totalBlocks)
  
  // Ascending sort based on leecher count
  def compareTo (o: SourceInfo): Int = (currentLeechers - o.currentLeechers)
}

object SourceInfo {
  // Constants for special values of listenPort
  val TxNotStartedRetry = -1
  val TxOverGoToHDFS = 0
  // Other constants
  val StopBroadcast = -2
  val UnusedParam = 0
}

@serializable
case class BroadcastBlock (val blockID: Int, val byteArray: Array[Byte]) { }

@serializable
case class VariableInfo (@transient val arrayOfBlocks : Array[BroadcastBlock], 
  val totalBlocks: Int, val totalBytes: Int) {  
  @transient var hasBlocks = 0
} 

@serializable
class SpeedTracker {
  // Mapping 'source' to '(totalTime, numBlocks)'
  private var sourceToSpeedMap = Map[SourceInfo, (Long, Int)] ()
  
  def addDataPoint (srcInfo: SourceInfo, timeInMillis: Long): Unit = {
    sourceToSpeedMap.synchronized {
      if (!sourceToSpeedMap.contains(srcInfo)) {
        sourceToSpeedMap += (srcInfo -> (timeInMillis, 1))
      } else {
        val tTnB = sourceToSpeedMap (srcInfo)
        sourceToSpeedMap += (srcInfo -> (tTnB._1 + timeInMillis, tTnB._2 + 1))
      }
    }
  }
  
  def getTimePerBlock (srcInfo: SourceInfo): Double = {
    sourceToSpeedMap.synchronized {
      val tTnB = sourceToSpeedMap (srcInfo)
      return tTnB._1 / tTnB._2
    }    
  }
  
  override def toString = sourceToSpeedMap.toString
}