package spark.broadcast

import java.io._
import java.net._
import java.util.{BitSet, UUID}
import java.util.concurrent.{Executors, ThreadFactory, ThreadPoolExecutor}

import spark._

@serializable
trait Broadcast[T] {
  val uuid = UUID.randomUUID

  def value: T

  // We cannot have an abstract readObject here due to some weird issues with
  // readObject having to be 'private' in sub-classes. Possibly a Scala bug!

  override def toString = "spark.Broadcast(" + uuid + ")"
}

object Broadcast
extends Logging {
  // Messages
  val REGISTER_BROADCAST_TRACKER = 0
  val UNREGISTER_BROADCAST_TRACKER = 1
  val FIND_BROADCAST_TRACKER = 2
  val GET_UPDATED_SHARE = 3

  private var initialized = false
  private var isMaster_ = false
  private var broadcastFactory: BroadcastFactory = null

  // Called by SparkContext or Executor before using Broadcast
  def initialize (isMaster__ : Boolean): Unit = synchronized {
    if (!initialized) {
      val broadcastFactoryClass = System.getProperty("spark.broadcast.factory",
        "spark.broadcast.DfsBroadcastFactory")

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

  // Load ChainedBroadcast config params

  // Load TreeBroadcast config params
  private var MaxDegree_ = System.getProperty("spark.broadcast.maxDegree", "2").toInt

  // Load BitTorrentBroadcast config params
  private var MaxPeersInGuideResponse_ = System.getProperty(
    "spark.broadcast.maxPeersInGuideResponse", "4").toInt

  private var MaxRxSlots_ = System.getProperty(
    "spark.broadcast.maxRxSlots", "4").toInt
  private var MaxTxSlots_ = System.getProperty(
    "spark.broadcast.maxTxSlots", "4").toInt

  private var MaxChatTime_ = System.getProperty(
    "spark.broadcast.maxChatTime", "500").toInt
  private var MaxChatBlocks_ = System.getProperty(
    "spark.broadcast.maxChatBlocks", "1024").toInt

  private var EndGameFraction_ = System.getProperty(
    "spark.broadcast.endGameFraction", "0.95").toDouble

  def isMaster = isMaster_

  // Common config params
  def MasterHostAddress = MasterHostAddress_
  def MasterTrackerPort = MasterTrackerPort_
  def BlockSize = BlockSize_
  def MaxRetryCount = MaxRetryCount_

  def TrackerSocketTimeout = TrackerSocketTimeout_
  def ServerSocketTimeout = ServerSocketTimeout_

  def MinKnockInterval = MinKnockInterval_
  def MaxKnockInterval = MaxKnockInterval_

  // ChainedBroadcast configs

  // TreeBroadcast configs
  def MaxDegree = MaxDegree_

  // BitTorrentBroadcast configs
  def MaxPeersInGuideResponse = MaxPeersInGuideResponse_

  def MaxRxSlots = MaxRxSlots_
  def MaxTxSlots = MaxTxSlots_

  def MaxChatTime = MaxChatTime_
  def MaxChatBlocks = MaxChatBlocks_

  def EndGameFraction = EndGameFraction_

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
  
  // Helper functions to convert an object to Array[BroadcastBlock]
  def blockifyObject[IN](obj: IN): VariableInfo = {
    val baos = new ByteArrayOutputStream
    val oos = new ObjectOutputStream(baos)
    oos.writeObject(obj)
    oos.close()
    baos.close()
    val byteArray = baos.toByteArray
    val bais = new ByteArrayInputStream(byteArray)

    var blockNum = (byteArray.length / Broadcast.BlockSize)
    if (byteArray.length % Broadcast.BlockSize != 0)
      blockNum += 1

    var retVal = new Array[BroadcastBlock](blockNum)
    var blockID = 0

    for (i <- 0 until (byteArray.length, Broadcast.BlockSize)) {
      val thisBlockSize = math.min(Broadcast.BlockSize, byteArray.length - i)
      var tempByteArray = new Array[Byte](thisBlockSize)
      val hasRead = bais.read(tempByteArray, 0, thisBlockSize)

      retVal(blockID) = new BroadcastBlock(blockID, tempByteArray)
      blockID += 1
    }
    bais.close()

    var variableInfo = VariableInfo(retVal, blockNum, byteArray.length)
    variableInfo.hasBlocks = blockNum

    return variableInfo
  }

  // Helper function to convert Array[BroadcastBlock] to object
  def unBlockifyObject[OUT](arrayOfBlocks: Array[BroadcastBlock], 
                            totalBytes: Int, 
                            totalBlocks: Int): OUT = {

    var retByteArray = new Array[Byte](totalBytes)
    for (i <- 0 until totalBlocks) {
      System.arraycopy(arrayOfBlocks(i).byteArray, 0, retByteArray,
        i * Broadcast.BlockSize, arrayOfBlocks(i).byteArray.length)
    }
    byteArrayToObject(retByteArray)
  }

  private def byteArrayToObject[OUT](bytes: Array[Byte]): OUT = {
    val in = new ObjectInputStream (new ByteArrayInputStream (bytes)){
      override def resolveClass(desc: ObjectStreamClass) =
        Class.forName(desc.getName, false, currentThread.getContextClassLoader)
    }    
    val retVal = in.readObject.asInstanceOf[OUT]
    in.close()
    return retVal
  }  
}

@serializable
case class BroadcastBlock (val blockID: Int, val byteArray: Array[Byte]) { }

@serializable
case class VariableInfo (@transient val arrayOfBlocks : Array[BroadcastBlock],
                                    val totalBlocks: Int, 
                                    val totalBytes: Int) {
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