package spark

import java.util.UUID
import java.util.concurrent.{Executors, ThreadPoolExecutor, ThreadFactory}

@serializable
trait Broadcast {
  val uuid = UUID.randomUUID

  // We cannot have an abstract readObject here due to some weird issues with 
  // readObject having to be 'private' in sub-classes. Possibly a Scala bug!
  def sendBroadcast: Unit

  override def toString = "spark.Broadcast(" + uuid + ")"
}

private object Broadcast
extends Logging {
  private var initialized = false 

  // Called by SparkContext or Executor before using Broadcast
  // Calls all other initializers here
  def initialize (isMaster: Boolean): Unit = {
    synchronized {
      if (!initialized) {
        // Initialization for DfsBroadcast
        DfsBroadcast.initialize 
        // Initialization for ChainedStreamingBroadcast
        ChainedBroadcast.initialize (isMaster)
        
        initialized = true
      }
    }
  }
  
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

@serializable
case class SourceInfo (val hostAddress: String, val listenPort: Int, 
  val totalBlocks: Int, val totalBytes: Int, val replicaID: Int)  
extends Comparable [SourceInfo] with Logging {

  var currentLeechers = 0
  var receptionFailed = false
  
  var hasBlocks = 0
  
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
