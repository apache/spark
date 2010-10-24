package spark

import java.io._
import java.net._
import java.util.{UUID, PriorityQueue, Comparator}

import java.util.concurrent.{Executors, ExecutorService}

import scala.actors.Actor
import scala.actors.Actor._

import scala.collection.mutable.Map

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path, RawLocalFileSystem}

import spark.compress.lzf.{LZFInputStream, LZFOutputStream}

@serializable
trait BroadcastRecipe {
  val uuid = UUID.randomUUID

  // We cannot have an abstract readObject here due to some weird issues with 
  // readObject having to be 'private' in sub-classes. Possibly a Scala bug!
  def sendBroadcast: Unit

  override def toString = "spark.Broadcast(" + uuid + ")"
}

// TODO: Right, now no parallelization between multiple broadcasts
@serializable
class ChainedStreamingBroadcast[T] (@transient var value_ : T, local: Boolean) 
extends BroadcastRecipe with Logging {
  
  def value = value_

  BroadcastCS.synchronized { BroadcastCS.values.put (uuid, value_) }
   
  if (!local) { sendBroadcast }
  
  def sendBroadcast () {
    // Create a variableInfo object and store it in valueInfos
    var variableInfo = blockifyObject (value_, BroadcastCS.blockSize)   
    // TODO: Even though this part is not in use now, there is problem in the 
    // following statement. Shouldn't use constant port and hostAddress anymore?
    // val masterSource = 
    //   new SourceInfo (BroadcastCS.masterHostAddress, BroadcastCS.masterListenPort, 
    //     variableInfo.totalBlocks, variableInfo.totalBytes, 0) 
    // variableInfo.pqOfSources.add (masterSource)
    
    BroadcastCS.synchronized { 
      // BroadcastCS.valueInfos.put (uuid, variableInfo)
      
      // TODO: Not using variableInfo in current implementation. Manually
      // setting all the variables inside BroadcastCS object
      
      BroadcastCS.initializeVariable (variableInfo)      
    }
    
    // Now store a persistent copy in HDFS, just in case 
    val out = new ObjectOutputStream (BroadcastCH.openFileForWriting(uuid))
    out.writeObject (value_)
    out.close
  }
  
  // Called by Java when deserializing an object
  private def readObject (in: ObjectInputStream) {
    in.defaultReadObject
    BroadcastCS.synchronized {
      val cachedVal = BroadcastCS.values.get (uuid)
      if (cachedVal != null) {
        value_ = cachedVal.asInstanceOf[T]
      } else {
        // Only a single worker (the first one) in the same node can ever be 
        // here. The rest will always get the value ready. 
        val start = System.nanoTime        

        val retByteArray = BroadcastCS.receiveBroadcast (uuid)
        // If does not succeed, then get from HDFS copy
        if (retByteArray != null) {
          value_ = byteArrayToObject[T] (retByteArray)
          BroadcastCS.values.put (uuid, value_)
          // val variableInfo = blockifyObject (value_, BroadcastCS.blockSize)    
          // BroadcastCS.valueInfos.put (uuid, variableInfo)
        }  else {
          val fileIn = new ObjectInputStream(BroadcastCH.openFileForReading(uuid))
          value_ = fileIn.readObject.asInstanceOf[T]
          BroadcastCH.values.put(uuid, value_)
          fileIn.close
        } 
        
        val time = (System.nanoTime - start) / 1e9
        logInfo("Reading Broadcasted variable " + uuid + " took " + time + " s")                  
      }
    }
  }
  
  private def blockifyObject (obj: T, blockSize: Int): VariableInfo = {
    val baos = new ByteArrayOutputStream
    val oos = new ObjectOutputStream (baos)
    oos.writeObject (obj)
    oos.close
    baos.close
    val byteArray = baos.toByteArray
    val bais = new ByteArrayInputStream (byteArray)
    
    var blockNum = (byteArray.length / blockSize) 
    if (byteArray.length % blockSize != 0) 
      blockNum += 1
      
    var retVal = new Array[BroadcastBlock] (blockNum)
    var blockID = 0

    // TODO: What happens in byteArray.length == 0 => blockNum == 0
    for (i <- 0 until (byteArray.length, blockSize)) {    
      val thisBlockSize = Math.min (blockSize, byteArray.length - i)
      var tempByteArray = new Array[Byte] (thisBlockSize)
      val hasRead = bais.read (tempByteArray, 0, thisBlockSize)
      
      retVal (blockID) = new BroadcastBlock (blockID, tempByteArray)
      blockID += 1
    } 
    bais.close

    var variableInfo = VariableInfo (retVal, blockNum, byteArray.length)
    variableInfo.hasBlocks = blockNum
    
    return variableInfo
  }  
  
  private def byteArrayToObject[A] (bytes: Array[Byte]): A = {
    val in = new ObjectInputStream (new ByteArrayInputStream (bytes))
    val retVal = in.readObject.asInstanceOf[A]
    in.close
    return retVal
  }
  
  private def getByteArrayOutputStream (obj: T): ByteArrayOutputStream = {
    val bOut = new ByteArrayOutputStream
    val out = new ObjectOutputStream (bOut)
    out.writeObject (obj)
    out.close
    bOut.close
    return bOut
  }  
}

@serializable 
class CentralizedHDFSBroadcast[T](@transient var value_ : T, local: Boolean) 
extends BroadcastRecipe with Logging {
  
  def value = value_

  BroadcastCH.synchronized { BroadcastCH.values.put(uuid, value_) }

  if (!local) { sendBroadcast }

  def sendBroadcast () {
    val out = new ObjectOutputStream (BroadcastCH.openFileForWriting(uuid))
    out.writeObject (value_)
    out.close
  }

  // Called by Java when deserializing an object
  private def readObject(in: ObjectInputStream) {
    in.defaultReadObject
    BroadcastCH.synchronized {
      val cachedVal = BroadcastCH.values.get(uuid)
      if (cachedVal != null) {
        value_ = cachedVal.asInstanceOf[T]
      } else {
        val start = System.nanoTime
        
        val fileIn = new ObjectInputStream(BroadcastCH.openFileForReading(uuid))
        value_ = fileIn.readObject.asInstanceOf[T]
        BroadcastCH.values.put(uuid, value_)
        fileIn.close
        
        val time = (System.nanoTime - start) / 1e9
        logInfo("Reading Broadcasted variable " + uuid + " took " + time + " s")
      }
    }
  }
}

@serializable
case class SourceInfo (val hostAddress: String, val listenPort: Int, 
  val totalBlocks: Int, val totalBytes: Int, val replicaID: Int)  
extends Comparable[SourceInfo]{

  var currentLeechers = 0
  var receptionFailed = false
  
  def compareTo (o: SourceInfo): Int = (currentLeechers - o.currentLeechers)
}

@serializable
case class BroadcastBlock (val blockID: Int, val byteArray: Array[Byte]) { }

@serializable
case class VariableInfo (@transient val arrayOfBlocks : Array[BroadcastBlock], 
  val totalBlocks: Int, val totalBytes: Int) {
  
  @transient var hasBlocks = 0

  val listenPortLock = new AnyRef
  val totalBlocksLock = new AnyRef
  val hasBlocksLock = new AnyRef
  
  @transient var pqOfSources = new PriorityQueue[SourceInfo]
} 

private object Broadcast {
  private var initialized = false 

  // Will be called by SparkContext or Executor before using Broadcast
  // Calls all other initializers here
  def initialize (isMaster: Boolean) {
    synchronized {
      if (!initialized) {
        // Initialization for CentralizedHDFSBroadcast
        BroadcastCH.initialize 
        // Initialization for ChainedStreamingBroadcast
        // BroadcastCS.initialize (isMaster)
        
        initialized = true
      }
    }
  }
}

private object BroadcastCS extends Logging {
  val values = Cache.newKeySpace()

  // private var valueToPort = Map[UUID, Int] ()

  private var initialized = false
  private var isMaster_ = false

  private var masterHostAddress_ = "127.0.0.1"
  private var masterListenPort_ : Int = 11111
  private var blockSize_ : Int = 512 * 1024
  private var maxRetryCount_ : Int = 2
  private var serverSocketTimout_ : Int = 50000
  private var dualMode_ : Boolean = false

  private val hostAddress = InetAddress.getLocalHost.getHostAddress
  private var listenPort = -1 
  
  var arrayOfBlocks: Array[BroadcastBlock] = null
  var totalBytes = -1
  var totalBlocks = -1
  var hasBlocks = 0

  val listenPortLock = new Object
  val totalBlocksLock = new Object
  val hasBlocksLock = new Object
  
  var pqOfSources = new PriorityQueue[SourceInfo]
  
  private var serveMR: ServeMultipleRequests = null 
  private var guideMR: GuideMultipleRequests = null 

  def initialize (isMaster__ : Boolean) {
    synchronized {
      if (!initialized) {
        masterHostAddress_ = 
          System.getProperty ("spark.broadcast.masterHostAddress", "127.0.0.1")
        masterListenPort_ = 
          System.getProperty ("spark.broadcast.masterListenPort", "11111").toInt
        blockSize_ = 
          System.getProperty ("spark.broadcast.blockSize", "512").toInt * 1024
        maxRetryCount_ = 
          System.getProperty ("spark.broadcast.maxRetryCount", "2").toInt          
        serverSocketTimout_ = 
          System.getProperty ("spark.broadcast.serverSocketTimout", "50000").toInt
        dualMode_ = 
          System.getProperty ("spark.broadcast.dualMode", "false").toBoolean          

        isMaster_ = isMaster__        
        
        if (isMaster) {
          guideMR = new GuideMultipleRequests
          guideMR.setDaemon (true)
          guideMR.start
          logInfo("GuideMultipleRequests started")
        }        

        serveMR = new ServeMultipleRequests
        serveMR.setDaemon (true)
        serveMR.start        
        logInfo("ServeMultipleRequests started")
        
        logInfo("BroadcastCS object has been initialized")
                  
        initialized = true
      }
    }
  }
  
  // TODO: This should change in future implementation. 
  // Called from the Master constructor to setup states for this particular that
  // is being broadcasted
  def initializeVariable (variableInfo: VariableInfo) {
    arrayOfBlocks = variableInfo.arrayOfBlocks
    totalBytes = variableInfo.totalBytes
    totalBlocks = variableInfo.totalBlocks
    hasBlocks = variableInfo.totalBlocks
    
    // listenPort should already be valid
    assert (listenPort != -1)
    
    pqOfSources = new PriorityQueue[SourceInfo]
    val masterSource_0 = 
      new SourceInfo (hostAddress, listenPort, totalBlocks, totalBytes, 0) 
    BroadcastCS.pqOfSources.add (masterSource_0)
    // Add one more time to have two replicas of any seeds in the PQ
    if (BroadcastCS.dualMode) {
      val masterSource_1 = 
        new SourceInfo (hostAddress, listenPort, totalBlocks, totalBytes, 1) 
      BroadcastCS.pqOfSources.add (masterSource_1)
    }
  }
  
  def masterHostAddress = masterHostAddress_
  def masterListenPort = masterListenPort_
  def blockSize = blockSize_
  def maxRetryCount = maxRetryCount_
  def serverSocketTimout = serverSocketTimout_
  def dualMode = dualMode_

  def isMaster = isMaster_ 

  def receiveBroadcast (variableUUID: UUID): Array[Byte] = {  
    // Wait until hostAddress and listenPort are created by the 
    // ServeMultipleRequests thread
    // NO need to wait; ServeMultipleRequests is created much further ahead
    while (listenPort == -1) { 
      listenPortLock.synchronized {
        listenPortLock.wait 
      }
    } 

    // Connect and receive broadcast from the specified source, retrying the
    // specified number of times in case of failures
    var retriesLeft = BroadcastCS.maxRetryCount
    var retByteArray: Array[Byte] = null
    do {
      // Connect to Master and send this worker's Information
      val clientSocketToMaster = 
        new Socket(BroadcastCS.masterHostAddress, BroadcastCS.masterListenPort)  
      logInfo("Connected to Master's guiding object")
      // TODO: Guiding object connection is reusable
      val oisMaster = 
        new ObjectInputStream (clientSocketToMaster.getInputStream)
      val oosMaster = 
        new ObjectOutputStream (clientSocketToMaster.getOutputStream)
      
      oosMaster.writeObject(new SourceInfo (hostAddress, listenPort, -1, -1, 0))
      oosMaster.flush

      // Receive source information from Master        
      var sourceInfo = oisMaster.readObject.asInstanceOf[SourceInfo]
      totalBlocks = sourceInfo.totalBlocks
      arrayOfBlocks = new Array[BroadcastBlock] (totalBlocks)
      totalBlocksLock.synchronized {
        totalBlocksLock.notifyAll
      }
      totalBytes = sourceInfo.totalBytes      
      logInfo("Received SourceInfo from Master:" + sourceInfo + " My Port: " + listenPort)    

      retByteArray = receiveSingleTransmission (sourceInfo)
      
      logInfo("I got this from receiveSingleTransmission: " + retByteArray)

      // TODO: Update sourceInfo to add error notifactions for Master
      if (retByteArray == null) { sourceInfo.receptionFailed = true }
      
      // TODO: Supposed to update values here, but we don't support advanced
      // statistics right now. Master can handle leecherCount by itself.

      // Send back statistics to the Master
      oosMaster.writeObject (sourceInfo) 
    
      oisMaster.close
      oosMaster.close
      clientSocketToMaster.close                    
      
      retriesLeft -= 1
    } while (retriesLeft > 0 && retByteArray == null)
    
    return retByteArray
  }

  // Tries to receive broadcast from the Master and returns Boolean status.
  // This might be called multiple times to retry a defined number of times.
  private def receiveSingleTransmission(sourceInfo: SourceInfo): Array[Byte] = {
    var clientSocketToSource: Socket = null    
    var oisSource: ObjectInputStream = null
    var oosSource: ObjectOutputStream = null
    
    var retByteArray:Array[Byte] = null
    
    try {
      // Connect to the source to get the object itself
      clientSocketToSource = 
        new Socket (sourceInfo.hostAddress, sourceInfo.listenPort)        
      oosSource = 
        new ObjectOutputStream (clientSocketToSource.getOutputStream)
      oisSource = 
        new ObjectInputStream (clientSocketToSource.getInputStream)
        
      logInfo("Inside receiveSingleTransmission")
      logInfo("totalBlocks: " + totalBlocks + " " + "hasBlocks: " + hasBlocks)
      retByteArray = new Array[Byte] (totalBytes)
      for (i <- 0 until totalBlocks) {
        val bcBlock = oisSource.readObject.asInstanceOf[BroadcastBlock]
        System.arraycopy (bcBlock.byteArray, 0, retByteArray, 
          i * BroadcastCS.blockSize, bcBlock.byteArray.length)
        arrayOfBlocks(hasBlocks) = bcBlock
        hasBlocks += 1
        hasBlocksLock.synchronized {
          hasBlocksLock.notifyAll
        }
        logInfo("Received block: " + i + " " + bcBlock)
      } 
      assert (hasBlocks == totalBlocks)
      logInfo("After the receive loop")
    } catch {
      case e: Exception => { 
        retByteArray = null 
        logInfo("receiveSingleTransmission had a " + e)
      }
    } finally {    
      if (oisSource != null) { oisSource.close }
      if (oosSource != null) { oosSource.close }
      if (clientSocketToSource != null) { clientSocketToSource.close }
    }
          
    return retByteArray
  } 
  
//  class TrackMultipleValues extends Thread with Logging {
//    override def run = {
//      var threadPool = Executors.newCachedThreadPool
//      var serverSocket: ServerSocket = null
//      
//      serverSocket = new ServerSocket (BroadcastCS.masterListenPort)
//      logInfo("TrackMultipleVariables" + serverSocket + " " + listenPort)
//      
//      var keepAccepting = true
//      try {
//        while (keepAccepting) {
//          var clientSocket: Socket = null
//          try {
//            serverSocket.setSoTimeout (serverSocketTimout)
//            clientSocket = serverSocket.accept
//          } catch {
//            case e: Exception => { 
//              logInfo("TrackMultipleValues Timeout. Stopping listening...") 
//              keepAccepting = false 
//            }
//          }
//          logInfo("TrackMultipleValues:Got new request:" + clientSocket)
//          if (clientSocket != null) {
//            try {            
//              threadPool.execute (new Runnable {
//                def run = {
//                  val oos = new ObjectOutputStream (clientSocket.getOutputStream)
//                  val ois = new ObjectInputStream (clientSocket.getInputStream)
//                  try {
//                    val variableUUID = ois.readObject.asInstanceOf[UUID]
//                    var contactPort = 0
//                    // TODO: Add logic and data structures to find out UUID->port
//                    // mapping. 0 = missed the broadcast, read from HDFS; <0 = 
//                    // Haven't started yet, wait & retry; >0 = Read from this port
//                    oos.writeObject (contactPort)
//                  } catch {
//                    case e: Exception => { }
//                  } finally {
//                    ois.close
//                    oos.close
//                    clientSocket.close
//                  }
//                }
//              })
//            } catch {
//              // In failure, close the socket here; else, the thread will close it
//              case ioe: IOException => clientSocket.close
//            }
//          }
//        }
//      } finally {
//        serverSocket.close
//      }      
//    }
//  }
//  
//  class TrackSingleValue {
//    
//  }
  
//  public static ExecutorService newCachedThreadPool() {
//    return new ThreadPoolExecutor(0, Integer.MAX_VALUE, 60L, TimeUnit.SECONDS,
//      new SynchronousQueue<Runnable>());
//  }
  
  
  class GuideMultipleRequests extends Thread with Logging {
    override def run = {
      var threadPool = Executors.newCachedThreadPool
      var serverSocket: ServerSocket = null

      serverSocket = new ServerSocket (BroadcastCS.masterListenPort)
      // listenPort = BroadcastCS.masterListenPort
      logInfo("GuideMultipleRequests" + serverSocket + " " + listenPort)
      
      var keepAccepting = true
      try {
        while (keepAccepting) {
          var clientSocket: Socket = null
          try {
            serverSocket.setSoTimeout (serverSocketTimout)
            clientSocket = serverSocket.accept
          } catch {
            case e: Exception => { 
              logInfo("GuideMultipleRequests Timeout. Stopping listening...") 
              keepAccepting = false 
            }
          }
          if (clientSocket != null) {
            logInfo("Guide:Accepted new client connection:" + clientSocket)
            try {            
              threadPool.execute (new GuideSingleRequest (clientSocket))
            } catch {
              // In failure, close the socket here; else, the thread will close it
              case ioe: IOException => clientSocket.close
            }
          }
        }
      } finally {
        serverSocket.close
      }
    }
    
    class GuideSingleRequest (val clientSocket: Socket)
    extends Runnable with Logging {
      private val oos = new ObjectOutputStream (clientSocket.getOutputStream)
      private val ois = new ObjectInputStream (clientSocket.getInputStream)

      private var selectedSourceInfo: SourceInfo = null
      private var thisWorkerInfo:SourceInfo = null
      
      def run = {
        try {
          logInfo("new GuideSingleRequest is running")
          // Connecting worker is sending in its hostAddress and listenPort it will 
          // be listening to. ReplicaID is 0 and other fields are invalid (-1)
          var sourceInfo = ois.readObject.asInstanceOf[SourceInfo]
          
          // Select a suitable source and send it back to the worker
          selectedSourceInfo = selectSuitableSource (sourceInfo)
          logInfo("Sending selectedSourceInfo:" + selectedSourceInfo)
          oos.writeObject (selectedSourceInfo)
          oos.flush

          // Add this new (if it can finish) source to the PQ of sources
          thisWorkerInfo = new SourceInfo(sourceInfo.hostAddress, 
            sourceInfo.listenPort, totalBlocks, totalBytes, 0)  
          logInfo("Adding possible new source to pqOfSources: " + thisWorkerInfo)    
          pqOfSources.synchronized {
            pqOfSources.add (thisWorkerInfo)
          }

          // Wait till the whole transfer is done. Then receive and update source 
          // statistics in pqOfSources
          sourceInfo = ois.readObject.asInstanceOf[SourceInfo]

          pqOfSources.synchronized {
            // This should work since SourceInfo is a case class
            assert (pqOfSources.contains (selectedSourceInfo))
            
            // Remove first
            pqOfSources.remove (selectedSourceInfo)        
            // TODO: Removing a source based on just one failure notification!
            // Update leecher count and put it back in IF reception succeeded
            if (!sourceInfo.receptionFailed) {          
              selectedSourceInfo.currentLeechers -= 1
              pqOfSources.add (selectedSourceInfo)
              
              // No need to find and update thisWorkerInfo, but add its replica
              if (BroadcastCS.dualMode) {
                pqOfSources.add (new SourceInfo (thisWorkerInfo.hostAddress, 
                  thisWorkerInfo.listenPort, totalBlocks, totalBytes, 1))
              }              
            }                        
          }      
        } catch {
          // If something went wrong, e.g., the worker at the other end died etc. 
          // then close everything up
          case e: Exception => { 
            // Assuming that exception caused due to receiver worker failure
            // Remove failed worker from pqOfSources and update leecherCount of 
            // corresponding source worker
            pqOfSources.synchronized {
              if (selectedSourceInfo != null) {
                // Remove first
                pqOfSources.remove (selectedSourceInfo)        
                // Update leecher count and put it back in
                selectedSourceInfo.currentLeechers -= 1
                pqOfSources.add (selectedSourceInfo)
              }
              
              // Remove thisWorkerInfo
              if (pqOfSources != null) { pqOfSources.remove (thisWorkerInfo) }
            }      
          }
        } finally {
          ois.close
          oos.close
          clientSocket.close
        }
      }
      
      // TODO: If a worker fails to get the broadcasted variable from a source and
      // comes back to Master, this function might choose the worker itself as a 
      // source tp create a dependency cycle (this worker was put into pqOfSources 
      // as a streming source when it first arrived). The length of this cycle can
      // be arbitrarily long. 
      private def selectSuitableSource(skipSourceInfo: SourceInfo): SourceInfo = {
        // Select one with the lowest number of leechers
        pqOfSources.synchronized {
          // take is a blocking call removing the element from PQ
          var selectedSource = pqOfSources.poll
          assert (selectedSource != null) 
          // Update leecher count
          selectedSource.currentLeechers += 1
          // Add it back and then return
          pqOfSources.add (selectedSource)
          return selectedSource
        }
      }
    }    
  }

  class ServeMultipleRequests extends Thread with Logging {
    override def run = {
      var threadPool = Executors.newCachedThreadPool
      var serverSocket: ServerSocket = null

      serverSocket = new ServerSocket (0) 
      listenPort = serverSocket.getLocalPort
      logInfo("ServeMultipleRequests" + serverSocket + " " + listenPort)
      
      listenPortLock.synchronized {
        listenPortLock.notifyAll
      }
            
      var keepAccepting = true
      try {
        while (keepAccepting) {
          var clientSocket: Socket = null
          try {
            serverSocket.setSoTimeout (serverSocketTimout)            
            clientSocket = serverSocket.accept
          } catch {
            case e: Exception => { 
              logInfo("ServeMultipleRequests Timeout. Stopping listening...") 
              keepAccepting = false 
            }
          }
          if (clientSocket != null) {
            logInfo("Serve:Accepted new client connection:" + clientSocket)
            try {            
              threadPool.execute (new ServeSingleRequest (clientSocket))
            } catch {
              // In failure, close socket here; else, the thread will close it
              case ioe: IOException => clientSocket.close
            }
          }
        }
      } finally {
        serverSocket.close
      }
    }
    
    class ServeSingleRequest (val clientSocket: Socket)
    extends Runnable with Logging {
      private val oos = new ObjectOutputStream (clientSocket.getOutputStream)
      private val ois = new ObjectInputStream (clientSocket.getInputStream)
      
      def run  = {
        try {
          logInfo("new ServeSingleRequest is running")
          sendObject
        } catch {
          // TODO: Need to add better exception handling here
          // If something went wrong, e.g., the worker at the other end died etc. 
          // then close everything up
          case e: Exception => { 
            logInfo("ServeSingleRequest had a " + e)
          }
        } finally {
          logInfo("ServeSingleRequest is closing streams and sockets")
          ois.close
          oos.close
          clientSocket.close
        }
      }

      private def sendObject = {
        // Wait till receiving the SourceInfo from Master
        while (totalBlocks == -1) { 
          totalBlocksLock.synchronized {
            totalBlocksLock.wait
          }
        }

        for (i <- 0 until totalBlocks) {
          while (i == hasBlocks) { 
            hasBlocksLock.synchronized {
              hasBlocksLock.wait
            }
          }
          try {
            oos.writeObject (arrayOfBlocks(i))
            oos.flush
          } catch {
            case e: Exception => { }
          }
          logInfo("Send block: " + i + " " + arrayOfBlocks(i))
        }
      }    
    }    
  }
}

private object BroadcastCH extends Logging {
  val values = Cache.newKeySpace()

  private var initialized = false

  private var fileSystem: FileSystem = null
  private var workDir: String = null
  private var compress: Boolean = false
  private var bufferSize: Int = 65536

  def initialize () {
    synchronized {
      if (!initialized) {
        bufferSize = System.getProperty("spark.buffer.size", "65536").toInt
        val dfs = System.getProperty("spark.dfs", "file:///")
        if (!dfs.startsWith("file://")) {
          val conf = new Configuration()
          conf.setInt("io.file.buffer.size", bufferSize)
          val rep = System.getProperty("spark.dfs.replication", "3").toInt
          conf.setInt("dfs.replication", rep)
          fileSystem = FileSystem.get(new URI(dfs), conf)
        }
        workDir = System.getProperty("spark.dfs.workdir", "/tmp")
        compress = System.getProperty("spark.compress", "false").toBoolean

        initialized = true
      }
    }
  }

  private def getPath(uuid: UUID) = new Path(workDir + "/broadcast-" + uuid)

  def openFileForReading(uuid: UUID): InputStream = {
    val fileStream = if (fileSystem != null) {
      fileSystem.open(getPath(uuid))
    } else {
      // Local filesystem
      new FileInputStream(getPath(uuid).toString)
    }
    if (compress)
      new LZFInputStream(fileStream) // LZF stream does its own buffering
    else if (fileSystem == null)
      new BufferedInputStream(fileStream, bufferSize)
    else
      fileStream // Hadoop streams do their own buffering
  }

  def openFileForWriting(uuid: UUID): OutputStream = {
    val fileStream = if (fileSystem != null) {
      fileSystem.create(getPath(uuid))
    } else {
      // Local filesystem
      new FileOutputStream(getPath(uuid).toString)
    }
    if (compress)
      new LZFOutputStream(fileStream) // LZF stream does its own buffering
    else if (fileSystem == null)
      new BufferedOutputStream(fileStream, bufferSize)
    else
      fileStream // Hadoop streams do their own buffering
  }
}
