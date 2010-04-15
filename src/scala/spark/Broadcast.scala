package spark

import java.io._
import java.net._
import java.util.{UUID, PriorityQueue, Comparator}

import com.google.common.collect.MapMaker

import java.util.concurrent.{Executors, ExecutorService}

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

@serializable
class ChainedStreamingBroadcast[T] (@transient var value_ : T, local: Boolean) 
  extends BroadcastRecipe {
  
  def value = value_

  BroadcastCS.synchronized { BroadcastCS.values.put (uuid, value_) }
   
  @transient var arrayOfBlocks: Array[BroadcastBlock] = null
  @transient var totalBytes = -1
  @transient var totalBlocks = -1
  @transient var hasBlocks = 0

  @transient var listenPortLock = new Object
  @transient var guidePortLock = new Object
  @transient var totalBlocksLock = new Object
  @transient var hasBlocksLock = new Object
  
  @transient var pqOfSources = new PriorityQueue[SourceInfo]

  @transient var serveMR: ServeMultipleRequests = null 
  @transient var guideMR: GuideMultipleRequests = null 

  @transient var hostAddress = InetAddress.getLocalHost.getHostAddress
  @transient var listenPort = -1    
  @transient var guidePort = -1
  
  @transient var hasCopyInHDFS = false

  if (!local) { sendBroadcast }
  
  def sendBroadcast () {
    // Create a variableInfo object and store it in valueInfos
    var variableInfo = blockifyObject (value_, BroadcastCS.blockSize)   
    
    guideMR = new GuideMultipleRequests
    // guideMR.setDaemon (true)
    guideMR.start
    // println (System.currentTimeMillis + ": " +  "GuideMultipleRequests started")
    
    serveMR = new ServeMultipleRequests
    // serveMR.setDaemon (true)
    serveMR.start
    // println (System.currentTimeMillis + ": " +  "ServeMultipleRequests started")

    // Prepare the value being broadcasted
    // TODO: Refactoring and clean-up required here
    arrayOfBlocks = variableInfo.arrayOfBlocks
    totalBytes = variableInfo.totalBytes
    totalBlocks = variableInfo.totalBlocks
    hasBlocks = variableInfo.totalBlocks      
   
    while (listenPort == -1) { 
      listenPortLock.synchronized {
        listenPortLock.wait 
      }
    } 

    pqOfSources = new PriorityQueue[SourceInfo]
    val masterSource_0 = 
      new SourceInfo (hostAddress, listenPort, totalBlocks, totalBytes, 0) 
    pqOfSources.add (masterSource_0)
    // Add one more time to have two replicas of any seeds in the PQ
    if (BroadcastCS.dualMode) {
      val masterSource_1 = 
        new SourceInfo (hostAddress, listenPort, totalBlocks, totalBytes, 1) 
      pqOfSources.add (masterSource_1)
    }      

    // Register with the Tracker
    while (guidePort == -1) { 
      guidePortLock.synchronized {
        guidePortLock.wait 
      }
    } 
    BroadcastCS.registerValue (uuid, guidePort)
    
    // Now store a persistent copy in HDFS, in a separate thread
    // new Runnable {
      // override def run = {
        // TODO: When threaded, its not written to file
        // TODO: On second thought, its better to have it stored before anything starts
        val out = new ObjectOutputStream (BroadcastCH.openFileForWriting(uuid))
        out.writeObject (value_)
        out.close    
        hasCopyInHDFS = true    
      // }
    // }
  }
  
  private def readObject (in: ObjectInputStream) {
    in.defaultReadObject
    BroadcastCS.synchronized {
      val cachedVal = BroadcastCS.values.get (uuid)
      if (cachedVal != null) {
        value_ = cachedVal.asInstanceOf[T]
      } else {
        // Only a single worker (the first one) in the same node can ever be 
        // here. The rest will always get the value ready 

        // Initializing everything because Master will only send null/0 values
        initializeSlaveVariables
        
        serveMR = new ServeMultipleRequests
        // serveMR.setDaemon (true)
        serveMR.start
        // println (System.currentTimeMillis + ": " +  "ServeMultipleRequests started")
        
        val start = System.nanoTime        

        val retByteArray = receiveBroadcast (uuid)
        // If does not succeed, then get from HDFS copy
        if (retByteArray != null) {
          value_ = byteArrayToObject[T] (retByteArray)
          BroadcastCS.values.put (uuid, value_)
        }  else {
          val fileIn = new ObjectInputStream(BroadcastCH.openFileForReading(uuid))
          value_ = fileIn.readObject.asInstanceOf[T]
          BroadcastCH.values.put(uuid, value_)
          fileIn.close
        } 
        
        val time = (System.nanoTime - start) / 1e9
        println( System.currentTimeMillis + ": " +  "Reading Broadcasted variable " + uuid + " took " + time + " s")                  
      }
    }
  }
  
  private def initializeSlaveVariables = {
    arrayOfBlocks = null
    totalBytes = -1
    totalBlocks = -1
    hasBlocks = 0
    listenPortLock = new Object
    totalBlocksLock = new Object
    hasBlocksLock = new Object
    serveMR =  null
    hostAddress = InetAddress.getLocalHost.getHostAddress
    listenPort = -1
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
  
  def receiveBroadcast (variableUUID: UUID): Array[Byte] = {
    var clientSocketToTracker: Socket = null
    var oisTracker: ObjectInputStream = null
    var oosTracker: ObjectOutputStream = null
    
    // masterListenPort aka guidePort value legend
    //  0 = missed the broadcast, read from HDFS; 
    // <0 = hasn't started yet, wait & retry;
    // >0 = Read from this port 
    var masterListenPort: Int = -1
    
    var retriesLeft = BroadcastCS.maxRetryCount
    do {
      try {  
        // Connect to the tracker to find out the guide 
        val clientSocketToTracker = 
          new Socket(BroadcastCS.masterHostAddress, BroadcastCS.masterTrackerPort)  
        val oisTracker = 
          new ObjectInputStream (clientSocketToTracker.getInputStream)
        val oosTracker = 
          new ObjectOutputStream (clientSocketToTracker.getOutputStream)
      
        // Send UUID and receive masterListenPort
        oosTracker.writeObject (uuid)
        masterListenPort = oisTracker.readObject.asInstanceOf[Int]
      } catch {
        // In case of any failure, set masterListenPort = 0 to read from HDFS        
        case e: Exception => (masterListenPort = 0)
      } finally {   
        if (oisTracker != null) { oisTracker.close }
        if (oosTracker != null) { oosTracker.close }
        if (clientSocketToTracker != null) { clientSocketToTracker.close }
      }

      retriesLeft -= 1     
    } while (retriesLeft > 0 && masterListenPort < 0)
    // println (System.currentTimeMillis + ": " +  "Got this guidePort from Tracker: " + masterListenPort)
        
    // If Tracker says that there is no guide for this object, read from HDFS
    if (masterListenPort == 0) { return null }

    // Wait until hostAddress and listenPort are created by the 
    // ServeMultipleRequests thread
    while (listenPort == -1) { 
      listenPortLock.synchronized {
        listenPortLock.wait 
      }
    } 

    // Connect and receive broadcast from the specified source, retrying the
    // specified number of times in case of failures
    retriesLeft = BroadcastCS.maxRetryCount
    var retByteArray: Array[Byte] = null
    do {      
      // Connect to Master and send this worker's Information
      val clientSocketToMaster = 
        new Socket(BroadcastCS.masterHostAddress, masterListenPort)  
      // println (System.currentTimeMillis + ": " +  "Connected to Master's guiding object")
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
      
      // println (System.currentTimeMillis + ": " +  "Received SourceInfo from Master:" + sourceInfo + " My Port: " + listenPort)    

      val start = System.nanoTime  
      retByteArray = receiveSingleTransmission (sourceInfo)
      val time = (System.nanoTime - start) / 1e9      
      
      // println (System.currentTimeMillis + ": " +  "I got this from receiveSingleTransmission: " + retByteArray)

      // TODO: Update sourceInfo to add error notifactions for Master
      if (retByteArray == null) { sourceInfo.receptionFailed = true }
      
      // Updating some statistics here. Master will be using them later
      sourceInfo.MBps = (sourceInfo.totalBytes.toDouble / 1048576) / time

      // Send back statistics to the Master
      oosMaster.writeObject (sourceInfo) 
    
      oisMaster.close
      oosMaster.close
      clientSocketToMaster.close                    
      
      retriesLeft -= 1
    } while (retriesLeft > 0 && retByteArray == null)
    
    return retByteArray
  }

  // Tries to receive broadcast from the source and returns Boolean status.
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
        
      // println (System.currentTimeMillis + ": " +  "Inside receiveSingleTransmission")
      // println (System.currentTimeMillis + ": " +  "totalBlocks: "+ totalBlocks + " " + "hasBlocks: " + hasBlocks)
      
      // Send the range       
      oosSource.writeObject((0, totalBlocks))
      
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
        // println (System.currentTimeMillis + ": " +  "Received block: " + i + " " + bcBlock)
      } 
      assert (hasBlocks == totalBlocks)
      // println (System.currentTimeMillis + ": " +  "After the receive loop")
    } catch {
      case e: Exception => { 
        retByteArray = null 
        // println (System.currentTimeMillis + ": " +  "receiveSingleTransmission had a " + e)
      }
    } finally {    
      if (oisSource != null) { oisSource.close }
      if (oosSource != null) { oosSource.close }
      if (clientSocketToSource != null) { clientSocketToSource.close }
    }
          
    return retByteArray
  } 

  class GuideMultipleRequests extends Thread {
    override def run = {
      // TODO: Cached threadpool has 60 s keep alive timer
      var threadPool = Executors.newCachedThreadPool
      var serverSocket: ServerSocket = null

      serverSocket = new ServerSocket (0)
      guidePort = serverSocket.getLocalPort
      // println (System.currentTimeMillis + ": " +  "GuideMultipleRequests" + serverSocket + " " + guidePort)
      
      guidePortLock.synchronized {
        guidePortLock.notifyAll
      }

      var keepAccepting = true
      try {
        // Don't stop until there is a copy in HDFS
        while (keepAccepting || !hasCopyInHDFS) {
          var clientSocket: Socket = null
          try {
            serverSocket.setSoTimeout (BroadcastCS.serverSocketTimout)
            clientSocket = serverSocket.accept
          } catch {
            case e: Exception => { 
              // println ("GuideMultipleRequests Timeout. Stopping listening..." + hasCopyInHDFS) 
              keepAccepting = false 
            }
          }
          if (clientSocket != null) {
            // println (System.currentTimeMillis + ": " +  "Guide:Accepted new client connection:" + clientSocket)
            try {            
              threadPool.execute (new GuideSingleRequest (clientSocket))
            } catch {
              // In failure, close the socket here; else, the thread will close it
              case ioe: IOException => clientSocket.close
            }
          }
        }
        BroadcastCS.unregisterValue (uuid)
      } finally {
        serverSocket.close
      }
    }
    
    class GuideSingleRequest (val clientSocket: Socket) extends Runnable {
      private val oos = new ObjectOutputStream (clientSocket.getOutputStream)
      private val ois = new ObjectInputStream (clientSocket.getInputStream)

      private var selectedSourceInfo: SourceInfo = null
      private var thisWorkerInfo:SourceInfo = null
      
      def run = {
        try {
          // println (System.currentTimeMillis + ": " +  "new GuideSingleRequest is running")
          // Connecting worker is sending in its hostAddress and listenPort it will 
          // be listening to. ReplicaID is 0 and other fields are invalid (-1)
          var sourceInfo = ois.readObject.asInstanceOf[SourceInfo]
          
          // Select a suitable source and send it back to the worker
          selectedSourceInfo = selectSuitableSource (sourceInfo)
          // println (System.currentTimeMillis + ": " +  "Sending selectedSourceInfo:" + selectedSourceInfo)
          oos.writeObject (selectedSourceInfo)
          oos.flush

          // Add this new (if it can finish) source to the PQ of sources
          thisWorkerInfo = new SourceInfo(sourceInfo.hostAddress, 
            sourceInfo.listenPort, totalBlocks, totalBytes, 0)  
          // println (System.currentTimeMillis + ": " +  "Adding possible new source to pqOfSources: " + thisWorkerInfo)    
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
            // Update sourceInfo and put it back in, IF reception succeeded
            if (!sourceInfo.receptionFailed) {          
              selectedSourceInfo.currentLeechers -= 1
              selectedSourceInfo.MBps = sourceInfo.MBps 
              
              // Put it back 
              pqOfSources.add (selectedSourceInfo)
              
              // Update global source speed statistics
              BroadcastCS.setSourceSpeed (
                sourceInfo.hostAddress, sourceInfo.MBps)

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
            // Assuming that exception caused due to receiver worker failure.
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

  class ServeMultipleRequests extends Thread {
    override def run = {
      var threadPool = Executors.newCachedThreadPool
      var serverSocket: ServerSocket = null

      serverSocket = new ServerSocket (0) 
      listenPort = serverSocket.getLocalPort
      // println (System.currentTimeMillis + ": " +  "ServeMultipleRequests" + serverSocket + " " + listenPort)
      
      listenPortLock.synchronized {
        listenPortLock.notifyAll
      }
            
      var keepAccepting = true
      try {
        while (keepAccepting) {
          var clientSocket: Socket = null
          try {
            serverSocket.setSoTimeout (BroadcastCS.serverSocketTimout)
            clientSocket = serverSocket.accept
          } catch {
            case e: Exception => { 
              // println ("ServeMultipleRequests Timeout. Stopping listening...") 
              keepAccepting = false 
            }
          }
          if (clientSocket != null) {
            // println (System.currentTimeMillis + ": " +  "Serve:Accepted new client connection:" + clientSocket)
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
    
    class ServeSingleRequest (val clientSocket: Socket) extends Runnable {
      private val oos = new ObjectOutputStream (clientSocket.getOutputStream)
      private val ois = new ObjectInputStream (clientSocket.getInputStream)
      
      private var sendFrom = 0
      private var sendUntil = totalBlocks
      
      def run  = {
        try {
          // println (System.currentTimeMillis + ": " +  "new ServeSingleRequest is running")
          
          // Receive range to send
          var sendRange = ois.readObject.asInstanceOf[(Int, Int)]
          sendFrom = sendRange._1
          sendUntil = sendRange._2
          
          sendObject
        } catch {
          // TODO: Need to add better exception handling here
          // If something went wrong, e.g., the worker at the other end died etc. 
          // then close everything up
          case e: Exception => { 
            // println (System.currentTimeMillis + ": " +  "ServeSingleRequest had a " + e)
          }
        } finally {
          // println (System.currentTimeMillis + ": " +  "ServeSingleRequest is closing streams and sockets")
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

        for (i <- sendFrom until sendUntil) {
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
          // println (System.currentTimeMillis + ": " +  "Send block: " + i + " " + arrayOfBlocks(i))
        }
      }    
    } 
  }  
}

@serializable 
class CentralizedHDFSBroadcast[T](@transient var value_ : T, local: Boolean) 
  extends BroadcastRecipe {
  
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
        // println( System.currentTimeMillis + ": " +  "Started reading Broadcasted variable " + uuid)
        val start = System.nanoTime
        
        val fileIn = new ObjectInputStream(BroadcastCH.openFileForReading(uuid))
        value_ = fileIn.readObject.asInstanceOf[T]
        BroadcastCH.values.put(uuid, value_)
        fileIn.close
        
        val time = (System.nanoTime - start) / 1e9
        println( System.currentTimeMillis + ": " +  "Reading Broadcasted variable " + uuid + " took " + time + " s")
      }
    }
  }
}

@serializable
case class SourceInfo (val hostAddress: String, val listenPort: Int, 
  val totalBlocks: Int, val totalBytes: Int, val replicaID: Int)  
  extends Comparable [SourceInfo]{

  var currentLeechers = 0
  var receptionFailed = false
  var MBps: Double = 0.0
  
  // Ascending sort based on leecher count
  // def compareTo (o: SourceInfo): Int = (currentLeechers - o.currentLeechers)
  
  // Descending sort based on speed
  // def compareTo (o: SourceInfo): Int = { 
    // if (MBps > o.MBps) -1
    // else if (MBps < o.MBps) 1
    // else 0
  // }
  
  // Descending sort based on globally stored speed
  def compareTo (o: SourceInfo): Int = { 
    val mySpeed = BroadcastCS.getSourceSpeed (hostAddress)
    val urSpeed = BroadcastCS.getSourceSpeed (o.hostAddress)

    if (mySpeed > urSpeed) -1
    else if (mySpeed < urSpeed) 1
    else 0
  } 
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
        BroadcastCS.initialize (isMaster)
        
        initialized = true
      }
    }
  }
}

private object BroadcastCS {
  val values = new MapMaker ().softValues ().makeMap[UUID, Any]

  var valueToGuidePortMap = Map[UUID, Int] ()
  
  var sourceToSpeedMap = Map[String, Double] ()

  private var initialized = false
  private var isMaster_ = false

  private var masterHostAddress_ = "127.0.0.1"
  private var masterTrackerPort_ : Int = 11111
  private var blockSize_ : Int = 512 * 1024
  private var maxRetryCount_ : Int = 2
  private var serverSocketTimout_ : Int = 50000
  private var dualMode_ : Boolean = false
 
  private var trackMV: TrackMultipleValues = null

  // newSpeed = ALPHA * oldSpeed + (1 - ALPHA) * curSpeed
  private val ALPHA = 0.7

  def initialize (isMaster__ : Boolean) {
    synchronized {
      if (!initialized) {
        masterHostAddress_ = 
          System.getProperty ("spark.broadcast.masterHostAddress", "127.0.0.1")
        masterTrackerPort_ = 
          System.getProperty ("spark.broadcast.masterTrackerPort", "11111").toInt
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
          trackMV = new TrackMultipleValues
          trackMV.setDaemon (true)
          trackMV.start
          // println (System.currentTimeMillis + ": " +  "TrackMultipleValues started")         
        }
                  
        initialized = true
      }
    }
  }
   
  def masterHostAddress = masterHostAddress_
  def masterTrackerPort = masterTrackerPort_
  def blockSize = blockSize_
  def maxRetryCount = maxRetryCount_
  def serverSocketTimout = serverSocketTimout_
  def dualMode = dualMode_

  def isMaster = isMaster_ 
  
  def registerValue (uuid: UUID, guidePort: Int) = {    
    valueToGuidePortMap.synchronized {    
      valueToGuidePortMap += (uuid -> guidePort)
      // println (System.currentTimeMillis + ": " +  "New value registered with the Tracker " + valueToGuidePortMap)             
    }
  }
  
  def unregisterValue (uuid: UUID) = {
    valueToGuidePortMap.synchronized {
      // Set to 0 to make sure that people read it from HDFS
      valueToGuidePortMap (uuid) = 0
      // println (System.currentTimeMillis + ": " +  "Value unregistered from the Tracker " + valueToGuidePortMap)             
    }
  }
  
  def getSourceSpeed (hostAddress: String): Double = {
    sourceToSpeedMap.synchronized {
      sourceToSpeedMap.getOrElseUpdate(hostAddress, 0.0)
    }
  }
  
  def setSourceSpeed (hostAddress: String, MBps: Double) = {
    sourceToSpeedMap.synchronized {
      var oldSpeed = sourceToSpeedMap.getOrElseUpdate(hostAddress, 0.0)
      var newSpeed = ALPHA * oldSpeed + (1 - ALPHA) * MBps
      sourceToSpeedMap.update (hostAddress, newSpeed)
    }
  }
  
  class TrackMultipleValues extends Thread {
    override def run = {
      var threadPool = Executors.newCachedThreadPool
      var serverSocket: ServerSocket = null
      
      serverSocket = new ServerSocket (BroadcastCS.masterTrackerPort)
      // println (System.currentTimeMillis + ": " +  "TrackMultipleValues" + serverSocket)
      
      var keepAccepting = true
      try {
        while (true) {
          var clientSocket: Socket = null
          try {
            // TODO: 
            // serverSocket.setSoTimeout (serverSocketTimout)
            clientSocket = serverSocket.accept
          } catch {
            case e: Exception => { 
              // println ("TrackMultipleValues Timeout. Stopping listening...") 
              // TODO: Tracking should be explicitly stopped by the SparkContext
              // keepAccepting = false 
            }
          }

          if (clientSocket != null) {
            try {            
              threadPool.execute (new Runnable {
                override def run = {
                  val oos = new ObjectOutputStream (clientSocket.getOutputStream)
                  val ois = new ObjectInputStream (clientSocket.getInputStream)
                  try {
                    val uuid = ois.readObject.asInstanceOf[UUID]
                    // masterListenPort/guidePort value legend
                    //  0 = missed the broadcast, read from HDFS; 
                    // <0 = hasn't started yet, wait & retry;
                    // >0 = Read from this port 
                    var guidePort = if (valueToGuidePortMap.contains (uuid)) {
                      valueToGuidePortMap (uuid)
                    } else -1
                    // println (System.currentTimeMillis + ": " +  "TrackMultipleValues:Got new request: " + clientSocket + " for " + uuid + " : " + guidePort)                    
                    oos.writeObject (guidePort)
                  } catch {
                    case e: Exception => { }
                  } finally {
                    ois.close
                    oos.close
                    clientSocket.close
                  }
                }
              })
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
  }
}

private object BroadcastCH {
  val values = new MapMaker ().softValues ().makeMap[UUID, Any]

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
