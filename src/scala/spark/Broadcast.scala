package spark

import java.io._
import java.net._
import java.util.{Comparator, PriorityQueue, Random, UUID}

import com.google.common.collect.MapMaker

import java.util.concurrent.{Executors, ExecutorService, ThreadPoolExecutor}

import scala.collection.mutable.{Map, Set}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path, RawLocalFileSystem}

import spark.compress.lzf.{LZFInputStream, LZFOutputStream}

//import rice.environment.Environment
//import rice.p2p.commonapi._
//import rice.p2p.commonapi.rawserialization.RawMessage
//import rice.pastry._
//import rice.pastry.commonapi.PastryIdFactory
//import rice.pastry.direct._
//import rice.pastry.socket.SocketPastryNodeFactory
//import rice.pastry.standard.RandomNodeIdFactory
//import rice.p2p.scribe._
//import rice.p2p.splitstream._

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
extends BroadcastRecipe  with Logging {
  
  def value = value_

  BroadcastCS.synchronized { 
    BroadcastCS.values.put (uuid, value_) 
  }
   
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
  @transient var stopBroadcast = false
  
  // Must call this after all the variables have been created/initialized
  if (!local) { 
    sendBroadcast 
  }

  def sendBroadcast () {    
    // Store a persistent copy in HDFS    
    // TODO: Turned OFF for now
    // val out = new ObjectOutputStream (BroadcastCH.openFileForWriting(uuid))
    // out.writeObject (value_)
    // out.close    
    // TODO: Fix this at some point  
    hasCopyInHDFS = true    

    // Create a variableInfo object and store it in valueInfos
    var variableInfo = blockifyObject (value_, BroadcastCS.BlockSize)   
    
    guideMR = new GuideMultipleRequests
    guideMR.setDaemon (true)
    guideMR.start
    logInfo ("GuideMultipleRequests started...")
    
    serveMR = new ServeMultipleRequests
    serveMR.setDaemon (true)
    serveMR.start
    logInfo ("ServeMultipleRequests started...")

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
      SourceInfo (hostAddress, listenPort, totalBlocks, totalBytes, 0) 
    pqOfSources.add (masterSource_0)
    // Add one more time to have two replicas of any seeds in the PQ
    if (BroadcastCS.DualMode) {
      val masterSource_1 = 
        SourceInfo (hostAddress, listenPort, totalBlocks, totalBytes, 1) 
      pqOfSources.add (masterSource_1)
    }      

    // Register with the Tracker
    while (guidePort == -1) { 
      guidePortLock.synchronized {
        guidePortLock.wait 
      }
    } 
    BroadcastCS.registerValue (uuid, guidePort)  
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
        serveMR.setDaemon (true)
        serveMR.start
        logInfo ("ServeMultipleRequests started...")
        
        val start = System.nanoTime        

        val receptionSucceeded = receiveBroadcast (uuid)
        // If does not succeed, then get from HDFS copy
        if (receptionSucceeded) {
          value_ = unBlockifyObject[T]
          BroadcastCS.values.put (uuid, value_)
        }  else {
          val fileIn = new ObjectInputStream(BroadcastCH.openFileForReading(uuid))
          value_ = fileIn.readObject.asInstanceOf[T]
          BroadcastCS.values.put(uuid, value_)
          fileIn.close
        } 
        
        val time = (System.nanoTime - start) / 1e9
        logInfo("Reading Broadcasted variable " + uuid + " took " + time + " s")                  
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
    
    stopBroadcast = false
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
  
  private def unBlockifyObject[A]: A = {
    var retByteArray = new Array[Byte] (totalBytes)
    for (i <- 0 until totalBlocks) {
      System.arraycopy (arrayOfBlocks(i).byteArray, 0, retByteArray, 
        i * BroadcastCS.BlockSize, arrayOfBlocks(i).byteArray.length)
    }    
    byteArrayToObject (retByteArray)
  }
  
  private def byteArrayToObject[A] (bytes: Array[Byte]): A = {
    val in = new ObjectInputStream (new ByteArrayInputStream (bytes))
    val retVal = in.readObject.asInstanceOf[A]
    in.close
    return retVal
  }
    
  def getMasterListenPort (variableUUID: UUID): Int = {
    var clientSocketToTracker: Socket = null
    var oosTracker: ObjectOutputStream = null
    var oisTracker: ObjectInputStream = null
    
    var masterListenPort: Int = SourceInfo.TxOverGoToHDFS
    
    var retriesLeft = BroadcastCS.MaxRetryCount
    do {
      try {  
        // Connect to the tracker to find out the guide 
        val clientSocketToTracker = 
          new Socket(BroadcastCS.MasterHostAddress, BroadcastCS.MasterTrackerPort)  
        val oosTracker = 
          new ObjectOutputStream (clientSocketToTracker.getOutputStream)
        oosTracker.flush
        val oisTracker = 
          new ObjectInputStream (clientSocketToTracker.getInputStream)
      
        // Send UUID and receive masterListenPort
        oosTracker.writeObject (uuid)
        oosTracker.flush
        masterListenPort = oisTracker.readObject.asInstanceOf[Int]
      } catch {
        case e: Exception => { }
      } finally {   
        if (oisTracker != null) { 
          oisTracker.close 
        }
        if (oosTracker != null) { 
          oosTracker.close 
        }
        if (clientSocketToTracker != null) { 
          clientSocketToTracker.close 
        }
      }
      retriesLeft -= 1     

      Thread.sleep (BroadcastCS.ranGen.nextInt (
        BroadcastCS.MaxKnockInterval - BroadcastCS.MinKnockInterval) +
        BroadcastCS.MinKnockInterval)

    } while (retriesLeft > 0 && masterListenPort == SourceInfo.TxNotStartedRetry)

    logInfo ("Got this guidePort from Tracker: " + masterListenPort)
    return masterListenPort
  }
  
  def receiveBroadcast (variableUUID: UUID): Boolean = {
    val masterListenPort = getMasterListenPort (variableUUID) 

    if (masterListenPort == SourceInfo.TxOverGoToHDFS ||
        masterListenPort == SourceInfo.TxNotStartedRetry) { 
      // TODO: SourceInfo.TxNotStartedRetry is not really in use because we go
      // to HDFS anyway when receiveBroadcast returns false
      return false
    }

    // Wait until hostAddress and listenPort are created by the 
    // ServeMultipleRequests thread
    while (listenPort == -1) { 
      listenPortLock.synchronized {
        listenPortLock.wait 
      }
    } 

    var clientSocketToMaster: Socket = null
    var oosMaster: ObjectOutputStream = null
    var oisMaster: ObjectInputStream = null

    // Connect and receive broadcast from the specified source, retrying the
    // specified number of times in case of failures
    var retriesLeft = BroadcastCS.MaxRetryCount
    do {      
      // Connect to Master and send this worker's Information
      clientSocketToMaster = 
        new Socket(BroadcastCS.MasterHostAddress, masterListenPort)  
      // TODO: Guiding object connection is reusable
      oosMaster = 
        new ObjectOutputStream (clientSocketToMaster.getOutputStream)
      oosMaster.flush
      oisMaster = 
        new ObjectInputStream (clientSocketToMaster.getInputStream)
      
      logInfo ("Connected to Master's guiding object")

      // Send local source information
      oosMaster.writeObject(SourceInfo (hostAddress, listenPort, -1, -1, 0))
      oosMaster.flush

      // Receive source information from Master        
      var sourceInfo = oisMaster.readObject.asInstanceOf[SourceInfo]
      totalBlocks = sourceInfo.totalBlocks
      arrayOfBlocks = new Array[BroadcastBlock] (totalBlocks)
      totalBlocksLock.synchronized {
        totalBlocksLock.notifyAll
      }
      totalBytes = sourceInfo.totalBytes
      
      logInfo ("Received SourceInfo from Master:" + sourceInfo + " My Port: " + listenPort)    

      val start = System.nanoTime  
      val receptionSucceeded = receiveSingleTransmission (sourceInfo)
      val time = (System.nanoTime - start) / 1e9      
      
      // Updating some statistics in sourceInfo. Master will be using them later
      if (!receptionSucceeded) { 
        sourceInfo.receptionFailed = true 
      }
      sourceInfo.MBps = (sourceInfo.totalBytes.toDouble / 1048576) / time

      // Send back statistics to the Master
      oosMaster.writeObject (sourceInfo) 
    
      if (oisMaster != null) {
        oisMaster.close
      }
      if (oosMaster != null) {
        oosMaster.close
      }
      if (clientSocketToMaster != null) {
        clientSocketToMaster.close
      }
      
      retriesLeft -= 1
    } while (retriesLeft > 0 && hasBlocks < totalBlocks)
    
    return (hasBlocks == totalBlocks)
  }

  // Tries to receive broadcast from the source and returns Boolean status.
  // This might be called multiple times to retry a defined number of times.
  private def receiveSingleTransmission(sourceInfo: SourceInfo): Boolean = {
    var clientSocketToSource: Socket = null    
    var oosSource: ObjectOutputStream = null
    var oisSource: ObjectInputStream = null
    
    var receptionSucceeded = false    
    try {
      // Connect to the source to get the object itself
      clientSocketToSource = 
        new Socket (sourceInfo.hostAddress, sourceInfo.listenPort)        
      oosSource = 
        new ObjectOutputStream (clientSocketToSource.getOutputStream)
      oosSource.flush
      oisSource = 
        new ObjectInputStream (clientSocketToSource.getInputStream)
        
      logInfo ("Inside receiveSingleTransmission")
      logInfo ("totalBlocks: "+ totalBlocks + " " + "hasBlocks: " + hasBlocks)
      
      // Send the range       
      oosSource.writeObject((hasBlocks, totalBlocks))
      oosSource.flush
      
      for (i <- hasBlocks until totalBlocks) {
        val recvStartTime = System.currentTimeMillis
        val bcBlock = oisSource.readObject.asInstanceOf[BroadcastBlock]
        val receptionTime = (System.currentTimeMillis - recvStartTime)
        
        logInfo ("Received block: " + bcBlock.blockID + " from " + sourceInfo + " in " + receptionTime + " millis.")

        arrayOfBlocks(hasBlocks) = bcBlock
        hasBlocks += 1
        // Set to true if at least one block is received
        receptionSucceeded = true
        hasBlocksLock.synchronized {
          hasBlocksLock.notifyAll
        }
      } 
    } catch {
      case e: Exception => { 
        logInfo ("receiveSingleTransmission had a " + e)
      }
    } finally {    
      if (oisSource != null) { 
        oisSource.close 
      }
      if (oosSource != null) { 
        oosSource.close 
      }
      if (clientSocketToSource != null) { 
        clientSocketToSource.close 
      }
    }
          
    return receptionSucceeded
  } 

  class GuideMultipleRequests
  extends Thread with Logging {
    // Keep track of sources that have completed reception
    private var setOfCompletedSources = Set[SourceInfo] ()
  
    override def run = {
      var threadPool = Executors.newCachedThreadPool
      var serverSocket: ServerSocket = null

      serverSocket = new ServerSocket (0)
      guidePort = serverSocket.getLocalPort
      logInfo ("GuideMultipleRequests => " + serverSocket + " " + guidePort)
      
      guidePortLock.synchronized {
        guidePortLock.notifyAll
      }

      try {
        // Don't stop until there is a copy in HDFS
        while (!stopBroadcast || !hasCopyInHDFS) {
          var clientSocket: Socket = null
          try {
            serverSocket.setSoTimeout (BroadcastCS.ServerSocketTimeout)
            clientSocket = serverSocket.accept
          } catch {
            case e: Exception => { 
              logInfo ("GuideMultipleRequests Timeout.")
              
              // Stop broadcast if at least one worker has connected and
              // everyone connected so far are done. Comparing with
              // pqOfSources.size - 1, because it includes the Guide itself
              if (pqOfSources.size > 1 &&
                setOfCompletedSources.size == pqOfSources.size - 1) {
                stopBroadcast = true
              }              
            }
          }
          if (clientSocket != null) {
            logInfo ("Guide: Accepted new client connection: " + clientSocket)
            try {            
              threadPool.execute (new GuideSingleRequest (clientSocket))
            } catch {
              // In failure, close the socket here; else, the thread will close it
              case ioe: IOException => clientSocket.close
            }
          }
        }
        
        logInfo ("Sending stopBroadcast notifications...")
        sendStopBroadcastNotifications
                
        BroadcastCS.unregisterValue (uuid)
      } finally {
        if (serverSocket != null) {
          logInfo ("GuideMultipleRequests now stopping...")
          serverSocket.close
        }
      }
      
      // Shutdown the thread pool
      threadPool.shutdown      
    }
    
    private def sendStopBroadcastNotifications = {
      pqOfSources.synchronized {
        var pqIter = pqOfSources.iterator        
        while (pqIter.hasNext) {
          var sourceInfo = pqIter.next

          var guideSocketToSource: Socket = null
          var gosSource: ObjectOutputStream = null
          var gisSource: ObjectInputStream = null
        
          try {
            // Connect to the source
            guideSocketToSource =
              new Socket (sourceInfo.hostAddress, sourceInfo.listenPort)
            gosSource =
              new ObjectOutputStream (guideSocketToSource.getOutputStream)
            gosSource.flush
            gisSource =
              new ObjectInputStream (guideSocketToSource.getInputStream)
            
            // Send stopBroadcast signal. Range = SourceInfo.StopBroadcast*2
            gosSource.writeObject ((SourceInfo.StopBroadcast, 
              SourceInfo.StopBroadcast))
            gosSource.flush
          } catch {
            case e: Exception => { }
          } finally {
            if (gisSource != null) {
              gisSource.close
            }
            if (gosSource != null) {
              gosSource.close
            }
            if (guideSocketToSource != null) {
              guideSocketToSource.close
            }
          }
        }
      }
    }
        
    class GuideSingleRequest (val clientSocket: Socket) 
    extends Thread with Logging {
      private val oos = new ObjectOutputStream (clientSocket.getOutputStream)
      oos.flush
      private val ois = new ObjectInputStream (clientSocket.getInputStream)

      private var selectedSourceInfo: SourceInfo = null
      private var thisWorkerInfo:SourceInfo = null
      
      override def run = {
        try {
          logInfo ("new GuideSingleRequest is running")
          // Connecting worker is sending in its hostAddress and listenPort it will 
          // be listening to. ReplicaID is 0 and other fields are invalid (-1)
          var sourceInfo = ois.readObject.asInstanceOf[SourceInfo]
          
          pqOfSources.synchronized {
            // Select a suitable source and send it back to the worker
            selectedSourceInfo = selectSuitableSource (sourceInfo)
            logInfo ("Sending selectedSourceInfo: " + selectedSourceInfo)
            oos.writeObject (selectedSourceInfo)
            oos.flush

            // Add this new (if it can finish) source to the PQ of sources
            thisWorkerInfo = SourceInfo (sourceInfo.hostAddress, 
              sourceInfo.listenPort, totalBlocks, totalBytes, 0)
            logInfo ("Adding possible new source to pqOfSources: " + thisWorkerInfo)    
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
              // Add thisWorkerInfo to sources that have completed reception
              setOfCompletedSources += thisWorkerInfo
                            
              selectedSourceInfo.currentLeechers -= 1
              selectedSourceInfo.MBps = sourceInfo.MBps 
              
              // Put it back 
              pqOfSources.add (selectedSourceInfo)
              
              // Update global source speed statistics
              BroadcastCS.setSourceSpeed (
                sourceInfo.hostAddress, sourceInfo.MBps)

              // No need to find and update thisWorkerInfo, but add its replica
              if (BroadcastCS.DualMode) {
                pqOfSources.add (SourceInfo (thisWorkerInfo.hostAddress, 
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
              if (pqOfSources != null) { 
                pqOfSources.remove (thisWorkerInfo) 
              }
            }      
          }
        } finally {
          ois.close
          oos.close
          clientSocket.close
        }
      }
      
      // TODO: Caller must have a synchronized block on pqOfSources
      // TODO: If a worker fails to get the broadcasted variable from a source and
      // comes back to Master, this function might choose the worker itself as a 
      // source tp create a dependency cycle (this worker was put into pqOfSources 
      // as a streming source when it first arrived). The length of this cycle can
      // be arbitrarily long. 
      private def selectSuitableSource(skipSourceInfo: SourceInfo): SourceInfo = {
        // Select one based on the ordering strategy (e.g., least leechers etc.)
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

  class ServeMultipleRequests
  extends Thread with Logging {
    override def run = {
      var threadPool = Executors.newCachedThreadPool
      var serverSocket: ServerSocket = null

      serverSocket = new ServerSocket (0) 
      listenPort = serverSocket.getLocalPort
      logInfo ("ServeMultipleRequests started with " + serverSocket)
      
      listenPortLock.synchronized {
        listenPortLock.notifyAll
      }
            
      try {
        while (!stopBroadcast) {
          var clientSocket: Socket = null
          try {
            serverSocket.setSoTimeout (BroadcastCS.ServerSocketTimeout)
            clientSocket = serverSocket.accept
          } catch {
            case e: Exception => { 
              logInfo ("ServeMultipleRequests Timeout.") 
            }
          }
          if (clientSocket != null) {
            logInfo ("Serve: Accepted new client connection: " + clientSocket)
            try {            
              threadPool.execute (new ServeSingleRequest (clientSocket))
            } catch {
              // In failure, close socket here; else, the thread will close it
              case ioe: IOException => clientSocket.close
            }
          }
        }
      } finally {
        if (serverSocket != null) {
          logInfo ("ServeMultipleRequests now stopping...") 
          serverSocket.close 
        }
      }
      
      // Shutdown the thread pool
      threadPool.shutdown      
    }
    
    class ServeSingleRequest (val clientSocket: Socket) 
    extends Thread with Logging {
      private val oos = new ObjectOutputStream (clientSocket.getOutputStream)
      oos.flush
      private val ois = new ObjectInputStream (clientSocket.getInputStream)
      
      private var sendFrom = 0
      private var sendUntil = totalBlocks
      
      override def run  = {
        try {
          logInfo ("new ServeSingleRequest is running")
          
          // Receive range to send
          var rangeToSend = ois.readObject.asInstanceOf[(Int, Int)]
          sendFrom = rangeToSend._1
          sendUntil = rangeToSend._2
          
          if (sendFrom == SourceInfo.StopBroadcast && 
            sendUntil == SourceInfo.StopBroadcast) {
            stopBroadcast = true
          } else {
            // Carry on
            sendObject
          }
        } catch {
          // TODO: Need to add better exception handling here
          // If something went wrong, e.g., the worker at the other end died etc. 
          // then close everything up
          case e: Exception => { 
            logInfo ("ServeSingleRequest had a " + e)
          }
        } finally {
          logInfo ("ServeSingleRequest is closing streams and sockets")
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
          logInfo ("Sent block: " + i + " to " + clientSocket)
        }
      }    
    } 
  }  
}

//@serializable 
//class SplitStreamBroadcast[T] (@transient var value_ : T, local: Boolean) 
//extends BroadcastRecipe with Logging {

//  def value = value_

//  BroadcastSS.synchronized { BroadcastSS.values.put (uuid, value_) }
//  
//  if (!local) { 
//    sendBroadcast 
//  }
//  
//  @transient var publishThread: PublishThread = null
//  @transient var hasCopyInHDFS = false
//  
//  def sendBroadcast () {
//    // Store a persistent copy in HDFS    
//    val out = new ObjectOutputStream (BroadcastCH.openFileForWriting(uuid))
//    out.writeObject (value_)
//    out.close    
//    hasCopyInHDFS = true    
//    
//    publishThread = new PublishThread
//    publishThread.start
//  }
//  
//  private def readObject (in: ObjectInputStream) {
//    in.defaultReadObject
//    BroadcastSS.synchronized {
//      val cachedVal = BroadcastSS.values.get(uuid)
//      if (cachedVal != null) {
//        value_ = cachedVal.asInstanceOf[T]
//      } else {
//        val start = System.nanoTime        

//        // Thread.sleep (5000) // TODO:
//        val receptionSucceeded = BroadcastSS.receiveVariable (uuid)
//        // If does not succeed, then get from HDFS copy
//        if (receptionSucceeded) {
//          value_ = BroadcastSS.values.get(uuid).asInstanceOf[T]
//        }  else {
//          logInfo ("Reading from HDFS")
//          val fileIn = new ObjectInputStream(BroadcastCH.openFileForReading(uuid))
//          value_ = fileIn.readObject.asInstanceOf[T]
//          BroadcastSS.values.put(uuid, value_)
//          fileIn.close
//        } 
//        
//        val time = (System.nanoTime - start) / 1e9
//        logInfo( "Reading Broadcasted variable " + uuid + " took " + time + " s")                  
//      }
//    }
//  }
//  
//  class PublishThread
//  extends Thread with Logging {
//    override def run = {
//      // TODO: Put some delay here to give time others to register
//      // Thread.sleep (5000)
//      logInfo ("Waited. Now sending...")
//      BroadcastSS.synchronized {
//        BroadcastSS.publishVariable[T] (uuid, value)
//      }
//    }
//  }
//}

@serializable 
class CentralizedHDFSBroadcast[T](@transient var value_ : T, local: Boolean) 
extends BroadcastRecipe with Logging {
  
  def value = value_

  BroadcastCH.synchronized { 
    BroadcastCH.values.put(uuid, value_) 
  }

  if (!local) { 
    sendBroadcast 
  }

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
        logInfo( "Started reading Broadcasted variable " + uuid)
        val start = System.nanoTime
        
        val fileIn = new ObjectInputStream(BroadcastCH.openFileForReading(uuid))
        value_ = fileIn.readObject.asInstanceOf[T]
        BroadcastCH.values.put(uuid, value_)
        fileIn.close
        
        val time = (System.nanoTime - start) / 1e9
        logInfo( "Reading Broadcasted variable " + uuid + " took " + time + " s")
      }
    }
  }
}

@serializable
case class SourceInfo (val hostAddress: String, val listenPort: Int, 
  val totalBlocks: Int, val totalBytes: Int, val replicaID: Int)  
extends Comparable [SourceInfo] with Logging {

  var currentLeechers = 0
  var receptionFailed = false
  var MBps: Double = BroadcastCS.MaxMBps
  
  var hasBlocks = 0
  
  // Ascending sort based on leecher count
  def compareTo (o: SourceInfo): Int = (currentLeechers - o.currentLeechers)
  
  // Descending sort based on speed
  // def compareTo (o: SourceInfo): Int = { 
    // if (MBps > o.MBps) -1
    // else if (MBps < o.MBps) 1
    // else 0
  // }
  
  // Descending sort based on globally stored speed
  // def compareTo (o: SourceInfo): Int = { 
    // val mySpeed = BroadcastCS.getSourceSpeed (hostAddress)
    // val urSpeed = BroadcastCS.getSourceSpeed (o.hostAddress)

    // if (mySpeed > urSpeed) -1
    // else if (mySpeed < urSpeed) 1
    // else 0
  // } 
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

private object Broadcast
extends Logging {
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
        // Initialization for SplitStreamBroadcast
        // TODO: SplitStream turned OFF
        // BroadcastSS.initialize (isMaster)
        
        initialized = true
      }
    }
  }
}

private object BroadcastCS
extends Logging {
  val values = new MapMaker ().softValues ().makeMap[UUID, Any]

  var valueToGuidePortMap = Map[UUID, Int] ()
  
  var sourceToSpeedMap = Map[String, Double] ()

  // Random number generator
  var ranGen = new Random

  private var initialized = false
  private var isMaster_ = false

  private var MasterHostAddress_ = "127.0.0.1"
  private var MasterTrackerPort_ : Int = 22222
  private var BlockSize_ : Int = 512 * 1024
  private var MaxRetryCount_ : Int = 2

  private var TrackerSocketTimeout_ : Int = 50000
  private var ServerSocketTimeout_ : Int = 10000

  private var DualMode_ : Boolean = false
 
  private var trackMV: TrackMultipleValues = null

  // newSpeed = ALPHA * oldSpeed + (1 - ALPHA) * curSpeed
  private val ALPHA = 0.7
  // 125.0 MBps = 1 Gbps link
  private val MaxMBps_ = 125.0 

  private var MinKnockInterval_ = 500
  private var MaxKnockInterval_ = 999

  def initialize (isMaster__ : Boolean) {
    synchronized {
      if (!initialized) {
        MasterHostAddress_ = 
          System.getProperty ("spark.broadcast.MasterHostAddress", "127.0.0.1")
        MasterTrackerPort_ = 
          System.getProperty ("spark.broadcast.MasterTrackerPort", "22222").toInt
        BlockSize_ = 
          System.getProperty ("spark.broadcast.BlockSize", "512").toInt * 1024
        MaxRetryCount_ = 
          System.getProperty ("spark.broadcast.MaxRetryCount", "2").toInt          

        TrackerSocketTimeout_ = 
          System.getProperty ("spark.broadcast.TrackerSocketTimeout", "50000").toInt          
        ServerSocketTimeout_ = 
          System.getProperty ("spark.broadcast.ServerSocketTimeout", "10000").toInt          

        MinKnockInterval_ =
          System.getProperty ("spark.broadcast.MinKnockInterval", "500").toInt
        MaxKnockInterval_ =
          System.getProperty ("spark.broadcast.MaxKnockInterval", "999").toInt

        DualMode_ = 
          System.getProperty ("spark.broadcast.DualMode", "false").toBoolean          

        isMaster_ = isMaster__        
                  
        if (isMaster) {
          trackMV = new TrackMultipleValues
          trackMV.setDaemon (true)
          trackMV.start
          logInfo ("TrackMultipleValues started...")
        }
                  
        initialized = true
      }
    }
  }
   
  def MasterHostAddress = MasterHostAddress_
  def MasterTrackerPort = MasterTrackerPort_
  def BlockSize = BlockSize_
  def MaxRetryCount = MaxRetryCount_

  def TrackerSocketTimeout = TrackerSocketTimeout_
  def ServerSocketTimeout = ServerSocketTimeout_

  def DualMode = DualMode_

  def isMaster = isMaster_ 
  
  def MinKnockInterval = MinKnockInterval_
  def MaxKnockInterval = MaxKnockInterval_

  def MaxMBps = MaxMBps_
  
  def registerValue (uuid: UUID, guidePort: Int) = {
    valueToGuidePortMap.synchronized {    
      valueToGuidePortMap += (uuid -> guidePort)
      logInfo ("New value registered with the Tracker " + valueToGuidePortMap)             
    }
  }
  
  def unregisterValue (uuid: UUID) = {
    valueToGuidePortMap.synchronized {
      valueToGuidePortMap (uuid) = SourceInfo.TxOverGoToHDFS
      logInfo ("Value unregistered from the Tracker " + valueToGuidePortMap)             
    }
  }
  
  def getSourceSpeed (hostAddress: String): Double = {
    sourceToSpeedMap.synchronized {
      sourceToSpeedMap.getOrElseUpdate(hostAddress, MaxMBps)
    }
  }
  
  def setSourceSpeed (hostAddress: String, MBps: Double) = {
    sourceToSpeedMap.synchronized {
      var oldSpeed = sourceToSpeedMap.getOrElseUpdate(hostAddress, MaxMBps)
      var newSpeed = ALPHA * oldSpeed + (1 - ALPHA) * MBps
      sourceToSpeedMap.update (hostAddress, newSpeed)
    }
  }
  
  class TrackMultipleValues
  extends Thread with Logging {
    var stopTracker = false
    
    override def run = {
      var threadPool = Executors.newCachedThreadPool
      var serverSocket: ServerSocket = null
      
      serverSocket = new ServerSocket (BroadcastCS.MasterTrackerPort)
      logInfo ("TrackMultipleValues" + serverSocket)      
      
      try {
        while (!stopTracker) {
          var clientSocket: Socket = null
          try {
            // TODO: 
            serverSocket.setSoTimeout (TrackerSocketTimeout)
            clientSocket = serverSocket.accept
          } catch {
            case e: Exception => { 
              logInfo ("TrackMultipleValues Timeout. Stopping listening...") 
              // TODO: Tracking should be explicitly stopped by the SparkContext
              stopTracker = true
            }
          }

          if (clientSocket != null) {
            try {            
              threadPool.execute (new Thread {
                override def run = {
                  val oos = new ObjectOutputStream (clientSocket.getOutputStream)
                  oos.flush
                  val ois = new ObjectInputStream (clientSocket.getInputStream)
                  try {
                    val uuid = ois.readObject.asInstanceOf[UUID]
                    var guidePort = 
                      if (valueToGuidePortMap.contains (uuid)) {
                        valueToGuidePortMap (uuid)
                      } else SourceInfo.TxNotStartedRetry
                    logInfo ("TrackMultipleValues:Got new request: " + clientSocket + " for " + uuid + " : " + guidePort)                    
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
              // In failure, close socket here; else, client thread will close
              case ioe: IOException => clientSocket.close
            }
          }
        }
      } finally {
        serverSocket.close
      }
      
      // Shutdown the thread pool
      threadPool.shutdown      
    }
  }
}

//private object BroadcastSS {
//  val values = new MapMaker ().softValues ().makeMap[UUID, Any]

//  private val valueBytes = new MapMaker().softValues().makeMap[UUID,Array[Byte]]

//  private var initialized = false
//  private var isMaster_ = false
//    
//  private var masterBootHost_ = "127.0.0.1"
//  private var masterBootPort_ : Int = 22222
//  private var blockSize_ : Int = 512 * 1024
//  private var MaxRetryCount_ : Int = 2
//  
//  private var masterBootAddress_ : InetSocketAddress = null
//  private var localBindPort_ : Int = -1
//  
//  private var pEnvironment_ : Environment = null
//  private var pastryNode_ : PastryNode = null
//  private var ssClient: SSClient = null
//  
//  // Current transmission state variables
//  private var curUUID: UUID = null
//  private var curTotalBlocks = -1
//  private var curTotalBytes = -1
//  private var curHasBlocks = -1
//  private var curBlockBitmap: Array[Boolean] = null
//  private var curArrayOfBytes: Array[Byte] = null
//  
//  // TODO: Add stuff so that we can handle out of order variable broadcast

//  def initialize (isMaster__ : Boolean) {
//    synchronized {
//      if (!initialized) {
//        masterBootHost_ = 
//          System.getProperty ("spark.broadcast.MasterHostAddress", "127.0.0.1")
//        masterBootPort_ = 
//          System.getProperty ("spark.broadcast.masterBootPort", "22222").toInt
//          
//        masterBootAddress_ = new InetSocketAddress(masterBootHost_, 
//          masterBootPort_)
//          
//        blockSize_ = 
//          System.getProperty ("spark.broadcast.blockSize", "512").toInt * 1024
//        MaxRetryCount_ = 
//          System.getProperty ("spark.broadcast.MaxRetryCount", "2").toInt          
//      
//        isMaster_ = isMaster__
//        
//        // Initialize the SplitStream tree
//        initializeSplitStream
//        
//        initialized = true  
//      }
//    }
//  }    
//  
//  def masterBootAddress = masterBootAddress_
//  def blockSize = blockSize_
//  def MaxRetryCount = MaxRetryCount_
//  
//  def pEnvironment: Environment = {
//    if (pEnvironment_ == null) { 
//      initializeSplitStream 
//    }
//    pEnvironment_
//  }
//  
//  def pastryNode: PastryNode = {
//    if (pastryNode_ == null) { 
//      initializeSplitStream 
//    }
//    pastryNode_
//  }
//  
//  def localBindPort = {
//    if (localBindPort_ == -1) {
//      if (isMaster) { 
//        localBindPort_ = masterBootPort_ 
//      }
//      else {
//        // TODO: What's the best way of finding a free port?
//        val sSocket = new ServerSocket (0)
//        val sPort = sSocket.getLocalPort
//        sSocket.close
//        localBindPort_ = sPort        
//      }
//    }
//    localBindPort_
//  }

//  def isMaster = isMaster_ 
//  
//  private def initializeSplitStream = {
//    pEnvironment_ = new Environment
//    
//    // Generate the NodeIds Randomly
//    val nidFactory = new RandomNodeIdFactory (pEnvironment)
//    
//    // Construct the PastryNodeFactory
//    val pastryNodeFactory = new SocketPastryNodeFactory (nidFactory, 
//      localBindPort, pEnvironment)    
//      
//    // Construct a Pastry node
//    pastryNode_ = pastryNodeFactory.newNode
//    
//    // Boot the node. 
//    pastryNode.boot (masterBootAddress)
//    // TODO: Some unknown messages are dropped in slaves at this point
//      
//    // The node may require sending several messages to fully boot into the ring
//    pastryNode.synchronized {
//      while(!pastryNode.isReady && !pastryNode.joinFailed) {
//        // Delay so we don't busy-wait
//        pastryNode.wait (500)
//        
//        // Abort if can't join
//        if (pastryNode.joinFailed()) {
//          // TODO: throw new IOException("Join failed " + node.joinFailedReason)
//        }
//      }       
//    }
//    
//    // Create the SplitStream client and subscribe
//    ssClient = new SSClient (BroadcastSS.pastryNode)
//    ssClient.subscribe
//  }
//  
//  def publishVariable[A] (uuid: UUID, obj: A) = {
//    ssClient.synchronized {
//      ssClient.publish[A] (uuid, obj)
//    }
//  }
//  
//  // Return status of the reception
//  def receiveVariable[A] (uuid: UUID): Boolean = {
//    // TODO: Things will change if out-of-order variable recepetion is supported
//    
//    logInfo ("In receiveVariable")
//    
//    // Check in valueBytes
//    if (xferValueBytesToValues[A] (uuid)) { 
//      return true 
//    }
//    
//    // Check if its in progress
//    for (i <- 0 until MaxRetryCount) {
//      logInfo (uuid + " " + curUUID)
//      while (uuid == curUUID) { 
//        Thread.sleep (100) // TODO: How long to sleep
//      }
//      if (xferValueBytesToValues[A] (uuid)) { 
//        return true 
//      }
//      
//      // Wait for a while to see if we've reached here before xmission started
//      Thread.sleep (100) 
//    }    
//    return false
//  }
//  
//  private def xferValueBytesToValues[A] (uuid: UUID): Boolean = {
//    var cachedValueBytes: Array[Byte] = null
//    valueBytes.synchronized { 
//      cachedValueBytes = valueBytes.get (uuid) 
//    }
//    if (cachedValueBytes != null) {
//      val cachedValue = byteArrayToObject[A] (cachedValueBytes)
//      values.synchronized { 
//        values.put (uuid, cachedValue) 
//      }
//      return true
//    }
//    return false
//  }
//  
//  private def objectToByteArray[A] (obj: A): Array[Byte] = {
//    val baos = new ByteArrayOutputStream
//    val oos = new ObjectOutputStream (baos)
//    oos.writeObject (obj)
//    oos.close
//    baos.close
//    return baos.toByteArray
//  }

//  private def byteArrayToObject[A] (bytes: Array[Byte]): A = {
//    val in = new ObjectInputStream (new ByteArrayInputStream (bytes))
//    val retVal = in.readObject.asInstanceOf[A]
//    in.close
//    return retVal
//  }

//  private def intToByteArray (value: Int): Array[Byte] = {
//    var retVal = new Array[Byte] (4)
//    for (i <- 0 until 4) {
//      retVal(i) = (value >> ((4 - 1 - i) * 8)).toByte
//    }
//    return retVal
//  }

//  private def byteArrayToInt (arr: Array[Byte], offset: Int): Int = {
//    var retVal = 0
//    for (i <- 0 until 4) {
//      retVal += ((arr(i + offset).toInt & 0x000000FF) << ((4 - 1 - i) * 8))
//    }
//    return retVal
//  }

//  class SSClient (pastryNode: PastryNode)
//  extends SplitStreamClient 
//    with Application {
//    // Magic bits: 11111100001100100100110000111111
//    val magicBits = 0xFC324C3F
//    
//    // Message Types
//    val INFO_MSG = 1
//    val DATA_MSG = 2
//        
//    // The Endpoint represents the underlying node. By making calls on the 
//    // Endpoint, it assures that the message will be delivered to the App on 
//    // whichever node the message is intended for.
//    protected val endPoint = pastryNode.buildEndpoint (this, "myInstance")

//    // Handle to a SplitStream implementation
//    val mySplitStream = new SplitStreamImpl (pastryNode, "mySplitStream")

//    // The ChannelId is constructed from a normal PastryId based on the UUID
//    val myChannelId = new ChannelId (new PastryIdFactory 
//      (pastryNode.getEnvironment).buildId ("myChannel"))
//    
//    // The channel
//    var myChannel: Channel = null
//    
//    // The stripes. Acquired from myChannel.
//    var myStripes: Array[Stripe] = null

//    // Now we can receive messages
//    endPoint.register
//    
//    // Subscribes to all stripes in myChannelId.
//    def subscribe = {
//      // Attaching makes you part of the Channel, and volunteers to be an 
//      // internal node of one of the trees
//      myChannel = mySplitStream.attachChannel (myChannelId)
//      
//      // Subscribing notifies your application when data comes through the tree
//      myStripes = myChannel.getStripes
//      for (curStripe <- myStripes) { 
//        curStripe.subscribe (this) 
//      }
//    }
//    
//    // Part of SplitStreamClient. Called when a published message is received.
//    def deliver (s: Stripe, data: Array[Byte]) = { 
//      // Unpack and verify magicBits
//      val topLevelInfo = byteArrayToObject[(Int, Int, Array[Byte])] (data)
//      
//      // Process only if magicBits are OK
//      if (topLevelInfo._1 == magicBits) {
//        // Process only for slaves         
//        if (!BroadcastSS.isMaster) {
//          // Match on Message Type
//          topLevelInfo._2 match {
//            case INFO_MSG => {
//              val realInfo = byteArrayToObject[(UUID, Int, Int)] (
//                topLevelInfo._3)
//              
//              // Setup states for impending transmission
//              curUUID = realInfo._1 // TODO: 
//              curTotalBlocks = realInfo._2
//              curTotalBytes  = realInfo._3            
//              
//              curHasBlocks = 0
//              curBlockBitmap = new Array[Boolean] (curTotalBlocks)
//              curArrayOfBytes = new Array[Byte] (curTotalBytes)
//              
//              logInfo (curUUID + " " + curTotalBlocks + " " + curTotalBytes)
//            } 
//            case DATA_MSG => {
//              val realInfo = byteArrayToObject[(UUID, Int, Array[Byte])] (
//                topLevelInfo._3)
//              val blockUUID  = realInfo._1
//              val blockIndex = realInfo._2
//              val blockData  = realInfo._3
//              
//              // TODO: Will change in future implementation. Right now we 
//              // require broadcast in order on the variable level. Blocks can 
//              // come out of order though
//              assert (blockUUID == curUUID)
//              
//              // Update everything
//              curHasBlocks += 1
//              curBlockBitmap(blockIndex) = true
//              System.arraycopy (blockData, 0, curArrayOfBytes, 
//                blockIndex * blockSize, blockData.length)
//              
//              logInfo ("Got stuff for: " + blockUUID)
//                              
//              // Done receiving
//              if (curHasBlocks == curTotalBlocks) { 
//                // Store as a Array[Byte]
//                valueBytes.synchronized {
//                  valueBytes.put (curUUID, curArrayOfBytes)
//                }
//                
//                logInfo ("Finished reading. Stored in valueBytes")
//                
//                // RESET
//                curUUID = null
//              }
//            }
//            case _ => {
//              // Should never happen
//            }
//          } 
//        }
//      }
//    }

//    // Multicasts data.
//    def publish[A] (uuid: UUID, obj: A) = {
//      val byteArray = objectToByteArray[A] (obj)
//      
//      var blockNum = (byteArray.length / blockSize) 
//      if (byteArray.length % blockSize != 0) 
//        blockNum += 1       
//      
//      //           -------------------------------------
//      // INFO_MSG: | UUID | Total Blocks | Total Bytes |
//      //           -------------------------------------      
//      var infoByteArray = objectToByteArray[(UUID, Int, Int)] ((uuid, blockNum, 
//        byteArray.length))                  
//      doPublish (0, INFO_MSG, infoByteArray)
//      
//      //           -------------------------------------
//      // DATA_MSG: | UUID | Block Index | Single Block |
//      //           -------------------------------------
//      var blockID = 0
//      for (i <- 0 until (byteArray.length, blockSize)) {          
//        val thisBlockSize = Math.min (blockSize, byteArray.length - i)        
//        var thisBlockData = new Array[Byte] (thisBlockSize)
//        System.arraycopy (byteArray, i * blockSize, thisBlockData, 0, 
//          thisBlockSize)

//        var dataByteArray = objectToByteArray[(UUID, Int, Array[Byte])] ((uuid, 
//          blockID, thisBlockData)) 
//        doPublish (blockID % myStripes.length, DATA_MSG, dataByteArray)

//        blockID += 1
//      }
//    }
//    
//    //                 --------------------------------
//    // Message Format: | MagicBits | Type | Real Data |
//    //                 --------------------------------
//    private def doPublish (stripeID: Int, msgType: Int, data: Array[Byte]) = {
//      val bytesToSend = objectToByteArray[(Int, Int, Array[Byte])] ((magicBits, 
//        msgType, data))
//      myStripes(stripeID).publish (bytesToSend)
//    }

//    /* class PublishContent
//      extends Message {
//      def getPriority: Int = { Message.MEDIUM_PRIORITY }
//    } */
//    
//    // Error handling
//    def joinFailed(s: Stripe) = { 
//      logInfo ("joinFailed(" + s + ")") 
//    }
//
//    // Rest of the Application interface. NOT USED.
//    def deliver (id: rice.p2p.commonapi.Id, message: Message) = { } 
//    def forward (message: RouteMessage): Boolean = false
//    def update (handle: rice.p2p.commonapi.NodeHandle, joined: Boolean) = { }    
//  }  
//}

private object BroadcastCH
extends Logging {
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
