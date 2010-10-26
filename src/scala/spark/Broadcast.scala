package spark

import java.io._
import java.net._
import java.util.{UUID, Comparator, BitSet}

import com.google.common.collect.MapMaker

import java.util.concurrent.{Executors, ExecutorService, ThreadPoolExecutor}

import scala.collection.mutable.{ListBuffer, Map}

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
class BitTorrentBroadcast[T] (@transient var value_ : T, local: Boolean) 
extends BroadcastRecipe  with Logging {
  
  def value = value_

  BroadcastBT.synchronized { BroadcastBT.values.put (uuid, value_) }
   
  @transient var arrayOfBlocks: Array[BroadcastBlock] = null
  @transient var totalBytes = -1
  @transient var totalBlocks = -1
  @transient var hasBlocks = 0

  @transient var listenPortLock = new Object
  @transient var guidePortLock = new Object
  @transient var totalBlocksLock = new Object
  @transient var hasBlocksLock = new Object
  
  @transient var listOfSources = ListBuffer[SourceInfo] ()  
  @transient var hasBlocksBitVector: BitSet = null
  @transient var numCopiesSent: Array[Int] = null

  @transient var serveMR: ServeMultipleRequests = null 
  
  // Used only in Master
  @transient var guideMR: GuideMultipleRequests = null
  
  // Used only in Workers
  @transient var ttGuide: TalkToGuide = null

  @transient var hostAddress = InetAddress.getLocalHost.getHostAddress
  @transient var listenPort = -1    
  @transient var guidePort = -1
  
  @transient var hasCopyInHDFS = false
  
  // Must call this after all the variables have been created/initialized
  if (!local) { sendBroadcast }

  def sendBroadcast () {    
    // Store a persistent copy in HDFS    
    // TODO: Turned OFF for now
    // val out = new ObjectOutputStream (BroadcastCH.openFileForWriting(uuid))
    // out.writeObject (value_)
    // out.close    
    hasCopyInHDFS = true

    // Create a variableInfo object and store it in valueInfos
    var variableInfo = blockifyObject (value_, BroadcastBT.blockSize)   
    
    // Prepare the value being broadcasted
    // TODO: Refactoring and clean-up required here
    arrayOfBlocks = variableInfo.arrayOfBlocks
    totalBytes = variableInfo.totalBytes
    totalBlocks = variableInfo.totalBlocks
    hasBlocks = variableInfo.totalBlocks
    
    // Guide has all the blocks
    hasBlocksBitVector = new BitSet (totalBlocks)
    hasBlocksBitVector.set (0, totalBlocks)
    
    // Guide still hasn't sent any block
    numCopiesSent = new Array[Int] (totalBlocks)
   
    guideMR = new GuideMultipleRequests
    guideMR.setDaemon (true)
    guideMR.start
    logInfo ("GuideMultipleRequests started...")
    
    // Must always come AFTER guideMR is created
    while (guidePort == -1) { 
      guidePortLock.synchronized {
        guidePortLock.wait 
      }
    }

    serveMR = new ServeMultipleRequests
    serveMR.setDaemon (true)
    serveMR.start
    logInfo ("ServeMultipleRequests started...")

    // Must always come AFTER serveMR is created
    while (listenPort == -1) { 
      listenPortLock.synchronized {
        listenPortLock.wait 
      }
    } 

    // Must always come AFTER listenPort is created
    val masterSource = 
      SourceInfo (hostAddress, listenPort, totalBlocks, totalBytes)
    hasBlocksBitVector.synchronized {
      masterSource.hasBlocksBitVector = hasBlocksBitVector
    }
    
    // In the beginning, this is the only known source to Guide
    listOfSources = listOfSources + masterSource

    // Register with the Tracker
    BroadcastBT.registerValue (uuid, 
      SourceInfo (hostAddress, guidePort, totalBlocks, totalBytes))
  }
  
  private def readObject (in: ObjectInputStream) {
    in.defaultReadObject
    BroadcastBT.synchronized {
      val cachedVal = BroadcastBT.values.get (uuid)
      
      if (cachedVal != null) {
        value_ = cachedVal.asInstanceOf[T]
      } else {
        // Only the first worker in a node can ever be inside this 'else'
        initializeWorkerVariables
        
        // Start local ServeMultipleRequests thread first
        serveMR = new ServeMultipleRequests
        serveMR.setDaemon (true)
        serveMR.start
        logInfo ("ServeMultipleRequests started...")
        
        val start = System.nanoTime

        val receptionSucceeded = receiveBroadcast (uuid)
        // If does not succeed, then get from HDFS copy
        if (receptionSucceeded) {
          value_ = unBlockifyObject[T]
          BroadcastBT.values.put (uuid, value_)
        }  else {
          // TODO: This part won't work, cause HDFS writing is turned OFF
          val fileIn = new ObjectInputStream(BroadcastCH.openFileForReading(uuid))
          value_ = fileIn.readObject.asInstanceOf[T]
          BroadcastBT.values.put(uuid, value_)
          fileIn.close
        }
        
        val time = (System.nanoTime - start) / 1e9
        logInfo("Reading Broadcasted variable " + uuid + " took " + time + " s")
      }
    }
  }
  
  // Initialize variables in the worker node. Master sends everything as 0/null 
  private def initializeWorkerVariables = {
    arrayOfBlocks = null
    hasBlocksBitVector = null
    numCopiesSent = null
    totalBytes = -1
    totalBlocks = -1
    hasBlocks = 0
    
    listenPortLock = new Object
    totalBlocksLock = new Object
    hasBlocksLock = new Object

    serveMR = null
    ttGuide = null

    hostAddress = InetAddress.getLocalHost.getHostAddress
    listenPort = -1
    
    listOfSources = ListBuffer[SourceInfo] ()
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
        i * BroadcastBT.blockSize, arrayOfBlocks(i).byteArray.length)
    }    
    byteArrayToObject (retByteArray)
  }
  
  private def byteArrayToObject[A] (bytes: Array[Byte]): A = {
    val in = new ObjectInputStream (new ByteArrayInputStream (bytes))
    val retVal = in.readObject.asInstanceOf[A]
    in.close
    return retVal
  }

  private def getLocalSourceInfo: SourceInfo = {
    // Wait till hostName and listenPort are OK
    while (listenPort == -1) { 
      listenPortLock.synchronized {
        listenPortLock.wait 
      }
    } 
    
    // Wait till totalBlocks and totalBytes are OK
    while (totalBlocks == -1) { 
      totalBlocksLock.synchronized {
        totalBlocksLock.wait 
      }
    } 

    var localSourceInfo = SourceInfo (hostAddress, listenPort, totalBlocks, 
      totalBytes)
    
    hasBlocksBitVector.synchronized {
      localSourceInfo.hasBlocksBitVector = hasBlocksBitVector
    }
    
    return localSourceInfo
  }

  class TalkToGuide (gInfo: SourceInfo) 
  extends Thread with Logging {
    override def run = {
      // Connect to Guide and send this worker's information
      var clientSocketToGuide: Socket = null        
      var oosGuide: ObjectOutputStream = null
      var oisGuide: ObjectInputStream = null

      // TODO: Do we need a breaking mechanism out of this infinite loop?
      while (true) {
        clientSocketToGuide = new Socket(gInfo.hostAddress, gInfo.listenPort)
        oosGuide = new ObjectOutputStream (clientSocketToGuide.getOutputStream)      
        oosGuide.flush
        oisGuide = new ObjectInputStream (clientSocketToGuide.getInputStream)

        // Send local information
        oosGuide.writeObject(getLocalSourceInfo)
        oosGuide.flush

        // Receive source information from Guide
        var suitableSources = 
          oisGuide.readObject.asInstanceOf[ListBuffer[SourceInfo]]
        
        logInfo("Received suitableSources from Master " + suitableSources)
        
        // Update local list of sources by adding or replacing
        // TODO: There might be some contradiciton on the use of listOfSources
        listOfSources.synchronized {
          suitableSources.foreach { srcInfo =>
            // Removing old copy of srcInfo to be replaced with a new one
            // It works because case clases are compared by constructor params
            if (listOfSources.contains(srcInfo)) 
              { listOfSources = listOfSources - srcInfo }
            listOfSources = listOfSources + srcInfo
          }
        }
        
        oisGuide.close
        oosGuide.close
        clientSocketToGuide.close

        // TODO: DO NOT use constant sleep value here
        // TODO: Guide should send back a backoff time, somehow
        Thread.sleep (100)        
      }      
    }          
  }
    
  def getGuideInfo (variableUUID: UUID): SourceInfo = {
    var clientSocketToTracker: Socket = null
    var oosTracker: ObjectOutputStream = null
    var oisTracker: ObjectInputStream = null
    
    var gInfo: SourceInfo = SourceInfo ("", SourceInfo.TxOverGoToHDFS, 
      SourceInfo.UnusedParam, SourceInfo.UnusedParam)
    
    var retriesLeft = BroadcastBT.maxRetryCount
    do {
      try {  
        // Connect to the tracker to find out GuideInfo
        val clientSocketToTracker = 
          new Socket(BroadcastBT.masterHostAddress, BroadcastBT.masterTrackerPort)  
        val oosTracker = 
          new ObjectOutputStream (clientSocketToTracker.getOutputStream)
        oosTracker.flush
        val oisTracker = 
          new ObjectInputStream (clientSocketToTracker.getInputStream)
      
        // Send UUID and receive GuideInfo
        oosTracker.writeObject (uuid)
        oosTracker.flush
        gInfo = oisTracker.readObject.asInstanceOf[SourceInfo]
      } catch {
        case e: Exception => { }
      } finally {   
        if (oisTracker != null) { oisTracker.close }
        if (oosTracker != null) { oosTracker.close }
        if (clientSocketToTracker != null) { clientSocketToTracker.close }
      }
      retriesLeft -= 1     
      // TODO: Should wait before retrying. Implement wait function.
      Thread.sleep (1234)
    } while (retriesLeft > 0 && gInfo.listenPort == SourceInfo.TxNotStartedRetry)

    logInfo ("Got this guidePort from Tracker: " + gInfo.listenPort)
    return gInfo
  }
  
  def receiveBroadcast (variableUUID: UUID): Boolean = {
    val gInfo = getGuideInfo (variableUUID)    
        
    if (gInfo.listenPort == SourceInfo.TxOverGoToHDFS || 
        gInfo.listenPort == SourceInfo.TxNotStartedRetry) { 
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

    // Setup initial states of variables
    totalBlocks = gInfo.totalBlocks
    arrayOfBlocks = new Array[BroadcastBlock] (totalBlocks)
    hasBlocksBitVector = new BitSet (totalBlocks)
    numCopiesSent = new Array[Int] (totalBlocks)
    totalBlocksLock.synchronized {
      totalBlocksLock.notifyAll
    }
    totalBytes = gInfo.totalBytes
      
    // Start ttGuide to periodically talk to the Guide 
    var ttGuide = new TalkToGuide (gInfo)
    ttGuide.setDaemon (true)
    ttGuide.start
    logInfo ("TalkToGuide started...")
    
    // Start pController to run TalkToPeer threads
    var pcController = new PeerChatterController
    pcController.setDaemon (true)
    pcController.start
    logInfo ("PeerChatterController started...")
    
    // TODO: Must fix this. This might never break if broadcast fails. 
    // We should be able to break and send false. Also need to kill threads
    while (hasBlocks != totalBlocks) { Thread.sleep(1234) }
   
    return true
  }

  class PeerChatterController
  extends Thread with Logging {
    private var peersNowTalking = ListBuffer[SourceInfo] ()

    override def run = {
      // TODO: Using constant number of max peers to connect to
      val MAX_PEERS = 4
      // TODO: Look into ExecutorService shutdown and shutdownNow methods
      var threadPool = 
        Executors.newFixedThreadPool(MAX_PEERS).asInstanceOf[ThreadPoolExecutor]
      
      while (hasBlocks < totalBlocks) {
        var numThreadsToCreate = Math.min (listOfSources.size, MAX_PEERS) - 
          threadPool.getActiveCount

        while(numThreadsToCreate > 0 && hasBlocks < totalBlocks) {
          var peerToTalkTo = pickPeerToTalkTo
          if (peerToTalkTo != null) {
            threadPool.execute (new TalkToPeer (peerToTalkTo))
          }
          
          // Add to peersNowTalking. Remove in the thread. We have to do this 
          // ASAP, otherwise pickPeerToTalkTo picks the same peer more than once
          peersNowTalking.synchronized { 
            peersNowTalking = peersNowTalking + peerToTalkTo
          }
          
          numThreadsToCreate = numThreadsToCreate - 1
        }
        
        // Sleep for a while before starting some more threads
        Thread.sleep (500)
      }
    }
    
    // TODO: Right now picking the one that has the most blocks this peer wants
    private def pickPeerToTalkTo: SourceInfo = {
      var curPeer: SourceInfo = null
      var curMax = 0
      
      logInfo ("Picking peers to talk to...")
      
      // Find peers that are not connected right now
      var peersNotInUse = ListBuffer[SourceInfo] ()
      synchronized {
        peersNotInUse = listOfSources -- peersNowTalking
      }
      
      peersNotInUse.foreach { eachSource =>
        var tempHasBlocksBitVector: BitSet = null
        hasBlocksBitVector.synchronized {
          tempHasBlocksBitVector = hasBlocksBitVector.clone.asInstanceOf[BitSet]
        }
        tempHasBlocksBitVector.flip (0, tempHasBlocksBitVector.size)
        tempHasBlocksBitVector.and (eachSource.hasBlocksBitVector)
        
        if (tempHasBlocksBitVector.cardinality > curMax) {
          curPeer = eachSource
          curMax = tempHasBlocksBitVector.cardinality
        }
      }
      
      if (curPeer != null)
        logInfo ("Peer chosen: " + curPeer + " with " + curPeer.hasBlocksBitVector)
      else
        logInfo ("No peer chosen...")
      
      return curPeer
    }
    
  class TalkToPeer (peerToTalkTo: SourceInfo)
    extends Thread with Logging {
      override def run = {
        var peerSocketToSource: Socket = null    
        var oosSource: ObjectOutputStream = null
        var oisSource: ObjectInputStream = null
        
        logInfo ("TalkToPeer started... => " + peerToTalkTo)
        
        try {
          // Connect to the source
          peerSocketToSource = 
            new Socket (peerToTalkTo.hostAddress, peerToTalkTo.listenPort)        
          oosSource = 
            new ObjectOutputStream (peerSocketToSource.getOutputStream)
          oosSource.flush
          oisSource = 
            new ObjectInputStream (peerSocketToSource.getInputStream)
            
          // TODO: Who decides which blocks to move back and forth? 
          // TODO: Should we transfer multiple instead of just one?
          
          // Send latest SourceInfo
          oosSource.writeObject(getLocalSourceInfo)
          oosSource.flush
          
          // Receive latest SourceInfo from peerToTalkTo
          var newPeerToTalkTo = oisSource.readObject.asInstanceOf[SourceInfo]
          // Update listOfSources
          listOfSources.synchronized {
            if (listOfSources.contains(newPeerToTalkTo))
              { listOfSources = listOfSources - newPeerToTalkTo }
            listOfSources = listOfSources + newPeerToTalkTo
          }

          // TODO: There is a problem with closing this way
          while (hasBlocks < totalBlocks) {
            val bcBlock = oisSource.readObject.asInstanceOf[BroadcastBlock]
            if (!hasBlocksBitVector.get(bcBlock.blockID)) {
              arrayOfBlocks(bcBlock.blockID) = bcBlock
              hasBlocksBitVector.synchronized {
                hasBlocksBitVector.set (bcBlock.blockID)
              }
              hasBlocks += 1
              hasBlocksLock.synchronized {
                hasBlocksLock.notifyAll
              }
              logInfo ("Received block: " + bcBlock.blockID + " from " + peerToTalkTo)
            }
          }
        } catch {
          case e: Exception => { 
            logInfo ("TalktoPeer had a " + e)
            // Remove this pInfo from listOfSources
            // TODO: We probably should have the following in some form, but not 
            // really here. This exception can happen if the sender just breaks connection
//            listOfSources.synchronized {
//              logInfo ("Exception in TalkToPeer. Removing source: " + peerToTalkTo)
//              listOfSources = listOfSources - peerToTalkTo
//            }
          }
        } finally {    
          if (oisSource != null) { oisSource.close }
          if (oosSource != null) { oosSource.close }
          if (peerSocketToSource != null) { peerSocketToSource.close }
          
          // Delete from peersNowTalking
          peersNowTalking.synchronized {
            peersNowTalking = peersNowTalking - peerToTalkTo
          }
        }
      }
    }  
  }
  
  class GuideMultipleRequests
  extends Thread with Logging {
    override def run = {
      // TODO: Cached threadpool has 60s keep alive timer
      var threadPool = Executors.newCachedThreadPool
      var serverSocket: ServerSocket = null

      serverSocket = new ServerSocket (0)
      guidePort = serverSocket.getLocalPort
      logInfo ("GuideMultipleRequests => " + serverSocket + " " + guidePort)
      
      guidePortLock.synchronized {
        guidePortLock.notifyAll
      }

      var keepAccepting = true
      try {
        // Don't stop until there is a copy in HDFS
        while (keepAccepting || !hasCopyInHDFS) {
          var clientSocket: Socket = null
          try {
            serverSocket.setSoTimeout (BroadcastBT.serverSocketTimout)
            clientSocket = serverSocket.accept
          } catch {
            case e: Exception => { 
              logInfo ("GuideMultipleRequests Timeout. Stopping listening..." + hasCopyInHDFS) 
              keepAccepting = false 
            }
          }
          if (clientSocket != null) {
            logInfo ("Guide:Accepted new client connection:" + clientSocket)
            try {            
              threadPool.execute (new GuideSingleRequest (clientSocket))
            } catch {
              // In failure, close the socket here; else, thread will close it
              case ioe: IOException => clientSocket.close
            }
          }
        }
        BroadcastBT.unregisterValue (uuid)
      } finally {
        serverSocket.close
      }
    }
    
    class GuideSingleRequest (val clientSocket: Socket) 
    extends Thread with Logging {
      private val oos = new ObjectOutputStream (clientSocket.getOutputStream)
      oos.flush
      private val ois = new ObjectInputStream (clientSocket.getInputStream)

      private var sourceInfo: SourceInfo = null
      private var selectedSources: ListBuffer[SourceInfo] = null
      
      private var rollOverIndex = 0
      
      override def run = {
        try {
          logInfo ("new GuideSingleRequest is running")
          // Connecting worker is sending in its information
          sourceInfo = ois.readObject.asInstanceOf[SourceInfo]
          
          // Select a suitable source and send it back to the worker
          selectedSources = selectSuitableSources (sourceInfo)
          logInfo ("Sending selectedSources:" + selectedSources)
          oos.writeObject (selectedSources)
          oos.flush

          listOfSources.synchronized {
            // Add this source to the listOfSources
            if (listOfSources.contains(sourceInfo)) 
              { listOfSources = listOfSources - sourceInfo }
            listOfSources = listOfSources + sourceInfo
          }
        } catch {
          case e: Exception => { 
            // Assuming exception caused by receiver failure: remove
            if (listOfSources != null) { 
              listOfSources.synchronized {
                listOfSources = listOfSources - sourceInfo
              }
            }    
          }
        } finally {
          ois.close
          oos.close
          clientSocket.close
        }
      }
      
      // TODO: Randomly select some sources to send back. 
      // Right now just rolls over the listOfSources to send back 
      // BroadcastBT.MaxPeersInGuideResponse number of possible sources
      private def selectSuitableSources(skipSourceInfo: SourceInfo): ListBuffer[SourceInfo] = { 
        var curIndex = rollOverIndex
        var selectedSources = ListBuffer[SourceInfo] ()
        listOfSources.synchronized {
          do {
            if (listOfSources(curIndex) != skipSourceInfo)
              { selectedSources = selectedSources + listOfSources(curIndex) }
            curIndex = (curIndex + 1) % listOfSources.size
          } while (curIndex != rollOverIndex && 
            selectedSources.size != BroadcastBT.MaxPeersInGuideResponse)
        }
        rollOverIndex = curIndex
        return selectedSources
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
      logInfo ("ServeMultipleRequests " + serverSocket)
      
      listenPortLock.synchronized {
        listenPortLock.notifyAll
      }
            
      var keepAccepting = true
      try {
        while (keepAccepting) {
          var clientSocket: Socket = null
          try {
            serverSocket.setSoTimeout (BroadcastBT.serverSocketTimout)
            clientSocket = serverSocket.accept
          } catch {
            case e: Exception => { 
              logInfo ("ServeMultipleRequests Timeout. Stopping listening...") 
              keepAccepting = false 
            }
          }
          if (clientSocket != null) {
            logInfo ("Serve:Accepted new client connection:" + clientSocket)
            try {            
              threadPool.execute (new ServeSingleRequest (clientSocket))
            } catch {
              // In failure, close socket here; else, the thread will close it
              case ioe: IOException => clientSocket.close
            }
          }
        }
      } finally {
        if (serverSocket != null) { serverSocket.close }
      }
    }
    
    class ServeSingleRequest (val clientSocket: Socket) 
    extends Thread with Logging {
      // TODO: This has to be fixed somehow
      private val MAX_MILLIS_TO_CHAT = 50
      
      private val oos = new ObjectOutputStream (clientSocket.getOutputStream)
      oos.flush
      private val ois = new ObjectInputStream (clientSocket.getInputStream)

      logInfo ("new ServeSingleRequest is running")
      
      override def run  = {
        try {
          // Receive latest SourceInfo from the receiver
          val rxSourceInfo = ois.readObject.asInstanceOf[SourceInfo]
          
          logInfo("rxSourceInfo: " + rxSourceInfo + " with " + rxSourceInfo.hasBlocksBitVector)
          
          // Update listOfSources
          listOfSources.synchronized {
            if (listOfSources.contains(rxSourceInfo)) 
              { listOfSources = listOfSources - rxSourceInfo }
            listOfSources = listOfSources + rxSourceInfo
          }         
          
          // Send latest local SourceInfo to the receiver
          oos.writeObject(getLocalSourceInfo)
          oos.flush
          
          // TODO: NOT the most efficient way to do time-based break; but using timer can cause a break in the middle :-S
          val startTime = System.currentTimeMillis
          var curTime = startTime
          var keepSending = true
          
          while (keepSending && curTime - startTime < MAX_MILLIS_TO_CHAT) {
            val sentBlock = pickAndSendBlock (rxSourceInfo.hasBlocksBitVector)
            if (sentBlock < 0) {
              keepSending = false 
            } else {
              rxSourceInfo.hasBlocksBitVector.set (sentBlock)
            }
            curTime = System.currentTimeMillis
          }
        } catch {
          // TODO: Need to add better exception handling here
          // If something went wrong, e.g., the worker at the other end died etc. 
          // then close everything up
          // Exception can happen if the receiver stops receiving
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

      // Right now picking the rarest first block 
      private def pickAndSendBlock (rxHasBlocksBitVector: BitSet): Int = {
        var blockIndex = -1
        var minCopies = Int.MaxValue
        var nextIndex = -1
        
        logInfo ("Picking a block to send...")

        // Figure out which blocks the receiver doesn't have
        var tempHasBlocksBitVector = 
          rxHasBlocksBitVector.clone.asInstanceOf[BitSet]
        tempHasBlocksBitVector.flip (0, tempHasBlocksBitVector.size)  
        hasBlocksBitVector.synchronized {
          tempHasBlocksBitVector.and (hasBlocksBitVector)
        }
        
        // Traverse over all the blocks
        numCopiesSent.synchronized {
          do {
            nextIndex = tempHasBlocksBitVector.nextSetBit(nextIndex + 1)
            if (nextIndex != -1 && numCopiesSent(nextIndex) < minCopies) {
              minCopies = numCopiesSent(nextIndex)
              numCopiesSent(nextIndex) = numCopiesSent(nextIndex) + 1
              blockIndex = nextIndex
            }
          } while (nextIndex != -1)
        }
        
        if (blockIndex < 0) { 
          logInfo ("No block to send...")
        } else {
          try {
            oos.writeObject (arrayOfBlocks(blockIndex))
            oos.flush
          } catch { 
            case e: Exception => { } 
          }
          logInfo ("Sent block: " + blockIndex + " to " + clientSocket)
        }
        
        return blockIndex
      }    
    } 
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
  val totalBlocks: Int, val totalBytes: Int)  
  extends Logging {

  var currentLeechers = 0
  var receptionFailed = false
  var MBps: Double = BroadcastBT.MaxMBps
  
  var hasBlocksBitVector: BitSet = new BitSet (totalBlocks)
}

object SourceInfo {
  // Constants for special values of listenPort
  val TxNotStartedRetry = -1
  val TxOverGoToHDFS = 0
  // Other constants
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
        // Initialization for BitTorrentBroadcast
        BroadcastBT.initialize (isMaster)
        
        initialized = true
      }
    }
  }
}

private object BroadcastBT
extends Logging {
  val values = new MapMaker ().softValues ().makeMap[UUID, Any]

  var valueToGuideMap = Map[UUID, SourceInfo] ()
  
  var sourceToSpeedMap = Map[String, Double] ()

  private var initialized = false
  private var isMaster_ = false

  private var masterHostAddress_ = "127.0.0.1"
  private var masterTrackerPort_ : Int = 11111
  private var blockSize_ : Int = 512 * 1024
  private var maxRetryCount_ : Int = 2
  private var serverSocketTimout_ : Int = 50000
 
  private var trackMV: TrackMultipleValues = null

  // newSpeed = ALPHA * oldSpeed + (1 - ALPHA) * curSpeed
  private val ALPHA = 0.7
  // 125.0 MBps = 1 Gbps link
  private val MaxMBps_ = 125.0 
  
  private var MaxPeersInGuideResponse_ = 4

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
        MaxPeersInGuideResponse_ = 
          System.getProperty ("spark.broadcast.MaxPeersInGuideResponse", "4").toInt

        isMaster_ = isMaster__        
                  
        if (isMaster) {
          trackMV = new TrackMultipleValues
          trackMV.setDaemon (true)
          trackMV.start
          logInfo ("TrackMultipleValues started")         
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

  def isMaster = isMaster_ 
  
  def MaxMBps = MaxMBps_
  
  def MaxPeersInGuideResponse = MaxPeersInGuideResponse_
  
  def registerValue (uuid: UUID, gInfo: SourceInfo) = {
    valueToGuideMap.synchronized {    
      valueToGuideMap += (uuid -> gInfo)
      logInfo ("New value registered with the Tracker " + valueToGuideMap)             
    }
  }
  
  def unregisterValue (uuid: UUID) = {
    valueToGuideMap.synchronized {
      valueToGuideMap (uuid) = SourceInfo ("", SourceInfo.TxOverGoToHDFS, 
        SourceInfo.UnusedParam, SourceInfo.UnusedParam)
      logInfo ("Value unregistered from the Tracker " + valueToGuideMap)             
    }
  }
  
//  def startMultiTracker
//  def stopMultiTracker
  
//  def getSourceSpeed (hostAddress: String): Double = {
//    sourceToSpeedMap.synchronized {
//      sourceToSpeedMap.getOrElseUpdate(hostAddress, MaxMBps)
//    }
//  }
  
//  def setSourceSpeed (hostAddress: String, MBps: Double) = {
//    sourceToSpeedMap.synchronized {
//      var oldSpeed = sourceToSpeedMap.getOrElseUpdate(hostAddress, MaxMBps)
//      var newSpeed = ALPHA * oldSpeed + (1 - ALPHA) * MBps
//      sourceToSpeedMap.update (hostAddress, newSpeed)
//    }
//  } 
    
  class TrackMultipleValues
  extends Thread with Logging {
    var keepAccepting = true
    
    override def run = {
      var threadPool = Executors.newCachedThreadPool
      var serverSocket: ServerSocket = null
      
      serverSocket = new ServerSocket (BroadcastBT.masterTrackerPort)
      logInfo ("TrackMultipleValues" + serverSocket)      
      
      try {
        while (keepAccepting) {
          var clientSocket: Socket = null
          try {
            // TODO: 
            serverSocket.setSoTimeout (serverSocketTimout)
            clientSocket = serverSocket.accept
          } catch {
            case e: Exception => { 
              logInfo ("TrackMultipleValues Timeout. Stopping listening...") 
              // TODO: Tracking should be explicitly stopped by the SparkContext
              keepAccepting = false 
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
                    var gInfo = 
                      if (valueToGuideMap.contains (uuid)) {
                        valueToGuideMap (uuid)
                      } else SourceInfo ("", SourceInfo.TxNotStartedRetry, 
                          SourceInfo.UnusedParam, SourceInfo.UnusedParam)
                    logInfo ("TrackMultipleValues:Got new request: " + clientSocket + " for " + uuid + " : " + gInfo.listenPort)                    
                    oos.writeObject (gInfo)
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
    }
  }
}

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
