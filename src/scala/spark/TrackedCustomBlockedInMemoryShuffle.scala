package spark

import java.io._
import java.net._
import java.util.{BitSet, Random, Timer, TimerTask, UUID}
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.{LinkedBlockingQueue, Executors, ThreadPoolExecutor, ThreadFactory}

import scala.collection.mutable.{ArrayBuffer, HashMap}

/**
 * An implementation of shuffle using memory served through custom server 
 * where receivers create simultaneous connections to multiple servers by 
 * setting the 'spark.shuffle.maxRxConnections' config option.
 *
 * By controlling the 'spark.shuffle.blockSize' config option one can also 
 * control the largest block size to divide each map output into. Essentially, 
 * instead of creating one large output file for each reducer, maps create
 * multiple smaller files to enable finer level of engagement.
 *
 * 'spark.shuffle.maxTxConnections' enforces server-side cap. Ideally, 
 * maxTxConnections >= maxRxConnections * numReducersPerMachine
 *
 * 'spark.shuffle.TrackerStrategy' decides which strategy to use in the tracker
 *
 * TODO: Add support for compression when spark.compress is set to true.
 */
@serializable
class TrackedCustomBlockedInMemoryShuffle[K, V, C] 
extends Shuffle[K, V, C] with Logging {
  @transient var totalSplits = 0
  @transient var hasSplits = 0
  
  @transient var totalBlocksInSplit: Array[Int] = null
  @transient var hasBlocksInSplit: Array[Int] = null
  
  @transient var hasSplitsBitVector: BitSet = null
  @transient var splitsInRequestBitVector: BitSet = null

  @transient var receivedData: LinkedBlockingQueue[(Int, Array[Byte])] = null  
  @transient var combiners: HashMap[K,C] = null
  
  override def compute(input: RDD[(K, V)],
                       numOutputSplits: Int,
                       createCombiner: V => C,
                       mergeValue: (C, V) => C,
                       mergeCombiners: (C, C) => C)
  : RDD[(K, C)] =
  {
    val sc = input.sparkContext
    val shuffleId = TrackedCustomBlockedInMemoryShuffle.newShuffleId()
    logInfo("Shuffle ID: " + shuffleId)

    val splitRdd = new NumberedSplitRDD(input)
    val numInputSplits = splitRdd.splits.size

    // Run a parallel map and collect to write the intermediate data files,
    // returning a list of inputSplitId -> serverUri pairs
    val outputLocs = splitRdd.map((pair: (Int, Iterator[(K, V)])) => {
      val myIndex = pair._1
      val myIterator = pair._2
      val buckets = Array.tabulate(numOutputSplits)(_ => new HashMap[K, C])
      for ((k, v) <- myIterator) {
        var bucketId = k.hashCode % numOutputSplits
        if (bucketId < 0) { // Fix bucket ID if hash code was negative
          bucketId += numOutputSplits
        }
        val bucket = buckets(bucketId)
        bucket(k) = bucket.get(k) match {
          case Some(c) => mergeValue(c, v)
          case None => createCombiner(v)
        }
      }
      
      // Keep track of number of blocks for each output split
      var numBlocksPerOutputSplit = Array.tabulate(numOutputSplits)(_ => 0)
      
      for (i <- 0 until numOutputSplits) {
        var blockNum = 0
        var isDirty = false

        var splitName = ""
        var baos: ByteArrayOutputStream = null
        var oos: ObjectOutputStream = null
        
        var writeStartTime: Long = 0
        
        buckets(i).foreach(pair => {
          // Open a new stream if necessary
          if (!isDirty) {
            splitName = TrackedCustomBlockedInMemoryShuffle.getSplitName(shuffleId, 
              myIndex, i, blockNum)
              
            baos = new ByteArrayOutputStream
            oos = new ObjectOutputStream(baos)
          
            writeStartTime = System.currentTimeMillis
            logInfo("BEGIN WRITE: " + splitName)
          }
          
          oos.writeObject(pair)
          isDirty = true
          
          // Close the old stream if has crossed the blockSize limit
          if (baos.size > Shuffle.BlockSize) {
            TrackedCustomBlockedInMemoryShuffle.splitsCache(splitName) = 
              baos.toByteArray
          
            logInfo("END WRITE: " + splitName)
            val writeTime = System.currentTimeMillis - writeStartTime
            logInfo("Writing " + splitName + " of size " + baos.size + " bytes took " + writeTime + " millis.")

            blockNum = blockNum + 1
            isDirty = false            
            oos.close()
          }
        })

        if (isDirty) {
          TrackedCustomBlockedInMemoryShuffle.splitsCache(splitName) = baos.toByteArray

          logInfo("END WRITE: " + splitName)
          val writeTime = System.currentTimeMillis - writeStartTime
          logInfo("Writing " + splitName + " of size " + baos.size + " bytes took " + writeTime + " millis.")

          blockNum = blockNum + 1
          oos.close()
        }
        
        // Store BLOCKNUM info
        splitName = TrackedCustomBlockedInMemoryShuffle.getBlockNumOutputName(
          shuffleId, myIndex, i)
        baos = new ByteArrayOutputStream
        oos = new ObjectOutputStream(baos)
        oos.writeObject(blockNum)
        TrackedCustomBlockedInMemoryShuffle.splitsCache(splitName) = baos.toByteArray

        // Close streams
        oos.close()

        // Store number of blocks for this outputSplit
        numBlocksPerOutputSplit(i) = blockNum
      }
      
      var retVal = SplitInfo(TrackedCustomBlockedInMemoryShuffle.serverAddress, 
        TrackedCustomBlockedInMemoryShuffle.serverPort, myIndex)
      retVal.totalBlocksPerOutputSplit = numBlocksPerOutputSplit

      (retVal)
    }).collect()

    // Start tracker
    var shuffleTracker = new ShuffleTracker(outputLocs)
    shuffleTracker.setDaemon(true)
    shuffleTracker.start()
    logInfo("ShuffleTracker started...")

    // Return an RDD that does each of the merges for a given partition
    val indexes = sc.parallelize(0 until numOutputSplits, numOutputSplits)
    return indexes.flatMap((myId: Int) => {
      totalSplits = outputLocs.size
      hasSplits = 0
      
      totalBlocksInSplit = Array.tabulate(totalSplits)(_ => -1)
      hasBlocksInSplit = Array.tabulate(totalSplits)(_ => 0)
      
      hasSplitsBitVector = new BitSet(totalSplits)
      splitsInRequestBitVector = new BitSet(totalSplits)
      
      receivedData = new LinkedBlockingQueue[(Int, Array[Byte])]
      combiners = new HashMap[K, C]
      
      var threadPool = Shuffle.newDaemonFixedThreadPool(
        Shuffle.MaxRxConnections)
        
      while (hasSplits < totalSplits) {
        // Local status of hasSplitsBitVector and splitsInRequestBitVector
        val localSplitInfo = getLocalSplitInfo(myId)

        // DO NOT talk to the tracker if all the required splits are already busy
        val hasOrWillHaveSplits = localSplitInfo.hasSplitsBitVector.cardinality

        var numThreadsToCreate =
          Math.min(totalSplits - hasOrWillHaveSplits, Shuffle.MaxRxConnections) -
          threadPool.getActiveCount
      
        while (hasSplits < totalSplits && numThreadsToCreate > 0) {
          // Receive which split to pull from the tracker
          logInfo("Talking to tracker...")
          val startTime = System.currentTimeMillis
          val splitIndices = getTrackerSelectedSplit(myId)
          logInfo("Got %s from tracker in %d millis".format(splitIndices, System.currentTimeMillis - startTime))
          
          if (splitIndices.size > 0) {
            splitIndices.foreach { splitIndex =>
              val selectedSplitInfo = outputLocs(splitIndex)
              val requestSplit = 
                "%d/%d/%d".format(shuffleId, selectedSplitInfo.splitId, myId)

              threadPool.execute(new ShuffleClient(splitIndex, selectedSplitInfo, 
                requestSplit, myId))
                
              // splitIndex is in transit. Will be unset in the ShuffleClient
              splitsInRequestBitVector.synchronized {
                splitsInRequestBitVector.set(splitIndex)
              }
            }
          } else {
            // Tracker replied back with a NO. Sleep for a while.
            Thread.sleep(Shuffle.MinKnockInterval)
          }
          
          numThreadsToCreate = numThreadsToCreate - 1
        }
        
        // Sleep for a while before creating new threads
        Thread.sleep(Shuffle.MinKnockInterval)
      }

      threadPool.shutdown()

      // Start consumer
      // TODO: Consumption is delayed until everything has been received. 
      // Otherwise it interferes with network performance
      var shuffleConsumer = new ShuffleConsumer(mergeCombiners)
      shuffleConsumer.setDaemon(true)
      shuffleConsumer.start()
      logInfo("ShuffleConsumer started...")

      // Don't return until consumption is finished
      // TODO: Replace with a lock later. 
      while (receivedData.size > 0) {
        Thread.sleep(Shuffle.MinKnockInterval)
      }
      
      combiners
    })
  }
  
  private def getLocalSplitInfo(myId: Int): SplitInfo = {
    var localSplitInfo = SplitInfo(InetAddress.getLocalHost.getHostAddress, 
      SplitInfo.UnusedParam, myId)
    
    // Store hasSplits
    localSplitInfo.hasSplits = hasSplits
    
    // Store hasSplitsBitVector
    hasSplitsBitVector.synchronized {
      localSplitInfo.hasSplitsBitVector = 
        hasSplitsBitVector.clone.asInstanceOf[BitSet]
    }

    // Store hasBlocksInSplit to hasBlocksPerInputSplit
    hasBlocksInSplit.synchronized {
      localSplitInfo.hasBlocksPerInputSplit = 
        hasBlocksInSplit.clone.asInstanceOf[Array[Int]]
    }

    // Include the splitsInRequest as well
    splitsInRequestBitVector.synchronized {
      localSplitInfo.hasSplitsBitVector.or(splitsInRequestBitVector)
    }
    
    return localSplitInfo
  }  

  def selectRandomSplit: Int = {
    var requiredSplits = new ArrayBuffer[Int]
    
    synchronized {
      for (i <- 0 until totalSplits) {
        if (!hasSplitsBitVector.get(i) && !splitsInRequestBitVector.get(i)) {
          requiredSplits += i
        }
      }
    }
    
    if (requiredSplits.size > 0) {
      requiredSplits(TrackedCustomBlockedInMemoryShuffle.ranGen.nextInt(
        requiredSplits.size))
    } else {
      -1
    }
  }
  
  // Talks to the tracker and receives instruction
  private def getTrackerSelectedSplit(myId: Int): ArrayBuffer[Int] = {
    // Local status of hasSplitsBitVector and splitsInRequestBitVector
    val localSplitInfo = getLocalSplitInfo(myId)

    // DO NOT talk to the tracker if all the required splits are already busy
    if (localSplitInfo.hasSplitsBitVector.cardinality == totalSplits) {
      return ArrayBuffer[Int]()
    }

    val clientSocketToTracker = new Socket(Shuffle.MasterHostAddress, 
      Shuffle.MasterTrackerPort)
    val oosTracker =
      new ObjectOutputStream(clientSocketToTracker.getOutputStream)
    oosTracker.flush()
    val oisTracker =
      new ObjectInputStream(clientSocketToTracker.getInputStream)

    var selectedSplitIndices = ArrayBuffer[Int]()

    // Setup the timeout mechanism
    var timeOutTask = new TimerTask {
      override def run: Unit = {
        logInfo("Waited enough for tracker response... Take random response...")
  
        // sockets will be closed in finally
        // TODO: Sometimes timer wont go off
        
        // TODO: Selecting randomly here. Tracker won't know about it and get an
        // asssertion failure when this thread leaves
        
        selectedSplitIndices = ArrayBuffer(selectRandomSplit)
      }
    }
    
    var timeOutTimer = new Timer
    // TODO: Which timeout to use?
    // timeOutTimer.schedule(timeOutTask, Shuffle.MinKnockInterval)

    try {
      // Send intention
      oosTracker.writeObject(Shuffle.ReducerEntering)
      oosTracker.flush()
      
      // Send what this reducer has
      oosTracker.writeObject(localSplitInfo)
      oosTracker.flush()
      
      // Receive reply from the tracker
      selectedSplitIndices = oisTracker.readObject.asInstanceOf[ArrayBuffer[Int]]
      
      // Turn the timer OFF
      timeOutTimer.cancel()
    } catch {
      case e: Exception => {
        logInfo("getTrackerSelectedSplit had a " + e)
      }
    } finally {
      oisTracker.close()
      oosTracker.close()
      clientSocketToTracker.close()
    }
    
    return selectedSplitIndices
  }
  
  class ShuffleTracker(outputLocs: Array[SplitInfo])
  extends Thread with Logging {
    var threadPool = Shuffle.newDaemonCachedThreadPool
    var serverSocket: ServerSocket = null

    // Create trackerStrategy object
    val trackerStrategyClass = System.getProperty(
      "spark.shuffle.trackerStrategy", 
      "spark.BalanceConnectionsShuffleTrackerStrategy")
    
    val trackerStrategy =
      Class.forName(trackerStrategyClass).newInstance().asInstanceOf[ShuffleTrackerStrategy]
      
    // Must initialize here by supplying the outputLocs param
    // TODO: This could be avoided by directly passing it to the constructor
    trackerStrategy.initialize(outputLocs)

    override def run: Unit = {
      serverSocket = new ServerSocket(Shuffle.MasterTrackerPort)
      logInfo("ShuffleTracker" + serverSocket)
      
      try {
        while (true) {
          var clientSocket: Socket = null
          try {
            clientSocket = serverSocket.accept()
          } catch {
            case e: Exception => {
              logInfo("ShuffleTracker had a " + e)
            }
          }

          if (clientSocket != null) {
            try {
              threadPool.execute(new Thread {
                override def run: Unit = {
                  val oos = new ObjectOutputStream(clientSocket.getOutputStream)
                  oos.flush()
                  val ois = new ObjectInputStream(clientSocket.getInputStream)
                  
                  try {
                    // Receive intention
                    val reducerIntention = ois.readObject.asInstanceOf[Int]
                    
                    if (reducerIntention == Shuffle.ReducerEntering) {
                      // Receive what the reducer has
                      val reducerSplitInfo = 
                        ois.readObject.asInstanceOf[SplitInfo]
                      
                      // Select splits and update stats if necessary
                      var selectedSplitIndices = ArrayBuffer[Int]()
                      trackerStrategy.synchronized {
                        selectedSplitIndices = trackerStrategy.selectSplit(
                          reducerSplitInfo)
                      }
                      
                      // Send reply back
                      oos.writeObject(selectedSplitIndices)
                      oos.flush()
                      
                      // Update internal stats, only if receiver got the reply
                      trackerStrategy.synchronized {
                        trackerStrategy.AddReducerToSplit(reducerSplitInfo, 
                          selectedSplitIndices)
                      }
                    }
                    else if (reducerIntention == Shuffle.ReducerLeaving) {
                      val reducerSplitInfo = 
                        ois.readObject.asInstanceOf[SplitInfo]

                      // Receive reception stats: how many blocks the reducer 
                      // read in how much time and from where
                      val receptionStat = 
                        ois.readObject.asInstanceOf[ReceptionStats]
                      
                      // Update stats
                      trackerStrategy.synchronized {
                        trackerStrategy.deleteReducerFrom(reducerSplitInfo, 
                          receptionStat)
                      }
                        
                      // Send ACK
                      oos.writeObject(receptionStat.serverSplitIndex)
                      oos.flush()
                    }
                    else {
                      throw new SparkException("Undefined reducerIntention")
                    }
                  } catch {
                    // EOFException is expected to happen because receiver can 
                    // break connection due to timeout and pick random instead
                    case eofe: java.io.EOFException => { }
                    case e: Exception => {
                      logInfo("ShuffleTracker had a " + e)
                    }
                  } finally {
                    ois.close()
                    oos.close()
                    clientSocket.close()
                  }
                }
              })
            } catch {
              // In failure, close socket here; else, client thread will close
              case ioe: IOException => {
                clientSocket.close()
              }
            }
          }
        }
      } finally {
        serverSocket.close()
      }
      // Shutdown the thread pool
      threadPool.shutdown()
    }  
  }  

  class ShuffleConsumer(mergeCombiners: (C, C) => C)
  extends Thread with Logging {   
    override def run: Unit = {
      // Run until all splits are here
      while (receivedData.size > 0) {
        var splitIndex = -1
        var recvByteArray: Array[Byte] = null
      
        try {
          var tempPair = receivedData.take().asInstanceOf[(Int, Array[Byte])]
          splitIndex = tempPair._1
          recvByteArray = tempPair._2
        } catch {
          case e: Exception => {
            logInfo("Exception during taking data from receivedData")
          }
        }      
      
        val inputStream = 
          new ObjectInputStream(new ByteArrayInputStream(recvByteArray))
          
        try{
          while (true) {
            val (k, c) = inputStream.readObject.asInstanceOf[(K, C)]
            combiners(k) = combiners.get(k) match {
              case Some(oldC) => mergeCombiners(oldC, c)
              case None => c
            }
          }
        } catch {
          case e: EOFException => { }
        }
        inputStream.close()
      }
    }
  }

  class ShuffleClient(splitIndex: Int, serversplitInfo: SplitInfo, 
    requestSplit: String, myId: Int)
  extends Thread with Logging {
    private var peerSocketToSource: Socket = null
    private var oosSource: ObjectOutputStream = null
    private var oisSource: ObjectInputStream = null

    private var receptionSucceeded = false

    // Make sure that multiple messages don't go to the tracker
    private var alreadySentLeavingNotification = false

    // Keep track of bytes received and time spent
    private var numBytesReceived = 0
    private var totalTimeSpent = 0

    override def run: Unit = {
      // Setup the timeout mechanism
      var timeOutTask = new TimerTask {
        override def run: Unit = {
          cleanUp()
        }
      }
      
      var timeOutTimer = new Timer
      timeOutTimer.schedule(timeOutTask, Shuffle.MaxKnockInterval)
      
      try {
        // Everything will break if BLOCKNUM is not correctly received
        // First get BLOCKNUM file if totalBlocksInSplit(splitIndex) is unknown
        peerSocketToSource = new Socket(
          serversplitInfo.hostAddress, serversplitInfo.listenPort)
        oosSource =
          new ObjectOutputStream(peerSocketToSource.getOutputStream)
        oosSource.flush()
        var isSource = peerSocketToSource.getInputStream
        oisSource = new ObjectInputStream(isSource)
        
        // Send path information
        oosSource.writeObject(requestSplit)   
        
        // TODO: Can be optimized. No need to do it everytime.
        // Receive BLOCKNUM
        totalBlocksInSplit(splitIndex) = oisSource.readObject.asInstanceOf[Int]

        // Turn the timer OFF, if the sender responds before timeout
        timeOutTimer.cancel()
        
        while (hasBlocksInSplit(splitIndex) < totalBlocksInSplit(splitIndex)) {
          // Set receptionSucceeded to false before trying for each block
          receptionSucceeded = false

          // Request specific block
          oosSource.writeObject(hasBlocksInSplit(splitIndex))
          
          // Good to go. First, receive the length of the requested file
          var requestedFileLen = oisSource.readObject.asInstanceOf[Int]
          logInfo("Received requestedFileLen = " + requestedFileLen)

          // Create a temp variable to be used in different places
          val requestPath = "http://%s:%d/shuffle/%s-%d".format(
            serversplitInfo.hostAddress, serversplitInfo.listenPort, requestSplit, 
            hasBlocksInSplit(splitIndex))

          // Receive the file
          if (requestedFileLen != -1) {
            val readStartTime = System.currentTimeMillis
            logInfo("BEGIN READ: " + requestPath)

            // Receive data in an Array[Byte]
            var recvByteArray = new Array[Byte](requestedFileLen)
            var alreadyRead = 0
            var bytesRead = 0
            
            while (alreadyRead != requestedFileLen) {
              bytesRead = isSource.read(recvByteArray, alreadyRead, 
                requestedFileLen - alreadyRead)
              if (bytesRead > 0) {
                alreadyRead  = alreadyRead + bytesRead
              }
            } 
            
            // Make it available to the consumer
            try {
              receivedData.put((splitIndex, recvByteArray))
            } catch {
              case e: Exception => {
                logInfo("Exception during putting data into receivedData")
              }
            }
                    
            // TODO: Updating stats before consumption is completed
            hasBlocksInSplit(splitIndex) = hasBlocksInSplit(splitIndex) + 1
            
            // Split has been received only if all the blocks have been received
            if (hasBlocksInSplit(splitIndex) == totalBlocksInSplit(splitIndex)) {
              hasSplitsBitVector.synchronized {
                hasSplitsBitVector.set(splitIndex)
              }
              hasSplits += 1
            }

            receptionSucceeded = true

            logInfo("END READ: " + requestPath)
            val readTime = System.currentTimeMillis - readStartTime
            logInfo("Reading " + requestPath + " took " + readTime + " millis.")
            
            // Update stats
            numBytesReceived = numBytesReceived + requestedFileLen
            totalTimeSpent = totalTimeSpent + readTime.toInt
          } else {
              throw new SparkException("ShuffleServer " + serversplitInfo.hostAddress + " does not have " + requestSplit)
          }
        }
      } catch {
        // EOFException is expected to happen because sender can break
        // connection due to timeout
        case eofe: java.io.EOFException => { }
        case e: Exception => {
          logInfo("ShuffleClient had a " + e)
        }
      } finally {
        splitsInRequestBitVector.synchronized {
          splitsInRequestBitVector.set(splitIndex, false)
        }
        cleanUp()
      }
    }
    
    // Connect to the tracker and update its stats
    private def sendLeavingNotification(): Unit = synchronized {
      if (!alreadySentLeavingNotification) {
        val clientSocketToTracker = new Socket(Shuffle.MasterHostAddress, 
          Shuffle.MasterTrackerPort)
        val oosTracker =
          new ObjectOutputStream(clientSocketToTracker.getOutputStream)
        oosTracker.flush()
        val oisTracker =
          new ObjectInputStream(clientSocketToTracker.getInputStream)

        try {
          // Send intention
          oosTracker.writeObject(Shuffle.ReducerLeaving)
          oosTracker.flush()
          
          // Send reducerSplitInfo
          oosTracker.writeObject(getLocalSplitInfo(myId))
          oosTracker.flush()
          
          // Send reception stats
          oosTracker.writeObject(ReceptionStats(
            numBytesReceived, totalTimeSpent, splitIndex))
          oosTracker.flush()
          
          // Receive ACK. No need to do anything with that
          oisTracker.readObject.asInstanceOf[Int]
 
          // Now update sentLeavingNotifacation
          alreadySentLeavingNotification = true
        } catch {
          case e: Exception => {
            logInfo("sendLeavingNotification had a " + e)
          }
        } finally {
          oisTracker.close()
          oosTracker.close()
          clientSocketToTracker.close()
        }
      }
    }
    
    private def cleanUp(): Unit = {
      // Update tracker stats first
      sendLeavingNotification()
    
      // Clean up the connections to the mapper
      if (oisSource != null) {
        oisSource.close()
      }
      if (oosSource != null) {
        oosSource.close()
      }
      if (peerSocketToSource != null) {
        peerSocketToSource.close()
      }
      
      logInfo("Leaving client")
    }
  }     
}

object TrackedCustomBlockedInMemoryShuffle extends Logging {
  // Cache for keeping the splits around
  val splitsCache = new HashMap[String, Array[Byte]]

  private var initialized = false
  private var nextShuffleId = new AtomicLong(0)

  // Variables initialized by initializeIfNeeded()
  private var shuffleDir: File = null

  private var shuffleServer: ShuffleServer = null
  private var serverAddress = InetAddress.getLocalHost.getHostAddress
  private var serverPort: Int = -1
  
  // Random number generator
  var ranGen = new Random
  
  private def initializeIfNeeded() = synchronized {
    if (!initialized) {
      // TODO: localDir should be created by some mechanism common to Spark
      // so that it can be shared among shuffle, broadcast, etc
      val localDirRoot = System.getProperty("spark.local.dir", "/tmp")
      var tries = 0
      var foundLocalDir = false
      var localDir: File = null
      var localDirUuid: UUID = null
      while (!foundLocalDir && tries < 10) {
        tries += 1
        try {
          localDirUuid = UUID.randomUUID
          localDir = new File(localDirRoot, "spark-local-" + localDirUuid)
          if (!localDir.exists) {
            localDir.mkdirs()
            foundLocalDir = true
          }
        } catch {
          case e: Exception =>
            logWarning("Attempt " + tries + " to create local dir failed", e)
        }
      }
      if (!foundLocalDir) {
        logError("Failed 10 attempts to create local dir in " + localDirRoot)
        System.exit(1)
      }
      shuffleDir = new File(localDir, "shuffle")
      shuffleDir.mkdirs()
      logInfo("Shuffle dir: " + shuffleDir)
      
      // Create and start the shuffleServer      
      shuffleServer = new ShuffleServer
      shuffleServer.setDaemon(true)
      shuffleServer.start()
      logInfo("ShuffleServer started...")

      initialized = true
    }
  }
  
  def getSplitName(shuffleId: Long, inputId: Int, outputId: Int, 
    blockId: Int): String = {
    initializeIfNeeded()
    // Adding shuffleDir is unnecessary. Added to keep the parsers working
    return "%s/%d/%d/%d-%d".format(shuffleDir, shuffleId, inputId, outputId, 
      blockId)
  }

  def getBlockNumOutputName(shuffleId: Long, inputId: Int, 
    outputId: Int): String = {
    initializeIfNeeded()
    return "%s/%d/%d/%d-BLOCKNUM".format(shuffleDir, shuffleId, inputId, 
      outputId)
  }

  def newShuffleId(): Long = {
    nextShuffleId.getAndIncrement()
  }
  
  class ShuffleServer
  extends Thread with Logging {
    var threadPool = Shuffle.newDaemonFixedThreadPool(Shuffle.MaxTxConnections)

    var serverSocket: ServerSocket = null

    override def run: Unit = {
      serverSocket = new ServerSocket(0)
      serverPort = serverSocket.getLocalPort

      logInfo("ShuffleServer started with " + serverSocket)
      logInfo("Local URI: http://" + serverAddress + ":" + serverPort)

      try {
        while (true) {
          var clientSocket: Socket = null
          try {
            clientSocket = serverSocket.accept()
          } catch {
            case e: Exception => { }
          }
          if (clientSocket != null) {
            logInfo("Serve: Accepted new client connection:" + clientSocket)
            try {
              threadPool.execute(new ShuffleServerThread(clientSocket))
            } catch {
              // In failure, close socket here; else, the thread will close it
              case ioe: IOException => {
                clientSocket.close()
              }
            }
          }
        }
      } finally {
        if (serverSocket != null) {
          logInfo("ShuffleServer now stopping...")
          serverSocket.close()
        }
      }
      // Shutdown the thread pool
      threadPool.shutdown()
    }
    
    class ShuffleServerThread(val clientSocket: Socket)
    extends Thread with Logging {
      private val os = clientSocket.getOutputStream.asInstanceOf[OutputStream]
      os.flush()
      private val bos = new BufferedOutputStream(os)
      bos.flush()
      private val oos = new ObjectOutputStream(os)
      oos.flush()
      private val ois = new ObjectInputStream(clientSocket.getInputStream)

      logInfo("new ShuffleServerThread is running")
      
      override def run: Unit = {
        try {
          // Receive basic path information
          var requestedSplitBase = ois.readObject.asInstanceOf[String]
          
          logInfo("requestedSplitBase: " + requestedSplitBase)
          
          // Read BLOCKNUM and send back the total number of blocks
          val blockNumName = "%s/%s-BLOCKNUM".format(shuffleDir, 
            requestedSplitBase)
            
          val blockNumIn = new ObjectInputStream(new ByteArrayInputStream(
            TrackedCustomBlockedInMemoryShuffle.splitsCache(blockNumName)))
          val BLOCKNUM = blockNumIn.readObject.asInstanceOf[Int]
          blockNumIn.close()
          
          oos.writeObject(BLOCKNUM)
          oos.flush()
          
          val startTime = System.currentTimeMillis
          var curTime = startTime
          var keepSending = true
          var numBlocksToSend = Shuffle.MaxChatBlocks
          
          while (keepSending && numBlocksToSend > 0) {
            // Receive specific block request
            val blockId = ois.readObject.asInstanceOf[Int]
            
            // Ready to send
            var requestedSplit = shuffleDir + "/" + requestedSplitBase + "-" + blockId
            
            // Send the length of the requestedSplit to let the receiver know that 
            // transfer is about to start
            // In the case of receiver timeout and connection close, this will
            // throw a java.net.SocketException: Broken pipe
            var requestedSplitLen = -1
            
            try {
              requestedSplitLen =
                TrackedCustomBlockedInMemoryShuffle.splitsCache(requestedSplit).length
            } catch {
              case e: Exception => { }
            }

            oos.writeObject(requestedSplitLen)
            oos.flush()
            
            logInfo("requestedSplitLen = " + requestedSplitLen)

            // Read and send the requested file
            if (requestedSplitLen != -1) {
              // Send
              bos.write(TrackedCustomBlockedInMemoryShuffle.splitsCache(requestedSplit),
                0, requestedSplitLen)
              bos.flush()

              // Update loop variables
              numBlocksToSend = numBlocksToSend - 1
              
              curTime = System.currentTimeMillis
              // Revoke sending only if there is anyone waiting in the queue
              // TODO: Turning OFF the optimization so that reducers go back to
              // tracker get advice
              if (curTime - startTime >= Shuffle.MaxChatTime /* &&
                  threadPool.getQueue.size > 0 */) {
                keepSending = false
              }
            } else {
              // Close the connection
            }
          }
        } catch {
          // If something went wrong, e.g., the worker at the other end died etc
          // then close everything up
          // Exception can happen if the receiver stops receiving
          // EOFException is expected to happen because receiver can break
          // connection as soon as it has all the blocks
          case eofe: java.io.EOFException => { }
          case e: Exception => {
            logInfo("ShuffleServerThread had a " + e)
          }
        } finally {
          logInfo("ShuffleServerThread is closing streams and sockets")
          ois.close()
          // TODO: Following can cause "java.net.SocketException: Socket closed"
          oos.close()
          bos.close()
          clientSocket.close()
        }
      }
    }
  }  
}
