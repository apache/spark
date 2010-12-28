package spark

import java.io._
import java.net._
import java.util.{BitSet, Random, Timer, TimerTask, UUID}
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.{LinkedBlockingQueue, Executors, ThreadPoolExecutor, ThreadFactory}

import scala.collection.mutable.{ArrayBuffer, HashMap}

/**
 * An implementation of shuffle using local files served through custom server 
 * where receivers create simultaneous connections to multiple servers by 
 * setting the 'spark.shuffle.maxRxConnections' config option.
 *
 * 'spark.shuffle.maxTxConnections' enforces server-side cap. Ideally,
 * maxTxConnections >= maxRxConnections * numReducersPerMachine
 *
 * 'spark.shuffle.TrackerStrategy' decides which strategy to use
 *
 * TODO: Add support for compression when spark.compress is set to true.
 */
@serializable
class TrackedCustomParallelLocalFileShuffle[K, V, C] 
extends Shuffle[K, V, C] with Logging {
  @transient var totalSplits = 0
  @transient var hasSplits = 0 
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
    val shuffleId = TrackedCustomParallelLocalFileShuffle.newShuffleId()
    logInfo("Shuffle ID: " + shuffleId)

    val splitRdd = new NumberedSplitRDD(input)
    val numInputSplits = splitRdd.splits.size

    // Run a parallel map and collect to write the intermediate data files
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
      
      for (i <- 0 until numOutputSplits) {
        val file = TrackedCustomParallelLocalFileShuffle.getOutputFile(shuffleId, 
          myIndex, i)
        val writeStartTime = System.currentTimeMillis
        logInfo("BEGIN WRITE: " + file)
        val out = new ObjectOutputStream(new FileOutputStream(file))
        buckets(i).foreach(pair => out.writeObject(pair))
        out.close()
        logInfo("END WRITE: " + file)
        val writeTime = System.currentTimeMillis - writeStartTime
        logInfo("Writing " + file + " of size " + file.length + " bytes took " + writeTime + " millis.")
      }
      
      (SplitInfo (TrackedCustomParallelLocalFileShuffle.serverAddress, 
        TrackedCustomParallelLocalFileShuffle.serverPort, myIndex))
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
      hasSplitsBitVector = new BitSet(totalSplits)
      splitsInRequestBitVector = new BitSet(totalSplits)

      receivedData = new LinkedBlockingQueue[(Int, Array[Byte])]
      combiners = new HashMap[K, C]
      
      var threadPool = 
        Shuffle.newDaemonFixedThreadPool(Shuffle.MaxRxConnections)
        
      // Start consumer
      var shuffleConsumer = new ShuffleConsumer(mergeCombiners)
      shuffleConsumer.setDaemon(true)
      shuffleConsumer.start()
      logInfo("ShuffleConsumer started...")
        
      while (hasSplits < totalSplits) {
        var numThreadsToCreate = 
          Math.min(totalSplits, Shuffle.MaxRxConnections) - 
          threadPool.getActiveCount
      
        while (hasSplits < totalSplits && numThreadsToCreate > 0) {
          // Receive which split to pull from the tracker
          val splitIndex = getTrackerSelectedSplit(outputLocs)
          
          if (splitIndex != -1) {
            val selectedSplitInfo = outputLocs(splitIndex)
            val requestSplit = 
              "%d/%d/%d".format(shuffleId, selectedSplitInfo.inputId, myId)

            threadPool.execute(new ShuffleClient(splitIndex, selectedSplitInfo, 
              requestSplit))
              
            // splitIndex is in transit. Will be unset in the ShuffleClient
            splitsInRequestBitVector.synchronized {
              splitsInRequestBitVector.set(splitIndex)
            }
          }
          
          numThreadsToCreate = numThreadsToCreate - 1
        }
        
        // Sleep for a while before creating new threads
        Thread.sleep(Shuffle.MinKnockInterval)
      }
      
      threadPool.shutdown()
      combiners
    })
  }
  
  private def getLocalSplitInfo: SplitInfo = {
    var localSplitInfo = SplitInfo(InetAddress.getLocalHost.getHostAddress, 
      SplitInfo.UnusedParam, SplitInfo.UnusedParam)
      
    localSplitInfo.hasSplits = hasSplits
    
    hasSplitsBitVector.synchronized {
      localSplitInfo.hasSplitsBitVector = hasSplitsBitVector
    }

    // Include the splitsInRequest as well
    splitsInRequestBitVector.synchronized {
      localSplitInfo.hasSplitsBitVector.or(splitsInRequestBitVector)
    }
    
    return localSplitInfo
  }  
  
  // Selects a random split using local information
  private def selectRandomSplit: Int = {
    var requiredSplits = new ArrayBuffer[Int]
    
    synchronized {
      for (i <- 0 until totalSplits) {
        if (!hasSplitsBitVector.get(i) && !splitsInRequestBitVector.get(i)) {
          requiredSplits += i
        }
      }
    }
    
    if (requiredSplits.size > 0) {
      requiredSplits(TrackedCustomParallelLocalFileShuffle.ranGen.nextInt(
        requiredSplits.size))
    } else {
      -1
    }
  }
  
  // Talks to the tracker and receives instruction
  private def getTrackerSelectedSplit(outputLocs: Array[SplitInfo]): Int = {
    val clientSocketToTracker = new Socket(Shuffle.MasterHostAddress, 
      Shuffle.MasterTrackerPort)
    val oosTracker =
      new ObjectOutputStream(clientSocketToTracker.getOutputStream)
    oosTracker.flush()
    val oisTracker =
      new ObjectInputStream(clientSocketToTracker.getInputStream)

    var selectedSplitIndex = -1

    try {
      // Send intention
      oosTracker.writeObject(
        TrackedCustomParallelLocalFileShuffle.ReducerEntering)
      oosTracker.flush()
      
      // Send what this reducer has
      oosTracker.writeObject(getLocalSplitInfo)      
      oosTracker.flush()
      
      // Receive reply from the tracker
      selectedSplitIndex = oisTracker.readObject.asInstanceOf[Int]
    } catch {
      case e: Exception => {
        logInfo("getTrackerSelectedSplit had a " + e)
      }
    } finally {
      oisTracker.close()
      oosTracker.close()
      clientSocketToTracker.close()
    }
    
    return selectedSplitIndex
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
                    
                    if (reducerIntention == 
                      TrackedCustomParallelLocalFileShuffle.ReducerEntering) {
                      // Receive what the reducer has
                      val reducerSplitInfo = 
                        ois.readObject.asInstanceOf[SplitInfo]                      
                      
                      // Select split and update stats if necessary
                      val selectedSplitIndex = 
                        trackerStrategy.selectSplitAndAddReducer(
                          reducerSplitInfo)
                      
                      // Send reply back
                      oos.writeObject(selectedSplitIndex)
                      oos.flush()
                    }
                    else if (reducerIntention == 
                      TrackedCustomParallelLocalFileShuffle.ReducerLeaving) {
                      // Receive reducerSplitInfo and serverSplitIndex
                      val reducerSplitInfo = 
                        ois.readObject.asInstanceOf[SplitInfo]                      
                      val serverSplitIndex = ois.readObject.asInstanceOf[Int]
                      
                      // Update stats
                      trackerStrategy.deleteReducerFrom(reducerSplitInfo, 
                        serverSplitIndex)
                        
                      // Send ACK
                      oos.writeObject(serverSplitIndex)
                      oos.flush()
                    }
                    else {
                      throw new SparkException("Undefined reducerIntention")
                    }
                  } catch {
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
      while (hasSplits < totalSplits) {
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
        
        // Consumption completed. Update stats.
        hasSplitsBitVector.synchronized {
          hasSplitsBitVector.set(splitIndex)
        }
        hasSplits += 1

        // We have received splitIndex
        splitsInRequestBitVector.synchronized {
          splitsInRequestBitVector.set(splitIndex, false)
        }        
      }
    }
  }
  
  class ShuffleClient(splitIndex: Int, serversplitInfo: SplitInfo, 
    requestSplit: String)
  extends Thread with Logging {
    private var peerSocketToSource: Socket = null
    private var oosSource: ObjectOutputStream = null
    private var oisSource: ObjectInputStream = null
    
    private var receptionSucceeded = false
    
    // Make sure that multiple messages don't go to the tracker
    private var alreadySentLeavingNotification = false

    override def run: Unit = {
      // Setup the timeout mechanism
      var timeOutTask = new TimerTask {
        override def run: Unit = {
          cleanUp()
        }
      }
      
      var timeOutTimer = new Timer
      timeOutTimer.schedule(timeOutTask, Shuffle.MaxKnockInterval)
        
      // Create a temp variable to be used in different places
      val requestPath = "http://%s:%d/shuffle/%s".format(
        serversplitInfo.hostAddress, serversplitInfo.listenPort, requestSplit)      

      logInfo("ShuffleClient started... => " + requestPath)
      
      try {
        // Connect to the source
        peerSocketToSource = 
          new Socket(serversplitInfo.hostAddress, serversplitInfo.listenPort)
        oosSource =
          new ObjectOutputStream(peerSocketToSource.getOutputStream)
        oosSource.flush()
        var isSource = peerSocketToSource.getInputStream
        oisSource = new ObjectInputStream(isSource)
        
        // Send the request
        oosSource.writeObject(requestSplit)
        oosSource.flush()
        
        // Receive the length of the requested file
        var requestedFileLen = oisSource.readObject.asInstanceOf[Int]
        logInfo("Received requestedFileLen = " + requestedFileLen)

        // Turn the timer OFF, if the sender responds before timeout
        timeOutTimer.cancel()
        
        // Receive the file
        if (requestedFileLen != -1) {
          val readStartTime = System.currentTimeMillis
          logInfo("BEGIN READ: " + requestPath)

          // Receive data in an Array[Byte]
          var recvByteArray = new Array[Byte](requestedFileLen)
          var alreadyRead = 0
          var bytesRead = 0
          
          while (alreadyRead < requestedFileLen) {
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
          
          // NOTE: Update of bitVectors are now done by the consumer
          
          receptionSucceeded = true

          logInfo("END READ: " + requestPath)
          val readTime = System.currentTimeMillis - readStartTime
          logInfo("Reading " + requestPath + " took " + readTime + " millis.")
        } else {
          throw new SparkException("ShuffleServer " + serversplitInfo.hostAddress + " does not have " + requestSplit)
        }
      } catch {
        // EOFException is expected to happen because sender can break
        // connection due to timeout
        case eofe: java.io.EOFException => { }
        case e: Exception => {
          logInfo("ShuffleClient had a " + e)
        }
      } finally {
        // If reception failed, unset for future retry
        if (!receptionSucceeded) {
          splitsInRequestBitVector.synchronized {
            splitsInRequestBitVector.set(splitIndex, false)
          }
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
          oosTracker.writeObject(
            TrackedCustomParallelLocalFileShuffle.ReducerLeaving)
          oosTracker.flush()
          
          // Send reducerSplitInfo
          oosTracker.writeObject(getLocalSplitInfo)
          oosTracker.flush()
          
          // Send serverSplitInfo so that tracker can update its stats
          oosTracker.writeObject(splitIndex)
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
    }
  }  
}

object TrackedCustomParallelLocalFileShuffle extends Logging {
  // Tracker communication constants
  val ReducerEntering = 0
  val ReducerLeaving = 1
  
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
  
  def getOutputFile(shuffleId: Long, inputId: Int, outputId: Int): File = {
    initializeIfNeeded()
    val dir = new File(shuffleDir, shuffleId + "/" + inputId)
    dir.mkdirs()
    val file = new File(dir, "" + outputId)
    return file
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
          // Receive requestPath from the receiver
          var requestPath = ois.readObject.asInstanceOf[String]
          logInfo("requestPath: " + shuffleDir + "/" + requestPath)
          
          // Open the file
          var requestedFile: File = null
          var requestedFileLen = -1
          try {
            requestedFile = new File(shuffleDir + "/" + requestPath)
            requestedFileLen = requestedFile.length.toInt
          } catch {
            case e: Exception => { }
          }
          
          // Send the length of the requestPath to let the receiver know that 
          // transfer is about to start
          // In the case of receiver timeout and connection close, this will
          // throw a java.net.SocketException: Broken pipe
          oos.writeObject(requestedFileLen)
          oos.flush()
          
          logInfo("requestedFileLen = " + requestedFileLen)

          // Read and send the requested file
          if (requestedFileLen != -1) {
            // Read
            var byteArray = new Array[Byte](requestedFileLen)
            val bis = 
              new BufferedInputStream(new FileInputStream(requestedFile))

            var bytesRead = bis.read(byteArray, 0, byteArray.length)
            var alreadyRead = bytesRead

            while (alreadyRead < requestedFileLen) {
              bytesRead = bis.read(byteArray, alreadyRead,
                (byteArray.length - alreadyRead))
              if(bytesRead > 0) {
                alreadyRead = alreadyRead + bytesRead
              }
            }            
            bis.close()
            
            // Send
            bos.write(byteArray, 0, byteArray.length)
            bos.flush()
          } else {
            // Close the connection
          }
        } catch {
          // If something went wrong, e.g., the worker at the other end died etc
          // then close everything up
          // Exception can happen if the receiver stops receiving
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
