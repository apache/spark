package spark

import java.io._
import java.net._
import java.util.{BitSet, Random, Timer, TimerTask, UUID}
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.{LinkedBlockingQueue, Executors, ThreadPoolExecutor, ThreadFactory}

import scala.collection.mutable.{ArrayBuffer, HashMap}

/**
 * TODO: THIS IS AN ABSOLUTELY EXPERIMENTAL IMPLEMENTATON (FOR NOW). 
 * 
 * An implementation of shuffle using local memory served through custom server 
 * where receivers create simultaneous connections to multiple servers by 
 * setting the 'spark.shuffle.maxRxConnections' config option.
 *
 * By controlling the 'spark.shuffle.blockSize' config option one can also 
 * control the largest block size to divide each map output into. Essentially, 
 * instead of creating one large output file for each reducer, maps create 
 * multiple smaller files to enable finer level of engagement.
 *
 * 'spark.shuffle.maxTxConnections' enforces server-side cap.  Ideally,
 * maxTxConnections >= maxRxConnections * numReducersPerMachine
 *
 * TODO: Add support for compression when spark.compress is set to true.
 */
@serializable
class CustomBlockedInMemoryShuffle[K, V, C] 
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
    val shuffleId = CustomBlockedInMemoryShuffle.newShuffleId()
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
      
      for (i <- 0 until numOutputSplits) {
        var blockNum = 0
        var isDirty = false

        var splitName = ""
        var baos: ByteArrayOutputStream = null
        var oos: ObjectOutputStream = null
        
        var writeStartTime: Long = 0
        
        buckets(i).foreach(pair => {
          // Open a new file if necessary
          if (!isDirty) {
            splitName = CustomBlockedInMemoryShuffle.getSplitName(shuffleId, 
              myIndex, i, blockNum)
              
            baos = new ByteArrayOutputStream
            oos = new ObjectOutputStream(baos)
          
            writeStartTime = System.currentTimeMillis
            logInfo("BEGIN WRITE: " + splitName)
          }
          
          oos.writeObject(pair)
          isDirty = true
          
          // Close the old file if has crossed the blockSize limit
          if (baos.size > CustomBlockedInMemoryShuffle.BlockSize) {
            CustomBlockedInMemoryShuffle.splitsCache(splitName) = 
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
          CustomBlockedInMemoryShuffle.splitsCache(splitName) = baos.toByteArray

          logInfo("END WRITE: " + splitName)
          val writeTime = System.currentTimeMillis - writeStartTime
          logInfo("Writing " + splitName + " of size " + baos.size + " bytes took " + writeTime + " millis.")

          blockNum = blockNum + 1
          oos.close()
        }
        
        // Store BLOCKNUM info
        splitName = CustomBlockedInMemoryShuffle.getBlockNumOutputName(
          shuffleId, myIndex, i)
        baos = new ByteArrayOutputStream
        oos = new ObjectOutputStream(baos)
        oos.writeObject(blockNum)
        CustomBlockedInMemoryShuffle.splitsCache(splitName) = baos.toByteArray

        // Close streams        
        oos.close()
      }
      
      (myIndex, CustomBlockedInMemoryShuffle.serverAddress, 
        CustomBlockedInMemoryShuffle.serverPort)
    }).collect()

    val splitsByUri = new ArrayBuffer[(String, Int, Int)]
    for ((inputId, serverAddress, serverPort) <- outputLocs) {
      splitsByUri += ((serverAddress, serverPort, inputId))
    }

    // TODO: Could broadcast outputLocs

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
      
      // Start consumer
      var shuffleConsumer = new ShuffleConsumer(mergeCombiners)
      shuffleConsumer.setDaemon(true)
      shuffleConsumer.start()
      logInfo("ShuffleConsumer started...")

      var threadPool = CustomBlockedInMemoryShuffle.newDaemonFixedThreadPool(
        CustomBlockedInMemoryShuffle.MaxRxConnections)
        
      while (hasSplits < totalSplits) {
        var numThreadsToCreate =
          Math.min(totalSplits, CustomBlockedInMemoryShuffle.MaxRxConnections) -
          threadPool.getActiveCount
      
        while (hasSplits < totalSplits && numThreadsToCreate > 0) {
          // Select a random split to pull
          val splitIndex = selectRandomSplit
          
          if (splitIndex != -1) {
            val (serverAddress, serverPort, inputId) = splitsByUri(splitIndex)

            threadPool.execute(new ShuffleClient(serverAddress, serverPort, 
              shuffleId.toInt, inputId, myId, splitIndex))
              
            // splitIndex is in transit. Will be unset in the ShuffleClient
            splitsInRequestBitVector.synchronized {
              splitsInRequestBitVector.set(splitIndex)
            }
          }
          
          numThreadsToCreate = numThreadsToCreate - 1
        }
        
        // Sleep for a while before creating new threads
        Thread.sleep(CustomBlockedInMemoryShuffle.MinKnockInterval)
      }

      threadPool.shutdown()
      combiners
    })
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
      requiredSplits(CustomBlockedInMemoryShuffle.ranGen.nextInt(
        requiredSplits.size))
    } else {
      -1
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
      }
    }
  }

  class ShuffleClient(hostAddress: String, listenPort: Int, shuffleId: Int, 
    inputId: Int, myId: Int, splitIndex: Int)
  extends Thread with Logging {
    private var peerSocketToSource: Socket = null
    private var oosSource: ObjectOutputStream = null
    private var oisSource: ObjectInputStream = null

    private var receptionSucceeded = false

    override def run: Unit = {
      // Setup the timeout mechanism
      var timeOutTask = new TimerTask {
        override def run: Unit = {
          cleanUpConnections()
        }
      }
      
      var timeOutTimer = new Timer
      timeOutTimer.schedule(timeOutTask, 
        CustomBlockedLocalFileShuffle.MaxKnockInterval)
      
      try {
        // Everything will break if BLOCKNUM is not correctly received
        // First get BLOCKNUM file if totalBlocksInSplit(splitIndex) is unknown
        peerSocketToSource = new Socket(hostAddress, listenPort)
        oosSource =
          new ObjectOutputStream(peerSocketToSource.getOutputStream)
        oosSource.flush()
        var isSource = peerSocketToSource.getInputStream
        oisSource = new ObjectInputStream(isSource)
        
        // Send path information
        oosSource.writeObject((shuffleId, inputId, myId))   
        
        // TODO: Can be optimized. No need to do it everytime.
        // Receive BLOCKNUM
        totalBlocksInSplit(splitIndex) = oisSource.readObject.asInstanceOf[Int]

        // Turn the timer OFF, if the sender responds before timeout
        timeOutTimer.cancel()

        // Request specific block
        oosSource.writeObject(hasBlocksInSplit(splitIndex))
        
        // Good to go. First, receive the length of the requested file
        var requestedFileLen = oisSource.readObject.asInstanceOf[Int]
        logInfo("Received requestedFileLen = " + requestedFileLen)

        val requestSplit = "%d/%d/%d-%d".format(shuffleId, inputId, myId, 
          hasBlocksInSplit(splitIndex))
        
        // Receive the file
        if (requestedFileLen != -1) {
          val readStartTime = System.currentTimeMillis
          logInfo("BEGIN READ: http://%s:%d/shuffle/%s".format(hostAddress, listenPort, requestSplit))

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

          // We have received splitIndex
          splitsInRequestBitVector.synchronized {
            splitsInRequestBitVector.set(splitIndex, false)
          }

          receptionSucceeded = true

          logInfo("END READ: http://%s:%d/shuffle/%s".format(hostAddress, listenPort, requestSplit))
          val readTime = System.currentTimeMillis - readStartTime
          logInfo("Reading http://%s:%d/shuffle/%s".format(hostAddress, listenPort, requestSplit) + " took " + readTime + " millis.")
        } else {
            throw new SparkException("ShuffleServer " + hostAddress + " does not have " + requestSplit)
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
        cleanUpConnections()       
      }
    }
    
    private def cleanUpConnections(): Unit = {
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

object CustomBlockedInMemoryShuffle extends Logging {
  // Cache for keeping the splits around
  val splitsCache = new HashMap[String, Array[Byte]]

  // Used thoughout the code for small and large waits/timeouts
  private var BlockSize_ = 1024 * 1024
  
  private var MinKnockInterval_ = 1000
  private var MaxKnockInterval_ = 5000
  
  // Maximum number of connections
  private var MaxRxConnections_ = 4
  private var MaxTxConnections_ = 8
  
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
      // Load config parameters
      BlockSize_ = System.getProperty(
        "spark.shuffle.blockSize", "1024").toInt * 1024
      
      MinKnockInterval_ = System.getProperty(
        "spark.shuffle.minKnockInterval", "1000").toInt
      MaxKnockInterval_ = System.getProperty(
        "spark.shuffle.maxKnockInterval", "5000").toInt

      MaxRxConnections_ = System.getProperty(
        "spark.shuffle.maxRxConnections", "4").toInt
      MaxTxConnections_ = System.getProperty(
        "spark.shuffle.maxTxConnections", "8").toInt
      
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
  
  def BlockSize = BlockSize_
  
  def MinKnockInterval = MinKnockInterval_
  def MaxKnockInterval = MaxKnockInterval_
  
  def MaxRxConnections = MaxRxConnections_
  def MaxTxConnections = MaxTxConnections_
  
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
    return "%s/%d/%d/BLOCKNUM-%d".format(shuffleDir, shuffleId, inputId, 
      outputId)
  }

  def newShuffleId(): Long = {
    nextShuffleId.getAndIncrement()
  }
  
  // Returns a standard ThreadFactory except all threads are daemons
  private def newDaemonThreadFactory: ThreadFactory = {
    new ThreadFactory {
      def newThread(r: Runnable): Thread = {
        var t = Executors.defaultThreadFactory.newThread(r)
        t.setDaemon(true)
        return t
      }
    }
  }

  // Wrapper over newFixedThreadPool
  def newDaemonFixedThreadPool(nThreads: Int): ThreadPoolExecutor = {
    var threadPool =
      Executors.newFixedThreadPool(nThreads).asInstanceOf[ThreadPoolExecutor]

    threadPool.setThreadFactory(newDaemonThreadFactory)
    
    return threadPool
  }
  
  class ShuffleServer
  extends Thread with Logging {
    var threadPool = 
      newDaemonFixedThreadPool(CustomBlockedInMemoryShuffle.MaxTxConnections)

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
          val (shuffleId, myIndex, outputId) = 
            ois.readObject.asInstanceOf[(Int, Int, Int)]
            
          var requestedSplit = 
            "%s/%d/%d/%d".format(shuffleDir, shuffleId, myIndex, outputId)
          logInfo("requestedSplit: " + requestedSplit)
          
          // Read BLOCKNUM and send back the total number of blocks
          val blockNumName = "%s/%d/%d/BLOCKNUM-%d".format(shuffleDir, 
            shuffleId, myIndex, outputId)
            
          val blockNumIn = new ObjectInputStream(new ByteArrayInputStream(
            CustomBlockedInMemoryShuffle.splitsCache(blockNumName)))
          val BLOCKNUM = blockNumIn.readObject.asInstanceOf[Int]
          blockNumIn.close()
          
          oos.writeObject(BLOCKNUM)
          
          // Receive specific block request
          val blockId = ois.readObject.asInstanceOf[Int]
          
          // Ready to send
          requestedSplit = requestedSplit + "-" + blockId
          
          // Send the length of the requestedSplit to let the receiver know that 
          // transfer is about to start
          // In the case of receiver timeout and connection close, this will
          // throw a java.net.SocketException: Broken pipe
          var requestedSplitLen = -1
          
          try {
            requestedSplitLen =
              CustomBlockedInMemoryShuffle.splitsCache(requestedSplit).length
          } catch {
            case e: Exception => { }
          }

          oos.writeObject(requestedSplitLen)
          oos.flush()
          
          logInfo("requestedSplitLen = " + requestedSplitLen)

          // Read and send the requested file
          if (requestedSplitLen != -1) {
            // Send
            bos.write(CustomBlockedInMemoryShuffle.splitsCache(requestedSplit),
              0, requestedSplitLen)
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
