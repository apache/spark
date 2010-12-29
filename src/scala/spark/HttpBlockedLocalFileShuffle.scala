package spark

import java.io._
import java.net._
import java.util.{BitSet, Random, Timer, TimerTask, UUID}
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.{LinkedBlockingQueue, Executors, ThreadPoolExecutor, ThreadFactory}

import scala.collection.mutable.{ArrayBuffer, HashMap}

/**
 * An implementation of shuffle using local files served through HTTP where 
 * receivers create simultaneous connections to multiple servers by setting the
 * 'spark.shuffle.maxRxConnections' config option.
 *
 * By controlling the 'spark.shuffle.blockSize' config option one can also 
 * control the largest block size to retrieve by each reducers. An INDEX file
 * keeps track of block boundaries instead of creating many smaller files. 
 *
 * TODO: Add support for compression when spark.compress is set to true.
 */
@serializable
class HttpBlockedLocalFileShuffle[K, V, C] 
extends Shuffle[K, V, C] with Logging {
  @transient var totalSplits = 0
  @transient var hasSplits = 0
  
  @transient var blocksInSplit: Array[ArrayBuffer[Long]] = null
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
    val shuffleId = HttpBlockedLocalFileShuffle.newShuffleId()
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
        // Open the INDEX file
        var indexFile: File = 
          HttpBlockedLocalFileShuffle.getBlockIndexOutputFile(shuffleId, 
            myIndex, i)
        var indexOut = new ObjectOutputStream(new FileOutputStream(indexFile))
        var indexDirty: Boolean = true
        var alreadyWritten: Long = 0
      
        // Open the actual file
        var file: File = 
          HttpBlockedLocalFileShuffle.getOutputFile(shuffleId, myIndex, i)
        val out = new ObjectOutputStream(new FileOutputStream(file))
        
        val writeStartTime = System.currentTimeMillis
        logInfo("BEGIN WRITE: " + file)

        buckets(i).foreach(pair => {
          out.writeObject(pair)
          out.flush()
          indexDirty = true
          
          // Update the INDEX file if more than blockSize limit has been written
          if (file.length - alreadyWritten > 
              HttpBlockedLocalFileShuffle.BlockSize) {
            indexOut.writeObject(file.length)
            indexDirty = false
            alreadyWritten = file.length
          }
        })
        
        // Write down the last range if it was not written
        if (indexDirty) {
          indexOut.writeObject(file.length)
        }
        
        out.close()
        indexOut.close()
        
        logInfo("END WRITE: " + file)
        val writeTime = (System.currentTimeMillis - writeStartTime)
        logInfo("Writing " + file + " of size " + file.length + " bytes took " + writeTime + " millis.")
      }
      
      (myIndex, HttpBlockedLocalFileShuffle.serverUri)
    }).collect()

    // TODO: Could broadcast outputLocs

    // Return an RDD that does each of the merges for a given partition
    val indexes = sc.parallelize(0 until numOutputSplits, numOutputSplits)
    return indexes.flatMap((myId: Int) => {
      totalSplits = outputLocs.size
      hasSplits = 0
      
      blocksInSplit = Array.tabulate(totalSplits)(_ => new ArrayBuffer[Long])
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

      var threadPool = HttpBlockedLocalFileShuffle.newDaemonFixedThreadPool(
        HttpBlockedLocalFileShuffle.MaxRxConnections)
        
      while (hasSplits < totalSplits) {
        var numThreadsToCreate =
          Math.min(totalSplits, HttpBlockedLocalFileShuffle.MaxRxConnections) -
          threadPool.getActiveCount
      
        while (hasSplits < totalSplits && numThreadsToCreate > 0) {
          // Select a random split to pull
          val splitIndex = selectRandomSplit
          
          if (splitIndex != -1) {
            val (inputId, serverUri) = outputLocs(splitIndex)

            threadPool.execute(new ShuffleClient(serverUri, shuffleId.toInt, 
              inputId, myId, splitIndex))
              
            // splitIndex is in transit. Will be unset in the ShuffleClient
            splitsInRequestBitVector.synchronized {
              splitsInRequestBitVector.set(splitIndex)
            }
          }
          
          numThreadsToCreate = numThreadsToCreate - 1
        }
        
        // Sleep for a while before creating new threads
        Thread.sleep(HttpBlockedLocalFileShuffle.MinKnockInterval)
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
      requiredSplits(HttpBlockedLocalFileShuffle.ranGen.nextInt(
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
  
  class ShuffleClient(serverUri: String, shuffleId: Int, 
    inputId: Int, myId: Int, splitIndex: Int)
  extends Thread with Logging {
    private var receptionSucceeded = false

    override def run: Unit = {
      try {
        // First get the INDEX file if totalBlocksInSplit(splitIndex) is unknown
        if (totalBlocksInSplit(splitIndex) == -1) {
          val url = "%s/shuffle/%d/%d/INDEX-%d".format(serverUri, shuffleId, 
            inputId, myId)
          val inputStream = new ObjectInputStream(new URL(url).openStream())
          
          try {
            while (true) {
              blocksInSplit(splitIndex) += 
                inputStream.readObject().asInstanceOf[Long]
            }
          } catch {
            case e: EOFException => {}
          }
          
          totalBlocksInSplit(splitIndex) = blocksInSplit(splitIndex).size
          inputStream.close()
        }
        
        // Open connection
        val urlString = 
          "%s/shuffle/%d/%d/%d".format(serverUri, shuffleId, inputId, myId)
        val url = new URL(urlString)
        val httpConnection = 
          url.openConnection().asInstanceOf[HttpURLConnection]
        
        // Set the range to download
        val blockStartsAt = hasBlocksInSplit(splitIndex) match {
          case 0 => 0
          case _ => blocksInSplit(splitIndex)(hasBlocksInSplit(splitIndex) - 1) + 1
        }
        val blockEndsAt = blocksInSplit(splitIndex)(hasBlocksInSplit(splitIndex))
        httpConnection.setRequestProperty("Range", 
          "bytes=" + blockStartsAt + "-" + blockEndsAt)
        
        // Connect to the server
        httpConnection.connect()
        
        val urStringWithRange = 
          urlString + "[%d:%d]".format(blockStartsAt, blockEndsAt)
        val readStartTime = System.currentTimeMillis
        logInfo("BEGIN READ: " + urStringWithRange)
      
        // Receive data in an Array[Byte]
        val requestedFileLen: Int = (blockEndsAt - blockStartsAt).toInt + 1
        var recvByteArray = new Array[Byte](requestedFileLen)
        var alreadyRead = 0
        var bytesRead = 0
        
        val isSource = httpConnection.getInputStream()
        while (alreadyRead != requestedFileLen) {
          bytesRead = isSource.read(recvByteArray, alreadyRead, 
            requestedFileLen - alreadyRead)
          if (bytesRead > 0) {
            alreadyRead = alreadyRead + bytesRead
          }
        } 
        
        // Disconnect
        httpConnection.disconnect()

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
        
        logInfo("END READ: " + urStringWithRange)
        val readTime = System.currentTimeMillis - readStartTime
        logInfo("Reading " + urStringWithRange + " took " + readTime + " millis.")
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
      }
    }
  }     
}

object HttpBlockedLocalFileShuffle extends Logging {
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
  private var server: HttpServer = null
  private var serverUri: String = null
  
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
      
      val extServerPort = System.getProperty(
        "spark.localFileShuffle.external.server.port", "-1").toInt
      if (extServerPort != -1) {
        // We're using an external HTTP server; set URI relative to its root
        var extServerPath = System.getProperty(
          "spark.localFileShuffle.external.server.path", "")
        if (extServerPath != "" && !extServerPath.endsWith("/")) {
          extServerPath += "/"
        }
        serverUri = "http://%s:%d/%s/spark-local-%s".format(
          Utils.localIpAddress, extServerPort, extServerPath, localDirUuid)
      } else {
        // Create our own server
        server = new HttpServer(localDir)
        server.start()
        serverUri = server.uri
      }
      initialized = true
      logInfo("Local URI: " + serverUri)
    }
  }
  
  def BlockSize = BlockSize_
  
  def MinKnockInterval = MinKnockInterval_
  def MaxKnockInterval = MaxKnockInterval_
  
  def MaxRxConnections = MaxRxConnections_
  def MaxTxConnections = MaxTxConnections_
  
  def getOutputFile(shuffleId: Long, inputId: Int, outputId: Int): File = {
    initializeIfNeeded()
    val dir = new File(shuffleDir, shuffleId + "/" + inputId)
    dir.mkdirs()
    val file = new File(dir, "" + outputId)
    return file
  }
  
  def getBlockIndexOutputFile(shuffleId: Long, inputId: Int, 
    outputId: Int): File = {
    initializeIfNeeded()
    val dir = new File(shuffleDir, shuffleId + "/" + inputId)
    dir.mkdirs()
    val file = new File(dir, "INDEX-" + outputId)
    return file
  }

  def getServerUri(): String = {
    initializeIfNeeded()
    serverUri
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
}
