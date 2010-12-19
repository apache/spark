package spark

import java.io._
import java.net._
import java.util.{BitSet, Random, Timer, TimerTask, UUID}
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.{Executors, ThreadPoolExecutor, ThreadFactory}

import scala.collection.mutable.{ArrayBuffer, HashMap}


/**
 * An implementation of shuffle using local files served through HTTP where 
 * receivers create simultaneous connections to multiple servers by setting the
 * 'spark.blockedLocalFileShuffle.maxConnections' config option.
 *
 * By controlling the 'spark.blockedLocalFileShuffle.blockSize' config option
 * one can also control the largest block size to divide each map output into.
 * Essentially, instead of creating one large output file for each reducer, maps
 * create multiple smaller files to enable finer level of engagement.
 *
 * TODO: Add support for compression when spark.compress is set to true.
 */
@serializable
class HttpBlockedLocalFileShuffle[K, V, C] extends Shuffle[K, V, C] with Logging {
  @transient var totalSplits = 0
  @transient var hasSplits = 0
  
  @transient var blocksInSplit: Array[ArrayBuffer[Long]] = null
  @transient var totalBlocksInSplit: Array[Int] = null
  @transient var hasBlocksInSplit: Array[Int] = null
  
  @transient var hasSplitsBitVector: BitSet = null
  @transient var splitsInRequestBitVector: BitSet = null

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
          HttpBlockedLocalFileShuffle.getBlockIndexOutputFile(shuffleId, myIndex, i)
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
          if (file.length - alreadyWritten > HttpBlockedLocalFileShuffle.BlockSize) {
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
      
      combiners = new HashMap[K, C]
      
      var threadPool = HttpBlockedLocalFileShuffle.newDaemonFixedThreadPool(
        HttpBlockedLocalFileShuffle.MaxConnections)
        
      while (hasSplits < totalSplits) {
        var numThreadsToCreate =
          Math.min(totalSplits, HttpBlockedLocalFileShuffle.MaxConnections) -
          threadPool.getActiveCount
      
        while (hasSplits < totalSplits && numThreadsToCreate > 0) {
          // Select a random split to pull
          val splitIndex = selectRandomSplit
          
          if (splitIndex != -1) {
            val (inputId, serverUri) = outputLocs(splitIndex)

            threadPool.execute(new ShuffleClient(serverUri, shuffleId.toInt, 
              inputId, myId, splitIndex, mergeCombiners))
              
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
  
  class ShuffleClient(serverUri: String, shuffleId: Int, 
    inputId: Int, myId: Int, splitIndex: Int, 
    mergeCombiners: (C, C) => C)
  extends Thread with Logging {
    private var receptionSucceeded = false

    override def run: Unit = {
      try {
        // First get the INDEX file if totalBlocksInSplit(inputId) is unknown
        if (totalBlocksInSplit(inputId) == -1) {
          val url = "%s/shuffle/%d/%d/INDEX-%d".format(serverUri, shuffleId, 
            inputId, myId)
          val inputStream = new ObjectInputStream(new URL(url).openStream())
          
          try {
            while (true) {
              blocksInSplit(inputId) += 
                inputStream.readObject().asInstanceOf[Long]
            }
          } catch {
            case e: EOFException => {}
          }
          
          totalBlocksInSplit(inputId) = blocksInSplit(inputId).size
          inputStream.close()
        }
          
        val urlString = 
          "%s/shuffle/%d/%d/%d".format(serverUri, shuffleId, inputId, myId)
        val url = new URL(urlString)
        val httpConnection = 
          url.openConnection().asInstanceOf[HttpURLConnection]
        
        // Set the range to download
        val blockStartsAt = hasBlocksInSplit(inputId) match {
          case 0 => 0
          case _ => blocksInSplit(inputId)(hasBlocksInSplit(inputId) - 1) + 1
        }
        val blockEndsAt = blocksInSplit(inputId)(hasBlocksInSplit(inputId))
        httpConnection.setRequestProperty("Range", 
          "bytes=" + blockStartsAt + "-" + blockEndsAt)
        
        // Connect to the server
        httpConnection.connect()
        
        val urStringWithRange = 
          urlString + "[%d:%d]".format(blockStartsAt, blockEndsAt)
        val readStartTime = System.currentTimeMillis
        logInfo("BEGIN READ: " + urStringWithRange)
      
        // Receive the block
        val inputStream = new ObjectInputStream(httpConnection.getInputStream())
        try {
          while (true) {
            val (k, c) = inputStream.readObject().asInstanceOf[(K, C)]
            combiners.synchronized {
              combiners(k) = combiners.get(k) match {
                case Some(oldC) => mergeCombiners(oldC, c)
                case None => c
              }
            }
          }
        } catch {
          case e: EOFException => {}
        }
        inputStream.close()
                  
        logInfo("END READ: " + urStringWithRange)
        val readTime = System.currentTimeMillis - readStartTime
        logInfo("Reading " + urStringWithRange + " took " + readTime + " millis.")

        // Disconnect
        httpConnection.disconnect()

        // Reception completed. Update stats.
        hasBlocksInSplit(inputId) = hasBlocksInSplit(inputId) + 1
        
        // Split has been received only if all the blocks have been received
        if (hasBlocksInSplit(inputId) == totalBlocksInSplit(inputId)) {
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
  private var MaxConnections_ = 4
  
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
        "spark.blockedLocalFileShuffle.blockSize", "1024").toInt * 1024
      
      MinKnockInterval_ = System.getProperty(
        "spark.blockedLocalFileShuffle.minKnockInterval", "1000").toInt
      MaxKnockInterval_ = System.getProperty(
        "spark.blockedLocalFileShuffle.maxKnockInterval", "5000").toInt

      MaxConnections_ = System.getProperty(
        "spark.blockedLocalFileShuffle.maxConnections", "4").toInt
      
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
  
  def MaxConnections = MaxConnections_
  
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
