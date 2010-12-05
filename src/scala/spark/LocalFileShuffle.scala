package spark

import java.io._
import java.net._
import java.util.{BitSet, Random, Timer, TimerTask, UUID}
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.{Executors, ThreadPoolExecutor, ThreadFactory}

import scala.collection.mutable.{ArrayBuffer, HashMap}


/**
 * A simple implementation of shuffle using local files served through HTTP.
 *
 * TODO: Add support for compression when spark.compress is set to true.
 */
@serializable
class LocalFileShuffle[K, V, C] extends Shuffle[K, V, C] with Logging {
  @transient var totalSplits = 0
  @transient var hasSplits = 0
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
    val shuffleId = LocalFileShuffle.newShuffleId()
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
        val file = LocalFileShuffle.getOutputFile(shuffleId, myIndex, i)
        val writeStartTime = System.currentTimeMillis
        logInfo ("BEGIN WRITE: " + file)
        val out = new ObjectOutputStream(new FileOutputStream(file))
        buckets(i).foreach(pair => out.writeObject(pair))
        out.close()
        logInfo ("END WRITE: " + file)
        val writeTime = (System.currentTimeMillis - writeStartTime)
        logInfo ("Writing " + file + " of size " + file.length + " bytes took " + writeTime + " millis.")
      }
      (myIndex, LocalFileShuffle.serverUri)
    }).collect()

    val splitsByUri = new ArrayBuffer[(String, Int)]
      for ((inputId, serverUri) <- outputLocs) {
        splitsByUri += ((serverUri, inputId))
      }

    // TODO: Could broadcast splitsByUri

    // Return an RDD that does each of the merges for a given partition
    val indexes = sc.parallelize(0 until numOutputSplits, numOutputSplits)
    return indexes.flatMap((myId: Int) => {
      totalSplits = splitsByUri.size
      hasSplits = 0
      hasSplitsBitVector = new BitSet (totalSplits)
      splitsInRequestBitVector = new BitSet (totalSplits)
      combiners = new HashMap[K, C]
      
      var threadPool = LocalFileShuffle.newDaemonFixedThreadPool (
        LocalFileShuffle.MaxConnections)
        
      while (hasSplits < totalSplits) {
        var numThreadsToCreate =
          Math.min (totalSplits, LocalFileShuffle.MaxConnections) -
          threadPool.getActiveCount
      
        while (hasSplits < totalSplits && numThreadsToCreate > 0) {
          // Select a random split to pull
          val splitIndex = selectRandomSplit
          
          if (splitIndex != -1) {
            val (serverUri, inputId) = splitsByUri (splitIndex)
            val url = 
              "%s/shuffle/%d/%d/%d".format(serverUri, shuffleId, inputId, myId)

            threadPool.execute (
              new ShuffleClient (url, splitIndex, mergeCombiners))
              
            // splitIndex is in transit. Will be unset in the ShuffleClient
            splitsInRequestBitVector.synchronized {
              splitsInRequestBitVector.set (splitIndex)
            }
          }
          
          numThreadsToCreate = numThreadsToCreate - 1
        }
        
        // Sleep for a while before creating new threads
        Thread.sleep (LocalFileShuffle.MinKnockInterval)
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
      requiredSplits(LocalFileShuffle.ranGen.nextInt (requiredSplits.size))
    } else {
      -1
    }
  }
  
  class ShuffleClient (url: String, splitIndex: Int, 
    mergeCombiners: (C, C) => C)
  extends Thread with Logging {
    private var receptionSucceeded = false

    override def run: Unit = {
      val readStartTime = System.currentTimeMillis
      logInfo ("BEGIN READ: " + url)
      
      try {    
        val inputStream = new ObjectInputStream(new URL(url).openStream())
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
        
        // Reception completed. Update stats.
        hasSplitsBitVector.synchronized {
          hasSplitsBitVector.set (splitIndex)
        }
        hasSplits += 1

        // We have received splitIndex
        splitsInRequestBitVector.synchronized {
          splitsInRequestBitVector.set (splitIndex, false)
        }
        
        receptionSucceeded = true
                  
        logInfo ("END READ: " + url)
        val readTime = (System.currentTimeMillis - readStartTime)
        logInfo ("Reading " + url + " took " + readTime + " millis.")        
      } catch {
        // EOFException is expected to happen because sender can break
        // connection due to timeout
        case eofe: java.io.EOFException => { }
        case e: Exception => {
          logInfo ("ShuffleClient had a " + e)
        }
      } finally {
        // If reception failed, unset for future retry
        if (!receptionSucceeded) {
          splitsInRequestBitVector.synchronized {
            splitsInRequestBitVector.set (splitIndex, false)
          }
        }
      }
    }
  }     
}


object LocalFileShuffle extends Logging {
  // Used thoughout the code for small and large waits/timeouts
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
      MinKnockInterval_ =
        System.getProperty ("spark.shuffle.MinKnockInterval", "1000").toInt
      MaxKnockInterval_ =
        System.getProperty ("spark.shuffle.MaxKnockInterval", "5000").toInt

      MaxConnections_ =
        System.getProperty ("spark.shuffle.MaxConnections", "4").toInt
      
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
          localDirUuid = UUID.randomUUID()
          localDir = new File(localDirRoot, "spark-local-" + localDirUuid)
          if (!localDir.exists()) {
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
      logInfo ("Local URI: " + serverUri)
    }
  }
  
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
        var t = Executors.defaultThreadFactory.newThread (r)
        t.setDaemon (true)
        return t
      }
    }
  }

  // Wrapper over newFixedThreadPool
  def newDaemonFixedThreadPool (nThreads: Int): ThreadPoolExecutor = {
    var threadPool =
      Executors.newFixedThreadPool (nThreads).asInstanceOf[ThreadPoolExecutor]

    threadPool.setThreadFactory (newDaemonThreadFactory)
    
    return threadPool
  }   
}
