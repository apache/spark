package spark

import java.io._
import java.net.URL
import java.util.UUID
import java.util.concurrent.atomic.AtomicLong

import scala.collection.mutable.{ArrayBuffer, HashMap}


/**
 * A basic implementation of shuffle using local files served through HTTP.
 *
 * TODO: Add support for compression when spark.compress is set to true.
 */
@serializable
class BasicLocalFileShuffle[K, V, C] extends Shuffle[K, V, C] with Logging {
  override def compute(input: RDD[(K, V)],
                       numOutputSplits: Int,
                       createCombiner: V => C,
                       mergeValue: (C, V) => C,
                       mergeCombiners: (C, C) => C)
  : RDD[(K, C)] =
  {
    val sc = input.sparkContext
    val shuffleId = BasicLocalFileShuffle.newShuffleId()
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
        val file = BasicLocalFileShuffle.getOutputFile(shuffleId, myIndex, i)
        val writeStartTime = System.currentTimeMillis
        logInfo("BEGIN WRITE: " + file)
        val out = new ObjectOutputStream(new FileOutputStream(file))
        buckets(i).foreach(pair => out.writeObject(pair))
        out.close()
        logInfo("END WRITE: " + file)
        val writeTime = (System.currentTimeMillis - writeStartTime)
        logInfo("Writing " + file + " of size " + file.length + " bytes took " + writeTime + " millis.")
      }
      
      (myIndex, BasicLocalFileShuffle.serverUri)
    }).collect()

    // Build a hashmap from server URI to list of splits (to facillitate
    // fetching all the URIs on a server within a single connection)
    val splitsByUri = new HashMap[String, ArrayBuffer[Int]]
    for ((inputId, serverUri) <- outputLocs) {
      splitsByUri.getOrElseUpdate(serverUri, ArrayBuffer()) += inputId
    }

    // TODO: Could broadcast splitsByUri

    // Return an RDD that does each of the merges for a given partition
    val indexes = sc.parallelize(0 until numOutputSplits, numOutputSplits)
    return indexes.flatMap((myId: Int) => {
      val combiners = new HashMap[K, C]
      for ((serverUri, inputIds) <- Utils.shuffle(splitsByUri)) {
        for (i <- inputIds) {
          val url = "%s/shuffle/%d/%d/%d".format(serverUri, shuffleId, i, myId)
          val readStartTime = System.currentTimeMillis
          logInfo("BEGIN READ: " + url)
          val inputStream = new ObjectInputStream(new URL(url).openStream())
          try {
            while (true) {
              val (k, c) = inputStream.readObject().asInstanceOf[(K, C)]
              combiners(k) = combiners.get(k) match {
                case Some(oldC) => mergeCombiners(oldC, c)
                case None => c
              }
            }
          } catch {
            case e: EOFException => {}
          }
          inputStream.close()
          logInfo("END READ: " + url)
          val readTime = System.currentTimeMillis - readStartTime
          logInfo("Reading " + url + " took " + readTime + " millis.")
        }
      }
      combiners
    })
  }
}

object BasicLocalFileShuffle extends Logging {
  private var initialized = false
  private var nextShuffleId = new AtomicLong(0)

  // Variables initialized by initializeIfNeeded()
  private var shuffleDir: File = null
  private var server: HttpServer = null
  private var serverUri: String = null

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
}
