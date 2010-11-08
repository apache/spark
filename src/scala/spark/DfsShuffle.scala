package spark

import java.io.{EOFException, ObjectInputStream, ObjectOutputStream}
import java.net.URI
import java.util.UUID

import scala.collection.mutable.HashMap

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path, RawLocalFileSystem}


/**
 * A simple implementation of shuffle using a distributed file system.
 *
 * TODO: Add support for compression when spark.compress is set to true.
 */
@serializable
class DfsShuffle[K, V, C] extends Shuffle[K, V, C] with Logging {
  override def compute(input: RDD[(K, V)],
                       numOutputSplits: Int,
                       createCombiner: V => C,
                       mergeValue: (C, V) => C,
                       mergeCombiners: (C, C) => C)
  : RDD[(K, C)] =
  {
    val sc = input.sparkContext
    val dir = DfsShuffle.newTempDirectory()
    logInfo("Intermediate data directory: " + dir)

    val numberedSplitRdd = new NumberedSplitRDD(input)
    val numInputSplits = numberedSplitRdd.splits.size

    // Run a parallel foreach to write the intermediate data files
    numberedSplitRdd.foreach((pair: (Int, Iterator[(K, V)])) => {
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
      val fs = DfsShuffle.getFileSystem()
      for (i <- 0 until numOutputSplits) {
        val path = new Path(dir, "%d-to-%d".format(myIndex, i))
        val out = new ObjectOutputStream(fs.create(path, true))
        buckets(i).foreach(pair => out.writeObject(pair))
        out.close()
      }
    })

    // Return an RDD that does each of the merges for a given partition
    val indexes = sc.parallelize(0 until numOutputSplits, numOutputSplits)
    return indexes.flatMap((myIndex: Int) => {
      val combiners = new HashMap[K, C]
      val fs = DfsShuffle.getFileSystem()
      for (i <- Utils.shuffle(0 until numInputSplits)) {
        val path = new Path(dir, "%d-to-%d".format(i, myIndex))
        val inputStream = new ObjectInputStream(fs.open(path))
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
      }
      combiners
    })
  }
}


/**
 * Companion object of DfsShuffle; responsible for initializing a Hadoop
 * FileSystem object based on the spark.dfs property and generating names
 * for temporary directories.
 */
object DfsShuffle {
  private var initialized = false
  private var fileSystem: FileSystem = null

  private def initializeIfNeeded() = synchronized {
    if (!initialized) {
      val bufferSize = System.getProperty("spark.buffer.size", "65536").toInt
      val dfs = System.getProperty("spark.dfs", "file:///")
      val conf = new Configuration()
      conf.setInt("io.file.buffer.size", bufferSize)
      conf.setInt("dfs.replication", 1)
      fileSystem = FileSystem.get(new URI(dfs), conf)
      initialized = true
    }
  }

  def getFileSystem(): FileSystem = {
    initializeIfNeeded()
    return fileSystem
  }

  def newTempDirectory(): String = {
    val fs = getFileSystem()
    val workDir = System.getProperty("spark.dfs.workdir", "/tmp")
    val uuid = UUID.randomUUID()
    val path = workDir + "/shuffle-" + uuid
    fs.mkdirs(new Path(path))
    return path
  }
}
