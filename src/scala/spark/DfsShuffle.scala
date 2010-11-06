package spark

import java.io.{EOFException, ObjectInputStream, ObjectOutputStream}
import java.net.URI
import java.util.UUID

import scala.collection.mutable.HashMap

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path, RawLocalFileSystem}

import mesos.SlaveOffer


/**
 * A simple implementation of shuffle using a distributed file system.
 *
 * TODO: Add support for compression when spark.compress is set to true.
 */
@serializable
class DfsShuffle[K, V, C](
  rdd: RDD[(K, V)],
  numOutputSplits: Int,
  createCombiner: V => C,
  mergeValue: (C, V) => C,
  mergeCombiners: (C, C) => C)
extends Logging
{
  def compute(): RDD[(K, C)] = {
    val sc = rdd.sparkContext
    val dir = DfsShuffle.newTempDirectory()
    logInfo("Intermediate data directory: " + dir)

    val numberedSplitRdd = new NumberedSplitRDD(rdd)
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
      }
      combiners
    })
  }
}


object DfsShuffle {
  var initialized = false
  var fileSystem: FileSystem = null

  private def initializeIfNeeded() = synchronized {
    if (!initialized) {
      val bufferSize = System.getProperty("spark.buffer.size", "65536").toInt
      val dfs = System.getProperty("spark.dfs", "file:///")
      val conf = new Configuration()
      conf.setInt("io.file.buffer.size", bufferSize)
      conf.setInt("dfs.replication", 1)
      fileSystem = FileSystem.get(new URI(dfs), conf)
    }
    initialized = true
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


/**
 * An RDD that captures the splits of a parent RDD and gives them unique indexes.
 * This is useful for a variety of shuffle implementations.
 */
class NumberedSplitRDD[T: ClassManifest](prev: RDD[T])
extends RDD[(Int, Iterator[T])](prev.sparkContext) {
  @transient val splits_ = {
    prev.splits.zipWithIndex.map {
      case (s, i) => new NumberedSplitRDDSplit(s, i): Split
    }.toArray
  }

  override def splits = splits_

  override def preferredLocations(split: Split) = {
    val nsplit = split.asInstanceOf[NumberedSplitRDDSplit]
    prev.preferredLocations(nsplit.prev)
  }

  override def iterator(split: Split) = {
    val nsplit = split.asInstanceOf[NumberedSplitRDDSplit]
    Iterator((nsplit.index, prev.iterator(nsplit.prev)))
  }

  override def taskStarted(split: Split, slot: SlaveOffer) = {
    val nsplit = split.asInstanceOf[NumberedSplitRDDSplit]
    prev.taskStarted(nsplit.prev, slot)
  }
}


class NumberedSplitRDDSplit(val prev: Split, val index: Int) extends Split {
  override def getId() = "NumberedSplitRDDSplit(%d)".format(index)
}
