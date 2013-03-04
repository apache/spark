package spark.scheduler

import java.io._
import java.util.{HashMap => JHashMap}
import java.util.zip.{GZIPInputStream, GZIPOutputStream}

import scala.collection.mutable.{ArrayBuffer, HashMap}
import scala.collection.JavaConversions._

import it.unimi.dsi.fastutil.io.FastBufferedOutputStream

import com.ning.compress.lzf.LZFInputStream
import com.ning.compress.lzf.LZFOutputStream

import spark._
import executor.ShuffleWriteMetrics
import spark.storage._
import util.{TimeStampedHashMap, MetadataCleaner}

private[spark] object ShuffleMapTask {

  // A simple map between the stage id to the serialized byte array of a task.
  // Served as a cache for task serialization because serialization can be
  // expensive on the master node if it needs to launch thousands of tasks.
  val serializedInfoCache = new TimeStampedHashMap[Int, Array[Byte]]

  val metadataCleaner = new MetadataCleaner("ShuffleMapTask", serializedInfoCache.clearOldValues)

  def serializeInfo(stageId: Int, rdd: RDD[_], dep: ShuffleDependency[_,_]): Array[Byte] = {
    synchronized {
      val old = serializedInfoCache.get(stageId).orNull
      if (old != null) {
        return old
      } else {
        val out = new ByteArrayOutputStream
        val ser = SparkEnv.get.closureSerializer.newInstance()
        val objOut = ser.serializeStream(new GZIPOutputStream(out))
        objOut.writeObject(rdd)
        objOut.writeObject(dep)
        objOut.close()
        val bytes = out.toByteArray
        serializedInfoCache.put(stageId, bytes)
        return bytes
      }
    }
  }

  def deserializeInfo(stageId: Int, bytes: Array[Byte]): (RDD[_], ShuffleDependency[_,_]) = {
    synchronized {
      val loader = Thread.currentThread.getContextClassLoader
      val in = new GZIPInputStream(new ByteArrayInputStream(bytes))
      val ser = SparkEnv.get.closureSerializer.newInstance()
      val objIn = ser.deserializeStream(in)
      val rdd = objIn.readObject().asInstanceOf[RDD[_]]
      val dep = objIn.readObject().asInstanceOf[ShuffleDependency[_,_]]
      return (rdd, dep)
    }
  }

  // Since both the JarSet and FileSet have the same format this is used for both.
  def deserializeFileSet(bytes: Array[Byte]) : HashMap[String, Long] = {
    val in = new GZIPInputStream(new ByteArrayInputStream(bytes))
    val objIn = new ObjectInputStream(in)
    val set = objIn.readObject().asInstanceOf[Array[(String, Long)]].toMap
    return (HashMap(set.toSeq: _*))
  }

  def clearCache() {
    synchronized {
      serializedInfoCache.clear()
    }
  }
}

private[spark] class ShuffleMapTask(
    stageId: Int,
    var rdd: RDD[_],
    var dep: ShuffleDependency[_,_],
    var partition: Int,
    @transient var locs: Seq[String])
  extends Task[MapStatus](stageId)
  with Externalizable
  with Logging {

  protected def this() = this(0, null, null, 0, null)

  var split = if (rdd == null) {
    null
  } else {
    rdd.partitions(partition)
  }

  override def writeExternal(out: ObjectOutput) {
    RDDCheckpointData.synchronized {
      split = rdd.partitions(partition)
      out.writeInt(stageId)
      val bytes = ShuffleMapTask.serializeInfo(stageId, rdd, dep)
      out.writeInt(bytes.length)
      out.write(bytes)
      out.writeInt(partition)
      out.writeLong(generation)
      out.writeObject(split)
    }
  }

  override def readExternal(in: ObjectInput) {
    val stageId = in.readInt()
    val numBytes = in.readInt()
    val bytes = new Array[Byte](numBytes)
    in.readFully(bytes)
    val (rdd_, dep_) = ShuffleMapTask.deserializeInfo(stageId, bytes)
    rdd = rdd_
    dep = dep_
    partition = in.readInt()
    generation = in.readLong()
    split = in.readObject().asInstanceOf[Partition]
  }

  override def run(attemptId: Long): MapStatus = {
    val numOutputSplits = dep.partitioner.numPartitions

    val taskContext = new TaskContext(stageId, partition, attemptId)
    metrics = Some(taskContext.taskMetrics)
    try {
      // Partition the map output.
      val buckets = Array.fill(numOutputSplits)(new ArrayBuffer[(Any, Any)])
      for (elem <- rdd.iterator(split, taskContext)) {
        val pair = elem.asInstanceOf[(Any, Any)]
        val bucketId = dep.partitioner.getPartition(pair._1)
        buckets(bucketId) += pair
      }

      val compressedSizes = new Array[Byte](numOutputSplits)

      var totalBytes = 0l

      val blockManager = SparkEnv.get.blockManager
      for (i <- 0 until numOutputSplits) {
        val blockId = "shuffle_" + dep.shuffleId + "_" + partition + "_" + i
        // Get a Scala iterator from Java map
        val iter: Iterator[(Any, Any)] = buckets(i).iterator
        val size = blockManager.put(blockId, iter, StorageLevel.DISK_ONLY, false)
        totalBytes += size
        compressedSizes(i) = MapOutputTracker.compressSize(size)
      }
      val shuffleMetrics = new ShuffleWriteMetrics
      shuffleMetrics.shuffleBytesWritten = totalBytes
      metrics.get.shuffleWriteMetrics = Some(shuffleMetrics)

      return new MapStatus(blockManager.blockManagerId, compressedSizes)
    } finally {
      // Execute the callbacks on task completion.
      taskContext.executeOnCompleteCallbacks()
    }
  }

  override def preferredLocations: Seq[String] = locs

  override def toString = "ShuffleMapTask(%d, %d)".format(stageId, partition)
}
