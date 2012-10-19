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
import spark.storage._

private[spark] object ShuffleMapTask {

  // A simple map between the stage id to the serialized byte array of a task.
  // Served as a cache for task serialization because serialization can be
  // expensive on the master node if it needs to launch thousands of tasks.
  val serializedInfoCache = new JHashMap[Int, Array[Byte]]

  def serializeInfo(stageId: Int, rdd: RDD[_], dep: ShuffleDependency[_,_,_]): Array[Byte] = {
    synchronized {
      val old = serializedInfoCache.get(stageId)
      if (old != null) {
        return old
      } else {
        val out = new ByteArrayOutputStream
        val ser = SparkEnv.get.closureSerializer.newInstance
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

  def deserializeInfo(stageId: Int, bytes: Array[Byte]): (RDD[_], ShuffleDependency[_,_,_]) = {
    synchronized {
      val loader = Thread.currentThread.getContextClassLoader
      val in = new GZIPInputStream(new ByteArrayInputStream(bytes))
      val ser = SparkEnv.get.closureSerializer.newInstance
      val objIn = ser.deserializeStream(in)
      val rdd = objIn.readObject().asInstanceOf[RDD[_]]
      val dep = objIn.readObject().asInstanceOf[ShuffleDependency[_,_,_]]
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
    var dep: ShuffleDependency[_,_,_],
    var partition: Int, 
    @transient var locs: Seq[String])
  extends Task[MapStatus](stageId)
  with Externalizable
  with Logging {

  def this() = this(0, null, null, 0, null)
  
  var split = if (rdd == null) {
    null 
  } else { 
    rdd.splits(partition)
  }

  override def writeExternal(out: ObjectOutput) {
    out.writeInt(stageId)
    val bytes = ShuffleMapTask.serializeInfo(stageId, rdd, dep)
    out.writeInt(bytes.length)
    out.write(bytes)
    out.writeInt(partition)
    out.writeLong(generation)
    out.writeObject(split)
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
    split = in.readObject().asInstanceOf[Split]
  }

  override def run(attemptId: Long): MapStatus = {
    val numOutputSplits = dep.partitioner.numPartitions
    val partitioner = dep.partitioner

    val bucketIterators =
      if (dep.aggregator.isDefined && dep.aggregator.get.mapSideCombine) {
        val aggregator = dep.aggregator.get.asInstanceOf[Aggregator[Any, Any, Any]]
        // Apply combiners (map-side aggregation) to the map output.
        val buckets = Array.tabulate(numOutputSplits)(_ => new JHashMap[Any, Any])
        for (elem <- rdd.iterator(split)) {
          val (k, v) = elem.asInstanceOf[(Any, Any)]
          val bucketId = partitioner.getPartition(k)
          val bucket = buckets(bucketId)
          val existing = bucket.get(k)
          if (existing == null) {
            bucket.put(k, aggregator.createCombiner(v))
          } else {
            bucket.put(k, aggregator.mergeValue(existing, v))
          }
        }
        buckets.map(_.iterator)
      } else {
        // No combiners (no map-side aggregation). Simply partition the map output.
        val buckets = Array.fill(numOutputSplits)(new ArrayBuffer[(Any, Any)])
        for (elem <- rdd.iterator(split)) {
          val pair = elem.asInstanceOf[(Any, Any)]
          val bucketId = partitioner.getPartition(pair._1)
          buckets(bucketId) += pair
        }
        buckets.map(_.iterator)
      }

    val compressedSizes = new Array[Byte](numOutputSplits)

    val blockManager = SparkEnv.get.blockManager
    for (i <- 0 until numOutputSplits) {
      val blockId = "shuffle_" + dep.shuffleId + "_" + partition + "_" + i
      // Get a Scala iterator from Java map
      val iter: Iterator[(Any, Any)] = bucketIterators(i)
      val size = blockManager.put(blockId, iter, StorageLevel.DISK_ONLY, false)
      compressedSizes(i) = MapOutputTracker.compressSize(size)
    }

    return new MapStatus(blockManager.blockManagerId, compressedSizes)
  }

  override def preferredLocations: Seq[String] = locs

  override def toString = "ShuffleMapTask(%d, %d)".format(stageId, partition)
}
