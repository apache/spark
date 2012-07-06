package spark.scheduler

import java.io._
import java.util.HashMap
import java.util.zip.{GZIPInputStream, GZIPOutputStream}

import scala.collection.mutable.ArrayBuffer

import it.unimi.dsi.fastutil.io.FastBufferedOutputStream

import com.ning.compress.lzf.LZFInputStream
import com.ning.compress.lzf.LZFOutputStream

import spark._
import spark.storage._

object ShuffleMapTask {
  val serializedInfoCache = new HashMap[Int, Array[Byte]]
  val deserializedInfoCache = new HashMap[Int, (RDD[_], ShuffleDependency[_,_,_])]

  def serializeInfo(stageId: Int, rdd: RDD[_], dep: ShuffleDependency[_,_,_]): Array[Byte] = {
    synchronized {
      val old = serializedInfoCache.get(stageId)
      if (old != null) {
        return old
      } else {
        val out = new ByteArrayOutputStream
        val objOut = new ObjectOutputStream(new GZIPOutputStream(out))
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
      val old = deserializedInfoCache.get(stageId)
      if (old != null) {
        return old
      } else {
        val loader = Thread.currentThread.getContextClassLoader
        val in = new GZIPInputStream(new ByteArrayInputStream(bytes))
        val objIn = new ObjectInputStream(in) {
          override def resolveClass(desc: ObjectStreamClass) =
            Class.forName(desc.getName, false, loader)
        }
        val rdd = objIn.readObject().asInstanceOf[RDD[_]]
        val dep = objIn.readObject().asInstanceOf[ShuffleDependency[_,_,_]]
        val tuple = (rdd, dep)
        deserializedInfoCache.put(stageId, tuple)
        return tuple
      }
    }
  }

  def clearCache() {
    synchronized {
      serializedInfoCache.clear()
      deserializedInfoCache.clear()
    }
  }
}

class ShuffleMapTask(
    stageId: Int,
    var rdd: RDD[_], 
    var dep: ShuffleDependency[_,_,_],
    var partition: Int, 
    @transient var locs: Seq[String])
  extends Task[BlockManagerId](stageId)
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
    split = in.readObject().asInstanceOf[Split]
  }

  override def run(attemptId: Long): BlockManagerId = {
    val numOutputSplits = dep.partitioner.numPartitions
    val aggregator = dep.aggregator.asInstanceOf[Aggregator[Any, Any, Any]]
    val partitioner = dep.partitioner
    val buckets = Array.tabulate(numOutputSplits)(_ => new HashMap[Any, Any])
    for (elem <- rdd.iterator(split)) {
      val (k, v) = elem.asInstanceOf[(Any, Any)]
      var bucketId = partitioner.getPartition(k)
      val bucket = buckets(bucketId)
      var existing = bucket.get(k)
      if (existing == null) {
        bucket.put(k, aggregator.createCombiner(v))
      } else {
        bucket.put(k, aggregator.mergeValue(existing, v))
      }
    }
    val ser = SparkEnv.get.serializer.newInstance()
    val blockManager = SparkEnv.get.blockManager
    for (i <- 0 until numOutputSplits) {
      val blockId = "shuffleid_" + dep.shuffleId + "_" + partition + "_" + i
      val arr = new ArrayBuffer[Any]
      val iter = buckets(i).entrySet().iterator()
      while (iter.hasNext()) {
        val entry = iter.next()
        arr += ((entry.getKey(), entry.getValue()))
      }
      // TODO: This should probably be DISK_ONLY
      blockManager.put(blockId, arr.iterator, StorageLevel.MEMORY_ONLY, false)
    }
    return SparkEnv.get.blockManager.blockManagerId
  }

  override def preferredLocations: Seq[String] = locs

  override def toString = "ShuffleMapTask(%d, %d)".format(stageId, partition)
}
