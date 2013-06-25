package spark.scheduler

import spark._
import java.io._
import util.{MetadataCleaner, TimeStampedHashMap}
import java.util.zip.{GZIPInputStream, GZIPOutputStream}

private[spark] object ResultTask {

  // A simple map between the stage id to the serialized byte array of a task.
  // Served as a cache for task serialization because serialization can be
  // expensive on the master node if it needs to launch thousands of tasks.
  val serializedInfoCache = new TimeStampedHashMap[Int, Array[Byte]]

  val metadataCleaner = new MetadataCleaner("ResultTask", serializedInfoCache.clearOldValues)

  def serializeInfo(stageId: Int, rdd: RDD[_], func: (TaskContext, Iterator[_]) => _): Array[Byte] = {
    synchronized {
      val old = serializedInfoCache.get(stageId).orNull
      if (old != null) {
        return old
      } else {
        val out = new ByteArrayOutputStream
        val ser = SparkEnv.get.closureSerializer.newInstance
        val objOut = ser.serializeStream(new GZIPOutputStream(out))
        objOut.writeObject(rdd)
        objOut.writeObject(func)
        objOut.close()
        val bytes = out.toByteArray
        serializedInfoCache.put(stageId, bytes)
        return bytes
      }
    }
  }

  def deserializeInfo(stageId: Int, bytes: Array[Byte]): (RDD[_], (TaskContext, Iterator[_]) => _) = {
    synchronized {
      val loader = Thread.currentThread.getContextClassLoader
      val in = new GZIPInputStream(new ByteArrayInputStream(bytes))
      val ser = SparkEnv.get.closureSerializer.newInstance
      val objIn = ser.deserializeStream(in)
      val rdd = objIn.readObject().asInstanceOf[RDD[_]]
      val func = objIn.readObject().asInstanceOf[(TaskContext, Iterator[_]) => _]
      return (rdd, func)
    }
  }

  def clearCache() {
    synchronized {
      serializedInfoCache.clear()
    }
  }
}


private[spark] class ResultTask[T, U](
    stageId: Int,
    var rdd: RDD[T],
    var func: (TaskContext, Iterator[T]) => U,
    var partition: Int,
    @transient locs: Seq[String],
    val outputId: Int)
  extends Task[U](stageId) with Externalizable {

  def this() = this(0, null, null, 0, null, 0)

  var split = if (rdd == null) {
    null
  } else {
    rdd.partitions(partition)
  }

  private val preferredLocs: Seq[String] = if (locs == null) Nil else locs.toSet.toSeq

  {
    // DEBUG code
    preferredLocs.foreach (hostPort => Utils.checkHost(Utils.parseHostPort(hostPort)._1, "preferredLocs : " + preferredLocs))
  }

  override def run(attemptId: Long): U = {
    val context = new TaskContext(stageId, partition, attemptId)
    metrics = Some(context.taskMetrics)
    try {
      func(context, rdd.iterator(split, context))
    } finally {
      context.executeOnCompleteCallbacks()
    }
  }

  override def preferredLocations: Seq[String] = preferredLocs

  override def toString = "ResultTask(" + stageId + ", " + partition + ")"

  override def writeExternal(out: ObjectOutput) {
    RDDCheckpointData.synchronized {
      split = rdd.partitions(partition)
      out.writeInt(stageId)
      val bytes = ResultTask.serializeInfo(
        stageId, rdd, func.asInstanceOf[(TaskContext, Iterator[_]) => _])
      out.writeInt(bytes.length)
      out.write(bytes)
      out.writeInt(partition)
      out.writeInt(outputId)
      out.writeObject(split)
    }
  }

  override def readExternal(in: ObjectInput) {
    val stageId = in.readInt()
    val numBytes = in.readInt()
    val bytes = new Array[Byte](numBytes)
    in.readFully(bytes)
    val (rdd_, func_) = ResultTask.deserializeInfo(stageId, bytes)
    rdd = rdd_.asInstanceOf[RDD[T]]
    func = func_.asInstanceOf[(TaskContext, Iterator[T]) => U]
    partition = in.readInt()
    val outputId = in.readInt()
    split = in.readObject().asInstanceOf[Partition]
  }
}
