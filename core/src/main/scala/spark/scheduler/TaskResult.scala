package spark.scheduler

import java.io._

import scala.collection.mutable.Map
import spark.executor.TaskMetrics
import spark.SparkEnv
import java.nio.ByteBuffer

// Task result. Also contains updates to accumulator variables.
// TODO: Use of distributed cache to return result is a hack to get around
// what seems to be a bug with messages over 60KB in libprocess; fix it
private[spark]
class TaskResult[T](var value: T, var accumUpdates: Map[Long, Any], var metrics: TaskMetrics) extends Externalizable {
  def this() = this(null.asInstanceOf[T], null, null)

  override def writeExternal(out: ObjectOutput) {

    val objectSer = SparkEnv.get.serializer.newInstance()
    val bb = objectSer.serialize(value)

    out.writeInt( bb.remaining())
    if (bb.hasArray) {
      out.write(bb.array(), bb.arrayOffset() + bb.position(), bb.remaining())
    } else {
      val bbval = new Array[Byte](bb.remaining())
      bb.get(bbval)
      out.write(bbval)
    }

    out.writeInt(accumUpdates.size)
    for ((key, value) <- accumUpdates) {
      out.writeLong(key)
      out.writeObject(value)
    }
    out.writeObject(metrics)
  }

  override def readExternal(in: ObjectInput) {

    //this doesn't work since SparkEnv.get == null
    // in this context
    val objectSer = SparkEnv.get.serializer.newInstance()

    val blen = in.readInt()
    val byteVal = new Array[Byte](blen)
    in.readFully(byteVal)
    value = objectSer.deserialize(ByteBuffer.wrap(byteVal))

    val numUpdates = in.readInt
    if (numUpdates == 0) {
      accumUpdates = null
    } else {
      accumUpdates = Map()
      for (i <- 0 until numUpdates) {
        accumUpdates(in.readLong()) = in.readObject()
      }
    }
    metrics = in.readObject().asInstanceOf[TaskMetrics]
  }
}
