package spark

import java.io._

import org.apache.hadoop.io.ObjectWritable
import org.apache.hadoop.io.Writable
import org.apache.hadoop.mapred.JobConf

class SerializableWritable[T <: Writable](@transient var t: T) extends Serializable {
  def value = t
  override def toString = t.toString

  private def writeObject(out: ObjectOutputStream) {
    out.defaultWriteObject()
    new ObjectWritable(t).write(out)
  }

  private def readObject(in: ObjectInputStream) {
    in.defaultReadObject()
    val ow = new ObjectWritable()
    ow.setConf(new JobConf())
    ow.readFields(in)
    t = ow.get().asInstanceOf[T]
  }
}
