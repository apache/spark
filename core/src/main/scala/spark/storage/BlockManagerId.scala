package spark.storage

import java.io.{Externalizable, ObjectInput, ObjectOutput}


private[spark] class BlockManagerId(var ip: String, var port: Int) extends Externalizable {
  def this() = this(null, 0)  // For deserialization only

  def this(in: ObjectInput) = this(in.readUTF(), in.readInt())

  override def writeExternal(out: ObjectOutput) {
    out.writeUTF(ip)
    out.writeInt(port)
  }

  override def readExternal(in: ObjectInput) {
    ip = in.readUTF()
    port = in.readInt()
  }

  override def toString = "BlockManagerId(" + ip + ", " + port + ")"

  override def hashCode = ip.hashCode * 41 + port

  override def equals(that: Any) = that match {
    case id: BlockManagerId => port == id.port && ip == id.ip
    case _ => false
  }
}