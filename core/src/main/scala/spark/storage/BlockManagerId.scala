package spark.storage

import java.io.{Externalizable, IOException, ObjectInput, ObjectOutput}
import java.util.concurrent.ConcurrentHashMap

/**
 * This class represent an unique identifier for a BlockManager.
 * The first 2 constructors of this class is made private to ensure that
 * BlockManagerId objects can be created only using the factory method in
 * [[spark.storage.BlockManager$]]. This allows de-duplication of id objects.
 * Also, constructor parameters are private to ensure that parameters cannot
 * be modified from outside this class.
 */
private[spark] class BlockManagerId private (
    private var ip_ : String,
    private var port_ : Int
  ) extends Externalizable {

  private def this(in: ObjectInput) = this(in.readUTF(), in.readInt())

  def this() = this(null, 0)  // For deserialization only

  def ip = ip_

  def port = port_

  override def writeExternal(out: ObjectOutput) {
    out.writeUTF(ip_)
    out.writeInt(port_)
  }

  override def readExternal(in: ObjectInput) {
    ip_ = in.readUTF()
    port_ = in.readInt()
  }

  @throws(classOf[IOException])
  private def readResolve(): Object = BlockManagerId.getCachedBlockManagerId(this)

  override def toString = "BlockManagerId(" + ip + ", " + port + ")"

  override def hashCode = ip.hashCode * 41 + port

  override def equals(that: Any) = that match {
    case id: BlockManagerId => port == id.port && ip == id.ip
    case _ => false
  }
}


private[spark] object BlockManagerId {

  def apply(ip: String, port: Int) =
    getCachedBlockManagerId(new BlockManagerId(ip, port))

  def apply(in: ObjectInput) =
    getCachedBlockManagerId(new BlockManagerId(in))

  val blockManagerIdCache = new ConcurrentHashMap[BlockManagerId, BlockManagerId]()

  def getCachedBlockManagerId(id: BlockManagerId): BlockManagerId = {
    if (blockManagerIdCache.containsKey(id)) {
      blockManagerIdCache.get(id)
    } else {
      blockManagerIdCache.put(id, id)
      id
    }
  }
}
