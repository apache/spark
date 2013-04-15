package spark.storage

import java.io.{Externalizable, IOException, ObjectInput, ObjectOutput}
import java.util.concurrent.ConcurrentHashMap
import spark.Utils

/**
 * This class represent an unique identifier for a BlockManager.
 * The first 2 constructors of this class is made private to ensure that
 * BlockManagerId objects can be created only using the factory method in
 * [[spark.storage.BlockManager$]]. This allows de-duplication of ID objects.
 * Also, constructor parameters are private to ensure that parameters cannot
 * be modified from outside this class.
 */
private[spark] class BlockManagerId private (
    private var executorId_ : String,
    private var host_ : String,
    private var port_ : Int
  ) extends Externalizable {

  private def this() = this(null, null, 0)  // For deserialization only

  def executorId: String = executorId_

  if (null != host_){
    Utils.checkHost(host_, "Expected hostname")
    assert (port_ > 0)
  }

  def hostPort: String = {
    // DEBUG code
    Utils.checkHost(host)
    assert (port > 0)

    host + ":" + port
  }

  def host: String = host_

  def port: Int = port_

  override def writeExternal(out: ObjectOutput) {
    out.writeUTF(executorId_)
    out.writeUTF(host_)
    out.writeInt(port_)
  }

  override def readExternal(in: ObjectInput) {
    executorId_ = in.readUTF()
    host_ = in.readUTF()
    port_ = in.readInt()
  }

  @throws(classOf[IOException])
  private def readResolve(): Object = BlockManagerId.getCachedBlockManagerId(this)

  override def toString = "BlockManagerId(%s, %s, %d)".format(executorId, host, port)

  override def hashCode: Int = (executorId.hashCode * 41 + host.hashCode) * 41 + port

  override def equals(that: Any) = that match {
    case id: BlockManagerId =>
      executorId == id.executorId && port == id.port && host == id.host
    case _ =>
      false
  }
}


private[spark] object BlockManagerId {

  def apply(execId: String, host: String, port: Int) =
    getCachedBlockManagerId(new BlockManagerId(execId, host, port))

  def apply(in: ObjectInput) = {
    val obj = new BlockManagerId()
    obj.readExternal(in)
    getCachedBlockManagerId(obj)
  }

  val blockManagerIdCache = new ConcurrentHashMap[BlockManagerId, BlockManagerId]()

  def getCachedBlockManagerId(id: BlockManagerId): BlockManagerId = {
    blockManagerIdCache.putIfAbsent(id, id)
    blockManagerIdCache.get(id)
  }
}
