package spark.storage

import java.io.{Externalizable, ObjectInput, ObjectOutput}

import akka.actor.ActorRef


//////////////////////////////////////////////////////////////////////////////////
// Messages from the master to slaves.
//////////////////////////////////////////////////////////////////////////////////
private[spark]
sealed trait ToBlockManagerSlave

// Remove a block from the slaves that have it. This can only be used to remove
// blocks that the master knows about.
private[spark]
case class RemoveBlock(blockId: String) extends ToBlockManagerSlave


//////////////////////////////////////////////////////////////////////////////////
// Messages from slaves to the master.
//////////////////////////////////////////////////////////////////////////////////
private[spark]
sealed trait ToBlockManagerMaster

private[spark]
case class RegisterBlockManager(
    blockManagerId: BlockManagerId,
    maxMemSize: Long,
    sender: ActorRef)
  extends ToBlockManagerMaster

private[spark]
case class HeartBeat(blockManagerId: BlockManagerId) extends ToBlockManagerMaster

private[spark]
class UpdateBlockInfo(
    var blockManagerId: BlockManagerId,
    var blockId: String,
    var storageLevel: StorageLevel,
    var memSize: Long,
    var diskSize: Long)
  extends ToBlockManagerMaster
  with Externalizable {

  def this() = this(null, null, null, 0, 0)  // For deserialization only

  override def writeExternal(out: ObjectOutput) {
    blockManagerId.writeExternal(out)
    out.writeUTF(blockId)
    storageLevel.writeExternal(out)
    out.writeLong(memSize)
    out.writeLong(diskSize)
  }

  override def readExternal(in: ObjectInput) {
    blockManagerId = BlockManagerId(in)
    blockId = in.readUTF()
    storageLevel = StorageLevel(in)
    memSize = in.readLong()
    diskSize = in.readLong()
  }
}

private[spark]
object UpdateBlockInfo {
  def apply(blockManagerId: BlockManagerId,
      blockId: String,
      storageLevel: StorageLevel,
      memSize: Long,
      diskSize: Long): UpdateBlockInfo = {
    new UpdateBlockInfo(blockManagerId, blockId, storageLevel, memSize, diskSize)
  }

  // For pattern-matching
  def unapply(h: UpdateBlockInfo): Option[(BlockManagerId, String, StorageLevel, Long, Long)] = {
    Some((h.blockManagerId, h.blockId, h.storageLevel, h.memSize, h.diskSize))
  }
}

private[spark]
case class GetLocations(blockId: String) extends ToBlockManagerMaster

private[spark]
case class GetLocationsMultipleBlockIds(blockIds: Array[String]) extends ToBlockManagerMaster

private[spark]
case class GetPeers(blockManagerId: BlockManagerId, size: Int) extends ToBlockManagerMaster

private[spark]
case class RemoveExecutor(execId: String) extends ToBlockManagerMaster

private[spark]
case object StopBlockManagerMaster extends ToBlockManagerMaster

private[spark]
case object GetMemoryStatus extends ToBlockManagerMaster

private[spark]
case object ExpireDeadHosts extends ToBlockManagerMaster

private[spark]
case object GetStorageStatus extends ToBlockManagerMaster
