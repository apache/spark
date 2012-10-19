package spark.storage

import java.io.{Externalizable, ObjectInput, ObjectOutput}

/**
 * Flags for controlling the storage of an RDD. Each StorageLevel records whether to use memory,
 * whether to drop the RDD to disk if it falls out of memory, whether to keep the data in memory
 * in a serialized format, and whether to replicate the RDD partitions on multiple nodes.
 * The [[spark.storage.StorageLevel$]] singleton object contains some static constants for
 * commonly useful storage levels.
 */
class StorageLevel(
    var useDisk: Boolean, 
    var useMemory: Boolean,
    var deserialized: Boolean,
    var replication: Int = 1)
  extends Externalizable {

  // TODO: Also add fields for caching priority, dataset ID, and flushing.
  
  def this(flags: Int, replication: Int) {
    this((flags & 4) != 0, (flags & 2) != 0, (flags & 1) != 0, replication)
  }

  def this() = this(false, true, false)  // For deserialization

  override def clone(): StorageLevel = new StorageLevel(
    this.useDisk, this.useMemory, this.deserialized, this.replication)

  override def equals(other: Any): Boolean = other match {
    case s: StorageLevel =>
      s.useDisk == useDisk && 
      s.useMemory == useMemory &&
      s.deserialized == deserialized &&
      s.replication == replication 
    case _ =>
      false
  }
 
  def isValid = ((useMemory || useDisk) && (replication > 0))

  def toInt: Int = {
    var ret = 0
    if (useDisk) {
      ret |= 4
    }
    if (useMemory) {
      ret |= 2
    }
    if (deserialized) {
      ret |= 1
    }
    return ret
  }

  override def writeExternal(out: ObjectOutput) {
    out.writeByte(toInt)
    out.writeByte(replication)
  }

  override def readExternal(in: ObjectInput) {
    val flags = in.readByte()
    useDisk = (flags & 4) != 0
    useMemory = (flags & 2) != 0
    deserialized = (flags & 1) != 0
    replication = in.readByte()
  }

  override def toString: String =
    "StorageLevel(%b, %b, %b, %d)".format(useDisk, useMemory, deserialized, replication)
}

object StorageLevel {
  val NONE = new StorageLevel(false, false, false)
  val DISK_ONLY = new StorageLevel(true, false, false)
  val DISK_ONLY_2 = new StorageLevel(true, false, false, 2)
  val MEMORY_ONLY = new StorageLevel(false, true, true)
  val MEMORY_ONLY_2 = new StorageLevel(false, true, true, 2)
  val MEMORY_ONLY_SER = new StorageLevel(false, true, false)
  val MEMORY_ONLY_SER_2 = new StorageLevel(false, true, false, 2)
  val MEMORY_AND_DISK = new StorageLevel(true, true, true)
  val MEMORY_AND_DISK_2 = new StorageLevel(true, true, true, 2)
  val MEMORY_AND_DISK_SER = new StorageLevel(true, true, false)
  val MEMORY_AND_DISK_SER_2 = new StorageLevel(true, true, false, 2)
}
