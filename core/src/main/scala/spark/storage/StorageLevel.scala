package spark.storage

import java.io.{Externalizable, IOException, ObjectInput, ObjectOutput}

/**
 * Flags for controlling the storage of an RDD. Each StorageLevel records whether to use memory,
 * whether to drop the RDD to disk if it falls out of memory, whether to keep the data in memory
 * in a serialized format, and whether to replicate the RDD partitions on multiple nodes.
 * The [[spark.storage.StorageLevel$]] singleton object contains some static constants for
 * commonly useful storage levels. To create your own storage level object, use the factor method
 * of the singleton object (`StorageLevel(...)`).
 */
class StorageLevel private(
    private var useDisk_ : Boolean,
    private var useMemory_ : Boolean,
    private var deserialized_ : Boolean,
    private var replication_ : Int = 1)
  extends Externalizable {

  // TODO: Also add fields for caching priority, dataset ID, and flushing.
  private def this(flags: Int, replication: Int) {
    this((flags & 4) != 0, (flags & 2) != 0, (flags & 1) != 0, replication)
  }

  def this() = this(false, true, false)  // For deserialization

  def useDisk = useDisk_
  def useMemory = useMemory_
  def deserialized = deserialized_
  def replication = replication_

  assert(replication < 40, "Replication restricted to be less than 40 for calculating hashcodes")

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
    if (useDisk_) {
      ret |= 4
    }
    if (useMemory_) {
      ret |= 2
    }
    if (deserialized_) {
      ret |= 1
    }
    return ret
  }

  override def writeExternal(out: ObjectOutput) {
    out.writeByte(toInt)
    out.writeByte(replication_)
  }

  override def readExternal(in: ObjectInput) {
    val flags = in.readByte()
    useDisk_ = (flags & 4) != 0
    useMemory_ = (flags & 2) != 0
    deserialized_ = (flags & 1) != 0
    replication_ = in.readByte()
  }

  @throws(classOf[IOException])
  private def readResolve(): Object = StorageLevel.getCachedStorageLevel(this)

  override def toString: String =
    "StorageLevel(%b, %b, %b, %d)".format(useDisk, useMemory, deserialized, replication)

  override def hashCode(): Int = toInt * 41 + replication
  def description : String = {
    var result = ""
    result += (if (useDisk) "Disk " else "")
    result += (if (useMemory) "Memory " else "")
    result += (if (deserialized) "Deserialized " else "Serialized")
    result += "%sx Replicated".format(replication)
    result
  }
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

  /** Create a new StorageLevel object */
  def apply(useDisk: Boolean, useMemory: Boolean, deserialized: Boolean, replication: Int = 1) =
    getCachedStorageLevel(new StorageLevel(useDisk, useMemory, deserialized, replication))

  /** Create a new StorageLevel object from its integer representation */
  def apply(flags: Int, replication: Int) =
    getCachedStorageLevel(new StorageLevel(flags, replication))

  /** Read StorageLevel object from ObjectInput stream */
  def apply(in: ObjectInput) = {
    val obj = new StorageLevel()
    obj.readExternal(in)
    getCachedStorageLevel(obj)
  }

  private[spark]
  val storageLevelCache = new java.util.concurrent.ConcurrentHashMap[StorageLevel, StorageLevel]()

  private[spark] def getCachedStorageLevel(level: StorageLevel): StorageLevel = {
    storageLevelCache.putIfAbsent(level, level)
    storageLevelCache.get(level)
  }
}
