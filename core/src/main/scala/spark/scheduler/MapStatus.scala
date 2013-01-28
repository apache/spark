package spark.scheduler

import spark.storage.BlockManagerId
import java.io.{ObjectOutput, ObjectInput, Externalizable}

/**
 * Result returned by a ShuffleMapTask to a scheduler. Includes the block manager address that the
 * task ran on as well as the sizes of outputs for each reducer, for passing on to the reduce tasks.
 * The map output sizes are compressed using MapOutputTracker.compressSize.
 */
private[spark] class MapStatus(var location: BlockManagerId, var compressedSizes: Array[Byte])
  extends Externalizable {

  def this() = this(null, null)  // For deserialization only

  def writeExternal(out: ObjectOutput) {
    location.writeExternal(out)
    out.writeInt(compressedSizes.length)
    out.write(compressedSizes)
  }

  def readExternal(in: ObjectInput) {
    location = BlockManagerId(in)
    compressedSizes = new Array[Byte](in.readInt())
    in.readFully(compressedSizes)
  }
}
