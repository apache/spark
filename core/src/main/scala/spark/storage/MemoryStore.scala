package spark.storage

import java.util.LinkedHashMap
import java.util.concurrent.ArrayBlockingQueue
import spark.{SizeEstimator, Utils}
import java.nio.ByteBuffer
import collection.mutable.ArrayBuffer

/**
 * Stores blocks in memory, either as ArrayBuffers of deserialized Java objects or as
 * serialized ByteBuffers.
 */
private class MemoryStore(blockManager: BlockManager, maxMemory: Long)
  extends BlockStore(blockManager) {

  case class Entry(value: Any, size: Long, deserialized: Boolean, var dropPending: Boolean = false)

  private val entries = new LinkedHashMap[String, Entry](32, 0.75f, true)
  private var currentMemory = 0L

  //private val blockDropper = Executors.newSingleThreadExecutor()
  private val blocksToDrop = new ArrayBlockingQueue[String](10000, true)
  private val blockDropper = new Thread("memory store - block dropper") {
    override def run() {
      try {
        while (true) {
          val blockId = blocksToDrop.take()
          logDebug("Block " + blockId + " ready to be dropped")
          blockManager.dropFromMemory(blockId)
        }
      } catch {
        case ie: InterruptedException =>
          logInfo("Shutting down block dropper")
      }
    }
  }
  blockDropper.start()
  logInfo("MemoryStore started with capacity %s.".format(Utils.memoryBytesToString(maxMemory)))

  def freeMemory: Long = maxMemory - currentMemory

  override def getSize(blockId: String): Long = {
    entries.synchronized {
      entries.get(blockId).size
    }
  }

  override def putBytes(blockId: String, bytes: ByteBuffer, level: StorageLevel) {
    if (level.deserialized) {
      bytes.rewind()
      val values = blockManager.dataDeserialize(bytes)
      val elements = new ArrayBuffer[Any]
      elements ++= values
      val sizeEstimate = SizeEstimator.estimate(elements.asInstanceOf[AnyRef])
      ensureFreeSpace(sizeEstimate)
      val entry = new Entry(elements, sizeEstimate, true)
      entries.synchronized { entries.put(blockId, entry) }
      currentMemory += sizeEstimate
      logInfo("Block %s stored as values to memory (estimated size %d, free %d)".format(
        blockId, sizeEstimate, freeMemory))
    } else {
      val entry = new Entry(bytes, bytes.limit, false)
      ensureFreeSpace(bytes.limit)
      entries.synchronized { entries.put(blockId, entry) }
      currentMemory += bytes.limit
      logInfo("Block %s stored as %d bytes to memory (free %d)".format(
        blockId, bytes.limit, freeMemory))
    }
  }

  override def putValues(
      blockId: String,
      values: Iterator[Any],
      level: StorageLevel,
      returnValues: Boolean)
    : Either[Iterator[Any], ByteBuffer] = {

    if (level.deserialized) {
      val elements = new ArrayBuffer[Any]
      elements ++= values
      val sizeEstimate = SizeEstimator.estimate(elements.asInstanceOf[AnyRef])
      ensureFreeSpace(sizeEstimate)
      val entry = new Entry(elements, sizeEstimate, true)
      entries.synchronized { entries.put(blockId, entry) }
      currentMemory += sizeEstimate
      logInfo("Block %s stored as values to memory (estimated size %d, free %d)".format(
        blockId, sizeEstimate, freeMemory))
      Left(elements.iterator)
    } else {
      val bytes = blockManager.dataSerialize(values)
      ensureFreeSpace(bytes.limit)
      val entry = new Entry(bytes, bytes.limit, false)
      entries.synchronized { entries.put(blockId, entry) }
      currentMemory += bytes.limit
      logInfo("Block %s stored as %d bytes to memory (free %d)".format(
        blockId, bytes.limit, freeMemory))
      Right(bytes)
    }
  }

  override def getBytes(blockId: String): Option[ByteBuffer] = {
    val entry = entries.synchronized {
      entries.get(blockId)
    }
    if (entry == null) {
      None
    } else if (entry.deserialized) {
      Some(blockManager.dataSerialize(entry.value.asInstanceOf[ArrayBuffer[Any]].iterator))
    } else {
      Some(entry.value.asInstanceOf[ByteBuffer].duplicate())   // Doesn't actually copy the data
    }
  }

  override def getValues(blockId: String): Option[Iterator[Any]] = {
    val entry = entries.synchronized {
      entries.get(blockId)
    }
    if (entry == null) {
      None
    } else if (entry.deserialized) {
      Some(entry.value.asInstanceOf[ArrayBuffer[Any]].iterator)
    } else {
      val buffer = entry.value.asInstanceOf[ByteBuffer].duplicate() // Doesn't actually copy data
      Some(blockManager.dataDeserialize(buffer))
    }
  }

  override def remove(blockId: String) {
    entries.synchronized {
      val entry = entries.get(blockId)
      if (entry != null) {
        entries.remove(blockId)
        currentMemory -= entry.size
        logInfo("Block %s of size %d dropped from memory (free %d)".format(
          blockId, entry.size, freeMemory))
      } else {
        logWarning("Block " + blockId + " could not be removed as it doesnt exist")
      }
    }
  }

  override def clear() {
    entries.synchronized {
      entries.clear()
    }
    blockDropper.interrupt()
    logInfo("MemoryStore cleared")
  }

  private def ensureFreeSpace(space: Long) {
    logInfo("ensureFreeSpace(%d) called with curMem=%d, maxMem=%d".format(
      space, currentMemory, maxMemory))

    if (maxMemory - currentMemory < space) {

      val selectedBlocks = new ArrayBuffer[String]()
      var selectedMemory = 0L

      entries.synchronized {
        val iter = entries.entrySet().iterator()
        while (maxMemory - (currentMemory - selectedMemory) < space && iter.hasNext) {
          val pair = iter.next()
          val blockId = pair.getKey
          val entry = pair.getValue
          if (!entry.dropPending) {
            selectedBlocks += blockId
            entry.dropPending = true
          }
          selectedMemory += pair.getValue.size
          logInfo("Block " + blockId + " selected for dropping")
        }
      }

      logInfo("" + selectedBlocks.size + " new blocks selected for dropping, " +
        blocksToDrop.size + " blocks pending")
      var i = 0
      while (i < selectedBlocks.size) {
        blocksToDrop.add(selectedBlocks(i))
        i += 1
      }
      selectedBlocks.clear()
    }
  }
}

