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

  logInfo("MemoryStore started with capacity %s.".format(Utils.memoryBytesToString(maxMemory)))

  def freeMemory: Long = maxMemory - currentMemory

  override def getSize(blockId: String): Long = {
    synchronized {
      entries.get(blockId).size
    }
  }

  override def putBytes(blockId: String, bytes: ByteBuffer, level: StorageLevel) {
    if (level.deserialized) {
      bytes.rewind()
      val values = blockManager.dataDeserialize(blockId, bytes)
      val elements = new ArrayBuffer[Any]
      elements ++= values
      val sizeEstimate = SizeEstimator.estimate(elements.asInstanceOf[AnyRef])
      tryToPut(blockId, elements, sizeEstimate, true)
    } else {
      val entry = new Entry(bytes, bytes.limit, false)
      ensureFreeSpace(blockId, bytes.limit)
      synchronized { entries.put(blockId, entry) }
      tryToPut(blockId, bytes, bytes.limit, false)
    }
  }

  override def putValues(
      blockId: String,
      values: ArrayBuffer[Any],
      level: StorageLevel,
      returnValues: Boolean)
    : PutResult = {

    if (level.deserialized) {
      val sizeEstimate = SizeEstimator.estimate(values.asInstanceOf[AnyRef])
      tryToPut(blockId, values, sizeEstimate, true)
      PutResult(sizeEstimate, Left(values.iterator))
    } else {
      val bytes = blockManager.dataSerialize(blockId, values.iterator)
      tryToPut(blockId, bytes, bytes.limit, false)
      PutResult(bytes.limit(), Right(bytes))
    }
  }

  override def getBytes(blockId: String): Option[ByteBuffer] = {
    val entry = synchronized {
      entries.get(blockId)
    }
    if (entry == null) {
      None
    } else if (entry.deserialized) {
      Some(blockManager.dataSerialize(blockId, entry.value.asInstanceOf[ArrayBuffer[Any]].iterator))
    } else {
      Some(entry.value.asInstanceOf[ByteBuffer].duplicate())   // Doesn't actually copy the data
    }
  }

  override def getValues(blockId: String): Option[Iterator[Any]] = {
    val entry = synchronized {
      entries.get(blockId)
    }
    if (entry == null) {
      None
    } else if (entry.deserialized) {
      Some(entry.value.asInstanceOf[ArrayBuffer[Any]].iterator)
    } else {
      val buffer = entry.value.asInstanceOf[ByteBuffer].duplicate() // Doesn't actually copy data
      Some(blockManager.dataDeserialize(blockId, buffer))
    }
  }

  override def remove(blockId: String) {
    synchronized {
      val entry = entries.get(blockId)
      if (entry != null) {
        entries.remove(blockId)
        currentMemory -= entry.size
        logInfo("Block %s of size %d dropped from memory (free %d)".format(
          blockId, entry.size, freeMemory))
      } else {
        logWarning("Block " + blockId + " could not be removed as it does not exist")
      }
    }
  }

  override def clear() {
    synchronized {
      entries.clear()
    }
    logInfo("MemoryStore cleared")
  }

  /**
   * Return the RDD ID that a given block ID is from, or null if it is not an RDD block.
   */
  private def getRddId(blockId: String): String = {
    if (blockId.startsWith("rdd_")) {
      blockId.split('_')(1)
    } else {
      null
    }
  }

  /**
   * Try to put in a set of values, if we can free up enough space. The value should either be
   * an ArrayBuffer if deserialized is true or a ByteBuffer otherwise. Its (possibly estimated)
   * size must also be passed by the caller.
   */
  private def tryToPut(blockId: String, value: Any, size: Long, deserialized: Boolean): Boolean = {
    synchronized {
      if (ensureFreeSpace(blockId, size)) {
        val entry = new Entry(value, size, deserialized)
        entries.put(blockId, entry)
        currentMemory += size
        if (deserialized) {
          logInfo("Block %s stored as values to memory (estimated size %s, free %s)".format(
            blockId, Utils.memoryBytesToString(size), Utils.memoryBytesToString(freeMemory)))
        } else {
          logInfo("Block %s stored as bytes to memory (size %s, free %s)".format(
            blockId, Utils.memoryBytesToString(size), Utils.memoryBytesToString(freeMemory)))
        }
        true
      } else {
        // Tell the block manager that we couldn't put it in memory so that it can drop it to
        // disk if the block allows disk storage.
        val data = if (deserialized) {
          Left(value.asInstanceOf[ArrayBuffer[Any]])
        } else {
          Right(value.asInstanceOf[ByteBuffer].duplicate())
        }
        blockManager.dropFromMemory(blockId, data)
        false
      }
    }
  }

  /**
   * Tries to free up a given amount of space to store a particular block, but can fail and return
   * false if either the block is bigger than our memory or it would require replacing another
   * block from the same RDD (which leads to a wasteful cyclic replacement pattern for RDDs that
   * don't fit into memory that we want to avoid).
   *
   * Assumes that a lock on the MemoryStore is held by the caller. (Otherwise, the freed space
   * might fill up before the caller puts in their new value.)
   */
  private def ensureFreeSpace(blockIdToAdd: String, space: Long): Boolean = {
    logInfo("ensureFreeSpace(%d) called with curMem=%d, maxMem=%d".format(
      space, currentMemory, maxMemory))

    if (space > maxMemory) {
      logInfo("Will not store " + blockIdToAdd + " as it is larger than our memory limit")
      return false
    }

    // TODO: This should relinquish the lock on the MemoryStore while flushing out old blocks
    // in order to allow parallelism in writing to disk
    if (maxMemory - currentMemory < space) {
      val rddToAdd = getRddId(blockIdToAdd)
      val selectedBlocks = new ArrayBuffer[String]()
      var selectedMemory = 0L

      val iterator = entries.entrySet().iterator()
      while (maxMemory - (currentMemory - selectedMemory) < space && iterator.hasNext) {
        val pair = iterator.next()
        val blockId = pair.getKey
        if (rddToAdd != null && rddToAdd == getRddId(blockId)) {
          logInfo("Will not store " + blockIdToAdd + " as it would require dropping another " +
            "block from the same RDD")
          return false
        }
        selectedBlocks += blockId
        selectedMemory += pair.getValue.size
      }

      if (maxMemory - (currentMemory - selectedMemory) >= space) {
        logInfo(selectedBlocks.size + " blocks selected for dropping")
        for (blockId <- selectedBlocks) {
          val entry = entries.get(blockId)
          val data = if (entry.deserialized) {
            Left(entry.value.asInstanceOf[ArrayBuffer[Any]])
          } else {
            Right(entry.value.asInstanceOf[ByteBuffer].duplicate())
          }
          blockManager.dropFromMemory(blockId, data)
        }
        return true
      } else {
        return false
      }
    }
    return true
  }

  override def contains(blockId: String): Boolean = {
    synchronized { entries.containsKey(blockId) }
  }
}

