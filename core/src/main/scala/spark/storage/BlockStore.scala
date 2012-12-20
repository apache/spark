package spark.storage

import java.nio.ByteBuffer
import scala.collection.mutable.ArrayBuffer

import spark.Logging

/**
 * Abstract class to store blocks
 */
private[spark]
abstract class BlockStore(val blockManager: BlockManager) extends Logging {
  def putBytes(blockId: String, bytes: ByteBuffer, level: StorageLevel)

  /**
   * Put in a block and, possibly, also return its content as either bytes or another Iterator.
   * This is used to efficiently write the values to multiple locations (e.g. for replication).
   *
   * @return a PutResult that contains the size of the data, as well as the values put if
   *         returnValues is true (if not, the result's data field can be null)
   */
  def putValues(blockId: String, values: ArrayBuffer[Any], level: StorageLevel, 
    returnValues: Boolean) : PutResult

  /**
   * Return the size of a block in bytes.
   */
  def getSize(blockId: String): Long

  def getBytes(blockId: String): Option[ByteBuffer]

  def getValues(blockId: String): Option[Iterator[Any]]

  /**
   * Remove a block, if it exists.
   * @param blockId the block to remove.
   * @return True if the block was found and removed, False otherwise.
   */
  def remove(blockId: String): Boolean

  def contains(blockId: String): Boolean

  def clear() { }
}
