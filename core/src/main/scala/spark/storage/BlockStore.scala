package spark.storage

import java.nio.ByteBuffer

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
   * @return the values put if returnValues is true, or null otherwise
   */
  def putValues(blockId: String, values: Iterator[Any], level: StorageLevel, returnValues: Boolean)
    : Either[Iterator[Any], ByteBuffer]

  /**
   * Return the size of a block.
   */
  def getSize(blockId: String): Long

  def getBytes(blockId: String): Option[ByteBuffer]

  def getValues(blockId: String): Option[Iterator[Any]]

  def remove(blockId: String)

  def contains(blockId: String): Boolean

  def clear() { }
}
