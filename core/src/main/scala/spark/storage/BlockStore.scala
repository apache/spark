package spark.storage

import java.nio.ByteBuffer

import spark.Logging

/**
 * Abstract class to store blocks
 */
abstract class BlockStore(val blockManager: BlockManager) extends Logging {
  def putBytes(blockId: String, bytes: ByteBuffer, level: StorageLevel)

  /**
   * Put in a block and return its content as either bytes or another Iterator. This is used
   * to efficiently write the values to multiple locations (e.g. for replication).
   */
  def putValues(blockId: String, values: Iterator[Any], level: StorageLevel)
    : Either[Iterator[Any], ByteBuffer]

  /**
   * Return the size of a block.
   */
  def getSize(blockId: String): Long

  def getBytes(blockId: String): Option[ByteBuffer]

  def getValues(blockId: String): Option[Iterator[Any]]

  def remove(blockId: String)

  def dataSerialize(values: Iterator[Any]): ByteBuffer = blockManager.dataSerialize(values)

  def dataDeserialize(bytes: ByteBuffer): Iterator[Any] = blockManager.dataDeserialize(bytes)

  def clear() { }
}
