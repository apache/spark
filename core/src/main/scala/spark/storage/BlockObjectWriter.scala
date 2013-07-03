package spark.storage

import java.nio.ByteBuffer


/**
 * An interface for writing JVM objects to some underlying storage. This interface allows
 * appending data to an existing block, and can guarantee atomicity in the case of faults
 * as it allows the caller to revert partial writes.
 *
 * This interface does not support concurrent writes.
 */
abstract class BlockObjectWriter(val blockId: String) {

  var closeEventHandler: () => Unit = _

  def open(): BlockObjectWriter

  def close() {
    closeEventHandler()
  }

  def isOpen: Boolean

  def registerCloseEventHandler(handler: () => Unit) {
    closeEventHandler = handler
  }

  /**
   * Flush the partial writes and commit them as a single atomic block. Return the
   * number of bytes written for this commit.
   */
  def commit(): Long

  /**
   * Reverts writes that haven't been flushed yet. Callers should invoke this function
   * when there are runtime exceptions.
   */
  def revertPartialWrites()

  /**
   * Writes an object.
   */
  def write(value: Any)

  /**
   * Size of the valid writes, in bytes.
   */
  def size(): Long
}
