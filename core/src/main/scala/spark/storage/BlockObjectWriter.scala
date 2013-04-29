package spark.storage

import java.nio.ByteBuffer


abstract class BlockObjectWriter(val blockId: String) {

  // TODO(rxin): What if there is an exception when the block is being written out?

  var closeEventHandler: () => Unit = _

  def registerCloseEventHandler(handler: () => Unit) {
    closeEventHandler = handler
  }

  def write(value: Any)

  def writeAll(value: Iterator[Any]) {
    value.foreach(write)
  }

  def close() {
    closeEventHandler()
  }

  def size(): Long
}
