package spark.streaming.dstream

import spark.Logging
import spark.storage.StorageLevel
import spark.streaming.StreamingContext

import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.nio.channels.{ReadableByteChannel, SocketChannel}
import java.io.EOFException
import java.util.concurrent.ArrayBlockingQueue


/**
 * An input stream that reads blocks of serialized objects from a given network address.
 * The blocks will be inserted directly into the block store. This is the fastest way to get
 * data into Spark Streaming, though it requires the sender to batch data and serialize it
 * in the format that the system is configured with.
 */
private[streaming]
class RawInputDStream[T: ClassManifest](
    @transient ssc_ : StreamingContext,
    host: String,
    port: Int,
    storageLevel: StorageLevel
  ) extends NetworkInputDStream[T](ssc_ ) with Logging {

  def getReceiver(): NetworkReceiver[T] = {
    new RawNetworkReceiver(host, port, storageLevel).asInstanceOf[NetworkReceiver[T]]
  }
}

private[streaming]
class RawNetworkReceiver(host: String, port: Int, storageLevel: StorageLevel)
  extends NetworkReceiver[Any] {

  var blockPushingThread: Thread = null

  override def getLocationPreference = None

  def onStart() {
    // Open a socket to the target address and keep reading from it
    logInfo("Connecting to " + host + ":" + port)
    val channel = SocketChannel.open()
    channel.configureBlocking(true)
    channel.connect(new InetSocketAddress(host, port))
    logInfo("Connected to " + host + ":" + port)

    val queue = new ArrayBlockingQueue[ByteBuffer](2)

    blockPushingThread = new Thread {
      setDaemon(true)
      override def run() {
        var nextBlockNumber = 0
        while (true) {
          val buffer = queue.take()
          val blockId = "input-" + streamId + "-" + nextBlockNumber
          nextBlockNumber += 1
          pushBlock(blockId, buffer, null, storageLevel)
        }
      }
    }
    blockPushingThread.start()

    val lengthBuffer = ByteBuffer.allocate(4)
    while (true) {
      lengthBuffer.clear()
      readFully(channel, lengthBuffer)
      lengthBuffer.flip()
      val length = lengthBuffer.getInt()
      val dataBuffer = ByteBuffer.allocate(length)
      readFully(channel, dataBuffer)
      dataBuffer.flip()
      logInfo("Read a block with " + length + " bytes")
      queue.put(dataBuffer)
    }
  }

  def onStop() {
    if (blockPushingThread != null) blockPushingThread.interrupt()
  }

  /** Read a buffer fully from a given Channel */
  private def readFully(channel: ReadableByteChannel, dest: ByteBuffer) {
    while (dest.position < dest.limit) {
      if (channel.read(dest) == -1) {
        throw new EOFException("End of channel")
      }
    }
  }
}
