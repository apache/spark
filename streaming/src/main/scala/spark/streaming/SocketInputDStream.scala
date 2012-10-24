package spark.streaming

import spark.streaming.util.{RecurringTimer, SystemClock}
import spark.storage.StorageLevel

import java.io.{EOFException, DataInputStream, BufferedInputStream, InputStream}
import java.net.Socket
import java.util.concurrent.ArrayBlockingQueue

import scala.collection.mutable.ArrayBuffer

class SocketInputDStream[T: ClassManifest](
    @transient ssc_ : StreamingContext,
    host: String,
    port: Int,
    bytesToObjects: InputStream => Iterator[T],
    storageLevel: StorageLevel
  ) extends NetworkInputDStream[T](ssc_) {

  def createReceiver(): NetworkReceiver[T] = {
    new SocketReceiver(id, host, port, bytesToObjects, storageLevel)
  }
}


class SocketReceiver[T: ClassManifest](
    streamId: Int,
    host: String,
    port: Int,
    bytesToObjects: InputStream => Iterator[T],
    storageLevel: StorageLevel
  ) extends NetworkReceiver[T](streamId) {

  lazy protected val dataHandler = new DataHandler(this)

  protected def onStart() {
    logInfo("Connecting to " + host + ":" + port)
    val socket = new Socket(host, port)
    logInfo("Connected to " + host + ":" + port)
    dataHandler.start()
    val iterator = bytesToObjects(socket.getInputStream())
    while(iterator.hasNext) {
      val obj = iterator.next
      dataHandler += obj
    }
  }

  protected def onStop() {
    dataHandler.stop()
  }

  /**
   * This is a helper object that manages the data received from the socket. It divides
   * the object received into small batches of 100s of milliseconds, pushes them as
   * blocks into the block manager and reports the block IDs to the network input
   * tracker. It starts two threads, one to periodically start a new batch and prepare
   * the previous batch of as a block, the other to push the blocks into the block
   * manager.
   */
  class DataHandler(receiver: NetworkReceiver[T]) extends Serializable {
    case class Block(id: String, iterator: Iterator[T])

    val clock = new SystemClock()
    val blockInterval = 200L
    val blockIntervalTimer = new RecurringTimer(clock, blockInterval, updateCurrentBuffer)
    val blockStorageLevel = storageLevel
    val blocksForPushing = new ArrayBlockingQueue[Block](1000)
    val blockPushingThread = new Thread() { override def run() { keepPushingBlocks() } }

    var currentBuffer = new ArrayBuffer[T]

    def start() {
      blockIntervalTimer.start()
      blockPushingThread.start()
      logInfo("Data handler started")
    }

    def stop() {
      blockIntervalTimer.stop()
      blockPushingThread.interrupt()
      logInfo("Data handler stopped")
    }

    def += (obj: T) {
      currentBuffer += obj
    }

    def updateCurrentBuffer(time: Long) {
      try {
        val newBlockBuffer = currentBuffer
        currentBuffer = new ArrayBuffer[T]
        if (newBlockBuffer.size > 0) {
          val blockId = "input-" + streamId + "- " + (time - blockInterval)
          val newBlock = new Block(blockId, newBlockBuffer.toIterator)
          blocksForPushing.add(newBlock)
        }
      } catch {
        case ie: InterruptedException =>
          logInfo("Block interval timer thread interrupted")
        case e: Exception =>
          receiver.stop()
      }
    }

    def keepPushingBlocks() {
      logInfo("Block pushing thread started")
      try {
        while(true) {
          val block = blocksForPushing.take()
          pushBlock(block.id, block.iterator, storageLevel)
        }
      } catch {
        case ie: InterruptedException =>
          logInfo("Block pushing thread interrupted")
        case e: Exception =>
          receiver.stop()
      }
    }
  }
}


object SocketReceiver  {

  /**
   * This methods translates the data from an inputstream (say, from a socket)
   * to '\n' delimited strings and returns an iterator to access the strings.
   */
  def bytesToLines(inputStream: InputStream): Iterator[String] = {
    val bufferedInputStream = new BufferedInputStream(inputStream)
    val dataInputStream = new DataInputStream(bufferedInputStream)

    val iterator = new Iterator[String] {
      var gotNext = false
      var finished = false
      var nextValue: String = null

      private def getNext() {
        try {
          nextValue = dataInputStream.readLine()
          if (nextValue != null) {
            println("[" + nextValue + "]")
          } else {
            gotNext = false
          }
        } catch {
          case eof: EOFException =>
            finished = true
        }
        gotNext = true
      }

      override def hasNext: Boolean = {
        if (!gotNext) {
          getNext()
        }
        if (finished) {
          dataInputStream.close()
        }
        !finished
      }

      override def next(): String = {
        if (!gotNext) {
          getNext()
        }
        if (finished) {
          throw new NoSuchElementException("End of stream")
        }
        gotNext = false
        nextValue
      }
    }
    iterator
  }
}
