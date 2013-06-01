package spark.network

import java.nio.ByteBuffer
import java.net.InetSocketAddress

import scala.collection.mutable.ArrayBuffer


private[spark] abstract class Message(val typ: Long, val id: Int) {
  var senderAddress: InetSocketAddress = null
  var started = false
  var startTime = -1L
  var finishTime = -1L

  def size: Int

  def getChunkForSending(maxChunkSize: Int): Option[MessageChunk]

  def getChunkForReceiving(chunkSize: Int): Option[MessageChunk]

  def timeTaken(): String = (finishTime - startTime).toString + " ms"

  override def toString = this.getClass.getSimpleName + "(id = " + id + ", size = " + size + ")"
}


private[spark] object Message {
  val BUFFER_MESSAGE = 1111111111L

  var lastId = 1

  def getNewId() = synchronized {
    lastId += 1
    if (lastId == 0) {
      lastId += 1
    }
    lastId
  }

  def createBufferMessage(dataBuffers: Seq[ByteBuffer], ackId: Int): BufferMessage = {
    if (dataBuffers == null) {
      return new BufferMessage(getNewId(), new ArrayBuffer[ByteBuffer], ackId)
    }
    if (dataBuffers.exists(_ == null)) {
      throw new Exception("Attempting to create buffer message with null buffer")
    }
    return new BufferMessage(getNewId(), new ArrayBuffer[ByteBuffer] ++= dataBuffers, ackId)
  }

  def createBufferMessage(dataBuffers: Seq[ByteBuffer]): BufferMessage =
    createBufferMessage(dataBuffers, 0)

  def createBufferMessage(dataBuffer: ByteBuffer, ackId: Int): BufferMessage = {
    if (dataBuffer == null) {
      return createBufferMessage(Array(ByteBuffer.allocate(0)), ackId)
    } else {
      return createBufferMessage(Array(dataBuffer), ackId)
    }
  }

  def createBufferMessage(dataBuffer: ByteBuffer): BufferMessage =
    createBufferMessage(dataBuffer, 0)

  def createBufferMessage(ackId: Int): BufferMessage = {
    createBufferMessage(new Array[ByteBuffer](0), ackId)
  }

  def create(header: MessageChunkHeader): Message = {
    val newMessage: Message = header.typ match {
      case BUFFER_MESSAGE => new BufferMessage(header.id,
        ArrayBuffer(ByteBuffer.allocate(header.totalSize)), header.other)
    }
    newMessage.senderAddress = header.address
    newMessage
  }
}
