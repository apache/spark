package spark.network

import spark._

import scala.collection.mutable.ArrayBuffer

import java.nio.ByteBuffer
import java.net.InetAddress
import java.net.InetSocketAddress
import storage.BlockManager

private[spark] class MessageChunkHeader(
    val typ: Long,
    val id: Int,
    val totalSize: Int,
    val chunkSize: Int,
    val other: Int,
    val address: InetSocketAddress) {
  lazy val buffer = {
    val ip = address.getAddress.getAddress() 
    val port = address.getPort()
    ByteBuffer.
      allocate(MessageChunkHeader.HEADER_SIZE).
      putLong(typ).
      putInt(id).
      putInt(totalSize).
      putInt(chunkSize).
      putInt(other).
      putInt(ip.size).
      put(ip).
      putInt(port).
      position(MessageChunkHeader.HEADER_SIZE).
      flip.asInstanceOf[ByteBuffer]
  }

  override def toString = "" + this.getClass.getSimpleName + ":" + id + " of type " + typ + 
      " and sizes " + totalSize + " / " + chunkSize + " bytes"
}

private[spark] class MessageChunk(val header: MessageChunkHeader, val buffer: ByteBuffer) {
  val size = if (buffer == null) 0 else buffer.remaining
  lazy val buffers = {
    val ab = new ArrayBuffer[ByteBuffer]()
    ab += header.buffer
    if (buffer != null) { 
      ab += buffer
    }
    ab
  }

  override def toString = "" + this.getClass.getSimpleName + " (id = " + header.id + ", size = " + size + ")"
}

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

private[spark] class BufferMessage(id_ : Int, val buffers: ArrayBuffer[ByteBuffer], var ackId: Int) 
extends Message(Message.BUFFER_MESSAGE, id_) {
  
  val initialSize = currentSize() 
  var gotChunkForSendingOnce = false
  
  def size = initialSize 

  def currentSize() = {
    if (buffers == null || buffers.isEmpty) {
      0 
    } else {
      buffers.map(_.remaining).reduceLeft(_ + _)
    }
  }
  
  def getChunkForSending(maxChunkSize: Int): Option[MessageChunk] = {
    if (maxChunkSize <= 0) {
      throw new Exception("Max chunk size is " + maxChunkSize)
    }

    if (size == 0 && gotChunkForSendingOnce == false) {
      val newChunk = new MessageChunk(new MessageChunkHeader(typ, id, 0, 0, ackId, senderAddress), null)
      gotChunkForSendingOnce = true
      return Some(newChunk)
    }

    while(!buffers.isEmpty) {
      val buffer = buffers(0)
      if (buffer.remaining == 0) {
        BlockManager.dispose(buffer)
        buffers -= buffer
      } else {
        val newBuffer = if (buffer.remaining <= maxChunkSize) {
          buffer.duplicate()
        } else {
          buffer.slice().limit(maxChunkSize).asInstanceOf[ByteBuffer]
        }
        buffer.position(buffer.position + newBuffer.remaining)
        val newChunk = new MessageChunk(new MessageChunkHeader(
            typ, id, size, newBuffer.remaining, ackId, senderAddress), newBuffer)
        gotChunkForSendingOnce = true
        return Some(newChunk)
      }
    }
    None
  }

  def getChunkForReceiving(chunkSize: Int): Option[MessageChunk] = {
    // STRONG ASSUMPTION: BufferMessage created when receiving data has ONLY ONE data buffer
    if (buffers.size > 1) {
      throw new Exception("Attempting to get chunk from message with multiple data buffers")
    }
    val buffer = buffers(0)
    if (buffer.remaining > 0) {
      if (buffer.remaining < chunkSize) {
        throw new Exception("Not enough space in data buffer for receiving chunk")
      }
      val newBuffer = buffer.slice().limit(chunkSize).asInstanceOf[ByteBuffer]
      buffer.position(buffer.position + newBuffer.remaining)
      val newChunk = new MessageChunk(new MessageChunkHeader(
          typ, id, size, newBuffer.remaining, ackId, senderAddress), newBuffer)
      return Some(newChunk)
    }
    None 
  }

  def flip() {
    buffers.foreach(_.flip)
  }

  def hasAckId() = (ackId != 0)

  def isCompletelyReceived() = !buffers(0).hasRemaining
  
  override def toString = {
    if (hasAckId) {
      "BufferAckMessage(aid = " + ackId + ", id = " + id + ", size = " + size + ")"
    } else {
      "BufferMessage(id = " + id + ", size = " + size + ")"
    }
  }
}

private[spark] object MessageChunkHeader {
  val HEADER_SIZE = 40 
  
  def create(buffer: ByteBuffer): MessageChunkHeader = {
    if (buffer.remaining != HEADER_SIZE) {
      throw new IllegalArgumentException("Cannot convert buffer data to Message")
    }
    val typ = buffer.getLong()
    val id = buffer.getInt()
    val totalSize = buffer.getInt()
    val chunkSize = buffer.getInt()
    val other = buffer.getInt()
    val ipSize = buffer.getInt()
    val ipBytes = new Array[Byte](ipSize)
    buffer.get(ipBytes)
    val ip = InetAddress.getByAddress(ipBytes)
    val port = buffer.getInt()
    new MessageChunkHeader(typ, id, totalSize, chunkSize, other, new InetSocketAddress(ip, port))
  }
}

private[spark] object Message {
  val BUFFER_MESSAGE = 1111111111L

  var lastId = 1

  def getNewId() = synchronized {
    lastId += 1
    if (lastId == 0) lastId += 1
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
  
  def createBufferMessage(ackId: Int): BufferMessage = createBufferMessage(new Array[ByteBuffer](0), ackId)

  def create(header: MessageChunkHeader): Message = {
    val newMessage: Message = header.typ match {
      case BUFFER_MESSAGE => new BufferMessage(header.id, ArrayBuffer(ByteBuffer.allocate(header.totalSize)), header.other)
    }
    newMessage.senderAddress = header.address
    newMessage
  }
}
