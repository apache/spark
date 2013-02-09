package spark.storage

import java.nio.ByteBuffer

import scala.collection.mutable.StringBuilder
import scala.collection.mutable.ArrayBuffer

import spark._
import spark.network._

private[spark] case class GetBlock(id: String)
private[spark] case class GotBlock(id: String, data: ByteBuffer)
private[spark] case class PutBlock(id: String, data: ByteBuffer, level: StorageLevel) 

private[spark] class BlockMessage() {
  // Un-initialized: typ = 0
  // GetBlock: typ = 1
  // GotBlock: typ = 2
  // PutBlock: typ = 3
  private var typ: Int = BlockMessage.TYPE_NON_INITIALIZED
  private var id: String = null
  private var data: ByteBuffer = null
  private var level: StorageLevel = null
 
  def set(getBlock: GetBlock) {
    typ = BlockMessage.TYPE_GET_BLOCK
    id = getBlock.id
  }

  def set(gotBlock: GotBlock) {
    typ = BlockMessage.TYPE_GOT_BLOCK
    id = gotBlock.id
    data = gotBlock.data
  }

  def set(putBlock: PutBlock) {
    typ = BlockMessage.TYPE_PUT_BLOCK
    id = putBlock.id
    data = putBlock.data
    level = putBlock.level
  }

  def set(buffer: ByteBuffer) {
    val startTime = System.currentTimeMillis
    /*
    println()
    println("BlockMessage: ")
    while(buffer.remaining > 0) {
      print(buffer.get())
    }
    buffer.rewind()
    println()
    println()
    */
    typ = buffer.getInt()
    val idLength = buffer.getInt()
    val idBuilder = new StringBuilder(idLength)
    for (i <- 1 to idLength) {
      idBuilder += buffer.getChar()
    }
    id = idBuilder.toString()
    
    if (typ == BlockMessage.TYPE_PUT_BLOCK) {

      val booleanInt = buffer.getInt()
      val replication = buffer.getInt()
      level = StorageLevel(booleanInt, replication)
      
      val dataLength = buffer.getInt()
      data = ByteBuffer.allocate(dataLength)
      if (dataLength != buffer.remaining) {
        throw new Exception("Error parsing buffer")
      }
      data.put(buffer)
      data.flip()
    } else if (typ == BlockMessage.TYPE_GOT_BLOCK) {

      val dataLength = buffer.getInt()
      data = ByteBuffer.allocate(dataLength)
      if (dataLength != buffer.remaining) {
        throw new Exception("Error parsing buffer")
      }
      data.put(buffer)
      data.flip()
    }

    val finishTime = System.currentTimeMillis
  }

  def set(bufferMsg: BufferMessage) {
    val buffer = bufferMsg.buffers.apply(0)
    buffer.clear()
    set(buffer)
  }
  
  def getType: Int = {
    return typ
  }
  
  def getId: String = {
    return id
  }
  
  def getData: ByteBuffer = {
    return data
  }
  
  def getLevel: StorageLevel = {
    return level
  }
  
  def toBufferMessage: BufferMessage = {
    val startTime = System.currentTimeMillis
    val buffers = new ArrayBuffer[ByteBuffer]()
    var buffer = ByteBuffer.allocate(4 + 4 + id.length() * 2)
    buffer.putInt(typ).putInt(id.length())
    id.foreach((x: Char) => buffer.putChar(x))
    buffer.flip()
    buffers += buffer

    if (typ == BlockMessage.TYPE_PUT_BLOCK) {
      buffer = ByteBuffer.allocate(8).putInt(level.toInt).putInt(level.replication)
      buffer.flip()
      buffers += buffer
      
      buffer = ByteBuffer.allocate(4).putInt(data.remaining)
      buffer.flip()
      buffers += buffer

      buffers += data
    } else if (typ == BlockMessage.TYPE_GOT_BLOCK) {
      buffer = ByteBuffer.allocate(4).putInt(data.remaining)
      buffer.flip()
      buffers += buffer

      buffers += data
    }
    
    /*
    println()
    println("BlockMessage: ")
    buffers.foreach(b => {
      while(b.remaining > 0) {
        print(b.get())
      }
      b.rewind()
    })
    println()
    println()
    */
    val finishTime = System.currentTimeMillis
    return Message.createBufferMessage(buffers)
  }

  override def toString: String = {
    "BlockMessage [type = " + typ + ", id = " + id + ", level = " + level + 
    ", data = " + (if (data != null) data.remaining.toString  else "null") + "]"
  }
}

private[spark] object BlockMessage {
  val TYPE_NON_INITIALIZED: Int = 0
  val TYPE_GET_BLOCK: Int = 1
  val TYPE_GOT_BLOCK: Int = 2
  val TYPE_PUT_BLOCK: Int = 3
 
  def fromBufferMessage(bufferMessage: BufferMessage): BlockMessage = {
    val newBlockMessage = new BlockMessage()
    newBlockMessage.set(bufferMessage)
    newBlockMessage
  }

  def fromByteBuffer(buffer: ByteBuffer): BlockMessage = {
    val newBlockMessage = new BlockMessage()
    newBlockMessage.set(buffer)
    newBlockMessage
  }

  def fromGetBlock(getBlock: GetBlock): BlockMessage = {
    val newBlockMessage = new BlockMessage()
    newBlockMessage.set(getBlock)
    newBlockMessage
  }

  def fromGotBlock(gotBlock: GotBlock): BlockMessage = {
    val newBlockMessage = new BlockMessage()
    newBlockMessage.set(gotBlock)
    newBlockMessage
  }
  
  def fromPutBlock(putBlock: PutBlock): BlockMessage = {
    val newBlockMessage = new BlockMessage()
    newBlockMessage.set(putBlock)
    newBlockMessage
  }

  def main(args: Array[String]) {
    val B = new BlockMessage()
    B.set(new PutBlock("ABC", ByteBuffer.allocate(10), StorageLevel.MEMORY_AND_DISK_SER_2))
    val bMsg = B.toBufferMessage
    val C = new BlockMessage()
    C.set(bMsg)
    
    println(B.getId + " " + B.getLevel)
    println(C.getId + " " + C.getLevel)
  }
}
