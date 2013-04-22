package spark.network

import spark._

import scala.collection.mutable.{HashMap, Queue, ArrayBuffer}

import java.io._
import java.nio._
import java.nio.channels._
import java.nio.channels.spi._
import java.net._


private[spark]
abstract class Connection(val channel: SocketChannel, val selector: Selector,
                          val remoteConnectionManagerId: ConnectionManagerId) extends Logging {
  def this(channel_ : SocketChannel, selector_ : Selector) = {
    this(channel_, selector_,
         ConnectionManagerId.fromSocketAddress(
            channel_.socket.getRemoteSocketAddress().asInstanceOf[InetSocketAddress]
         ))
  }

  channel.configureBlocking(false)
  channel.socket.setTcpNoDelay(true)
  channel.socket.setReuseAddress(true)
  channel.socket.setKeepAlive(true)
  /*channel.socket.setReceiveBufferSize(32768) */

  var onCloseCallback: Connection => Unit = null
  var onExceptionCallback: (Connection, Exception) => Unit = null
  var onKeyInterestChangeCallback: (Connection, Int) => Unit = null

  val remoteAddress = getRemoteAddress()

  def key() = channel.keyFor(selector)

  def getRemoteAddress() = channel.socket.getRemoteSocketAddress().asInstanceOf[InetSocketAddress]

  def read() { 
    throw new UnsupportedOperationException("Cannot read on connection of type " + this.getClass.toString) 
  }
  
  def write() { 
    throw new UnsupportedOperationException("Cannot write on connection of type " + this.getClass.toString) 
  }

  def close() {
    val k = key()
    if (k != null) {
      k.cancel()
    }
    channel.close()
    callOnCloseCallback()
  }

  def onClose(callback: Connection => Unit) {onCloseCallback = callback}

  def onException(callback: (Connection, Exception) => Unit) {onExceptionCallback = callback}

  def onKeyInterestChange(callback: (Connection, Int) => Unit) {onKeyInterestChangeCallback = callback}

  def callOnExceptionCallback(e: Exception) {
    if (onExceptionCallback != null) {
      onExceptionCallback(this, e)
    } else {
      logError("Error in connection to " + remoteConnectionManagerId + 
        " and OnExceptionCallback not registered", e)
    }
  }
  
  def callOnCloseCallback() {
    if (onCloseCallback != null) {
      onCloseCallback(this)
    } else {
      logWarning("Connection to " + remoteConnectionManagerId + 
        " closed and OnExceptionCallback not registered")
    }

  }

  def changeConnectionKeyInterest(ops: Int) {
    if (onKeyInterestChangeCallback != null) {
      onKeyInterestChangeCallback(this, ops) 
    } else {
      throw new Exception("OnKeyInterestChangeCallback not registered")
    }
  }

  def printRemainingBuffer(buffer: ByteBuffer) {
    val bytes = new Array[Byte](buffer.remaining)
    val curPosition = buffer.position
    buffer.get(bytes)
    bytes.foreach(x => print(x + " "))
    buffer.position(curPosition)
    print(" (" + bytes.size + ")")
  }

  def printBuffer(buffer: ByteBuffer, position: Int, length: Int) {
    val bytes = new Array[Byte](length)
    val curPosition = buffer.position
    buffer.position(position)
    buffer.get(bytes)
    bytes.foreach(x => print(x + " "))
    print(" (" + position + ", " + length + ")")
    buffer.position(curPosition)
  }

}


private[spark] class SendingConnection(val address: InetSocketAddress, selector_ : Selector,
                                       remoteId_ : ConnectionManagerId)
extends Connection(SocketChannel.open, selector_, remoteId_) {

  class Outbox(fair: Int = 0) {
    val messages = new Queue[Message]()
    val defaultChunkSize = 65536  //32768 //16384 
    var nextMessageToBeUsed = 0

    def addMessage(message: Message) {
      messages.synchronized{ 
        /*messages += message*/
        messages.enqueue(message)
        logDebug("Added [" + message + "] to outbox for sending to [" + remoteConnectionManagerId + "]")
      }
    }

    def getChunk(): Option[MessageChunk] = {
      fair match {
        case 0 => getChunkFIFO()
        case 1 => getChunkRR()
        case _ => throw new Exception("Unexpected fairness policy in outbox")
      }
    }

    private def getChunkFIFO(): Option[MessageChunk] = {
      /*logInfo("Using FIFO")*/
      messages.synchronized {
        while (!messages.isEmpty) {
          val message = messages(0)
          val chunk = message.getChunkForSending(defaultChunkSize)
          if (chunk.isDefined) {
            messages += message  // this is probably incorrect, it wont work as fifo
            if (!message.started) {
              logDebug("Starting to send [" + message + "]")
              message.started = true
              message.startTime = System.currentTimeMillis
            }
            return chunk 
          } else {
            /*logInfo("Finished sending [" + message + "] to [" + remoteConnectionManagerId + "]")*/
            message.finishTime = System.currentTimeMillis
            logDebug("Finished sending [" + message + "] to [" + remoteConnectionManagerId +
              "] in "  + message.timeTaken )
          }
        }
      }
      None
    }
    
    private def getChunkRR(): Option[MessageChunk] = {
      messages.synchronized {
        while (!messages.isEmpty) {
          /*nextMessageToBeUsed = nextMessageToBeUsed % messages.size */
          /*val message = messages(nextMessageToBeUsed)*/
          val message = messages.dequeue
          val chunk = message.getChunkForSending(defaultChunkSize)
          if (chunk.isDefined) {
            messages.enqueue(message)
            nextMessageToBeUsed = nextMessageToBeUsed + 1
            if (!message.started) {
              logDebug("Starting to send [" + message + "] to [" + remoteConnectionManagerId + "]")
              message.started = true
              message.startTime = System.currentTimeMillis
            }
            logTrace("Sending chunk from [" + message+ "] to [" + remoteConnectionManagerId + "]")
            return chunk 
          } else {
            message.finishTime = System.currentTimeMillis
            logDebug("Finished sending [" + message + "] to [" + remoteConnectionManagerId +
              "] in "  + message.timeTaken )
          }
        }
      }
      None
    }
  }
  
  val outbox = new Outbox(1) 
  val currentBuffers = new ArrayBuffer[ByteBuffer]()

  /*channel.socket.setSendBufferSize(256 * 1024)*/

  override def getRemoteAddress() = address 

  def send(message: Message) {
    outbox.synchronized {
      outbox.addMessage(message)
      if (channel.isConnected) {
        changeConnectionKeyInterest(SelectionKey.OP_WRITE | SelectionKey.OP_READ)
      }
    }
  }

  def connect() {
    try{
      channel.connect(address)
      channel.register(selector, SelectionKey.OP_CONNECT)
      logInfo("Initiating connection to [" + address + "]")
    } catch {
      case e: Exception => {
        logError("Error connecting to " + address, e)
        callOnExceptionCallback(e)
      }
    }
  }

  def finishConnect() {
    try {
      channel.finishConnect
      changeConnectionKeyInterest(SelectionKey.OP_WRITE | SelectionKey.OP_READ)
      logInfo("Connected to [" + address + "], " + outbox.messages.size + " messages pending")
    } catch {
      case e: Exception => {
        logWarning("Error finishing connection to " + address, e)
        callOnExceptionCallback(e)
      }
    }
  }

  override def write() {
    try{
      while(true) {
        if (currentBuffers.size == 0) {
          outbox.synchronized {
            outbox.getChunk() match {
              case Some(chunk) => {
                currentBuffers ++= chunk.buffers 
              }
              case None => {
                changeConnectionKeyInterest(SelectionKey.OP_READ)
                return
              }
            }
          }
        }
        
        if (currentBuffers.size > 0) {
          val buffer = currentBuffers(0)
          val remainingBytes = buffer.remaining
          val writtenBytes = channel.write(buffer)
          if (buffer.remaining == 0) {
            currentBuffers -= buffer
          }
          if (writtenBytes < remainingBytes) {
            return
          }
        }
      }
    } catch {
      case e: Exception => { 
        logWarning("Error writing in connection to " + remoteConnectionManagerId, e)
        callOnExceptionCallback(e)
        close()
      }
    }
  }

  override def read() {
    // We don't expect the other side to send anything; so, we just read to detect an error or EOF.
    try {
      val length = channel.read(ByteBuffer.allocate(1))
      if (length == -1) { // EOF
        close()
      } else if (length > 0) {
        logWarning("Unexpected data read from SendingConnection to " + remoteConnectionManagerId)
      }
    } catch {
      case e: Exception =>
        logError("Exception while reading SendingConnection to " + remoteConnectionManagerId, e)
        callOnExceptionCallback(e)
        close()
    }
  }
}


private[spark] class ReceivingConnection(channel_ : SocketChannel, selector_ : Selector) 
extends Connection(channel_, selector_) {
  
  class Inbox() {
    val messages = new HashMap[Int, BufferMessage]()
    
    def getChunk(header: MessageChunkHeader): Option[MessageChunk] = {
      
      def createNewMessage: BufferMessage = {
        val newMessage = Message.create(header).asInstanceOf[BufferMessage]
        newMessage.started = true
        newMessage.startTime = System.currentTimeMillis
        logDebug("Starting to receive [" + newMessage + "] from [" + remoteConnectionManagerId + "]") 
        messages += ((newMessage.id, newMessage))
        newMessage
      }
      
      val message = messages.getOrElseUpdate(header.id, createNewMessage)
      logTrace("Receiving chunk of [" + message + "] from [" + remoteConnectionManagerId + "]")
      message.getChunkForReceiving(header.chunkSize)
    }
    
    def getMessageForChunk(chunk: MessageChunk): Option[BufferMessage] = {
      messages.get(chunk.header.id) 
    }

    def removeMessage(message: Message) {
      messages -= message.id
    }
  }
  
  val inbox = new Inbox()
  val headerBuffer: ByteBuffer = ByteBuffer.allocate(MessageChunkHeader.HEADER_SIZE)
  var onReceiveCallback: (Connection , Message) => Unit = null
  var currentChunk: MessageChunk = null

  channel.register(selector, SelectionKey.OP_READ)

  override def read() {
    try {
      while (true) {
        if (currentChunk == null) {
          val headerBytesRead = channel.read(headerBuffer)
          if (headerBytesRead == -1) {
            close()
            return
          }
          if (headerBuffer.remaining > 0) {
            return
          }
          headerBuffer.flip
          if (headerBuffer.remaining != MessageChunkHeader.HEADER_SIZE) {
            throw new Exception("Unexpected number of bytes (" + headerBuffer.remaining + ") in the header")
          }
          val header = MessageChunkHeader.create(headerBuffer)
          headerBuffer.clear()
          header.typ match {
            case Message.BUFFER_MESSAGE => {
              if (header.totalSize == 0) {
                if (onReceiveCallback != null) {
                  onReceiveCallback(this, Message.create(header))
                }
                currentChunk = null
                return
              } else {
                currentChunk = inbox.getChunk(header).orNull
              }
            }
            case _ => throw new Exception("Message of unknown type received")
          }
        }
        
        if (currentChunk == null) throw new Exception("No message chunk to receive data")
       
        val bytesRead = channel.read(currentChunk.buffer)
        if (bytesRead == 0) {
          return
        } else if (bytesRead == -1) {
          close()
          return
        }

        /*logDebug("Read " + bytesRead + " bytes for the buffer")*/
        
        if (currentChunk.buffer.remaining == 0) {
          /*println("Filled buffer at " + System.currentTimeMillis)*/
          val bufferMessage = inbox.getMessageForChunk(currentChunk).get
          if (bufferMessage.isCompletelyReceived) {
            bufferMessage.flip
            bufferMessage.finishTime = System.currentTimeMillis
            logDebug("Finished receiving [" + bufferMessage + "] from [" + remoteConnectionManagerId + "] in " + bufferMessage.timeTaken) 
            if (onReceiveCallback != null) {
              onReceiveCallback(this, bufferMessage)
            }
            inbox.removeMessage(bufferMessage)
          }
          currentChunk = null
        }
      }
    } catch {
      case e: Exception  => { 
        logWarning("Error reading from connection to " + remoteConnectionManagerId, e)
        callOnExceptionCallback(e)
        close()
      }
    }
  }
  
  def onReceive(callback: (Connection, Message) => Unit) {onReceiveCallback = callback}
}
