/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.network.nio

import java.net._
import java.nio._
import java.nio.channels._
import java.util.LinkedList

import org.apache.spark._

import scala.collection.mutable.{ArrayBuffer, HashMap}

private[nio]
abstract class Connection(val channel: SocketChannel, val selector: Selector,
    val socketRemoteConnectionManagerId: ConnectionManagerId, val connectionId: ConnectionId,
    val securityMgr: SecurityManager)
  extends Logging {

  var sparkSaslServer: SparkSaslServer = null
  var sparkSaslClient: SparkSaslClient = null

  def this(channel_ : SocketChannel, selector_ : Selector, id_ : ConnectionId,
      securityMgr_ : SecurityManager) = {
    this(channel_, selector_,
      ConnectionManagerId.fromSocketAddress(
        channel_.socket.getRemoteSocketAddress.asInstanceOf[InetSocketAddress]),
        id_, securityMgr_)
  }

  channel.configureBlocking(false)
  channel.socket.setTcpNoDelay(true)
  channel.socket.setReuseAddress(true)
  channel.socket.setKeepAlive(true)
  /* channel.socket.setReceiveBufferSize(32768) */

  @volatile private var closed = false
  var onCloseCallback: Connection => Unit = null
  var onExceptionCallback: (Connection, Exception) => Unit = null
  var onKeyInterestChangeCallback: (Connection, Int) => Unit = null

  val remoteAddress = getRemoteAddress()

  def isSaslComplete(): Boolean

  def resetForceReregister(): Boolean

  // Read channels typically do not register for write and write does not for read
  // Now, we do have write registering for read too (temporarily), but this is to detect
  // channel close NOT to actually read/consume data on it !
  // How does this work if/when we move to SSL ?

  // What is the interest to register with selector for when we want this connection to be selected
  def registerInterest()

  // What is the interest to register with selector for when we want this connection to
  // be de-selected
  // Traditionally, 0 - but in our case, for example, for close-detection on SendingConnection hack,
  // it will be SelectionKey.OP_READ (until we fix it properly)
  def unregisterInterest()

  // On receiving a read event, should we change the interest for this channel or not ?
  // Will be true for ReceivingConnection, false for SendingConnection.
  def changeInterestForRead(): Boolean

  private def disposeSasl() {
    if (sparkSaslServer != null) {
      sparkSaslServer.dispose()
    }

    if (sparkSaslClient != null) {
      sparkSaslClient.dispose()
    }
  }

  // On receiving a write event, should we change the interest for this channel or not ?
  // Will be false for ReceivingConnection, true for SendingConnection.
  // Actually, for now, should not get triggered for ReceivingConnection
  def changeInterestForWrite(): Boolean

  def getRemoteConnectionManagerId(): ConnectionManagerId = {
    socketRemoteConnectionManagerId
  }

  def key() = channel.keyFor(selector)

  def getRemoteAddress() = channel.socket.getRemoteSocketAddress().asInstanceOf[InetSocketAddress]

  // Returns whether we have to register for further reads or not.
  def read(): Boolean = {
    throw new UnsupportedOperationException(
      "Cannot read on connection of type " + this.getClass.toString)
  }

  // Returns whether we have to register for further writes or not.
  def write(): Boolean = {
    throw new UnsupportedOperationException(
      "Cannot write on connection of type " + this.getClass.toString)
  }

  def close() {
    closed = true
    val k = key()
    if (k != null) {
      k.cancel()
    }
    channel.close()
    disposeSasl()
    callOnCloseCallback()
  }

  protected def isClosed: Boolean = closed

  def onClose(callback: Connection => Unit) {
    onCloseCallback = callback
  }

  def onException(callback: (Connection, Exception) => Unit) {
    onExceptionCallback = callback
  }

  def onKeyInterestChange(callback: (Connection, Int) => Unit) {
    onKeyInterestChangeCallback = callback
  }

  def callOnExceptionCallback(e: Exception) {
    if (onExceptionCallback != null) {
      onExceptionCallback(this, e)
    } else {
      logError("Error in connection to " + getRemoteConnectionManagerId() +
        " and OnExceptionCallback not registered", e)
    }
  }

  def callOnCloseCallback() {
    if (onCloseCallback != null) {
      onCloseCallback(this)
    } else {
      logWarning("Connection to " + getRemoteConnectionManagerId() +
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


private[nio]
class SendingConnection(val address: InetSocketAddress, selector_ : Selector,
    remoteId_ : ConnectionManagerId, id_ : ConnectionId,
    securityMgr_ : SecurityManager)
  extends Connection(SocketChannel.open, selector_, remoteId_, id_, securityMgr_) {

  def isSaslComplete(): Boolean = {
    if (sparkSaslClient != null) sparkSaslClient.isComplete() else false
  }

  private class Outbox {
    val messages = new LinkedList[Message]()
    val defaultChunkSize = 65536
    var nextMessageToBeUsed = 0

    def addMessage(message: Message) {
      messages.synchronized {
        messages.add(message)
        logDebug("Added [" + message + "] to outbox for sending to " +
          "[" + getRemoteConnectionManagerId() + "]")
      }
    }

    def getChunk(): Option[MessageChunk] = {
      messages.synchronized {
        while (!messages.isEmpty) {
          /* nextMessageToBeUsed = nextMessageToBeUsed % messages.size */
          /* val message = messages(nextMessageToBeUsed) */

          val message = if (securityMgr.isAuthenticationEnabled() && !isSaslComplete()) {
            // only allow sending of security messages until sasl is complete
            var pos = 0
            var securityMsg: Message = null
            while (pos < messages.size() && securityMsg == null) {
              if (messages.get(pos).isSecurityNeg) {
                securityMsg = messages.remove(pos)
              }
              pos = pos + 1
            }
            // didn't find any security messages and auth isn't completed so return
            if (securityMsg == null) return None
            securityMsg
          } else {
            messages.removeFirst()
          }

          val chunk = message.getChunkForSending(defaultChunkSize)
          if (chunk.isDefined) {
            messages.add(message)
            nextMessageToBeUsed = nextMessageToBeUsed + 1
            if (!message.started) {
              logDebug(
                "Starting to send [" + message + "] to [" + getRemoteConnectionManagerId() + "]")
              message.started = true
              message.startTime = System.currentTimeMillis
            }
            logTrace(
              "Sending chunk from [" + message + "] to [" + getRemoteConnectionManagerId() + "]")
            return chunk
          } else {
            message.finishTime = System.currentTimeMillis
            logDebug("Finished sending [" + message + "] to [" + getRemoteConnectionManagerId() +
              "] in "  + message.timeTaken )
          }
        }
      }
      None
    }
  }

  // outbox is used as a lock - ensure that it is always used as a leaf (since methods which
  // lock it are invoked in context of other locks)
  private val outbox = new Outbox()
  /*
    This is orthogonal to whether we have pending bytes to write or not - and satisfies a slightly
    different purpose. This flag is to see if we need to force reregister for write even when we
    do not have any pending bytes to write to socket.
    This can happen due to a race between adding pending buffers, and checking for existing of
    data as detailed in https://github.com/mesos/spark/pull/791
   */
  private var needForceReregister = false

  val currentBuffers = new ArrayBuffer[ByteBuffer]()

  /* channel.socket.setSendBufferSize(256 * 1024) */

  override def getRemoteAddress() = address

  val DEFAULT_INTEREST = SelectionKey.OP_READ

  override def registerInterest() {
    // Registering read too - does not really help in most cases, but for some
    // it does - so let us keep it for now.
    changeConnectionKeyInterest(SelectionKey.OP_WRITE | DEFAULT_INTEREST)
  }

  override def unregisterInterest() {
    changeConnectionKeyInterest(DEFAULT_INTEREST)
  }

  def registerAfterAuth(): Unit = {
    outbox.synchronized {
      needForceReregister = true
    }
    if (channel.isConnected) {
      registerInterest()
    }
  }

  def send(message: Message) {
    outbox.synchronized {
      outbox.addMessage(message)
      needForceReregister = true
    }
    if (channel.isConnected) {
      registerInterest()
    }
  }

  // return previous value after resetting it.
  def resetForceReregister(): Boolean = {
    outbox.synchronized {
      val result = needForceReregister
      needForceReregister = false
      result
    }
  }

  // MUST be called within the selector loop
  def connect() {
    try{
      channel.register(selector, SelectionKey.OP_CONNECT)
      channel.connect(address)
      logInfo("Initiating connection to [" + address + "]")
    } catch {
      case e: Exception => {
        logError("Error connecting to " + address, e)
        callOnExceptionCallback(e)
      }
    }
  }

  def finishConnect(force: Boolean): Boolean = {
    try {
      // Typically, this should finish immediately since it was triggered by a connect
      // selection - though need not necessarily always complete successfully.
      val connected = channel.finishConnect
      if (!force && !connected) {
        logInfo(
          "finish connect failed [" + address + "], " + outbox.messages.size + " messages pending")
        return false
      }

      // Fallback to previous behavior - assume finishConnect completed
      // This will happen only when finishConnect failed for some repeated number of times
      // (10 or so)
      // Is highly unlikely unless there was an unclean close of socket, etc
      registerInterest()
      logInfo("Connected to [" + address + "], " + outbox.messages.size + " messages pending")
    } catch {
      case e: Exception => {
        logWarning("Error finishing connection to " + address, e)
        callOnExceptionCallback(e)
      }
    }
    true
  }

  override def write(): Boolean = {
    try {
      while (true) {
        if (currentBuffers.size == 0) {
          outbox.synchronized {
            outbox.getChunk() match {
              case Some(chunk) => {
                val buffers = chunk.buffers
                // If we have 'seen' pending messages, then reset flag - since we handle that as
                // normal registering of event (below)
                if (needForceReregister && buffers.exists(_.remaining() > 0)) resetForceReregister()

                currentBuffers ++= buffers
              }
              case None => {
                // changeConnectionKeyInterest(0)
                /* key.interestOps(0) */
                return false
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
            // re-register for write.
            return true
          }
        }
      }
    } catch {
      case e: Exception => {
        logWarning("Error writing in connection to " + getRemoteConnectionManagerId(), e)
        callOnExceptionCallback(e)
        close()
        return false
      }
    }
    // should not happen - to keep scala compiler happy
    true
  }

  // This is a hack to determine if remote socket was closed or not.
  // SendingConnection DOES NOT expect to receive any data - if it does, it is an error
  // For a bunch of cases, read will return -1 in case remote socket is closed : hence we
  // register for reads to determine that.
  override def read(): Boolean = {
    // We don't expect the other side to send anything; so, we just read to detect an error or EOF.
    try {
      val length = channel.read(ByteBuffer.allocate(1))
      if (length == -1) { // EOF
        close()
      } else if (length > 0) {
        logWarning(
          "Unexpected data read from SendingConnection to " + getRemoteConnectionManagerId())
      }
    } catch {
      case e: Exception =>
        logError("Exception while reading SendingConnection to " + getRemoteConnectionManagerId(),
          e)
        callOnExceptionCallback(e)
        close()
    }

    false
  }

  override def changeInterestForRead(): Boolean = false

  override def changeInterestForWrite(): Boolean = ! isClosed
}


// Must be created within selector loop - else deadlock
private[spark] class ReceivingConnection(
    channel_ : SocketChannel,
    selector_ : Selector,
    id_ : ConnectionId,
    securityMgr_ : SecurityManager)
    extends Connection(channel_, selector_, id_, securityMgr_) {

  def isSaslComplete(): Boolean = {
    if (sparkSaslServer != null) sparkSaslServer.isComplete() else false
  }

  class Inbox() {
    val messages = new HashMap[Int, BufferMessage]()

    def getChunk(header: MessageChunkHeader): Option[MessageChunk] = {

      def createNewMessage: BufferMessage = {
        val newMessage = Message.create(header).asInstanceOf[BufferMessage]
        newMessage.started = true
        newMessage.startTime = System.currentTimeMillis
        newMessage.isSecurityNeg = header.securityNeg == 1
        logDebug(
          "Starting to receive [" + newMessage + "] from [" + getRemoteConnectionManagerId() + "]")
        messages += ((newMessage.id, newMessage))
        newMessage
      }

      val message = messages.getOrElseUpdate(header.id, createNewMessage)
      logTrace(
        "Receiving chunk of [" + message + "] from [" + getRemoteConnectionManagerId() + "]")
      message.getChunkForReceiving(header.chunkSize)
    }

    def getMessageForChunk(chunk: MessageChunk): Option[BufferMessage] = {
      messages.get(chunk.header.id)
    }

    def removeMessage(message: Message) {
      messages -= message.id
    }
  }

  @volatile private var inferredRemoteManagerId: ConnectionManagerId = null

  override def getRemoteConnectionManagerId(): ConnectionManagerId = {
    val currId = inferredRemoteManagerId
    if (currId != null) currId else super.getRemoteConnectionManagerId()
  }

  // The receiver's remote address is the local socket on remote side : which is NOT
  // the connection manager id of the receiver.
  // We infer that from the messages we receive on the receiver socket.
  private def processConnectionManagerId(header: MessageChunkHeader) {
    val currId = inferredRemoteManagerId
    if (header.address == null || currId != null) return

    val managerId = ConnectionManagerId.fromSocketAddress(header.address)

    if (managerId != null) {
      inferredRemoteManagerId = managerId
    }
  }


  val inbox = new Inbox()
  val headerBuffer: ByteBuffer = ByteBuffer.allocate(MessageChunkHeader.HEADER_SIZE)
  var onReceiveCallback: (Connection, Message) => Unit = null
  var currentChunk: MessageChunk = null

  channel.register(selector, SelectionKey.OP_READ)

  override def read(): Boolean = {
    try {
      while (true) {
        if (currentChunk == null) {
          val headerBytesRead = channel.read(headerBuffer)
          if (headerBytesRead == -1) {
            close()
            return false
          }
          if (headerBuffer.remaining > 0) {
            // re-register for read event ...
            return true
          }
          headerBuffer.flip
          if (headerBuffer.remaining != MessageChunkHeader.HEADER_SIZE) {
            throw new Exception(
              "Unexpected number of bytes (" + headerBuffer.remaining + ") in the header")
          }
          val header = MessageChunkHeader.create(headerBuffer)
          headerBuffer.clear()

          processConnectionManagerId(header)

          header.typ match {
            case Message.BUFFER_MESSAGE => {
              if (header.totalSize == 0) {
                if (onReceiveCallback != null) {
                  onReceiveCallback(this, Message.create(header))
                }
                currentChunk = null
                // re-register for read event ...
                return true
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
          // re-register for read event ...
          return true
        } else if (bytesRead == -1) {
          close()
          return false
        }

        /* logDebug("Read " + bytesRead + " bytes for the buffer") */

        if (currentChunk.buffer.remaining == 0) {
          /* println("Filled buffer at " + System.currentTimeMillis) */
          val bufferMessage = inbox.getMessageForChunk(currentChunk).get
          if (bufferMessage.isCompletelyReceived) {
            bufferMessage.flip()
            bufferMessage.finishTime = System.currentTimeMillis
            logDebug("Finished receiving [" + bufferMessage + "] from " +
              "[" + getRemoteConnectionManagerId() + "] in " + bufferMessage.timeTaken)
            if (onReceiveCallback != null) {
              onReceiveCallback(this, bufferMessage)
            }
            inbox.removeMessage(bufferMessage)
          }
          currentChunk = null
        }
      }
    } catch {
      case e: Exception => {
        logWarning("Error reading from connection to " + getRemoteConnectionManagerId(), e)
        callOnExceptionCallback(e)
        close()
        return false
      }
    }
    // should not happen - to keep scala compiler happy
    true
  }

  def onReceive(callback: (Connection, Message) => Unit) {onReceiveCallback = callback}

  // override def changeInterestForRead(): Boolean = ! isClosed
  override def changeInterestForRead(): Boolean = true

  override def changeInterestForWrite(): Boolean = {
    throw new IllegalStateException("Unexpected invocation right now")
  }

  override def registerInterest() {
    // Registering read too - does not really help in most cases, but for some
    // it does - so let us keep it for now.
    changeConnectionKeyInterest(SelectionKey.OP_READ)
  }

  override def unregisterInterest() {
    changeConnectionKeyInterest(0)
  }

  // For read conn, always false.
  override def resetForceReregister(): Boolean = false
}
