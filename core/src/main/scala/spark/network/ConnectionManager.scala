package spark.network

import spark._

import java.nio._
import java.nio.channels._
import java.nio.channels.spi._
import java.net._
import java.util.concurrent.{LinkedBlockingDeque, TimeUnit, ThreadPoolExecutor}

import scala.collection.mutable.HashSet
import scala.collection.mutable.HashMap
import scala.collection.mutable.SynchronizedMap
import scala.collection.mutable.SynchronizedQueue
import scala.collection.mutable.ArrayBuffer

import akka.dispatch.{Await, Promise, ExecutionContext, Future}
import akka.util.Duration
import akka.util.duration._


private[spark] class ConnectionManager(port: Int) extends Logging {

  class MessageStatus(
      val message: Message,
      val connectionManagerId: ConnectionManagerId,
      completionHandler: MessageStatus => Unit) {

    var ackMessage: Option[Message] = None
    var attempted = false
    var acked = false

    def markDone() { completionHandler(this) }
  }

  private val selector = SelectorProvider.provider.openSelector()

  private val handleMessageExecutor = new ThreadPoolExecutor(
    System.getProperty("spark.core.connection.handler.threads.min","20").toInt,
    System.getProperty("spark.core.connection.handler.threads.max","60").toInt,
    System.getProperty("spark.core.connection.handler.threads.keepalive","60").toInt, TimeUnit.SECONDS,
    new LinkedBlockingDeque[Runnable]())

  private val handleReadWriteExecutor = new ThreadPoolExecutor(
    System.getProperty("spark.core.connection.io.threads.min","4").toInt,
    System.getProperty("spark.core.connection.io.threads.max","32").toInt,
    System.getProperty("spark.core.connection.io.threads.keepalive","60").toInt, TimeUnit.SECONDS,
    new LinkedBlockingDeque[Runnable]())

  // Use a different, yet smaller, thread pool - infrequently used with very short lived tasks : which should be executed asap
  private val handleConnectExecutor = new ThreadPoolExecutor(
    System.getProperty("spark.core.connection.connect.threads.min","1").toInt,
    System.getProperty("spark.core.connection.connect.threads.max","8").toInt,
    System.getProperty("spark.core.connection.connect.threads.keepalive","60").toInt, TimeUnit.SECONDS,
    new LinkedBlockingDeque[Runnable]())

  private val serverChannel = ServerSocketChannel.open()
  private val connectionsByKey = new HashMap[SelectionKey, Connection] with SynchronizedMap[SelectionKey, Connection]
  private val connectionsById = new HashMap[ConnectionManagerId, SendingConnection] with SynchronizedMap[ConnectionManagerId, SendingConnection]
  private val messageStatuses = new HashMap[Int, MessageStatus]
  private val keyInterestChangeRequests = new SynchronizedQueue[(SelectionKey, Int)]
  private val registerRequests = new SynchronizedQueue[SendingConnection]

  implicit val futureExecContext = ExecutionContext.fromExecutor(Utils.newDaemonCachedThreadPool())

  private var onReceiveCallback: (BufferMessage, ConnectionManagerId) => Option[Message]= null

  serverChannel.configureBlocking(false)
  serverChannel.socket.setReuseAddress(true)
  serverChannel.socket.setReceiveBufferSize(256 * 1024)

  serverChannel.socket.bind(new InetSocketAddress(port))
  serverChannel.register(selector, SelectionKey.OP_ACCEPT)

  val id = new ConnectionManagerId(Utils.localHostName, serverChannel.socket.getLocalPort)
  logInfo("Bound socket to port " + serverChannel.socket.getLocalPort() + " with id = " + id)

  private val selectorThread = new Thread("connection-manager-thread") {
    override def run() = ConnectionManager.this.run()
  }
  selectorThread.setDaemon(true)
  selectorThread.start()

  private val writeRunnableStarted: HashSet[SelectionKey] = new HashSet[SelectionKey]()

  private def triggerWrite(key: SelectionKey) {
    val conn = connectionsByKey.getOrElse(key, null)
    if (conn == null) return

    writeRunnableStarted.synchronized {
      // So that we do not trigger more write events while processing this one.
      // The write method will re-register when done.
      if (conn.changeInterestForWrite()) conn.unregisterInterest()
      if (writeRunnableStarted.contains(key)) {
        // key.interestOps(key.interestOps() & ~ SelectionKey.OP_WRITE)
        return
      }

      writeRunnableStarted += key
    }
    handleReadWriteExecutor.execute(new Runnable {
      override def run() {
        var register: Boolean = false
        try {
          register = conn.write()
        } finally {
          writeRunnableStarted.synchronized {
            writeRunnableStarted -= key
            if (register && conn.changeInterestForWrite()) {
              conn.registerInterest()
            }
          }
        }
      }
    } )
  }

  private val readRunnableStarted: HashSet[SelectionKey] = new HashSet[SelectionKey]()

  private def triggerRead(key: SelectionKey) {
    val conn = connectionsByKey.getOrElse(key, null)
    if (conn == null) return

    readRunnableStarted.synchronized {
      // So that we do not trigger more read events while processing this one.
      // The read method will re-register when done.
      if (conn.changeInterestForRead())conn.unregisterInterest()
      if (readRunnableStarted.contains(key)) {
        return
      }

      readRunnableStarted += key
    }
    handleReadWriteExecutor.execute(new Runnable {
      override def run() {
        var register: Boolean = false
        try {
          register = conn.read()
        } finally {
          readRunnableStarted.synchronized {
            readRunnableStarted -= key
            if (register && conn.changeInterestForRead()) {
              conn.registerInterest()
            }
          }
        }
      }
    } )
  }

  private def triggerConnect(key: SelectionKey) {
    val conn = connectionsByKey.getOrElse(key, null).asInstanceOf[SendingConnection]
    if (conn == null) return

    // prevent other events from being triggered
    // Since we are still trying to connect, we do not need to do the additional steps in triggerWrite
    conn.changeConnectionKeyInterest(0)

    handleConnectExecutor.execute(new Runnable {
      override def run() {

        var tries: Int = 10
        while (tries >= 0) {
          if (conn.finishConnect(false)) return
          // Sleep ?
          Thread.sleep(1)
          tries -= 1
        }

        // fallback to previous behavior : we should not really come here since this method was
        // triggered since channel became connectable : but at times, the first finishConnect need not
        // succeed : hence the loop to retry a few 'times'.
        conn.finishConnect(true)
      }
    } )
  }

  // MUST be called within selector loop - else deadlock.
  private def triggerForceCloseByException(key: SelectionKey, e: Exception) {
    try {
      key.interestOps(0)
    } catch {
      // ignore exceptions
      case e: Exception => logDebug("Ignoring exception", e)
    }

    val conn = connectionsByKey.getOrElse(key, null)
    if (conn == null) return

    // Pushing to connect threadpool
    handleConnectExecutor.execute(new Runnable {
      override def run() {
        try {
          conn.callOnExceptionCallback(e)
        } catch {
          // ignore exceptions
          case e: Exception => logDebug("Ignoring exception", e)
        }
        try {
          conn.close()
        } catch {
          // ignore exceptions
          case e: Exception => logDebug("Ignoring exception", e)
        }
      }
    })
  }


  def run() {
    try {
      while(!selectorThread.isInterrupted) {
        while (! registerRequests.isEmpty) {
          val conn: SendingConnection = registerRequests.dequeue
          addListeners(conn)
          conn.connect()
          addConnection(conn)
        }

        while(!keyInterestChangeRequests.isEmpty) {
          val (key, ops) = keyInterestChangeRequests.dequeue

          try {
            if (key.isValid) {
              val connection = connectionsByKey.getOrElse(key, null)
              if (connection != null) {
                val lastOps = key.interestOps()
                key.interestOps(ops)

                // hot loop - prevent materialization of string if trace not enabled.
                if (isTraceEnabled()) {
                  def intToOpStr(op: Int): String = {
                    val opStrs = ArrayBuffer[String]()
                    if ((op & SelectionKey.OP_READ) != 0) opStrs += "READ"
                    if ((op & SelectionKey.OP_WRITE) != 0) opStrs += "WRITE"
                    if ((op & SelectionKey.OP_CONNECT) != 0) opStrs += "CONNECT"
                    if ((op & SelectionKey.OP_ACCEPT) != 0) opStrs += "ACCEPT"
                    if (opStrs.size > 0) opStrs.reduceLeft(_ + " | " + _) else " "
                  }

                  logTrace("Changed key for connection to [" + connection.getRemoteConnectionManagerId()  +
                    "] changed from [" + intToOpStr(lastOps) + "] to [" + intToOpStr(ops) + "]")
                }
              }
            } else {
              logInfo("Key not valid ? " + key)
              throw new CancelledKeyException()
            }
          } catch {
            case e: CancelledKeyException => {
              logInfo("key already cancelled ? " + key, e)
              triggerForceCloseByException(key, e)
            }
            case e: Exception => {
              logError("Exception processing key " + key, e)
              triggerForceCloseByException(key, e)
            }
          }
        }

        val selectedKeysCount =
          try {
            selector.select()
          } catch {
            // Explicitly only dealing with CancelledKeyException here since other exceptions should be dealt with differently.
            case e: CancelledKeyException => {
              // Some keys within the selectors list are invalid/closed. clear them.
              val allKeys = selector.keys().iterator()

              while (allKeys.hasNext()) {
                val key = allKeys.next()
                try {
                  if (! key.isValid) {
                    logInfo("Key not valid ? " + key)
                    throw new CancelledKeyException()
                  }
                } catch {
                  case e: CancelledKeyException => {
                    logInfo("key already cancelled ? " + key, e)
                    triggerForceCloseByException(key, e)
                  }
                  case e: Exception => {
                    logError("Exception processing key " + key, e)
                    triggerForceCloseByException(key, e)
                  }
                }
              }
            }
            0
          }

        if (selectedKeysCount == 0) {
          logDebug("Selector selected " + selectedKeysCount + " of " + selector.keys.size + " keys")
        }
        if (selectorThread.isInterrupted) {
          logInfo("Selector thread was interrupted!")
          return
        }

        if (0 != selectedKeysCount) {
          val selectedKeys = selector.selectedKeys().iterator()
          while (selectedKeys.hasNext()) {
            val key = selectedKeys.next
            selectedKeys.remove()
            try {
              if (key.isValid) {
                if (key.isAcceptable) {
                  acceptConnection(key)
                } else
                if (key.isConnectable) {
                  triggerConnect(key)
                } else
                if (key.isReadable) {
                  triggerRead(key)
                } else
                if (key.isWritable) {
                  triggerWrite(key)
                }
              } else {
                logInfo("Key not valid ? " + key)
                throw new CancelledKeyException()
              }
            } catch {
              // weird, but we saw this happening - even though key.isValid was true, key.isAcceptable would throw CancelledKeyException.
              case e: CancelledKeyException => {
                logInfo("key already cancelled ? " + key, e)
                triggerForceCloseByException(key, e)
              }
              case e: Exception => {
                logError("Exception processing key " + key, e)
                triggerForceCloseByException(key, e)
              }
            }
          }
        }
      }
    } catch {
      case e: Exception => logError("Error in select loop", e)
    }
  }

  def acceptConnection(key: SelectionKey) {
    val serverChannel = key.channel.asInstanceOf[ServerSocketChannel]

    var newChannel = serverChannel.accept()

    // accept them all in a tight loop. non blocking accept with no processing, should be fine
    while (newChannel != null) {
      try {
        val newConnection = new ReceivingConnection(newChannel, selector)
        newConnection.onReceive(receiveMessage)
        addListeners(newConnection)
        addConnection(newConnection)
        logInfo("Accepted connection from [" + newConnection.remoteAddress.getAddress + "]")
      } catch {
        // might happen in case of issues with registering with selector
        case e: Exception => logError("Error in accept loop", e)
      }

      newChannel = serverChannel.accept()
    }
  }

  private def addListeners(connection: Connection) {
    connection.onKeyInterestChange(changeConnectionKeyInterest)
    connection.onException(handleConnectionError)
    connection.onClose(removeConnection)
  }

  def addConnection(connection: Connection) {
    connectionsByKey += ((connection.key, connection))
  }

  def removeConnection(connection: Connection) {
    connectionsByKey -= connection.key

    try {
      if (connection.isInstanceOf[SendingConnection]) {
        val sendingConnection = connection.asInstanceOf[SendingConnection]
        val sendingConnectionManagerId = sendingConnection.getRemoteConnectionManagerId()
        logInfo("Removing SendingConnection to " + sendingConnectionManagerId)

        connectionsById -= sendingConnectionManagerId

        messageStatuses.synchronized {
          messageStatuses
            .values.filter(_.connectionManagerId == sendingConnectionManagerId).foreach(status => {
              logInfo("Notifying " + status)
              status.synchronized {
              status.attempted = true
               status.acked = false
               status.markDone()
              }
            })

          messageStatuses.retain((i, status) => {
            status.connectionManagerId != sendingConnectionManagerId
          })
        }
      } else if (connection.isInstanceOf[ReceivingConnection]) {
        val receivingConnection = connection.asInstanceOf[ReceivingConnection]
        val remoteConnectionManagerId = receivingConnection.getRemoteConnectionManagerId()
        logInfo("Removing ReceivingConnection to " + remoteConnectionManagerId)

        val sendingConnectionOpt = connectionsById.get(remoteConnectionManagerId)
          if (! sendingConnectionOpt.isDefined) {
          logError("Corresponding SendingConnectionManagerId not found")
          return
        }

        val sendingConnection = sendingConnectionOpt.get
        connectionsById -= remoteConnectionManagerId
        sendingConnection.close()

        val sendingConnectionManagerId = sendingConnection.getRemoteConnectionManagerId()

        assert (sendingConnectionManagerId == remoteConnectionManagerId)

        messageStatuses.synchronized {
          for (s <- messageStatuses.values if s.connectionManagerId == sendingConnectionManagerId) {
            logInfo("Notifying " + s)
            s.synchronized {
              s.attempted = true
              s.acked = false
              s.markDone()
            }
          }

          messageStatuses.retain((i, status) => {
            status.connectionManagerId != sendingConnectionManagerId
          })
        }
      }
    } finally {
      // So that the selection keys can be removed.
      wakeupSelector()
    }
  }

  def handleConnectionError(connection: Connection, e: Exception) {
    logInfo("Handling connection error on connection to " + connection.getRemoteConnectionManagerId())
    removeConnection(connection)
  }

  def changeConnectionKeyInterest(connection: Connection, ops: Int) {
    keyInterestChangeRequests += ((connection.key, ops))
    // so that registerations happen !
    wakeupSelector()
  }

  def receiveMessage(connection: Connection, message: Message) {
    val connectionManagerId = ConnectionManagerId.fromSocketAddress(message.senderAddress)
    logDebug("Received [" + message + "] from [" + connectionManagerId + "]")
    val runnable = new Runnable() {
      val creationTime = System.currentTimeMillis
      def run() {
        logDebug("Handler thread delay is " + (System.currentTimeMillis - creationTime) + " ms")
        handleMessage(connectionManagerId, message)
        logDebug("Handling delay is " + (System.currentTimeMillis - creationTime) + " ms")
      }
    }
    handleMessageExecutor.execute(runnable)
    /*handleMessage(connection, message)*/
  }

  private def handleMessage(connectionManagerId: ConnectionManagerId, message: Message) {
    logDebug("Handling [" + message + "] from [" + connectionManagerId + "]")
    message match {
      case bufferMessage: BufferMessage => {
        if (bufferMessage.hasAckId) {
          val sentMessageStatus = messageStatuses.synchronized {
            messageStatuses.get(bufferMessage.ackId) match {
              case Some(status) => {
                messageStatuses -= bufferMessage.ackId
                status
              }
              case None => {
                throw new Exception("Could not find reference for received ack message " + message.id)
                null
              }
            }
          }
          sentMessageStatus.synchronized {
            sentMessageStatus.ackMessage = Some(message)
            sentMessageStatus.attempted = true
            sentMessageStatus.acked = true
            sentMessageStatus.markDone()
          }
        } else {
          val ackMessage = if (onReceiveCallback != null) {
            logDebug("Calling back")
            onReceiveCallback(bufferMessage, connectionManagerId)
          } else {
            logDebug("Not calling back as callback is null")
            None
          }

          if (ackMessage.isDefined) {
            if (!ackMessage.get.isInstanceOf[BufferMessage]) {
              logDebug("Response to " + bufferMessage + " is not a buffer message, it is of type " + ackMessage.get.getClass())
            } else if (!ackMessage.get.asInstanceOf[BufferMessage].hasAckId) {
              logDebug("Response to " + bufferMessage + " does not have ack id set")
              ackMessage.get.asInstanceOf[BufferMessage].ackId = bufferMessage.id
            }
          }

          sendMessage(connectionManagerId, ackMessage.getOrElse {
            Message.createBufferMessage(bufferMessage.id)
          })
        }
      }
      case _ => throw new Exception("Unknown type message received")
    }
  }

  private def sendMessage(connectionManagerId: ConnectionManagerId, message: Message) {
    def startNewConnection(): SendingConnection = {
      val inetSocketAddress = new InetSocketAddress(connectionManagerId.host, connectionManagerId.port)
      val newConnection = new SendingConnection(inetSocketAddress, selector, connectionManagerId)
      registerRequests.enqueue(newConnection)

      newConnection
    }
    // I removed the lookupKey stuff as part of merge ... should I re-add it ? We did not find it useful in our test-env ...
    // If we do re-add it, we should consistently use it everywhere I guess ?
    val connection = connectionsById.getOrElseUpdate(connectionManagerId, startNewConnection())
    message.senderAddress = id.toSocketAddress()
    logDebug("Sending [" + message + "] to [" + connectionManagerId + "]")
    connection.send(message)

    wakeupSelector()
  }

  private def wakeupSelector() {
    selector.wakeup()
  }

  def sendMessageReliably(connectionManagerId: ConnectionManagerId, message: Message)
      : Future[Option[Message]] = {
    val promise = Promise[Option[Message]]
    val status = new MessageStatus(message, connectionManagerId, s => promise.success(s.ackMessage))
    messageStatuses.synchronized {
      messageStatuses += ((message.id, status))
    }
    sendMessage(connectionManagerId, message)
    promise.future
  }

  def sendMessageReliablySync(connectionManagerId: ConnectionManagerId, message: Message): Option[Message] = {
    Await.result(sendMessageReliably(connectionManagerId, message), Duration.Inf)
  }

  def onReceiveMessage(callback: (Message, ConnectionManagerId) => Option[Message]) {
    onReceiveCallback = callback
  }

  def stop() {
    selectorThread.interrupt()
    selectorThread.join()
    selector.close()
    val connections = connectionsByKey.values
    connections.foreach(_.close())
    if (connectionsByKey.size != 0) {
      logWarning("All connections not cleaned up")
    }
    handleMessageExecutor.shutdown()
    handleReadWriteExecutor.shutdown()
    handleConnectExecutor.shutdown()
    logInfo("ConnectionManager stopped")
  }
}


private[spark] object ConnectionManager {

  def main(args: Array[String]) {
    val manager = new ConnectionManager(9999)
    manager.onReceiveMessage((msg: Message, id: ConnectionManagerId) => {
      println("Received [" + msg + "] from [" + id + "]")
      None
    })

    /*testSequentialSending(manager)*/
    /*System.gc()*/

    /*testParallelSending(manager)*/
    /*System.gc()*/

    /*testParallelDecreasingSending(manager)*/
    /*System.gc()*/

    testContinuousSending(manager)
    System.gc()
  }

  def testSequentialSending(manager: ConnectionManager) {
    println("--------------------------")
    println("Sequential Sending")
    println("--------------------------")
    val size = 10 * 1024 * 1024
    val count = 10

    val buffer = ByteBuffer.allocate(size).put(Array.tabulate[Byte](size)(x => x.toByte))
    buffer.flip

    (0 until count).map(i => {
      val bufferMessage = Message.createBufferMessage(buffer.duplicate)
      manager.sendMessageReliablySync(manager.id, bufferMessage)
    })
    println("--------------------------")
    println()
  }

  def testParallelSending(manager: ConnectionManager) {
    println("--------------------------")
    println("Parallel Sending")
    println("--------------------------")
    val size = 10 * 1024 * 1024
    val count = 10

    val buffer = ByteBuffer.allocate(size).put(Array.tabulate[Byte](size)(x => x.toByte))
    buffer.flip

    val startTime = System.currentTimeMillis
    (0 until count).map(i => {
      val bufferMessage = Message.createBufferMessage(buffer.duplicate)
      manager.sendMessageReliably(manager.id, bufferMessage)
    }).foreach(f => {
      val g = Await.result(f, 1 second)
      if (!g.isDefined) println("Failed")
    })
    val finishTime = System.currentTimeMillis

    val mb = size * count / 1024.0 / 1024.0
    val ms = finishTime - startTime
    val tput = mb * 1000.0 / ms
    println("--------------------------")
    println("Started at " + startTime + ", finished at " + finishTime)
    println("Sent " + count + " messages of size " + size + " in " + ms + " ms (" + tput + " MB/s)")
    println("--------------------------")
    println()
  }

  def testParallelDecreasingSending(manager: ConnectionManager) {
    println("--------------------------")
    println("Parallel Decreasing Sending")
    println("--------------------------")
    val size = 10 * 1024 * 1024
    val count = 10
    val buffers = Array.tabulate(count)(i => ByteBuffer.allocate(size * (i + 1)).put(Array.tabulate[Byte](size * (i + 1))(x => x.toByte)))
    buffers.foreach(_.flip)
    val mb = buffers.map(_.remaining).reduceLeft(_ + _) / 1024.0 / 1024.0

    val startTime = System.currentTimeMillis
    (0 until count).map(i => {
      val bufferMessage = Message.createBufferMessage(buffers(count - 1 - i).duplicate)
      manager.sendMessageReliably(manager.id, bufferMessage)
    }).foreach(f => {
      val g = Await.result(f, 1 second)
      if (!g.isDefined) println("Failed")
    })
    val finishTime = System.currentTimeMillis

    val ms = finishTime - startTime
    val tput = mb * 1000.0 / ms
    println("--------------------------")
    /*println("Started at " + startTime + ", finished at " + finishTime) */
    println("Sent " + mb + " MB in " + ms + " ms (" + tput + " MB/s)")
    println("--------------------------")
    println()
  }

  def testContinuousSending(manager: ConnectionManager) {
    println("--------------------------")
    println("Continuous Sending")
    println("--------------------------")
    val size = 10 * 1024 * 1024
    val count = 10

    val buffer = ByteBuffer.allocate(size).put(Array.tabulate[Byte](size)(x => x.toByte))
    buffer.flip

    val startTime = System.currentTimeMillis
    while(true) {
      (0 until count).map(i => {
          val bufferMessage = Message.createBufferMessage(buffer.duplicate)
          manager.sendMessageReliably(manager.id, bufferMessage)
        }).foreach(f => {
          val g = Await.result(f, 1 second)
          if (!g.isDefined) println("Failed")
        })
      val finishTime = System.currentTimeMillis
      Thread.sleep(1000)
      val mb = size * count / 1024.0 / 1024.0
      val ms = finishTime - startTime
      val tput = mb * 1000.0 / ms
      println("Sent " + mb + " MB in " + ms + " ms (" + tput + " MB/s)")
      println("--------------------------")
      println()
    }
  }
}
