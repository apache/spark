package spark.network

import spark._

import java.nio._
import java.nio.channels._
import java.nio.channels.spi._
import java.net._
import java.util.concurrent.Executors

import scala.collection.mutable.HashMap
import scala.collection.mutable.SynchronizedMap
import scala.collection.mutable.SynchronizedQueue
import scala.collection.mutable.Queue
import scala.collection.mutable.ArrayBuffer

import akka.dispatch.{Await, Promise, ExecutionContext, Future}
import akka.util.Duration
import akka.util.duration._

private[spark] case class ConnectionManagerId(host: String, port: Int) {
  def toSocketAddress() = new InetSocketAddress(host, port)
}

private[spark] object ConnectionManagerId {
  def fromSocketAddress(socketAddress: InetSocketAddress): ConnectionManagerId = {
    new ConnectionManagerId(socketAddress.getHostName(), socketAddress.getPort())
  }
}
  
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
  
  val selector = SelectorProvider.provider.openSelector()
  val handleMessageExecutor = Executors.newFixedThreadPool(System.getProperty("spark.core.connection.handler.threads","20").toInt)
  val serverChannel = ServerSocketChannel.open()
  val connectionsByKey = new HashMap[SelectionKey, Connection] with SynchronizedMap[SelectionKey, Connection] 
  val connectionsById = new HashMap[ConnectionManagerId, SendingConnection] with SynchronizedMap[ConnectionManagerId, SendingConnection]
  val messageStatuses = new HashMap[Int, MessageStatus] 
  val connectionRequests = new HashMap[ConnectionManagerId, SendingConnection] with SynchronizedMap[ConnectionManagerId, SendingConnection]
  val keyInterestChangeRequests = new SynchronizedQueue[(SelectionKey, Int)]
  val sendMessageRequests = new Queue[(Message, SendingConnection)]

  implicit val futureExecContext = ExecutionContext.fromExecutor(Utils.newDaemonCachedThreadPool())

  var onReceiveCallback: (BufferMessage, ConnectionManagerId) => Option[Message]= null

  serverChannel.configureBlocking(false)
  serverChannel.socket.setReuseAddress(true)
  serverChannel.socket.setReceiveBufferSize(256 * 1024) 

  serverChannel.socket.bind(new InetSocketAddress(port))
  serverChannel.register(selector, SelectionKey.OP_ACCEPT)

  val id = new ConnectionManagerId(Utils.localHostName, serverChannel.socket.getLocalPort)
  logInfo("Bound socket to port " + serverChannel.socket.getLocalPort() + " with id = " + id)
  
  val selectorThread = new Thread("connection-manager-thread") {
    override def run() = ConnectionManager.this.run()
  }
  selectorThread.setDaemon(true)
  selectorThread.start()

  private def run() {
    try {
      while(!selectorThread.isInterrupted) {
        for ((connectionManagerId, sendingConnection) <- connectionRequests) {
          sendingConnection.connect() 
          addConnection(sendingConnection)
          connectionRequests -= connectionManagerId
        }
        sendMessageRequests.synchronized {
          while (!sendMessageRequests.isEmpty) {
            val (message, connection) = sendMessageRequests.dequeue
            connection.send(message)
          }
        }

        while (!keyInterestChangeRequests.isEmpty) {
          val (key, ops) = keyInterestChangeRequests.dequeue
          val connection = connectionsByKey(key)
          val lastOps = key.interestOps()
          key.interestOps(ops)
          
          def intToOpStr(op: Int): String = {
            val opStrs = ArrayBuffer[String]()
            if ((op & SelectionKey.OP_READ) != 0) opStrs += "READ"
            if ((op & SelectionKey.OP_WRITE) != 0) opStrs += "WRITE"
            if ((op & SelectionKey.OP_CONNECT) != 0) opStrs += "CONNECT"
            if ((op & SelectionKey.OP_ACCEPT) != 0) opStrs += "ACCEPT"
            if (opStrs.size > 0) opStrs.reduceLeft(_ + " | " + _) else " "
          }
          
          logTrace("Changed key for connection to [" + connection.remoteConnectionManagerId  + 
            "] changed from [" + intToOpStr(lastOps) + "] to [" + intToOpStr(ops) + "]")
          
        }

        val selectedKeysCount = selector.select()
        if (selectedKeysCount == 0) {
          logDebug("Selector selected " + selectedKeysCount + " of " + selector.keys.size + " keys")
        }
        if (selectorThread.isInterrupted) {
          logInfo("Selector thread was interrupted!")
          return
        }
        
        val selectedKeys = selector.selectedKeys().iterator()
        while (selectedKeys.hasNext()) {
          val key = selectedKeys.next
          selectedKeys.remove()
          if (key.isValid) {
            if (key.isAcceptable) {
              acceptConnection(key)
            } else if (key.isConnectable) {
              connectionsByKey(key).asInstanceOf[SendingConnection].finishConnect()
            } else if (key.isReadable) {
              connectionsByKey(key).read()
            } else if (key.isWritable) {
              connectionsByKey(key).write()
            }
          }
        }
      }
    } catch {
      case e: Exception => logError("Error in select loop", e)
    }
  }
  
  private def acceptConnection(key: SelectionKey) {
    val serverChannel = key.channel.asInstanceOf[ServerSocketChannel]
    val newChannel = serverChannel.accept()
    val newConnection = new ReceivingConnection(newChannel, selector)
    newConnection.onReceive(receiveMessage)
    newConnection.onClose(removeConnection)
    addConnection(newConnection)
    logInfo("Accepted connection from [" + newConnection.remoteAddress.getAddress + "]")
  }

  private def addConnection(connection: Connection) {
    connectionsByKey += ((connection.key, connection))
    if (connection.isInstanceOf[SendingConnection]) {
      val sendingConnection = connection.asInstanceOf[SendingConnection]
      connectionsById += ((sendingConnection.remoteConnectionManagerId, sendingConnection))
    }
    connection.onKeyInterestChange(changeConnectionKeyInterest)
    connection.onException(handleConnectionError)
    connection.onClose(removeConnection)
  }

  private def removeConnection(connection: Connection) {
    connectionsByKey -= connection.key
    if (connection.isInstanceOf[SendingConnection]) {
      val sendingConnection = connection.asInstanceOf[SendingConnection]
      val sendingConnectionManagerId = sendingConnection.remoteConnectionManagerId
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
      val remoteConnectionManagerId = receivingConnection.remoteConnectionManagerId
      logInfo("Removing ReceivingConnection to " + remoteConnectionManagerId)
      
      val sendingConnectionManagerId = connectionsById.keys.find(_.host == remoteConnectionManagerId.host).orNull
      if (sendingConnectionManagerId == null) {
        logError("Corresponding SendingConnectionManagerId not found")
        return
      }
      logInfo("Corresponding SendingConnectionManagerId is " + sendingConnectionManagerId)
      
      val sendingConnection = connectionsById(sendingConnectionManagerId)
      sendingConnection.close()
      connectionsById -= sendingConnectionManagerId
      
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
  }

  private def handleConnectionError(connection: Connection, e: Exception) {
    logInfo("Handling connection error on connection to " + connection.remoteConnectionManagerId)
    removeConnection(connection)
  }

  private def changeConnectionKeyInterest(connection: Connection, ops: Int) {
    keyInterestChangeRequests += ((connection.key, ops))  
  }

  private def receiveMessage(connection: Connection, message: Message) {
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
      val newConnection = connectionRequests.getOrElseUpdate(connectionManagerId,
          new SendingConnection(inetSocketAddress, selector, connectionManagerId))
      newConnection   
    }
    val lookupKey = ConnectionManagerId.fromSocketAddress(connectionManagerId.toSocketAddress)
    val connection = connectionsById.getOrElse(lookupKey, startNewConnection())
    message.senderAddress = id.toSocketAddress()
    logDebug("Sending [" + message + "] to [" + connectionManagerId + "]")
    /*connection.send(message)*/
    sendMessageRequests.synchronized {
      sendMessageRequests += ((message, connection))
    }
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
