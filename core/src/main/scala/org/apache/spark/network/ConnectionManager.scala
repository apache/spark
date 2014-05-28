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

package org.apache.spark.network

import java.nio._
import java.nio.channels._
import java.nio.channels.spi._
import java.net._
import java.util.concurrent.atomic.AtomicInteger

import java.util.concurrent.{LinkedBlockingDeque, TimeUnit, ThreadPoolExecutor}

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap
import scala.collection.mutable.HashSet
import scala.collection.mutable.SynchronizedMap
import scala.collection.mutable.SynchronizedQueue

import scala.concurrent.{Await, ExecutionContext, Future, Promise}
import scala.concurrent.duration._
import scala.language.postfixOps

import org.apache.spark._
import org.apache.spark.util.{SystemClock, Utils}

private[spark] class ConnectionManager(port: Int, conf: SparkConf,
    securityManager: SecurityManager) extends Logging {

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

  // default to 30 second timeout waiting for authentication
  private val authTimeout = conf.getInt("spark.core.connection.auth.wait.timeout", 30)

  private val handleMessageExecutor = new ThreadPoolExecutor(
    conf.getInt("spark.core.connection.handler.threads.min", 20),
    conf.getInt("spark.core.connection.handler.threads.max", 60),
    conf.getInt("spark.core.connection.handler.threads.keepalive", 60), TimeUnit.SECONDS,
    new LinkedBlockingDeque[Runnable]())

  private val handleReadWriteExecutor = new ThreadPoolExecutor(
    conf.getInt("spark.core.connection.io.threads.min", 4),
    conf.getInt("spark.core.connection.io.threads.max", 32),
    conf.getInt("spark.core.connection.io.threads.keepalive", 60), TimeUnit.SECONDS,
    new LinkedBlockingDeque[Runnable]())

  // Use a different, yet smaller, thread pool - infrequently used with very short lived tasks :
  // which should be executed asap
  private val handleConnectExecutor = new ThreadPoolExecutor(
    conf.getInt("spark.core.connection.connect.threads.min", 1),
    conf.getInt("spark.core.connection.connect.threads.max", 8),
    conf.getInt("spark.core.connection.connect.threads.keepalive", 60), TimeUnit.SECONDS,
    new LinkedBlockingDeque[Runnable]())

  private val serverChannel = ServerSocketChannel.open()
  // used to track the SendingConnections waiting to do SASL negotiation
  private val connectionsAwaitingSasl = new HashMap[ConnectionId, SendingConnection]
    with SynchronizedMap[ConnectionId, SendingConnection]
  private val connectionsByKey =
    new HashMap[SelectionKey, Connection] with SynchronizedMap[SelectionKey, Connection]
  private val connectionsById = new HashMap[ConnectionManagerId, SendingConnection]
    with SynchronizedMap[ConnectionManagerId, SendingConnection]
  private val messageStatuses = new HashMap[Int, MessageStatus]
  private val keyInterestChangeRequests = new SynchronizedQueue[(SelectionKey, Int)]
  private val registerRequests = new SynchronizedQueue[SendingConnection]

  implicit val futureExecContext = ExecutionContext.fromExecutor(
    Utils.newDaemonCachedThreadPool("Connection manager future execution context"))

  @volatile
  private var onReceiveCallback: (BufferMessage, ConnectionManagerId) => Option[Message] = null

  private val authEnabled = securityManager.isAuthenticationEnabled()

  serverChannel.configureBlocking(false)
  serverChannel.socket.setReuseAddress(true)
  serverChannel.socket.setReceiveBufferSize(256 * 1024)

  serverChannel.socket.bind(new InetSocketAddress(port))
  serverChannel.register(selector, SelectionKey.OP_ACCEPT)

  val id = new ConnectionManagerId(Utils.localHostName, serverChannel.socket.getLocalPort)
  logInfo("Bound socket to port " + serverChannel.socket.getLocalPort() + " with id = " + id)

  // used in combination with the ConnectionManagerId to create unique Connection ids
  // to be able to track asynchronous messages
  private val idCount: AtomicInteger = new AtomicInteger(1)

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
            val needReregister = register || conn.resetForceReregister()
            if (needReregister && conn.changeInterestForWrite()) {
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
    // Since we are still trying to connect, we do not need to do the additional steps in
    // triggerWrite
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
        // triggered since channel became connectable : but at times, the first finishConnect need
        // not succeed : hence the loop to retry a few 'times'.
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

                  logTrace("Changed key for connection to [" +
                    connection.getRemoteConnectionManagerId()  + "] changed from [" +
                      intToOpStr(lastOps) + "] to [" + intToOpStr(ops) + "]")
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
            // Explicitly only dealing with CancelledKeyException here since other exceptions
            // should be dealt with differently.
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
          logDebug("Selector selected " + selectedKeysCount + " of " + selector.keys.size +
            " keys")
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
              // weird, but we saw this happening - even though key.isValid was true,
              // key.isAcceptable would throw CancelledKeyException.
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
        val newConnectionId = new ConnectionId(id, idCount.getAndIncrement.intValue)
        val newConnection = new ReceivingConnection(newChannel, selector, newConnectionId)
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
        connectionsAwaitingSasl -= connection.connectionId

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
    logInfo("Handling connection error on connection to " +
      connection.getRemoteConnectionManagerId())
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
        handleMessage(connectionManagerId, message, connection)
        logDebug("Handling delay is " + (System.currentTimeMillis - creationTime) + " ms")
      }
    }
    handleMessageExecutor.execute(runnable)
    /* handleMessage(connection, message) */
  }

  private def handleClientAuthentication(
      waitingConn: SendingConnection,
      securityMsg: SecurityMessage,
      connectionId : ConnectionId) {
    if (waitingConn.isSaslComplete()) {
      logDebug("Client sasl completed for id: "  + waitingConn.connectionId)
      connectionsAwaitingSasl -= waitingConn.connectionId
      waitingConn.getAuthenticated().synchronized {
        waitingConn.getAuthenticated().notifyAll();
      }
      return
    } else {
      var replyToken : Array[Byte] = null
      try {
        replyToken = waitingConn.sparkSaslClient.saslResponse(securityMsg.getToken);
        if (waitingConn.isSaslComplete()) {
          logDebug("Client sasl completed after evaluate for id: " + waitingConn.connectionId)
          connectionsAwaitingSasl -= waitingConn.connectionId
          waitingConn.getAuthenticated().synchronized {
            waitingConn.getAuthenticated().notifyAll()
          }
          return
        }
        var securityMsgResp = SecurityMessage.fromResponse(replyToken,
          securityMsg.getConnectionId.toString())
        var message = securityMsgResp.toBufferMessage
        if (message == null) throw new Exception("Error creating security message")
        sendSecurityMessage(waitingConn.getRemoteConnectionManagerId(), message)
      } catch  {
        case e: Exception => {
          logError("Error handling sasl client authentication", e)
          waitingConn.close()
          throw new Exception("Error evaluating sasl response: " + e)
        }
      }
    }
  }

  private def handleServerAuthentication(
      connection: Connection,
      securityMsg: SecurityMessage,
      connectionId: ConnectionId) {
    if (!connection.isSaslComplete()) {
      logDebug("saslContext not established")
      var replyToken : Array[Byte] = null
      try {
        connection.synchronized {
          if (connection.sparkSaslServer == null) {
            logDebug("Creating sasl Server")
            connection.sparkSaslServer = new SparkSaslServer(securityManager)
          }
        }
        replyToken = connection.sparkSaslServer.response(securityMsg.getToken)
        if (connection.isSaslComplete()) {
          logDebug("Server sasl completed: " + connection.connectionId)
        } else {
          logDebug("Server sasl not completed: " + connection.connectionId)
        }
        if (replyToken != null) {
          var securityMsgResp = SecurityMessage.fromResponse(replyToken,
            securityMsg.getConnectionId)
          var message = securityMsgResp.toBufferMessage
          if (message == null) throw new Exception("Error creating security Message")
          sendSecurityMessage(connection.getRemoteConnectionManagerId(), message)
        }
      } catch {
        case e: Exception => {
          logError("Error in server auth negotiation: " + e)
          // It would probably be better to send an error message telling other side auth failed
          // but for now just close
          connection.close()
        }
      }
    } else {
      logDebug("connection already established for this connection id: " + connection.connectionId)
    }
  }


  private def handleAuthentication(conn: Connection, bufferMessage: BufferMessage): Boolean = {
    if (bufferMessage.isSecurityNeg) {
      logDebug("This is security neg message")

      // parse as SecurityMessage
      val securityMsg = SecurityMessage.fromBufferMessage(bufferMessage)
      val connectionId = ConnectionId.createConnectionIdFromString(securityMsg.getConnectionId)

      connectionsAwaitingSasl.get(connectionId) match {
        case Some(waitingConn) => {
          // Client - this must be in response to us doing Send
          logDebug("Client handleAuth for id: " +  waitingConn.connectionId)
          handleClientAuthentication(waitingConn, securityMsg, connectionId)
        }
        case None => {
          // Server - someone sent us something and we haven't authenticated yet
          logDebug("Server handleAuth for id: " + connectionId)
          handleServerAuthentication(conn, securityMsg, connectionId)
        }
      }
      return true
    } else {
      if (!conn.isSaslComplete()) {
        // We could handle this better and tell the client we need to do authentication
        // negotiation, but for now just ignore them.
        logError("message sent that is not security negotiation message on connection " +
                 "not authenticated yet, ignoring it!!")
        return true
      }
    }
    return false
  }

  private def handleMessage(
      connectionManagerId: ConnectionManagerId,
      message: Message,
      connection: Connection) {
    logDebug("Handling [" + message + "] from [" + connectionManagerId + "]")
    message match {
      case bufferMessage: BufferMessage => {
        if (authEnabled) {
          val res = handleAuthentication(connection, bufferMessage)
          if (res == true) {
            // message was security negotiation so skip the rest
            logDebug("After handleAuth result was true, returning")
            return
          }
        }
        if (bufferMessage.hasAckId) {
          val sentMessageStatus = messageStatuses.synchronized {
            messageStatuses.get(bufferMessage.ackId) match {
              case Some(status) => {
                messageStatuses -= bufferMessage.ackId
                status
              }
              case None => {
                throw new Exception("Could not find reference for received ack message " +
                  message.id)
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
              logDebug("Response to " + bufferMessage + " is not a buffer message, it is of type "
                + ackMessage.get.getClass())
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

  private def checkSendAuthFirst(connManagerId: ConnectionManagerId, conn: SendingConnection) {
    // see if we need to do sasl before writing
    // this should only be the first negotiation as the Client!!!
    if (!conn.isSaslComplete()) {
      conn.synchronized {
        if (conn.sparkSaslClient == null) {
          conn.sparkSaslClient = new SparkSaslClient(securityManager)
          var firstResponse: Array[Byte] = null
          try {
            firstResponse = conn.sparkSaslClient.firstToken()
            var securityMsg = SecurityMessage.fromResponse(firstResponse,
              conn.connectionId.toString())
            var message = securityMsg.toBufferMessage
            if (message == null) throw new Exception("Error creating security message")
            connectionsAwaitingSasl += ((conn.connectionId, conn))
            sendSecurityMessage(connManagerId, message)
            logDebug("adding connectionsAwaitingSasl id: " + conn.connectionId)
          } catch {
            case e: Exception => {
              logError("Error getting first response from the SaslClient.", e)
              conn.close()
              throw new Exception("Error getting first response from the SaslClient")
            }
          }
        }
      }
    } else {
      logDebug("Sasl already established ")
    }
  }

  // allow us to add messages to the inbox for doing sasl negotiating
  private def sendSecurityMessage(connManagerId: ConnectionManagerId, message: Message) {
    def startNewConnection(): SendingConnection = {
      val inetSocketAddress = new InetSocketAddress(connManagerId.host, connManagerId.port)
      val newConnectionId = new ConnectionId(id, idCount.getAndIncrement.intValue)
      val newConnection = new SendingConnection(inetSocketAddress, selector, connManagerId,
        newConnectionId)
      logInfo("creating new sending connection for security! " + newConnectionId )
      registerRequests.enqueue(newConnection)

      newConnection
    }
    // I removed the lookupKey stuff as part of merge ... should I re-add it ?
    // We did not find it useful in our test-env ...
    // If we do re-add it, we should consistently use it everywhere I guess ?
    message.senderAddress = id.toSocketAddress()
    logTrace("Sending Security [" + message + "] to [" + connManagerId + "]")
    val connection = connectionsById.getOrElseUpdate(connManagerId, startNewConnection())

    // send security message until going connection has been authenticated
    connection.send(message)

    wakeupSelector()
  }

  private def sendMessage(connectionManagerId: ConnectionManagerId, message: Message) {
    def startNewConnection(): SendingConnection = {
      val inetSocketAddress = new InetSocketAddress(connectionManagerId.host,
        connectionManagerId.port)
      val newConnectionId = new ConnectionId(id, idCount.getAndIncrement.intValue)
      val newConnection = new SendingConnection(inetSocketAddress, selector, connectionManagerId,
        newConnectionId)
      logTrace("creating new sending connection: " + newConnectionId)
      registerRequests.enqueue(newConnection)

      newConnection
    }
    val connection = connectionsById.getOrElseUpdate(connectionManagerId, startNewConnection())
    if (authEnabled) {
      checkSendAuthFirst(connectionManagerId, connection)
    }
    message.senderAddress = id.toSocketAddress()
    logDebug("Before Sending [" + message + "] to [" + connectionManagerId + "]" + " " +
      "connectionid: "  + connection.connectionId)

    if (authEnabled) {
      // if we aren't authenticated yet lets block the senders until authentication completes
      try {
        connection.getAuthenticated().synchronized {
          val clock = SystemClock
          val startTime = clock.getTime()

          while (!connection.isSaslComplete()) {
            logDebug("getAuthenticated wait connectionid: " + connection.connectionId)
            // have timeout in case remote side never responds
            connection.getAuthenticated().wait(500)
            if (((clock.getTime() - startTime) >= (authTimeout * 1000))
              && (!connection.isSaslComplete())) {
              // took to long to authenticate the connection, something probably went wrong
              throw new Exception("Took to long for authentication to " + connectionManagerId +
                ", waited " + authTimeout + "seconds, failing.")
            }
          }
        }
      } catch {
        case e: Exception => logError("Exception while waiting for authentication.", e)

        // need to tell sender it failed
        messageStatuses.synchronized {
          val s = messageStatuses.get(message.id)
          s match {
            case Some(msgStatus) => {
              messageStatuses -= message.id
              logInfo("Notifying " + msgStatus.connectionManagerId)
              msgStatus.synchronized {
                msgStatus.attempted = true
                msgStatus.acked = false
                msgStatus.markDone()
              }
            }
            case None => {
              logError("no messageStatus for failed message id: " + message.id)
            }
          }
        }
      }
    }
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
    val status = new MessageStatus(
      message, connectionManagerId, s => promise.success(s.ackMessage))
    messageStatuses.synchronized {
      messageStatuses += ((message.id, status))
    }
    sendMessage(connectionManagerId, message)
    promise.future
  }

  def sendMessageReliablySync(connectionManagerId: ConnectionManagerId,
      message: Message): Option[Message] = {
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
    val conf = new SparkConf
    val manager = new ConnectionManager(9999, conf, new SecurityManager(conf))
    manager.onReceiveMessage((msg: Message, id: ConnectionManagerId) => {
      println("Received [" + msg + "] from [" + id + "]")
      None
    })

    /* testSequentialSending(manager) */
    /* System.gc() */

    /* testParallelSending(manager) */
    /* System.gc() */

    /* testParallelDecreasingSending(manager) */
    /* System.gc() */

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
    println("Sent " + count + " messages of size " + size + " in " + ms + " ms " +
      "(" + tput + " MB/s)")
    println("--------------------------")
    println()
  }

  def testParallelDecreasingSending(manager: ConnectionManager) {
    println("--------------------------")
    println("Parallel Decreasing Sending")
    println("--------------------------")
    val size = 10 * 1024 * 1024
    val count = 10
    val buffers = Array.tabulate(count) { i =>
      val bufferLen = size * (i + 1)
      val bufferContent = Array.tabulate[Byte](bufferLen)(x => x.toByte)
      ByteBuffer.allocate(bufferLen).put(bufferContent)
    }
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
    /* println("Started at " + startTime + ", finished at " + finishTime) */
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
