package spark.streaming.util

import spark.Logging

import scala.collection.mutable.{ArrayBuffer, SynchronizedQueue}

import java.net._
import java.io._
import java.nio._
import java.nio.charset._
import java.nio.channels._
import java.nio.channels.spi._

abstract class ConnectionHandler(host: String, port: Int, connect: Boolean) 
extends Thread with Logging {

  val selector = SelectorProvider.provider.openSelector()
  val interestChangeRequests = new SynchronizedQueue[(SelectionKey, Int)]
  
  initLogging()
  
  override def run() {
    try {
      if (connect) {
        connect()
      } else {
        listen()
      }
      
      var interrupted = false
      while(!interrupted) {
        
        preSelect()
  
        while(!interestChangeRequests.isEmpty) {
          val (key, ops) = interestChangeRequests.dequeue
          val lastOps = key.interestOps()
          key.interestOps(ops)
          
          def intToOpStr(op: Int): String = {
            val opStrs = new ArrayBuffer[String]()
            if ((op & SelectionKey.OP_READ) != 0) opStrs += "READ"
            if ((op & SelectionKey.OP_WRITE) != 0) opStrs += "WRITE"
            if ((op & SelectionKey.OP_CONNECT) != 0) opStrs += "CONNECT"
            if ((op & SelectionKey.OP_ACCEPT) != 0) opStrs += "ACCEPT"
            if (opStrs.size > 0) opStrs.reduceLeft(_ + " | " + _) else " "
          }
          
          logTrace("Changed ops from [" + intToOpStr(lastOps) + "] to [" + intToOpStr(ops) + "]")
        }

        selector.select()
        interrupted = Thread.currentThread.isInterrupted

        val selectedKeys = selector.selectedKeys().iterator()
        while (selectedKeys.hasNext) {
          val key = selectedKeys.next.asInstanceOf[SelectionKey]
          selectedKeys.remove()
          if (key.isValid) {
            if (key.isAcceptable) {
              accept(key)
            } else if (key.isConnectable) {
              finishConnect(key)
            } else if (key.isReadable) {
              read(key)
            } else if (key.isWritable) {
              write(key)
            }
          }
        }
      }
    } catch {
      case e: Exception => {
        logError("Error in select loop", e)
      }
    }
  }

  def connect() {
    val socketAddress = new InetSocketAddress(host, port)
    val channel = SocketChannel.open()
    channel.configureBlocking(false)
    channel.socket.setReuseAddress(true)
    channel.socket.setTcpNoDelay(true)
    channel.connect(socketAddress)
    channel.register(selector, SelectionKey.OP_CONNECT)
    logInfo("Initiating connection to [" + socketAddress + "]")
  }

  def listen() {
    val channel = ServerSocketChannel.open()
    channel.configureBlocking(false)
    channel.socket.setReuseAddress(true)
    channel.socket.setReceiveBufferSize(256 * 1024) 
    channel.socket.bind(new InetSocketAddress(port))
    channel.register(selector, SelectionKey.OP_ACCEPT)
    logInfo("Listening on port " + port)
  }

  def finishConnect(key: SelectionKey) {
    try {
      val channel = key.channel.asInstanceOf[SocketChannel]
      val address = channel.socket.getRemoteSocketAddress
      channel.finishConnect()
      logInfo("Connected to [" + host + ":" + port + "]")
      ready(key)
    } catch {
      case e: IOException => {
        logError("Error finishing connect to " + host + ":" + port)
        close(key)
      }
    }
  }

  def accept(key: SelectionKey) {
    try {
      val serverChannel = key.channel.asInstanceOf[ServerSocketChannel]
      val channel = serverChannel.accept()
      val address = channel.socket.getRemoteSocketAddress
      channel.configureBlocking(false)
      logInfo("Accepted connection from [" + address + "]")
      ready(channel.register(selector, 0))
    } catch {
      case e: IOException => {
        logError("Error accepting connection", e)
      }
    }
  }

  def changeInterest(key: SelectionKey, ops: Int) {
    logTrace("Added request to change ops to " + ops)
    interestChangeRequests += ((key, ops))  
  }

  def ready(key: SelectionKey) 

  def preSelect() {
  }

  def read(key: SelectionKey) { 
    throw new UnsupportedOperationException("Cannot read on connection of type " + this.getClass.toString) 
  } 
  
  def write(key: SelectionKey) { 
    throw new UnsupportedOperationException("Cannot write on connection of type " + this.getClass.toString) 
  }

  def close(key: SelectionKey) {
    try {
      key.channel.close()
      key.cancel()
      Thread.currentThread.interrupt
    } catch {
      case e: Exception => logError("Error closing connection", e)
    }
  }
}
