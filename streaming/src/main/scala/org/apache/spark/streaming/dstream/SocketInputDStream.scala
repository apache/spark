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

package org.apache.spark.streaming.dstream

import java.io._
import java.net.{ConnectException, Socket}
import java.nio.charset.StandardCharsets

import scala.reflect.ClassTag
import scala.util.control.NonFatal

import org.apache.spark.internal.Logging
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.util.NextIterator

private[streaming]
class SocketInputDStream[T: ClassTag](
    _ssc: StreamingContext,
    host: String,
    port: Int,
    bytesToObjects: InputStream => Iterator[T],
    storageLevel: StorageLevel
  ) extends ReceiverInputDStream[T](_ssc) {

  def getReceiver(): Receiver[T] = {
    new SocketReceiver(host, port, bytesToObjects, storageLevel)
  }
}

private[streaming]
class SocketReceiver[T: ClassTag](
    host: String,
    port: Int,
    bytesToObjects: InputStream => Iterator[T],
    storageLevel: StorageLevel
  ) extends Receiver[T](storageLevel) with Logging {

  private var socket: Socket = _

  def onStart(): Unit = {

    logInfo(s"Connecting to $host:$port")
    try {
      socket = new Socket(host, port)
    } catch {
      case e: ConnectException =>
        restart(s"Error connecting to $host:$port", e)
        return
    }
    logInfo(s"Connected to $host:$port")

    // Start the thread that receives data over a connection
    new Thread("Socket Receiver") {
      setDaemon(true)
      override def run(): Unit = { receive() }
    }.start()
  }

  def onStop(): Unit = {
    // in case restart thread close it twice
    synchronized {
      if (socket != null) {
        socket.close()
        socket = null
        logInfo(s"Closed socket to $host:$port")
      }
    }
  }

  /** Create a socket connection and receive data until receiver is stopped */
  def receive(): Unit = {
    try {
      val iterator = bytesToObjects(socket.getInputStream())
      while(!isStopped && iterator.hasNext) {
        store(iterator.next())
      }
      if (!isStopped()) {
        restart("Socket data stream had no more data")
      } else {
        logInfo("Stopped receiving")
      }
    } catch {
      case NonFatal(e) =>
        logWarning("Error receiving data", e)
        restart("Error receiving data", e)
    } finally {
      onStop()
    }
  }
}

private[streaming]
object SocketReceiver  {

  /**
   * This methods translates the data from an inputstream (say, from a socket)
   * to '\n' delimited strings and returns an iterator to access the strings.
   */
  def bytesToLines(inputStream: InputStream): Iterator[String] = {
    val dataInputStream = new BufferedReader(
      new InputStreamReader(inputStream, StandardCharsets.UTF_8))
    new NextIterator[String] {
      protected override def getNext() = {
        val nextValue = dataInputStream.readLine()
        if (nextValue == null) {
          finished = true
        }
        nextValue
      }

      protected override def close(): Unit = {
        dataInputStream.close()
      }
    }
  }
}
