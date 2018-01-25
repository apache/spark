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

package org.apache.spark.sql.execution.streaming

import java.io.{BufferedReader, InputStreamReader, IOException}
import java.net.Socket
import java.sql.Timestamp
import java.util.Calendar
import javax.annotation.concurrent.GuardedBy

import scala.collection.mutable.ListBuffer

import org.apache.spark.internal.Logging

trait TextSocketReader extends Logging {

  protected def host: String

  protected def port: Int

  @GuardedBy("this")
  private var socket: Socket = null

  @GuardedBy("this")
  private var readThread: Thread = null

  /**
   * All batches from `lastCommittedOffset + 1` to `currentOffset`, inclusive.
   * Stored in a ListBuffer to facilitate removing committed batches.
   */
  @GuardedBy("this")
  protected val batches = new ListBuffer[(String, Timestamp)]

  @GuardedBy("this")
  protected var currentOffset: Long = -1L

  @GuardedBy("this")
  protected var lastOffsetCommitted : Long = -1L

  protected def initialize(): Unit = synchronized {
    socket = new Socket(host, port)
    val reader = new BufferedReader(new InputStreamReader(socket.getInputStream))
    readThread = new Thread(s"TextSocketSource($host, $port)") {
      setDaemon(true)

      override def run(): Unit = {
        try {
          while (true) {
            val line = reader.readLine()
            if (line == null) {
              // End of file reached
              logWarning(s"Stream closed by $host:$port")
              return
            }
            TextSocketReader.this.synchronized {
              val newData = (line,
                Timestamp.valueOf(
                  TextSocketSource.DATE_FORMAT.format(Calendar.getInstance().getTime()))
              )
              currentOffset = currentOffset + 1
              batches.append(newData)
            }
          }
        } catch {
          case e: IOException =>
        }
      }
    }
    readThread.start()
  }

  protected def getOffsetInternal: Option[Long] = synchronized {
    if (currentOffset == -1L) {
      None
    } else {
      Some(currentOffset)
    }
  }

  protected def getBatchInternal(
      start: Option[Long],
      end: Option[Long]): Seq[(String, Timestamp)] = {
    val startOrdinal = start.getOrElse(-1L).toInt + 1
    val endOrdinal = end.getOrElse(-1L).toInt + 1

    // Internal buffer only holds the batches after lastOffsetCommitted
    synchronized {
      val sliceStart = startOrdinal - lastOffsetCommitted.toInt - 1
      val sliceEnd = endOrdinal - lastOffsetCommitted.toInt - 1
      batches.slice(sliceStart, sliceEnd)
    }
  }

  protected def commitInternal(end: Long): Unit = synchronized {
    val offsetDiff = (end - lastOffsetCommitted).toInt

    if (offsetDiff < 0) {
      sys.error(s"Offsets committed out of order: $lastOffsetCommitted followed by $end")
    }

    batches.trimStart(offsetDiff)
    lastOffsetCommitted = end
  }

  /** Stop this source. */
  def stop(): Unit = synchronized {
    if (socket != null) {
      try {
        // Unfortunately, BufferedReader.readLine() cannot be interrupted, so the only way to
        // stop the readThread is to close the socket.
        socket.close()
      } catch {
        case e: IOException =>
      }
      socket = null
    }
  }
}
