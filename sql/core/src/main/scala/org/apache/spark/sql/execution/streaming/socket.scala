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
import java.text.SimpleDateFormat
import java.util.Calendar
import javax.annotation.concurrent.GuardedBy

import scala.collection.mutable.ListBuffer
import scala.util.{Failure, Success, Try}

import org.apache.spark.internal.Logging
import org.apache.spark.sql._
import org.apache.spark.sql.sources.{DataSourceRegister, StreamSourceProvider}
import org.apache.spark.sql.types.{StringType, StructField, StructType, TimestampType}


object TextSocketSource {
  val SCHEMA_REGULAR = StructType(StructField("value", StringType) :: Nil)
  val SCHEMA_TIMESTAMP = StructType(StructField("value", StringType) ::
    StructField("timestamp", TimestampType) :: Nil)
  val DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
}

/**
 * A source that reads text lines through a TCP socket, designed only for tutorials and debugging.
 * This source will *not* work in production applications due to multiple reasons, including no
 * support for fault recovery and keeping all of the text read in memory forever.
 */
class TextSocketSource(host: String, port: Int, includeTimestamp: Boolean, sqlContext: SQLContext)
  extends Source with Logging
{
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
  protected var currentOffset: LongOffset = new LongOffset(-1)

  @GuardedBy("this")
  protected var lastOffsetCommitted : LongOffset = new LongOffset(-1)

  initialize()

  private def initialize(): Unit = synchronized {
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
            TextSocketSource.this.synchronized {
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

  /** Returns the schema of the data from this source */
  override def schema: StructType = if (includeTimestamp) TextSocketSource.SCHEMA_TIMESTAMP
  else TextSocketSource.SCHEMA_REGULAR

  override def lastCommittedOffset: Option[Offset] = synchronized {
    if (lastOffsetCommitted.offset == -1) {
      None
    } else {
      Some(lastOffsetCommitted)
    }
  }

  override def getOffset: Option[Offset] = synchronized {
    if (currentOffset.offset == -1) {
      None
    } else {
      Some(currentOffset)
    }
  }

  /** Returns the data that is between the offsets (`start`, `end`]. */
  override def getBatch(start: Option[Offset], end: Offset): DataFrame = synchronized {
    val startOrdinal =
      start.map(_.asInstanceOf[LongOffset]).getOrElse(LongOffset(-1)).offset.toInt + 1
    val endOrdinal = end.asInstanceOf[LongOffset].offset.toInt + 1

    // Internal buffer only holds the batches after lastOffsetCommitted
    val rawList = synchronized {
      val sliceStart = startOrdinal - lastOffsetCommitted.offset.toInt - 1
      val sliceEnd = endOrdinal - lastOffsetCommitted.offset.toInt - 1
      batches.slice(sliceStart, sliceEnd)
    }

    import sqlContext.implicits._
    val rawBatch = sqlContext.createDataset(rawList)



    // Underlying MemoryStream has schema (String, Timestamp); strip out the timestamp
    // if requested.
    if (includeTimestamp) {
      rawBatch.toDF("value", "timestamp")
    } else {
      // Strip out timestamp
      rawBatch.select("_1").toDF("value")
    }
  }

  override def commit(end: Offset): Unit = synchronized {
    if (end.isInstanceOf[LongOffset]) {
      val newOffset = end.asInstanceOf[LongOffset]
      val offsetDiff = (newOffset.offset - lastOffsetCommitted.offset).toInt

      if (offsetDiff < 0) {
        sys.error(s"Offsets committed out of order: $lastOffsetCommitted followed by $end")
      }

      batches.trimStart(offsetDiff)
      lastOffsetCommitted = newOffset
    } else {
      sys.error(s"TextSocketStream.commit() received an offset ($end) that did not " +
        s"originate with an instance of this class")
    }
  }

  /** Stop this source. */
  override def stop(): Unit = synchronized {
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

class TextSocketSourceProvider extends StreamSourceProvider with DataSourceRegister with Logging {
  private def parseIncludeTimestamp(params: Map[String, String]): Boolean = {
    Try(params.getOrElse("includeTimestamp", "false").toBoolean) match {
      case Success(bool) => bool
      case Failure(_) =>
        throw new AnalysisException("includeTimestamp must be set to either \"true\" or \"false\"")
    }
  }

  /** Returns the name and schema of the source that can be used to continually read data. */
  override def sourceSchema(
      sqlContext: SQLContext,
      schema: Option[StructType],
      providerName: String,
      parameters: Map[String, String]): (String, StructType) = {
    logWarning("The socket source should not be used for production applications! " +
      "It does not support recovery.")
    if (!parameters.contains("host")) {
      throw new AnalysisException("Set a host to read from with option(\"host\", ...).")
    }
    if (!parameters.contains("port")) {
      throw new AnalysisException("Set a port to read from with option(\"port\", ...).")
    }
    val schema =
      if (parseIncludeTimestamp(parameters)) {
        TextSocketSource.SCHEMA_TIMESTAMP
      } else {
        TextSocketSource.SCHEMA_REGULAR
      }
    ("textSocket", schema)
  }

  override def createSource(
      sqlContext: SQLContext,
      metadataPath: String,
      schema: Option[StructType],
      providerName: String,
      parameters: Map[String, String]): Source = {
    val host = parameters("host")
    val port = parameters("port").toInt
    new TextSocketSource(host, port, parseIncludeTimestamp(parameters), sqlContext)
  }

  /** String that represents the format that this data source provider uses. */
  override def shortName(): String = "socket"
}
