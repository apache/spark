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

package org.apache.spark.sql.execution.streaming.sources

import java.io.{BufferedReader, InputStreamReader, IOException}
import java.net.Socket
import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.{Calendar, List => JList, Locale, Optional}
import javax.annotation.concurrent.GuardedBy

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import scala.util.{Failure, Success, Try}

import org.apache.spark.internal.Logging
import org.apache.spark.sql._
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.sources.v2.{DataSourceV2, DataSourceV2Options}
import org.apache.spark.sql.sources.v2.reader.{DataReader, ReadTask}
import org.apache.spark.sql.sources.v2.streaming.MicroBatchReadSupport
import org.apache.spark.sql.sources.v2.streaming.reader.{MicroBatchReader, Offset => V2Offset}
import org.apache.spark.sql.types.{StringType, StructField, StructType, TimestampType}

object TextSocketSource {
  val SCHEMA_REGULAR = StructType(StructField("value", StringType) :: Nil)
  val SCHEMA_TIMESTAMP = StructType(StructField("value", StringType) ::
    StructField("timestamp", TimestampType) :: Nil)
  val DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.US)
}

case class TextSocketOffset(offset: Long) extends V2Offset {
  override def json(): String = offset.toString
}

/**
 * A MicroBatchReader that reads text lines through a TCP socket, designed only for tutorials and
 * debugging. This MicroBatchReader will *not* work in production applications due to multiple
 * reasons, including no support for fault recovery and keeping all of the text read in memory
 * forever.
 */
class TextSocketMicroBatchReader(options: DataSourceV2Options)
  extends MicroBatchReader with Logging {

  private var startOffset: TextSocketOffset = _
  private var endOffset: TextSocketOffset = _

  private val host: String = options.get("host").get()
  private val port: Int = options.get("port").get().toInt

  @GuardedBy("this")
  private var socket: Socket = null

  @GuardedBy("this")
  private var readThread: Thread = null

  /**
   * All batches from `lastCommittedOffset + 1` to `currentOffset`, inclusive.
   * Stored in a ListBuffer to facilitate removing committed batches.
   */
  @GuardedBy("this")
  private val batches = new ListBuffer[(String, Timestamp)]

  @GuardedBy("this")
  private var currentOffset: Long = -1L

  @GuardedBy("this")
  private var lastOffsetCommitted: Long = -1L

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
            TextSocketMicroBatchReader.this.synchronized {
              val newData = (line,
                Timestamp.valueOf(
                  TextSocketSource.DATE_FORMAT.format(Calendar.getInstance().getTime()))
              )
              currentOffset += 1
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

  override def setOffsetRange(
      start: Optional[V2Offset],
      end: Optional[V2Offset]): Unit = synchronized {
    startOffset = start.orElse(TextSocketOffset(-1L)).asInstanceOf[TextSocketOffset]
    endOffset = end.orElse(TextSocketOffset(currentOffset)).asInstanceOf[TextSocketOffset]
  }

  override def getStartOffset(): V2Offset = {
    Option(startOffset).getOrElse(throw new IllegalStateException("start offset not set"))
  }

  override def getEndOffset(): V2Offset = {
    Option(endOffset).getOrElse(throw new IllegalStateException("end offset not set"))
  }

  override def deserializeOffset(json: String): V2Offset = {
    TextSocketOffset(json.toLong)
  }

  override def readSchema(): StructType = {
    val includeTimestamp = options.getBoolean("includeTimestamp", false)
    if (includeTimestamp) TextSocketSource.SCHEMA_TIMESTAMP else TextSocketSource.SCHEMA_REGULAR
  }

  override def createReadTasks(): JList[ReadTask[Row]] = {
    assert(startOffset != null && endOffset != null,
      "start offset and end offset should already be set before create read tasks.")

    val startOrdinal = startOffset.offset.toInt + 1
    val endOrdinal = endOffset.offset.toInt + 1

    // Internal buffer only holds the batches after lastOffsetCommitted
    val rawList = synchronized {
      val sliceStart = startOrdinal - lastOffsetCommitted.toInt - 1
      val sliceEnd = endOrdinal - lastOffsetCommitted.toInt - 1
      batches.slice(sliceStart, sliceEnd)
    }

    assert(SparkSession.getActiveSession.isDefined)
    val spark = SparkSession.getActiveSession.get
    val numPartitions = spark.sparkContext.defaultParallelism

    val slices = Array.fill(numPartitions)(new ListBuffer[(String, Timestamp)])
    rawList.zipWithIndex.foreach { case (r, idx) =>
      slices(idx % numPartitions).append(r)
    }

    (0 until numPartitions).map { i =>
      val slice = slices(i)
      new ReadTask[Row] {
        override def createDataReader(): DataReader[Row] = new DataReader[Row] {
          private var currentIdx = -1

          override def next(): Boolean = {
            currentIdx += 1
            currentIdx < slice.size
          }

          override def get(): Row = {
            Row(slice(currentIdx)._1, slice(currentIdx)._2)
          }

          override def close(): Unit = {}
        }
      }
    }.toList.asJava
  }

  override def commit(end: V2Offset): Unit = synchronized {
    val newOffset = end.asInstanceOf[TextSocketOffset]

    val offsetDiff = (newOffset.offset - lastOffsetCommitted).toInt

    if (offsetDiff < 0) {
      sys.error(s"Offsets committed out of order: $lastOffsetCommitted followed by $end")
    }

    batches.trimStart(offsetDiff)
    lastOffsetCommitted = newOffset.offset
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

  override def toString: String = s"TextSocketMicroBatchReader[host: $host, port: $port]"
}

class TextSocketSourceProvider extends DataSourceV2
  with MicroBatchReadSupport with DataSourceRegister with Logging {

  private def checkParameters(params: Map[String, String]): Unit = {
    logWarning("The socket source should not be used for production applications! " +
      "It does not support recovery.")
    if (!params.contains("host")) {
      throw new AnalysisException("Set a host to read from with option(\"host\", ...).")
    }
    if (!params.contains("port")) {
      throw new AnalysisException("Set a port to read from with option(\"port\", ...).")
    }
    Try {
      params.get("includeTimestamp")
        .orElse(params.get("includetimestamp"))
        .getOrElse("false")
        .toBoolean
    } match {
      case Success(_) =>
      case Failure(_) =>
        throw new AnalysisException("includeTimestamp must be set to either \"true\" or \"false\"")
    }
  }

  override def createMicroBatchReader(
      schema: Optional[StructType],
      checkpointLocation: String,
      options: DataSourceV2Options): MicroBatchReader = {
    checkParameters(options.asMap().asScala.toMap)
    if (schema.isPresent) {
      throw new AnalysisException("The socket source does not support a user-specified schema.")
    }

    new TextSocketMicroBatchReader(options)
  }

  /** String that represents the format that this data source provider uses. */
  override def shortName(): String = "socket"
}
