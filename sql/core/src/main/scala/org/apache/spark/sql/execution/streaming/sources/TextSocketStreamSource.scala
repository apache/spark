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
import java.util._
import java.util.{List => JList}
import java.util.concurrent.atomic.AtomicLong
import javax.annotation.concurrent.GuardedBy

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import scala.util.{Failure, Success, Try}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{AnalysisException, Row}
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.sources.v2.{DataSourceV2, DataSourceV2Options}
import org.apache.spark.sql.sources.v2.reader.{DataReader, ReadTask}
import org.apache.spark.sql.sources.v2.streaming.MicroBatchReadSupport
import org.apache.spark.sql.sources.v2.streaming.reader.{MicroBatchReader, Offset}
import org.apache.spark.sql.types.{StringType, StructField, StructType, TimestampType}


object TextSocketSourceProviderV2 {
  val HOST = "host"
  val PORT = "port"
  val INCLUDE_TIMESTAMP = "includeTimestamp"
  val NUM_PARTITIONS = "numPartitions"
  val SCHEMA_REGULAR = StructType(StructField("value", StringType) :: Nil)
  val SCHEMA_TIMESTAMP = StructType(StructField("value", StringType) ::
    StructField("timestamp", TimestampType) :: Nil)
  val DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.US)
}

class TextSocketSourceProviderV2 extends DataSourceV2
    with MicroBatchReadSupport with DataSourceRegister with Logging {
  override def shortName(): String = "socketv2"

  override def createMicroBatchReader(
      schema: Optional[StructType],
      checkpointLocation: String,
      options: DataSourceV2Options): MicroBatchReader = {
    logWarning("The socket source should not be used for production applications! " +
      "It does not support recovery.")
    if (!options.get(TextSocketSourceProviderV2.HOST).isPresent) {
      throw new AnalysisException("Set a host to read from with option(\"host\", ...).")
    }
    if (!options.get(TextSocketSourceProviderV2.PORT).isPresent) {
      throw new AnalysisException("Set a port to read from with option(\"port\", ...).")
    }
    if (schema.isPresent) {
      throw new AnalysisException("The socket source does not support a user-specified schema.")
    }

    if (options.get(TextSocketSourceProviderV2.INCLUDE_TIMESTAMP).isPresent) {
      Try(options.get(TextSocketSourceProviderV2.INCLUDE_TIMESTAMP).get().toBoolean) match {
        case Success(bool) =>
        case Failure(_) =>
          throw new AnalysisException(
            "includeTimestamp must be set to either \"true\" or \"false\"")
      }
    }

    new TextSocketStreamMicroBatchReader(options)
  }
}

case class TextSocketStreamOffset(offset: Long) extends Offset {
  override def json(): String = offset.toString
}

class TextSocketStreamMicroBatchReader(options: DataSourceV2Options)
  extends MicroBatchReader with Logging {

  import TextSocketSourceProviderV2._

  private var start: TextSocketStreamOffset = _
  private var end: TextSocketStreamOffset = _

  private val host = options.get(HOST).get()
  private val port = options.get(PORT).get().toInt
  private val includeTimestamp = options.getBoolean(INCLUDE_TIMESTAMP, false)
  private val numPartitions = options.getInt(NUM_PARTITIONS, 1)

  @GuardedBy("this")
  private var socket: Socket = _

  @GuardedBy("this")
  private var readThread: Thread = _

  @GuardedBy("this")
  private val batches = new ListBuffer[(String, Timestamp)]

  private val currentOffset = new AtomicLong(-1L)

  private var initialized = false

  @GuardedBy("this")
  private var lastOffsetCommitted: Long = -1L

  override def setOffsetRange(start: Optional[Offset], end: Optional[Offset]): Unit = {
    if (!initialized) {
      initialize()
      initialized = true
    }

    this.start = start.orElse(TextSocketStreamOffset(-1L)).asInstanceOf[TextSocketStreamOffset]
    this.end =
      end.orElse(TextSocketStreamOffset(currentOffset.get())).asInstanceOf[TextSocketStreamOffset]
  }

  override def getStartOffset(): Offset = {
    Option(start).getOrElse(throw new IllegalStateException("start offset not set"))
  }

  override def getEndOffset(): Offset = {
    Option(end).getOrElse(throw new IllegalStateException("end offset not set"))
  }

  override def deserializeOffset(json: String): Offset = {
    TextSocketStreamOffset(json.toLong)
  }

  override def readSchema(): StructType = {
    if (includeTimestamp) {
      SCHEMA_TIMESTAMP
    } else {
      SCHEMA_REGULAR
    }
  }

  override def createReadTasks(): JList[ReadTask[Row]] = {
    val startOrdinal = start.offset.toInt + 1
    val endOrdinal = end.offset.toInt + 1
    val sliceStart = startOrdinal - lastOffsetCommitted.toInt - 1
    val sliceEnd = endOrdinal - lastOffsetCommitted.toInt - 1

    val rawList = TextSocketStreamMicroBatchReader.this.synchronized {
      batches.slice(sliceStart, sliceEnd)
    }

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

  override def commit(end: Offset): Unit = synchronized {
    val newOffset = end.asInstanceOf[TextSocketStreamOffset]
    val offsetDiff = (newOffset.offset - lastOffsetCommitted).toInt

    if (offsetDiff < 0) {
      sys.error(s"Offsets committed out of order: $lastOffsetCommitted followed by $end")
    }

    batches.trimStart(offsetDiff)
    lastOffsetCommitted = newOffset.offset
  }

  override def stop(): Unit = synchronized {
    if (socket != null) {
      try {
        // Unfortunately, BufferedReader.readLine() cannot be interrupted, so the only way to
        // stop the readThread is to close the socket.
        socket.close()
      } catch {
        case _: IOException =>
      }
      socket = null
    }
  }

  override def toString: String = s"TextSocketStreamMicroBatchRead[host: $host, port: $port]"

  private def initialize(): Unit = synchronized {
    socket = new Socket(host, port)
    val reader = new BufferedReader(new InputStreamReader(socket.getInputStream))
    readThread = new Thread(s"TextSocketStreamMicroBatchReader($host:$port)") {
      setDaemon(true)

      override def run(): Unit = {
        try {
          while (true) {
            val line = reader.readLine()
            if (line == null) {
              logWarning(s"Stream closed by $host:$port")
              return
            }
            TextSocketStreamMicroBatchReader.this.synchronized {
              val newData = (line,
                Timestamp.valueOf(
                  DATE_FORMAT.format(Calendar.getInstance().getTime()))
              )
              currentOffset.getAndIncrement()
              batches.append(newData)
            }
          }
        } catch {
          case e: IOException =>
            logWarning(s"Caught exception during receiving new data from $host:$port", e)

          case _: InterruptedException =>
        }
      }
    }
    readThread.start()
  }
}
