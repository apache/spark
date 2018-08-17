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
import java.text.SimpleDateFormat
import java.util.{Calendar, List => JList, Locale, Optional}
import java.util.concurrent.atomic.AtomicBoolean
import javax.annotation.concurrent.GuardedBy

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import scala.util.{Failure, Success, Try}

import org.apache.spark.internal.Logging
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.execution.streaming.LongOffset
import org.apache.spark.sql.execution.streaming.continuous.TextSocketContinuousReader
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.sources.v2.{ContinuousReadSupport, DataSourceOptions, DataSourceV2, MicroBatchReadSupport}
import org.apache.spark.sql.sources.v2.reader.{InputPartition, InputPartitionReader}
import org.apache.spark.sql.sources.v2.reader.streaming.{ContinuousReader, MicroBatchReader, Offset}
import org.apache.spark.sql.types.{StringType, StructField, StructType, TimestampType}
import org.apache.spark.unsafe.types.UTF8String

// Shared object for micro-batch and continuous reader
object TextSocketReader {
  val SCHEMA_REGULAR = StructType(StructField("value", StringType) :: Nil)
  val SCHEMA_TIMESTAMP = StructType(StructField("value", StringType) ::
    StructField("timestamp", TimestampType) :: Nil)
  val DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.US)
}

/**
 * A MicroBatchReader that reads text lines through a TCP socket, designed only for tutorials and
 * debugging. This MicroBatchReader will *not* work in production applications due to multiple
 * reasons, including no support for fault recovery.
 */
class TextSocketMicroBatchReader(options: DataSourceOptions) extends MicroBatchReader with Logging {

  private var startOffset: Offset = _
  private var endOffset: Offset = _

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
  private val batches = new ListBuffer[(UTF8String, Long)]

  @GuardedBy("this")
  private var currentOffset: LongOffset = LongOffset(-1L)

  @GuardedBy("this")
  private var lastOffsetCommitted: LongOffset = LongOffset(-1L)

  private val initialized: AtomicBoolean = new AtomicBoolean(false)

  /** This method is only used for unit test */
  private[sources] def getCurrentOffset(): LongOffset = synchronized {
    currentOffset.copy()
  }

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
              val newData = (
                UTF8String.fromString(line),
                DateTimeUtils.fromMillis(Calendar.getInstance().getTimeInMillis)
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

  override def setOffsetRange(start: Optional[Offset], end: Optional[Offset]): Unit = synchronized {
    startOffset = start.orElse(LongOffset(-1L))
    endOffset = end.orElse(currentOffset)
  }

  override def getStartOffset(): Offset = {
    Option(startOffset).getOrElse(throw new IllegalStateException("start offset not set"))
  }

  override def getEndOffset(): Offset = {
    Option(endOffset).getOrElse(throw new IllegalStateException("end offset not set"))
  }

  override def deserializeOffset(json: String): Offset = {
    LongOffset(json.toLong)
  }

  override def readSchema(): StructType = {
    if (options.getBoolean("includeTimestamp", false)) {
      TextSocketReader.SCHEMA_TIMESTAMP
    } else {
      TextSocketReader.SCHEMA_REGULAR
    }
  }

  override def planInputPartitions(): JList[InputPartition[InternalRow]] = {
    assert(startOffset != null && endOffset != null,
      "start offset and end offset should already be set before create read tasks.")

    val startOrdinal = LongOffset.convert(startOffset).get.offset.toInt + 1
    val endOrdinal = LongOffset.convert(endOffset).get.offset.toInt + 1

    // Internal buffer only holds the batches after lastOffsetCommitted
    val rawList = synchronized {
      if (initialized.compareAndSet(false, true)) {
        initialize()
      }

      val sliceStart = startOrdinal - lastOffsetCommitted.offset.toInt - 1
      val sliceEnd = endOrdinal - lastOffsetCommitted.offset.toInt - 1
      batches.slice(sliceStart, sliceEnd)
    }

    assert(SparkSession.getActiveSession.isDefined)
    val spark = SparkSession.getActiveSession.get
    val numPartitions = spark.sparkContext.defaultParallelism

    val slices = Array.fill(numPartitions)(new ListBuffer[(UTF8String, Long)])
    rawList.zipWithIndex.foreach { case (r, idx) =>
      slices(idx % numPartitions).append(r)
    }

    (0 until numPartitions).map { i =>
      val slice = slices(i)
      new InputPartition[InternalRow] {
        override def createPartitionReader(): InputPartitionReader[InternalRow] =
          new InputPartitionReader[InternalRow] {
            private var currentIdx = -1

            override def next(): Boolean = {
              currentIdx += 1
              currentIdx < slice.size
            }

            override def get(): InternalRow = {
              InternalRow(slice(currentIdx)._1, slice(currentIdx)._2)
            }

            override def close(): Unit = {}
          }
      }
    }.toList.asJava
  }

  override def commit(end: Offset): Unit = synchronized {
    val newOffset = LongOffset.convert(end).getOrElse(
      sys.error(s"TextSocketStream.commit() received an offset ($end) that did not " +
        s"originate with an instance of this class")
    )

    val offsetDiff = (newOffset.offset - lastOffsetCommitted.offset).toInt

    if (offsetDiff < 0) {
      sys.error(s"Offsets committed out of order: $lastOffsetCommitted followed by $end")
    }

    batches.trimStart(offsetDiff)
    lastOffsetCommitted = newOffset
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

  override def toString: String = s"TextSocketV2[host: $host, port: $port]"
}

class TextSocketSourceProvider extends DataSourceV2
  with MicroBatchReadSupport with ContinuousReadSupport with DataSourceRegister with Logging {

  private def checkParameters(params: DataSourceOptions): Unit = {
    logWarning("The socket source should not be used for production applications! " +
      "It does not support recovery.")
    if (!params.get("host").isPresent) {
      throw new AnalysisException("Set a host to read from with option(\"host\", ...).")
    }
    if (!params.get("port").isPresent) {
      throw new AnalysisException("Set a port to read from with option(\"port\", ...).")
    }
    Try {
      params.get("includeTimestamp").orElse("false").toBoolean
    } match {
      case Success(_) =>
      case Failure(_) =>
        throw new AnalysisException("includeTimestamp must be set to either \"true\" or \"false\"")
    }
  }

  override def createMicroBatchReader(
      schema: Optional[StructType],
      checkpointLocation: String,
      options: DataSourceOptions): MicroBatchReader = {
    checkParameters(options)
    if (schema.isPresent) {
      throw new AnalysisException("The socket source does not support a user-specified schema.")
    }

    new TextSocketMicroBatchReader(options)
  }

  override def createContinuousReader(
      schema: Optional[StructType],
      checkpointLocation: String,
      options: DataSourceOptions): ContinuousReader = {
    checkParameters(options)
    if (schema.isPresent) {
      throw new AnalysisException("The socket source does not support a user-specified schema.")
    }
    new TextSocketContinuousReader(options)
  }

  /** String that represents the format that this data source provider uses. */
  override def shortName(): String = "socket"
}
