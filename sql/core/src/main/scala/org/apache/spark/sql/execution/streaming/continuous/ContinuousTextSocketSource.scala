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

package org.apache.spark.sql.execution.streaming.continuous

import java.io.{BufferedReader, InputStreamReader, IOException}
import java.net.Socket
import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.{Calendar, List => JList, Locale}
import javax.annotation.concurrent.GuardedBy

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

import org.json4s.{DefaultFormats, NoTypeHints}
import org.json4s.jackson.Serialization

import org.apache.spark.SparkEnv
import org.apache.spark.internal.Logging
import org.apache.spark.rpc.{RpcCallContext, RpcEndpointRef, RpcEnv, ThreadSafeRpcEndpoint}
import org.apache.spark.sql._
import org.apache.spark.sql.execution.streaming.continuous.TextSocketContinuousReader.GetRecord
import org.apache.spark.sql.sources.v2.DataSourceOptions
import org.apache.spark.sql.sources.v2.reader.{DataReader, DataReaderFactory}
import org.apache.spark.sql.sources.v2.reader.streaming.{ContinuousDataReader, ContinuousReader, Offset, PartitionOffset}
import org.apache.spark.sql.types.{StringType, StructField, StructType, TimestampType}
import org.apache.spark.util.RpcUtils


object TextSocketContinuousReader {
  val SCHEMA_REGULAR = StructType(StructField("value", StringType) :: Nil)
  val SCHEMA_TIMESTAMP = StructType(
    StructField("value", StringType)
      :: StructField("timestamp", TimestampType) :: Nil)
  val DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.US)

  case class GetRecord(offset: TextSocketPartitionOffset)

}

/**
 * A ContinuousReader that reads text lines through a TCP socket, designed only for tutorials and
 * debugging. This ContinuousReader will *not* work in production applications due to multiple
 * reasons, including no support for fault recovery.
 *
 * The driver maintains a socket connection to the host-port, keeps the received messages in
 * buckets and serves the messages to the executors via a RPC endpoint.
 */
class TextSocketContinuousReader(options: DataSourceOptions) extends ContinuousReader with Logging {
  implicit val defaultFormats: DefaultFormats = DefaultFormats

  private val host: String = options.get("host").get()
  private val port: Int = options.get("port").get().toInt

  assert(SparkSession.getActiveSession.isDefined)
  private val spark = SparkSession.getActiveSession.get
  private val numPartitions = spark.sparkContext.defaultParallelism

  @GuardedBy("this")
  private var socket: Socket = _

  @GuardedBy("this")
  private var readThread: Thread = _

  @GuardedBy("this")
  private val buckets = Seq.fill(numPartitions)(new ListBuffer[(String, Timestamp)])

  @GuardedBy("this")
  private var currentOffset: Int = -1

  private var startOffset: TextSocketOffset = _

  private val recordEndpoint = new RecordEndpoint()
  @volatile private var endpointRef: RpcEndpointRef = _

  initialize()

  override def mergeOffsets(offsets: Array[PartitionOffset]): Offset = {
    assert(offsets.length == numPartitions)
    val offs = offsets
      .map(_.asInstanceOf[TextSocketPartitionOffset])
      .sortBy(_.partitionId)
      .map(_.offset)
      .toList
    TextSocketOffset(offs)
  }

  override def deserializeOffset(json: String): Offset = {
    TextSocketOffset(Serialization.read[List[Int]](json))
  }

  override def setStartOffset(offset: java.util.Optional[Offset]): Unit = {
    this.startOffset = offset
      .orElse(TextSocketOffset(List.fill(numPartitions)(0)))
      .asInstanceOf[TextSocketOffset]
  }

  override def getStartOffset: Offset = startOffset

  override def readSchema(): StructType = {
    if (includeTimestamp) {
      TextSocketContinuousReader.SCHEMA_TIMESTAMP
    } else {
      TextSocketContinuousReader.SCHEMA_REGULAR
    }
  }

  override def createDataReaderFactories(): JList[DataReaderFactory[Row]] = {

    val endpointName = s"TextSocketContinuousReaderEndpoint-${java.util.UUID.randomUUID()}"
    endpointRef = recordEndpoint.rpcEnv.setupEndpoint(endpointName, recordEndpoint)

    val offsets = startOffset match {
      case off: TextSocketOffset => off.offsets
      case off =>
        throw new IllegalArgumentException(
          s"invalid offset type ${off.getClass} for TextSocketContinuousReader")
    }

    if (offsets.size != numPartitions) {
      throw new IllegalArgumentException(
        s"The previous run contained ${offsets.size} partitions, but" +
          s" $numPartitions partitions are currently configured. The numPartitions option" +
          " cannot be changed.")
    }

    startOffset.offsets.zipWithIndex.map {
      case (offset, i) =>
        TextSocketContinuousDataReaderFactory(
          endpointName, i, offset): DataReaderFactory[Row]
    }.asJava

  }

  override def commit(end: Offset): Unit = synchronized {
    val endOffset = end.asInstanceOf[TextSocketOffset]
    endOffset.offsets.zipWithIndex.foreach {
      case (offset, partition) =>
        buckets(partition).trimStart(offset - startOffset.offsets(partition))
    }
    startOffset = endOffset
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
    if (endpointRef != null) recordEndpoint.rpcEnv.stop(endpointRef)
  }

  private def initialize(): Unit = synchronized {
    socket = new Socket(host, port)
    val reader = new BufferedReader(new InputStreamReader(socket.getInputStream))
    // Thread continuously reads from a socket and inserts data into buckets
    readThread = new Thread(s"TextSocketContinuousReader($host, $port)") {
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
            TextSocketContinuousReader.this.synchronized {
              currentOffset += 1
              val newData = (line,
                Timestamp.valueOf(
                  TextSocketContinuousReader.DATE_FORMAT.format(Calendar.getInstance().getTime()))
              )
              buckets(currentOffset % numPartitions) += newData
            }
          }
        } catch {
          case e: IOException =>
        }
      }
    }

    readThread.start()
  }

  /**
   * Endpoint for executors to poll for records.
   */
  private class RecordEndpoint extends ThreadSafeRpcEndpoint {

    override val rpcEnv: RpcEnv = SparkEnv.get.rpcEnv

    override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
      case GetRecord(TextSocketPartitionOffset(partition, index)) =>
        TextSocketContinuousReader.this.synchronized {
          val bufIndex = index - startOffset.offsets(partition)
          val record = if (buckets(partition).size <= bufIndex) {
            None
          } else {
            Some(buckets(partition)(bufIndex))
          }
          context.reply(
            record.map(r => if (includeTimestamp) Row(r) else Row(r._1)
            )
          )
        }
    }
  }

  override def toString: String = s"TextSocketContinuousReader[host: $host, port: $port]"

  private def includeTimestamp: Boolean = options.getBoolean("includeTimestamp", false)

}

/**
 * Continuous text socket data reader factory.
 */
case class TextSocketContinuousDataReaderFactory(
    driverEndpointName: String,
    partitionId: Int,
    startOffset: Int)
extends DataReaderFactory[Row] {

  override def createDataReader(): DataReader[Row] =
    new TextSocketContinuousDataReader(driverEndpointName, partitionId, startOffset)
}

/**
 * Continuous text socket data reader.
 *
 * Polls the driver endpoint for new records.
 */
class TextSocketContinuousDataReader(
    driverEndpointName: String,
    partitionId: Int,
    startOffset: Int)
  extends ContinuousDataReader[Row] {

  private val endpoint = RpcUtils.makeDriverRef(
    driverEndpointName,
    SparkEnv.get.conf,
    SparkEnv.get.rpcEnv)

  private var currentOffset = startOffset
  private var current: Option[Row] = None

  override def next(): Boolean = {
    try {
      current = getRecord
      while (current.isEmpty) {
        Thread.sleep(100)
        current = getRecord
      }
      currentOffset += 1
    } catch {
      case _: InterruptedException =>
        // Someone's trying to end the task; just let them.
        return false
    }
    true
  }

  override def get(): Row = {
    current.get
  }

  override def close(): Unit = {}

  override def getOffset: PartitionOffset = TextSocketPartitionOffset(partitionId, currentOffset)

  private def getRecord: Option[Row] =
    endpoint.askSync[Option[Row]](GetRecord(TextSocketPartitionOffset(partitionId, currentOffset)))

}

case class TextSocketPartitionOffset(partitionId: Int, offset: Int) extends PartitionOffset

case class TextSocketOffset(offsets: List[Int]) extends Offset {
  private implicit val formats = Serialization.formats(NoTypeHints)
  override def json: String = Serialization.write(offsets)
}
