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

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.{List => JList, Locale, Optional}

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import scala.util.{Failure, Success, Try}

import org.apache.spark.internal.Logging
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.{DataSourceRegister, StreamSourceProvider}
import org.apache.spark.sql.sources.v2.{DataSourceV2, DataSourceV2Options}
import org.apache.spark.sql.sources.v2.reader.{DataReader, ReadTask}
import org.apache.spark.sql.sources.v2.streaming.MicroBatchReadSupport
import org.apache.spark.sql.sources.v2.streaming.reader.{MicroBatchReader, Offset => V2Offset}
import org.apache.spark.sql.types.{StringType, StructField, StructType, TimestampType}
import org.apache.spark.unsafe.types.UTF8String

object TextSocketSource {
  val SCHEMA_REGULAR = StructType(StructField("value", StringType) :: Nil)
  val SCHEMA_TIMESTAMP = StructType(StructField("value", StringType) ::
    StructField("timestamp", TimestampType) :: Nil)
  val DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.US)
}

/**
 * A source that reads text lines through a TCP socket, designed only for tutorials and debugging.
 * This source will *not* work in production applications due to multiple reasons, including no
 * support for fault recovery and keeping all of the text read in memory forever.
 */
class TextSocketSource(
    protected val host: String,
    protected val port: Int,
    includeTimestamp: Boolean,
    sqlContext: SQLContext)
  extends Source with TextSocketReader with Logging {

  initialize()

  /** Returns the schema of the data from this source */
  override def schema: StructType =
    if (includeTimestamp) TextSocketSource.SCHEMA_TIMESTAMP else TextSocketSource.SCHEMA_REGULAR

  override def getOffset: Option[Offset] = getOffsetInternal.map(LongOffset(_))

  /** Returns the data that is between the offsets (`start`, `end`]. */
  override def getBatch(start: Option[Offset], end: Offset): DataFrame = {
    val rawList = getBatchInternal(start.flatMap(LongOffset.convert).map(_.offset),
      LongOffset.convert(end).map(_.offset))

    val rdd = sqlContext.sparkContext
      .parallelize(rawList)
      .map { case (v, ts) => InternalRow(UTF8String.fromString(v), ts.getTime) }
    sqlContext.internalCreateDataFrame(rdd, schema, isStreaming = true)
  }

  override def commit(end: Offset): Unit = {
    val newOffset = LongOffset.convert(end).getOrElse(
      sys.error(s"TextSocketStream.commit() received an offset ($end) that did not " +
        s"originate with an instance of this class")
    )

    commitInternal(newOffset.offset)
  }

  override def toString: String = s"TextSocketSource[host: $host, port: $port]"
}

case class TextSocketOffset(offset: Long) extends V2Offset {
  override def json(): String = offset.toString
}

class TextSocketMicroBatchReader(options: DataSourceV2Options)
  extends MicroBatchReader with TextSocketReader with Logging {

  private var startOffset: TextSocketOffset = _
  private var endOffset: TextSocketOffset = _

  protected val host: String = options.get("host").get()
  protected val port: Int = options.get("port").get().toInt

  initialize()

  override def setOffsetRange(start: Optional[V2Offset], end: Optional[V2Offset]): Unit = {
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
    val rawList = getBatchInternal(Option(startOffset.offset), Option(endOffset.offset))

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

  override def commit(end: V2Offset): Unit = {
    val newOffset = end.asInstanceOf[TextSocketOffset]
    commitInternal(newOffset.offset)
  }

  override def toString: String = s"TextSocketMicroBatchReader[host: $host, port: $port]"
}

class TextSocketSourceProvider extends DataSourceV2
  with MicroBatchReadSupport with StreamSourceProvider with DataSourceRegister with Logging {
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

  /** Returns the name and schema of the source that can be used to continually read data. */
  override def sourceSchema(
      sqlContext: SQLContext,
      schema: Option[StructType],
      providerName: String,
      parameters: Map[String, String]): (String, StructType) = {
    checkParameters(parameters)
    if (schema.nonEmpty) {
      throw new AnalysisException("The socket source does not support a user-specified schema.")
    }

    val sourceSchema = if (parameters.getOrElse("includeTimestamp", "false").toBoolean) {
        TextSocketSource.SCHEMA_TIMESTAMP
      } else {
        TextSocketSource.SCHEMA_REGULAR
      }
    ("textSocket", sourceSchema)
  }

  override def createSource(
      sqlContext: SQLContext,
      metadataPath: String,
      schema: Option[StructType],
      providerName: String,
      parameters: Map[String, String]): Source = {
    val host = parameters("host")
    val port = parameters("port").toInt
    new TextSocketSource(
      host, port, parameters.getOrElse("includeTimestamp", "false").toBoolean, sqlContext)
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
