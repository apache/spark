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

import java.io.{InputStream, OutputStream}
import java.nio.charset.StandardCharsets._

import scala.io.{Source => IOSource}

import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.Serialization

import org.apache.spark.sql.SparkSession

case class StreamExecutionProgress(offsetSeq: OffsetSeq, watermark: Long)

/**
 * This class is used to log [[StreamExecutionProgress]] (offsets, watermark, etc.) to persistent
 * files in HDFS.
 *
 * Each file corresponds to a specific batch of [[StreamExecutionProgress]]. The file format
 * contain a version string in the first line, followed by a the JSON string representation
 * of the progress. If a source offset is missing, then that offset will contain a string value
 * defined in the `SERIALIZED_VOID_OFFSET` variable in [[StreamExecutionProgressLog]] companion
 * object.
 *
 * For instance, when dealing with [[LongOffset]] types:
 *   v1                                      // version 1
 *   {
 *     "offsets" : [ "1", "-", "-", "2" ],   // LongOffset 1, No offset, No offset, LongOffset 2
 *     "watermark" : 200                     // watermark 200
 *   }
 */
class StreamExecutionProgressLog(sparkSession: SparkSession, path: String)
  extends HDFSMetadataLog[StreamExecutionProgress](sparkSession, path) {

  private implicit val formats =
    Serialization.formats(NoTypeHints) + new StreamExecutionProgressSerializer

  override def deserialize(in: InputStream): StreamExecutionProgress = {
    val lines = IOSource.fromInputStream(in, UTF_8.name()).getLines()
    if (!lines.hasNext) {
      throw new IllegalStateException("Incomplete log file")
    }
    val version = lines.next()
    if (version != StreamExecutionProgressLog.VERSION) {
      throw new IllegalStateException(s"Unknown log version: ${version}")
    }
    Serialization.read[StreamExecutionProgress](lines.mkString)
  }

  override def serialize(metadata: StreamExecutionProgress, out: OutputStream): Unit = {
    // called inside a try-finally where the underlying stream is closed in the caller
    out.write(StreamExecutionProgressLog.VERSION.getBytes(UTF_8))
    out.write('\n')
    out.write(Serialization.writePretty(metadata).getBytes(UTF_8))
  }
}

class StreamExecutionProgressSerializer extends Serializer[StreamExecutionProgress] {

  private val clazz = classOf[StreamExecutionProgress]

  private def parseOffset(value: String): Offset = value match {
    case StreamExecutionProgressLog.SERIALIZED_VOID_OFFSET => null
    case json => SerializedOffset(json)
  }

  def deserialize(implicit format: Formats):
      PartialFunction[(TypeInfo, JValue), StreamExecutionProgress] = {
    case (TypeInfo(clazz, _), json) => json match {
      case JObject(
      JField("offsets", JArray(offsets)) :: JField("watermark", JInt(watermark)) :: Nil) =>
        new StreamExecutionProgress(
          OffsetSeq.fill(offsets.map(_.asInstanceOf[JString].s).map(parseOffset).toArray: _*),
          watermark.toLong)
      case other => throw new MappingException(s"Can't convert $other to StreamExecutionProgress")
    }
  }

  def serialize(implicit formats: Formats): PartialFunction[Any, JValue] = {
    case sep: StreamExecutionProgress =>
      ("offsets" ->
        sep.offsetSeq.offsets.map(_.map(_.json)).map { offset =>
          offset match {
            case Some(json: String) => json
            case None => StreamExecutionProgressLog.SERIALIZED_VOID_OFFSET
          }
        }) ~ ("watermark" -> sep.watermark)
  }
}

object StreamExecutionProgressLog {
  private[streaming] val VERSION = "v1"
  private[streaming] val SERIALIZED_VOID_OFFSET = "-"
}
