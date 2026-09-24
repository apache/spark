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

package org.apache.spark.api.python

import java.io.DataInputStream

import scala.jdk.CollectionConverters._

import com.fasterxml.jackson.core.{JsonParser, JsonProcessingException}
import com.fasterxml.jackson.databind.{DeserializationFeature, JsonNode, ObjectMapper}

import org.apache.spark.SparkException

/**
 * Reads the metrics message emitted by pyspark.worker after a successful task. The message has a
 * 32-bit byte length followed by a UTF-8 JSON object with kind, version, and metrics fields.
 * The metrics object maps each name to a JSON value and a nonempty unit string. The codec retains
 * every metric name and value, while consumers validate the types and units of the metrics they
 * use. Aggregation and registration are handled by consumers.
 * Additional JSON fields are ignored.
 * A JVM runner advertises v1 per task; without that advertisement, workers use TIMING_DATA.
 */
private[python] object PythonWorkerMetricsDecoder {
  val protocolVersionConfKey = "spark.python.worker.metrics.protocol.version"
  val protocolVersion = "1"
  val maxPayloadLength = 64 * 1024

  // Message kind matched against the envelope written by Python's report_metrics.
  private val messageKind = "spark.python.worker.metrics"
  private val mapper = new ObjectMapper()
    .enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION)
    .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)

  // Keep JSON types and numeric precision until a consumer chooses how to interpret the value.
  case class Metric(value: JsonNode, unit: String)

  /**
   * Read after METRICS_DATA has been consumed, leaving the binary trailer to the caller.
   */
  def read(stream: DataInputStream): Map[String, Metric] = {
    val length = stream.readInt()
    if (length <= 0 || length > maxPayloadLength) {
      throw invalid(s"payload length $length")
    }
    val payload = new Array[Byte](length)
    stream.readFully(payload)

    // Parse a bounded byte array so the JSON parser cannot read past the declared frame.
    val parser = mapper.getFactory.createParser(payload)
    try {
      val root: JsonNode = mapper.readTree(parser)
      if (root == null || !root.isObject || parser.nextToken() != null) {
        throw invalid("expected one JSON object")
      }
      val kindNode = root.get("kind")
      if (kindNode == null || !kindNode.isTextual || kindNode.textValue() != messageKind) {
        throw invalid("missing or invalid message kind")
      }
      val version = root.get("version")
      if (version == null || !version.isIntegralNumber || !version.canConvertToInt ||
          version.intValue() != 1) {
        throw invalid("unsupported version")
      }
      val metrics = root.get("metrics")
      if (metrics == null || !metrics.isObject) {
        throw invalid("expected metrics object")
      }
      // Preserve names even when the current timing consumer does not use them.
      metrics.fields().asScala.map { entry =>
        val name = entry.getKey
        if (name.isEmpty) {
          throw invalid("metric name must be nonempty")
        }
        name -> readMetric(name, entry.getValue)
      }.toMap
    } catch {
      case e: JsonProcessingException =>
        throw new SparkException("Malformed Python worker metrics JSON", e)
    } finally {
      parser.close()
    }
  }

  private def readMetric(name: String, metric: JsonNode): Metric = {
    if (!metric.isObject) {
      throw invalid(s"expected an object for metric $name")
    }
    val value = metric.get("value")
    if (value == null) {
      throw invalid(s"missing value for $name")
    }
    val unit = metric.get("unit")
    if (unit == null || !unit.isTextual || unit.textValue().isEmpty) {
      throw invalid(s"missing or invalid unit for metric $name")
    }
    Metric(value, unit.textValue())
  }

  private def invalid(detail: String): SparkException = {
    new SparkException(s"Invalid Python worker metrics payload: $detail")
  }
}
