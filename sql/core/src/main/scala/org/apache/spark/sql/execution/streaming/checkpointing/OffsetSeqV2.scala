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

import org.json4s.{Formats, NoTypeHints}
import org.json4s.jackson.Serialization

import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.read.streaming.{Offset => OffsetV2, SparkDataStream}

/**
 * An ordered collection of offsets with named sources, used to track the progress of processing
 * data from one or more [[SparkDataStream]]s that are present in a streaming query.
 * This version supports named sources which allows for source evolution (adding/removing sources).
 *
 * @param version The format version (should be 2)
 * @param metadata Optional metadata associated with this offset sequence
 * @param sources Map from source name to offset
 */
case class OffsetSeqV2(
    version: Int = 2,
    metadata: Option[OffsetSeqMetadata] = None,
    sources: Map[String, OffsetV2]) extends Logging {

  /**
   * Converts this V2 offset sequence to a V1 offset sequence.
   * This requires the source names to be provided in the correct order.
   *
   * @param orderedSourceNames The ordered list of source names
   * @return The equivalent V1 OffsetSeq
   */
  def toOffsetSeq(orderedSourceNames: Seq[String]): OffsetSeq = {
    val offsets = orderedSourceNames.map { name =>
      sources.get(name).map(Option(_)).getOrElse(None)
    }
    OffsetSeq(offsets, metadata)
  }

  /**
   * Unpacks an offset into [[StreamProgress]] by associating each named offset with the
   * corresponding source.
   *
   * @param namedSources Map from source name to SparkDataStream
   * @return The StreamProgress containing the offset mapping
   */
  def toStreamProgress(namedSources: Map[String, SparkDataStream]): StreamProgress = {
    val sourceOffsetPairs = sources.flatMap { case (name, offset) =>
      namedSources.get(name) match {
        case Some(source) => Some((source, offset))
        case None =>
          logWarning(s"Source with name '$name' found in checkpoint but not in current query")
          None
      }
    }
    new StreamProgress ++ sourceOffsetPairs
  }

  /**
   * Serializes this offset sequence to JSON format.
   *
   * @return JSON representation of this offset sequence
   */
  def json: String = {
    implicit val format: Formats = Serialization.formats(NoTypeHints)
    val sourceOffsets = sources.map { case (name, offset) =>
      name -> offset.json
    }
    Serialization.write(Map(
      "version" -> version,
      "metadata" -> metadata.map(_.json),
      "sources" -> sourceOffsets
    ))
  }

  override def toString: String = {
    val offsetStr = sources.map { case (name, offset) =>
      s"$name: ${offset.json}"
    }.mkString("{", ", ", "}")
    s"OffsetSeqV2[$offsetStr]"
  }
}

object OffsetSeqV2 extends Logging {
  private implicit val format: Formats = Serialization.formats(NoTypeHints)

  /**
   * Creates an OffsetSeqV2 from a V1 OffsetSeq and source names.
   *
   * @param offsetSeq The V1 offset sequence
   * @param sourceNames The ordered list of source names
   * @return The equivalent V2 offset sequence
   */
  def fromOffsetSeq(offsetSeq: OffsetSeq, sourceNames: Seq[String]): OffsetSeqV2 = {
    require(offsetSeq.offsets.length == sourceNames.length,
      s"Number of offsets (${offsetSeq.offsets.length}) must match number of source names " +
      s"(${sourceNames.length})")

    val sources = sourceNames.zip(offsetSeq.offsets).collect {
      case (name, Some(offset)) => name -> offset
    }.toMap

    OffsetSeqV2(version = 2, metadata = offsetSeq.metadata, sources = sources)
  }

  /**
   * Deserializes an OffsetSeqV2 from JSON format.
   *
   * @param json The JSON string
   * @return The deserialized OffsetSeqV2
   */
  def fromJson(json: String): OffsetSeqV2 = {
    val data = Serialization.read[Map[String, Any]](json)

    val version = data("version").asInstanceOf[Number].intValue()
    require(version == 2, s"Expected version 2 but got $version")

    val metadata = data.get("metadata").map {
      case s: String => OffsetSeqMetadata(s)
      case _ => throw new IllegalArgumentException("Invalid metadata format")
    }

    val sources = data("sources").asInstanceOf[Map[String, String]].map { case (name, offsetJson) =>
      name -> SerializedOffset(offsetJson)
    }

    OffsetSeqV2(version, metadata, sources)
  }

  /**
   * Validates a source name according to the naming rules.
   *
   * @param name The source name to validate
   * @throws IllegalArgumentException if the name is invalid
   */
  def validateSourceName(name: String): Unit = {
    require(name != null && name.nonEmpty, "Source name cannot be null or empty")
    require(name.length <= 64, s"Source name '$name' exceeds maximum length of 64 characters")
    require(name.matches("^[a-zA-Z0-9_-]+$"),
      s"Source name '$name' contains invalid characters. Only alphanumeric, underscore, " +
      "and hyphen characters are allowed")
  }
}

