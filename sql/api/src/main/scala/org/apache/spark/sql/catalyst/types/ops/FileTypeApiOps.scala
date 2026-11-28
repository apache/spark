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
package org.apache.spark.sql.catalyst.types.ops

import java.io.StringWriter

import com.fasterxml.jackson.core.JsonFactory

import org.apache.spark.sql.catalyst.encoders.AgnosticEncoder
import org.apache.spark.sql.catalyst.encoders.AgnosticEncoders.FileRefEncoder
import org.apache.spark.sql.types.{DataType, FileRef, FileRefInternals, FileType}

/**
 * Client-side (spark-api) operations for FileType.
 *
 * This class implements TypeApiOps methods for the FILE data type:
 *   - String formatting: the canonical JSON of a value (see [[FileTypeApiOps.formatJson]])
 *   - Row encoding: FileRefEncoder for [[FileRef]]
 *   - Hive and Row JSON formatting of a FileRef
 *
 * RELATIONSHIP TO FileTypeOps: FileTypeOps (in catalyst package) extends this class to inherit
 * client-side operations while adding server-side operations (physical type, literals, etc.).
 *
 * @param ft
 *   The FileType with its mode
 * @since 4.5.0
 */
class FileTypeApiOps(val ft: FileType) extends TypeApiOps {

  override def dataType: DataType = ft

  // ==================== String Formatting ====================

  // FileTypeOps overrides this for the internal InternalRow value, as InternalRow is not
  // available in sql/api. A FileRef is formatted the same way.
  override def format(v: Any): String = v match {
    case f: FileRef => FileTypeApiOps.formatJson(f)
    case _ => v.toString
  }

  // FILE has no literal syntax, so this is for display only, such as in the SQL form of a
  // literal (Literal.sql) used in generated column names.
  override def toSQLValue(v: Any): String = s"FILE ${format(v)}"

  // ==================== Row Encoding ====================

  override def getEncoder: AgnosticEncoder[_] = FileRefEncoder(ft.mode)

  // ==================== External-Value Formatting ====================

  override def formatExternal(value: Any): Option[String] = value match {
    case f: FileRef => Some(FileTypeApiOps.formatJson(f))
    case _ => None
  }
}

object FileTypeApiOps {

  // JsonFactory is thread-safe and reused; a JsonGenerator is created per call.
  private val jsonFactory = new JsonFactory()

  /** Formats a FileRef as the canonical JSON of FILE, see the other overload. */
  def formatJson(f: FileRef): String =
    formatJson(f.uri, f.offset, f.size, f.contentType, f.checksum, FileRefInternals.inline(f))

  /**
   * Formats the fields of a FILE value as its canonical JSON: an object with the fields of
   * [[FileType.FIELDS]] in that order, `{"uri":...,"offset":...,"size":...,"content_type":...,
   * "checksum":...,"inline":...}`, without the fields that are null. `inline` is base64-encoded.
   */
  def formatJson(
      uri: String,
      offset: java.lang.Long,
      size: java.lang.Long,
      contentType: String,
      checksum: String,
      inline: Array[Byte]): String = {
    val writer = new StringWriter(64)
    val gen = jsonFactory.createGenerator(writer)
    try {
      gen.writeStartObject()
      if (uri != null) gen.writeStringField("uri", uri)
      if (offset != null) gen.writeNumberField("offset", offset.longValue())
      if (size != null) gen.writeNumberField("size", size.longValue())
      if (contentType != null) gen.writeStringField("content_type", contentType)
      if (checksum != null) gen.writeStringField("checksum", checksum)
      if (inline != null) gen.writeBinaryField("inline", inline)
      gen.writeEndObject()
    } finally {
      gen.close()
    }
    writer.toString
  }
}
