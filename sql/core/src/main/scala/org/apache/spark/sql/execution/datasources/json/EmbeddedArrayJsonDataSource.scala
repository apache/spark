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

package org.apache.spark.sql.execution.datasources.json

import java.io.{InputStream, InputStreamReader, Reader}
import java.nio.charset.StandardCharsets

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileStatus

import org.apache.spark.SparkException
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.json.{CreateJacksonParser, JacksonParser, JSONOptions}
import org.apache.spark.sql.catalyst.util.FailureSafeParser
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.execution.datasources.{CodecStreams, PartitionedFile}
import org.apache.spark.sql.types.StructType
import org.apache.spark.unsafe.types.UTF8String

/**
 * A [[JsonDataSource]] for the `explodeEmbeddedArray` option. The input JSON files are expected
 * to be object documents with a top-level array-valued field named by the option, i.e.
 * `{..., "<fieldName>": [record1, record2, ...], ...}`. An [[EmbeddedArraySplitter]] streams the
 * records out of the array one at a time, and each record is parsed by the regular JSON parsing
 * logic as if it were an independent input record, producing one row per record. Neither the
 * whole array text nor the result rows are ever buffered in memory.
 */
object EmbeddedArrayJsonDataSource extends JsonDataSource {
  // The embedded array spans the whole file, so the file cannot be split.
  override val isSplitable: Boolean = false

  override protected def infer(
      sparkSession: SparkSession,
      inputPaths: Seq[FileStatus],
      parsedOptions: JSONOptions): StructType = {
    // Unreachable: `inferSchema` always returns a single variant column when
    // `explodeEmbeddedArray` is set.
    throw SparkException.internalError(
      "The schema is always a single variant column when `explodeEmbeddedArray` is set.")
  }

  override def readFile(
      conf: Configuration,
      file: PartitionedFile,
      parser: JacksonParser,
      schema: StructType): Iterator[InternalRow] = {
    val stream = CodecStreams.createInputStreamWithCloseResource(conf, file.toPath)
    parseStream(stream, parser, schema)
  }

  override protected def readStream(
      in: InputStream,
      parser: JacksonParser,
      schema: StructType): Iterator[InternalRow] = {
    parseStream(in, parser, schema)
  }

  private def parseStream(
      stream: InputStream,
      parser: JacksonParser,
      schema: StructType): Iterator[InternalRow] = {
    val encoding = parser.options.encoding.getOrElse(StandardCharsets.UTF_8.name())
    val splitter = new EmbeddedArraySplitter(
      new InputStreamReader(stream, encoding), parser.options.explodeEmbeddedArray.get)
    val records = new Iterator[String] {
      private[this] var nextRecord: Option[String] = None

      override def hasNext: Boolean = {
        if (nextRecord.isEmpty) {
          nextRecord = splitter.nextRecord()
        }
        nextRecord.isDefined
      }

      override def next(): String = {
        if (!hasNext) {
          throw QueryExecutionErrors.endOfStreamError()
        }
        val record = nextRecord.get
        nextRecord = None
        record
      }
    }
    val safeParser = new FailureSafeParser[String](
      input => parser.parse(input, CreateJacksonParser.string, UTF8String.fromString),
      parser.options.parseMode,
      schema,
      parser.options.columnNameOfCorruptRecord)
    records.flatMap(safeParser.parse)
  }
}

/**
 * Splits the array value of the top-level field `arrayFieldName` of a JSON object document
 * (`{..., "<arrayFieldName>": [record1, record2, ...], ...}`) into individual record texts. Only
 * one record is held in memory at a time; the whole array is never materialized. Buffering the
 * record text does not change the memory bound: each record is parsed into a variant value that
 * must be fully materialized in the result row anyway, so the peak memory usage is O(record
 * size) either way.
 *
 * The splitter only understands enough JSON structure (strings, escapes, nesting) to find the
 * record boundaries. The returned record text is not validated here; malformed records are
 * rejected later by the regular JSON parser. Anything outside the array is ignored, and a
 * document without a matching top-level array field yields no records. If the document ends in
 * the middle of a record, the partial record text is returned and left for the JSON parser to
 * reject.
 */
class EmbeddedArraySplitter(reader: Reader, arrayFieldName: String) {
  private[this] val buffer = new Array[Char](EmbeddedArraySplitter.BUFFER_SIZE)
  private[this] var bufferLen = 0
  private[this] var pos = 0
  private[this] var initialized = false
  // Whether we are positioned inside the embedded array with records potentially remaining.
  private[this] var inArray = false

  /** Returns the text of the next record, or None when there are no more records. */
  def nextRecord(): Option[String] = {
    if (!initialized) {
      initialized = true
      inArray = findEmbeddedArray()
    }
    if (!inArray) {
      return None
    }
    var c = nextNonWhitespace()
    // Tolerate stray commas between records rather than producing garbage records.
    while (c == ',') {
      c = nextNonWhitespace()
    }
    if (c == -1 || c == ']') {
      inArray = false
      return None
    }
    val out = new StringBuilder
    consumeValue(c, out)
    Some(out.toString)
  }

  /**
   * Scans the top-level JSON object until the reader is positioned inside the embedded array,
   * just after its opening '['. Returns false if there is no top-level array field named
   * `arrayFieldName`.
   */
  private def findEmbeddedArray(): Boolean = {
    var c = nextNonWhitespace()
    if (c == '\uFEFF') { // skip the byte order mark
      c = nextNonWhitespace()
    }
    if (c != '{') {
      return false
    }
    while (true) {
      c = nextNonWhitespace()
      while (c == ',') {
        c = nextNonWhitespace()
      }
      if (c != '"') {
        // '}', EOF, or malformed content: in all cases there is no embedded array ahead.
        return false
      }
      val key = readKey()
      c = nextNonWhitespace()
      if (c != ':') {
        return false
      }
      c = nextNonWhitespace()
      if (c == -1) {
        return false
      }
      if (key == arrayFieldName && c == '[') {
        return true
      }
      consumeValue(c, out = null)
    }
    false // unreachable
  }

  /**
   * Reads a JSON object key after its opening quote and returns the raw key text. Escape
   * sequences are not processed: the key is matched against `arrayFieldName` by raw text, so a
   * key written with escape sequences in the document does not match.
   */
  private def readKey(): String = {
    val out = new StringBuilder
    consumeString(out)
    if (out.nonEmpty && out.charAt(out.length - 1) == '"') {
      out.setLength(out.length - 1)
    }
    out.toString
  }

  /**
   * Consumes one JSON value whose first character (already consumed) is `first`, appending the
   * value text to `out` unless it is null. The trailing delimiter (',', ']', '}' or whitespace)
   * is left unconsumed.
   */
  private def consumeValue(first: Int, out: StringBuilder): Unit = {
    if (out != null) {
      out.append(first.toChar)
    }
    if (first == '"') {
      consumeString(out)
    } else if (first == '{' || first == '[') {
      var depth = 1
      while (depth > 0) {
        val c = nextChar()
        if (c == -1) {
          return
        }
        if (out != null) {
          out.append(c.toChar)
        }
        if (c == '"') {
          consumeString(out)
        } else if (c == '{' || c == '[') {
          depth += 1
        } else if (c == '}' || c == ']') {
          depth -= 1
        }
      }
    } else {
      // A scalar (number, true, false, null): consume until a delimiter.
      var c = peekChar()
      while (c != -1 && c != ',' && c != '}' && c != ']' && !isWhitespace(c)) {
        if (out != null) {
          out.append(c.toChar)
        }
        nextChar()
        c = peekChar()
      }
    }
  }

  /**
   * Consumes a JSON string after its opening quote, appending the consumed characters (including
   * the closing quote) to `out` unless it is null.
   */
  private def consumeString(out: StringBuilder): Unit = {
    while (true) {
      val c = nextChar()
      if (c == -1) {
        return
      }
      if (out != null) {
        out.append(c.toChar)
      }
      if (c == '\\') {
        val escaped = nextChar()
        if (escaped != -1 && out != null) {
          out.append(escaped.toChar)
        }
      } else if (c == '"') {
        return
      }
    }
  }

  private def isWhitespace(c: Int): Boolean = {
    c == ' ' || c == '\t' || c == '\n' || c == '\r'
  }

  /** Returns the next character that is not JSON whitespace, or -1 at EOF. */
  private def nextNonWhitespace(): Int = {
    var c = nextChar()
    while (isWhitespace(c)) {
      c = nextChar()
    }
    c
  }

  /** Returns the next character and consumes it, or -1 at EOF. */
  private def nextChar(): Int = {
    if (pos >= bufferLen && !fill()) {
      -1
    } else {
      val c = buffer(pos)
      pos += 1
      c
    }
  }

  /** Returns the next character without consuming it, or -1 at EOF. */
  private def peekChar(): Int = {
    if (pos >= bufferLen && !fill()) {
      -1
    } else {
      buffer(pos)
    }
  }

  private def fill(): Boolean = {
    pos = 0
    bufferLen = reader.read(buffer)
    bufferLen > 0
  }
}

object EmbeddedArraySplitter {
  private val BUFFER_SIZE = 64 * 1024
}
