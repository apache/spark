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

package org.apache.spark.input

import java.io.{BufferedReader, IOException, InputStreamReader}

import scala.collection.mutable.ArrayBuffer

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.compress.CompressionCodecFactory
import org.apache.hadoop.mapreduce.{InputSplit, RecordReader, TaskAttemptContext}
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat, FileSplit}

/**
 * Input format for text records saved with in-record delimiter and newline characters escaped.
 *
 * For example, a record containing two fields: `"a\n"` and `"|b\\"` saved with delimiter `|`
 * should be the following:
 * {{{
 * a\\\n|\\|b\\\\\n
 * }}},
 * where the in-record `|`, `\n`, and `\\` characters are escaped by `\\`.
 * Users can configure the delimiter via [[EscapedTextInputFormat$#KEY_DELIMITER]].
 * Its default value [[EscapedTextInputFormat$#DEFAULT_DELIMITER]] is set to match Redshift's UNLOAD
 * with the ESCAPE option:
 * {{{
 *   UNLOAD ('select_statement')
 *   TO 's3://object_path_prefix'
 *   ESCAPE
 * }}}
 *
 * @see org.apache.spark.SparkContext#newAPIHadoopFile
 */
class EscapedTextInputFormat extends FileInputFormat[Long, Array[String]] {

  override def createRecordReader(
      split: InputSplit,
      context: TaskAttemptContext): RecordReader[Long, Array[String]] = {
    new EscapedTextRecordReader
  }
}

object EscapedTextInputFormat {

  /** configuration key for delimiter */
  val KEY_DELIMITER = "spark.input.escapedText.delimiter"
  /** default delimiter */
  val DEFAULT_DELIMITER = '|'

  /** Gets the delimiter char from conf or the default. */
  private[input] def getDelimiterOrDefault(conf: Configuration): Char = {
    val c = conf.get(KEY_DELIMITER, DEFAULT_DELIMITER.toString)
    if (c.length != 1) {
      throw new IllegalArgumentException(s"Expect delimiter be a single character but got '$c'.")
    } else {
      c.charAt(0)
    }
  }
}

private[input] class EscapedTextRecordReader extends RecordReader[Long, Array[String]] {

  private var reader: BufferedReader = _

  private var key: Long = _
  private var value: Array[String] = _

  private var start: Long = _
  private var end: Long = _
  private var cur: Long = _

  private var delimiter: Char = _
  @inline private[this] final val escapeChar = '\\'
  @inline private[this] final val newline = '\n'

  @inline private[this] final val defaultBufferSize = 64 * 1024

  override def initialize(inputSplit: InputSplit, context: TaskAttemptContext): Unit = {
    val split = inputSplit.asInstanceOf[FileSplit]
    val file = split.getPath
    val conf = context.getConfiguration
    delimiter = EscapedTextInputFormat.getDelimiterOrDefault(conf)
    require(delimiter != escapeChar,
      s"The delimiter and the escape char cannot be the same but found $delimiter.")
    require(delimiter != newline, "The delimiter cannot be the newline character.")
    val compressionCodecs = new CompressionCodecFactory(conf)
    val codec = compressionCodecs.getCodec(file)
    if (codec != null) {
      throw new IOException(s"Do not support compressed files but found $file.")
    }
    val fs = file.getFileSystem(conf)
    val size = fs.getFileStatus(file).getLen
    start = findNext(fs, file, size, split.getStart)
    end = findNext(fs, file, size, split.getStart + split.getLength)
    cur = start
    val in = fs.open(file)
    if (cur > 0L) {
      in.seek(cur - 1L)
      in.read()
    }
    reader = new BufferedReader(new InputStreamReader(in), defaultBufferSize)
  }

  override def getProgress: Float = {
    if (start >= end) {
      1.0f
    } else {
      math.min((cur - start).toFloat / (end - start), 1.0f)
    }
  }

  override def nextKeyValue(): Boolean = {
    if (cur < end) {
      key = cur
      value = nextValue()
      true
    } else {
      false
    }
  }

  override def getCurrentValue: Array[String] = value

  override def getCurrentKey: Long = key

  override def close(): Unit = {
    if (reader != null) {
      reader.close()
    }
  }

  /**
   * Finds the start of the next record.
   * Because we don't know whether the first char is escaped or not, we need to first find a
   * position that is not escaped.
   * @return the start position of the next record
   */
  private def findNext(fs: FileSystem, file: Path, size: Long, offset: Long): Long = {
    if (offset == 0L) return 0L
    if (offset >= size) return size
    var pos = offset
    val in = fs.open(file)
    in.seek(pos)
    val br = new BufferedReader(new InputStreamReader(in), defaultBufferSize)
    var escaped = true
    var eof = false
    while (escaped && !eof) {
      val v = br.read()
      if (v < 0) {
        eof = true
      } else {
        pos += 1
        if (v != escapeChar) {
          escaped = false
        }
      }
    }
    var newline = false
    while ((escaped || !newline) && !eof) {
      val v = br.read()
      if (v < 0) {
        eof = true
      } else {
        pos += 1
        if (v == escapeChar) {
          escaped = true
        } else {
          if (!escaped) {
            newline = v == '\n'
          } else {
            escaped = false
          }
        }
      }
    }
    in.close()
    pos
  }

  private def nextValue(): Array[String] = {
    var escaped = false
    val fields = ArrayBuffer.empty[String]
    var endOfRecord = false
    var eof = false
    while (!endOfRecord && !eof) {
      var endOfField = false
      val sb = new StringBuilder
      while (!endOfField && !endOfRecord && !eof) {
        val v = reader.read()
        if (v < 0) {
          eof = true
        } else {
          cur += 1
          if (escaped) {
            if (v != escapeChar && v != delimiter && v != newline) {
              throw new IllegalStateException(s"Found ${v.asInstanceOf[Char]} after $escapeChar.")
            }
            sb.append(v.asInstanceOf[Char])
            escaped = false
          } else {
            if (v == escapeChar) {
              escaped = true
            } else if (v == delimiter) {
              endOfField = true
            } else if (v == newline) {
              endOfRecord = true
            } else {
              sb.append(v.asInstanceOf[Char])
            }
          }
        }
      }
      fields.append(sb.toString())
    }
    if (escaped) {
      throw new IllegalStateException(s"Found hanging escape char.")
    }
    fields.toArray
  }
}
