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
package org.apache.spark.sql.execution.datasources.xml

import java.io.{InputStream, InputStreamReader, IOException, Reader}
import java.nio.ByteBuffer
import java.nio.charset.Charset

import org.apache.commons.io.input.BoundedInputStream
import org.apache.hadoop.fs.Seekable
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.io.compress._
import org.apache.hadoop.mapreduce.{InputSplit, RecordReader, TaskAttemptContext}
import org.apache.hadoop.mapreduce.lib.input.{FileSplit, TextInputFormat}

import org.apache.spark.sql.catalyst.xml.XmlOptions

/**
 * Reads records that are delimited by a specific start/end tag.
 */
class XmlInputFormat extends TextInputFormat {

  override def createRecordReader(
      split: InputSplit,
      context: TaskAttemptContext): RecordReader[LongWritable, Text] = {
    new XmlRecordReader
  }
}

object XmlInputFormat {
  /** configuration key for start tag */
  val START_TAG_KEY: String = "xmlinput.start"
  /** configuration key for end tag */
  val END_TAG_KEY: String = "xmlinput.end"
  /** configuration key for encoding type */
  val ENCODING_KEY: String = "xmlinput.encoding"
}

/**
 * XMLRecordReader class to read through a given xml document to output xml blocks as records
 * as specified by the start tag and end tag.
 *
 * This implementation is ultimately loosely based on LineRecordReader in Hadoop.
 */
private[xml] class XmlRecordReader extends RecordReader[LongWritable, Text] {

  private var startTag: String = _
  private var currentStartTag: String = _
  private var endTag: String = _
  private var currentKey: LongWritable = _
  private var currentValue: Text = _
  private var start: Long = _
  private var end: Long = _
  private var reader: Reader = _
  private var filePosition: Seekable = _
  private var countingIn: BoundedInputStream = _
  private var readerLeftoverCharFn: () => Boolean = _
  private var readerByteBuffer: ByteBuffer = _
  private var decompressor: Decompressor = _
  private var buffer = new StringBuilder()

  override def initialize(split: InputSplit, context: TaskAttemptContext): Unit = {
    val fileSplit = split.asInstanceOf[FileSplit]
    val conf = context.getConfiguration
    val charset =
      Charset.forName(conf.get(XmlInputFormat.ENCODING_KEY, XmlOptions.DEFAULT_CHARSET))
    startTag = conf.get(XmlInputFormat.START_TAG_KEY)
    endTag = conf.get(XmlInputFormat.END_TAG_KEY)
    start = fileSplit.getStart
    end = start + fileSplit.getLength

    // open the file and seek to the start of the split
    val path = fileSplit.getPath
    val fs = path.getFileSystem(conf)
    val fsin = fs.open(fileSplit.getPath)

    var in: InputStream = null
    val codec = new CompressionCodecFactory(conf).getCodec(path)
    if (null != codec) {
      decompressor = CodecPool.getDecompressor(codec)
      codec match {
        case sc: SplittableCompressionCodec =>
          val cIn = sc.createInputStream(
            fsin,
            decompressor,
            start,
            end,
            SplittableCompressionCodec.READ_MODE.BYBLOCK)
          start = cIn.getAdjustedStart
          end = cIn.getAdjustedEnd
          in = cIn
          filePosition = cIn
        case c: CompressionCodec =>
          if (start != 0) {
            // So we have a split that is only part of a file stored using
            // a Compression codec that cannot be split.
            throw new IOException("Cannot seek in " +
              codec.getClass.getSimpleName + " compressed stream")
          }
          val cIn = c.createInputStream(fsin, decompressor)
          in = cIn
          filePosition = fsin
      }
    } else {
      fsin.seek(start)
      countingIn = BoundedInputStream.builder()
        .setInputStream(fsin)
        .get()
      in = countingIn
      // don't use filePosition in this case. We have to count bytes read manually
    }

    reader = new InputStreamReader(in, charset)

    if (codec == null) {
      // Hack: in the uncompressed case (see more below), we must know how much the
      // InputStreamReader has buffered but not processed
      // to accurately assess how many bytes have been processed
      val sdField = reader.getClass.getDeclaredField("sd")
      sdField.setAccessible(true)
      val sd = sdField.get(reader)
      val readerLeftoverCharField = sd.getClass.getDeclaredField("haveLeftoverChar")
      readerLeftoverCharField.setAccessible(true)
      readerLeftoverCharFn = () => { readerLeftoverCharField.get(sd).asInstanceOf[Boolean] }
      val bbField = sd.getClass.getDeclaredField("bb")
      bbField.setAccessible(true)
      readerByteBuffer = bbField.get(sd).asInstanceOf[ByteBuffer]
    }
  }

  /**
   * Tries to determine how many bytes of the underlying split have been read. There are two
   * distinct cases.
   *
   * For compressed input, it attempts to read the current position read in the compressed input
   * stream. This logic is copied from LineRecordReader, essentially.
   *
   * For uncompressed input, it counts the number of bytes read directly from the split. It
   * further compensates for the fact that the intervening InputStreamReader buffers input and
   * accounts for data it has read but not yet returned.
   */
  private def getFilePosition(): Long = {
    // filePosition != null when input is compressed
    if (filePosition != null) {
      filePosition.getPos
    } else {
      start + countingIn.getCount -
        readerByteBuffer.remaining() -
        (if (readerLeftoverCharFn()) 1 else 0)
    }
  }

  override def nextKeyValue: Boolean = {
    currentKey = new LongWritable
    currentValue = new Text
    next(currentKey, currentValue)
  }

  /**
   * Finds the start of the next record.
   * It treats data from `startTag` and `endTag` as a record.
   *
   * @param key the current key that will be written
   * @param value  the object that will be written
   * @return whether it reads successfully
   */
  private def next(key: LongWritable, value: Text): Boolean = {
    if (readUntilStartElement()) {
      try {
        buffer.append(currentStartTag)
        // Don't check whether the end element was found. Even if not, return everything
        // that was read, which will invariably cause a parse error later
        readUntilEndElement(currentStartTag.endsWith(">"))
        key.set(getFilePosition())
        value.set(buffer.toString())
        return true
      } finally {
        buffer = new StringBuilder()
      }
    }
    false
  }

  private def readUntilStartElement(): Boolean = {
    currentStartTag = startTag
    var i = 0
    while (true) {
      val cOrEOF = reader.read()
      if (cOrEOF == -1 || (i == 0 && getFilePosition() > end)) {
        // End of file or end of split.
        return false
      }
      val c = cOrEOF.toChar
      if (c == startTag(i)) {
        if (i >= startTag.length - 1) {
          // Found start tag.
          return true
        }
        // else in start tag
        i += 1
      } else {
        // if doesn't match the closing angle bracket, check if followed by attributes
        if (i == (startTag.length - 1) && Character.isWhitespace(c)) {
          // Found start tag with attributes. Remember to write with following whitespace
          // char, not angle bracket
          currentStartTag = startTag.dropRight(1) + c
          return true
        }
        // else not in start tag
        i = 0
      }
    }
    // Unreachable.
    false
  }

  private def readUntilEndElement(startTagClosed: Boolean): Boolean = {
    // Index into the start or end tag that has matched so far
    var si = 0
    var ei = 0
    // How many other start tags enclose the one that's started already?
    var depth = 0
    // Previously read character
    var prevC = '\u0000'

    // The current start tag already found may or may not have terminated with
    // a '>' as it may have attributes we read here. If not, we search for
    // a self-close tag, but only until a non-self-closing end to the start
    // tag is found
    var canSelfClose = !startTagClosed

    while (true) {

      val cOrEOF = reader.read()
      if (cOrEOF == -1) {
        // End of file (ignore end of split).
        return false
      }

      val c = cOrEOF.toChar
      buffer.append(c)

      if (c == '>' && prevC != '/') {
        canSelfClose = false
      }

      // Still matching a start tag?
      if (c == startTag(si)) {
        // Still also matching an end tag?
        if (c == endTag(ei)) {
          // In start tag or end tag.
          si += 1
          ei += 1
        } else {
          if (si >= startTag.length - 1) {
            // Found start tag.
            si = 0
            ei = 0
            depth += 1
          } else {
            // In start tag.
            si += 1
            ei = 0
          }
        }
      } else if (c == endTag(ei)) {
        if (ei >= endTag.length - 1) {
          if (depth == 0) {
            // Found closing end tag.
            return true
          }
          // else found nested end tag.
          si = 0
          ei = 0
          depth -= 1
        } else {
          // In end tag.
          si = 0
          ei += 1
        }
      } else if (c == '>' && prevC == '/' && canSelfClose) {
        if (depth == 0) {
          // found a self-closing tag (end tag)
          return true
        }
        // else found self-closing nested tag (end tag)
        si = 0
        ei = 0
        depth -= 1
      } else if (si == (startTag.length - 1) && Character.isWhitespace(c)) {
        // found a start tag with attributes
        si = 0
        ei = 0
        depth += 1
      } else {
        // Not in start tag or end tag.
        si = 0
        ei = 0
      }
      prevC = c
    }
    // Unreachable.
    false
  }

  override def getProgress: Float = {
    if (start == end) {
      0.0f
    } else {
      math.min(1.0f, (getFilePosition() - start) / (end - start).toFloat)
    }
  }

  override def getCurrentKey: LongWritable = currentKey

  override def getCurrentValue: Text = currentValue

  def close(): Unit = {
    try {
      if (reader != null) {
        reader.close()
        reader = null
      }
    } finally {
      if (decompressor != null) {
        CodecPool.returnDecompressor(decompressor)
        decompressor = null
      }
    }
  }
}
