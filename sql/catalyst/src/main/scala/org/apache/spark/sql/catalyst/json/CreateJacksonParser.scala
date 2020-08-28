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

package org.apache.spark.sql.catalyst.json

import java.io.{ByteArrayInputStream, InputStream, InputStreamReader}
import java.nio.channels.Channels
import java.nio.charset.Charset
import java.nio.charset.StandardCharsets

import com.fasterxml.jackson.core.{JsonFactory, JsonParser}
import org.apache.hadoop.io.Text
import sun.nio.cs.StreamDecoder

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.unsafe.types.UTF8String

private[sql] object CreateJacksonParser extends Serializable {
  def string(jsonFactory: JsonFactory, record: String): JsonParser = {
    jsonFactory.createParser(record)
  }

  def utf8String(jsonFactory: JsonFactory, record: UTF8String): JsonParser = {
    val bb = record.getByteBuffer
    assert(bb.hasArray)

    val bain = new ByteArrayInputStream(
      bb.array(), bb.arrayOffset() + bb.position(), bb.remaining())

    jsonFactory.createParser(new InputStreamReader(bain, StandardCharsets.UTF_8))
  }

  def text(jsonFactory: JsonFactory, record: Text): JsonParser = {
    jsonFactory.createParser(record.getBytes, 0, record.getLength)
  }

  // Jackson parsers can be ranked according to their performance:
  // 1. Array based with actual encoding UTF-8 in the array. This is the fastest parser
  //    but it doesn't allow to set encoding explicitly. Actual encoding is detected automatically
  //    by checking leading bytes of the array.
  // 2. InputStream based with actual encoding UTF-8 in the stream. Encoding is detected
  //    automatically by analyzing first bytes of the input stream.
  // 3. Reader based parser. This is the slowest parser used here but it allows to create
  //    a reader with specific encoding.
  // The method creates a reader for an array with given encoding and sets size of internal
  // decoding buffer according to size of input array.
  private def getStreamDecoder(enc: String, in: Array[Byte], length: Int): StreamDecoder = {
    val bais = new ByteArrayInputStream(in, 0, length)
    val byteChannel = Channels.newChannel(bais)
    val decodingBufferSize = Math.min(length, 8192)
    val decoder = Charset.forName(enc).newDecoder()

    StreamDecoder.forDecoder(byteChannel, decoder, decodingBufferSize)
  }

  def text(enc: String, jsonFactory: JsonFactory, record: Text): JsonParser = {
    val sd = getStreamDecoder(enc, record.getBytes, record.getLength)
    jsonFactory.createParser(sd)
  }

  def inputStream(jsonFactory: JsonFactory, is: InputStream): JsonParser = {
    jsonFactory.createParser(is)
  }

  def inputStream(enc: String, jsonFactory: JsonFactory, is: InputStream): JsonParser = {
    jsonFactory.createParser(new InputStreamReader(is, enc))
  }

  def internalRow(jsonFactory: JsonFactory, row: InternalRow): JsonParser = {
    val ba = row.getBinary(0)

    jsonFactory.createParser(ba, 0, ba.length)
  }

  def internalRow(enc: String, jsonFactory: JsonFactory, row: InternalRow): JsonParser = {
    val binary = row.getBinary(0)
    val sd = getStreamDecoder(enc, binary, binary.length)

    jsonFactory.createParser(sd)
  }
}
