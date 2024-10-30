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

package org.apache.spark.sql.catalyst.xml

import java.io.{ByteArrayInputStream, InputStream, InputStreamReader, Reader, StringReader}
import java.nio.channels.Channels
import java.nio.charset.{Charset, StandardCharsets}
import javax.xml.stream.{EventFilter, XMLEventReader, XMLInputFactory, XMLStreamConstants}
import javax.xml.stream.events.XMLEvent

import org.apache.hadoop.io.Text

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.unsafe.types.UTF8String

object CreateXmlParser extends Serializable {
  val filter = new EventFilter {
    override def accept(event: XMLEvent): Boolean =
    // Ignore comments and processing instructions
      event.getEventType match {
        case XMLStreamConstants.COMMENT | XMLStreamConstants.PROCESSING_INSTRUCTION => false
        case _ => true
      }
  }

  def string(xmlInputFactory: XMLInputFactory, record: String): XMLEventReader = {
    // It does not have to skip for white space, since `XmlInputFormat`
    // always finds the root tag without a heading space.
    val eventReader = xmlInputFactory.createXMLEventReader(new StringReader(record))
    xmlInputFactory.createFilteredReader(eventReader, filter)
  }

  def utf8String(xmlInputFactory: XMLInputFactory, record: UTF8String): XMLEventReader = {
    val bb = record.getByteBuffer
    assert(bb.hasArray)

    val bain = new ByteArrayInputStream(
      bb.array(), bb.arrayOffset() + bb.position(), bb.remaining())

    val eventReader = xmlInputFactory.createXMLEventReader(
      new InputStreamReader(bain, StandardCharsets.UTF_8))
    xmlInputFactory.createFilteredReader(eventReader, filter)
  }

  def text(xmlInputFactory: XMLInputFactory, record: Text): XMLEventReader = {
    val bs = new ByteArrayInputStream(record.getBytes, 0, record.getLength)

    val eventReader = xmlInputFactory.createXMLEventReader(bs)
    xmlInputFactory.createFilteredReader(eventReader, filter)
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
  private def getStreamDecoder(enc: String, in: Array[Byte], length: Int): Reader = {
    val bais = new ByteArrayInputStream(in, 0, length)
    val byteChannel = Channels.newChannel(bais)
    val decodingBufferSize = Math.min(length, 8192)
    val decoder = Charset.forName(enc).newDecoder()

    Channels.newReader(byteChannel, decoder, decodingBufferSize)
  }

  def text(enc: String, xmlInputFactory: XMLInputFactory, record: Text): XMLEventReader = {
    val sd = getStreamDecoder(enc, record.getBytes, record.getLength)
    val eventReader = xmlInputFactory.createXMLEventReader(sd)
    xmlInputFactory.createFilteredReader(eventReader, filter)
  }

  def inputStream(xmlInputFactory: XMLInputFactory, is: InputStream): XMLEventReader = {
    val eventReader = xmlInputFactory.createXMLEventReader(is)
    xmlInputFactory.createFilteredReader(eventReader, filter)
  }

  def inputStream(enc: String, xmlInputFactory: XMLInputFactory,
      is: InputStream): XMLEventReader = {
    val eventReader = xmlInputFactory.createXMLEventReader(new InputStreamReader(is, enc))
    xmlInputFactory.createFilteredReader(eventReader, filter)
  }

  def internalRow(xmlInputFactory: XMLInputFactory, row: InternalRow): XMLEventReader = {
    val ba = row.getBinary(0)

    val bs = new ByteArrayInputStream(ba, 0, ba.length)

    val eventReader = xmlInputFactory.createXMLEventReader(bs)
    xmlInputFactory.createFilteredReader(eventReader, filter)
  }

  def internalRow(enc: String, xmlInputFactory: XMLInputFactory,
      row: InternalRow): XMLEventReader = {
    val binary = row.getBinary(0)
    val sd = getStreamDecoder(enc, binary, binary.length)

    val eventReader = xmlInputFactory.createXMLEventReader(sd)
    xmlInputFactory.createFilteredReader(eventReader, filter)
  }
}
