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

import java.io.InputStream
import javax.xml.stream.{
  EventFilter,
  StreamFilter,
  XMLEventReader,
  XMLStreamConstants,
  XMLStreamReader
}
import javax.xml.stream.events.{EndDocument, StartElement, XMLEvent}
import javax.xml.transform.stax.StAXSource
import javax.xml.validation.Schema

import scala.util.control.NonFatal

import org.apache.commons.io.input.BOMInputStream
import org.apache.hadoop.shaded.com.ctc.wstx.exc.WstxEOFException

import org.apache.spark.internal.Logging
import org.apache.spark.util.SparkErrorUtils

/**
 * XML record reader that reads the next XML record in the underlying XML stream. It can support XSD
 * schema validation by maintaining a separate XML reader and keep it in sync with the primary XML
 * reader.
 */
case class StaxXMLRecordReader(inputStream: () => InputStream, options: XmlOptions)
    extends XMLEventReader
    with Logging {
  // Reader for the XML record parsing.
  private val in1 = inputStream()
  private val primaryEventReader = filteredEventReader(in1, options)

  // Reader for the XSD validation, if an XSD schema is provided.
  private val in2 = Option(options.rowValidationXSDPath).map(_ => inputStream())
  // An XMLStreamReader used by StAXSource for XSD validation.
  private val xsdValidationStreamReader = in2.map(in => filteredStreamReader(in, options))
  // An XMLEventReader wrapping the XMLStreamReader for XSD validation. This is used to ensure that
  // the inputStream for XSD validation is synced with the primaryEventReader.
  private val xsdValidationEventReader = xsdValidationStreamReader.map(filteredEventReader)

  final var hasMoreRecord: Boolean = true

  /**
   * Skip through the XML stream until we find the next row start element.
   * Returns true if a row start element is found, false if end of stream is reached.
   */
  def skipToNextRecord(): Boolean = {
    hasMoreRecord = skipToNextRowStart(primaryEventReader)
    xsdValidationEventReader.foreach { r =>
      val xsdReaderHasMoreRecord = skipToNextRowStart(r)
      assert(hasMoreRecord == xsdReaderHasMoreRecord)
    }
    if (!hasMoreRecord) {
      closeAllReaders()
    }
    hasMoreRecord
  }

  /**
   * Skip through the XML stream until we find the next row start element.
   */
  private def skipToNextRowStart(reader: XMLEventReader): Boolean = {
    val rowTagName = options.rowTag
    try {
      while (reader.hasNext) {
        val event = reader.peek()
        event match {
          case startElement: StartElement =>
            val elementName = StaxXmlParserUtils.getName(startElement.getName, options)
            if (elementName == rowTagName) {
              return true
            }
          case _: EndDocument =>
            return false
          case _ =>
          // Continue searching
        }
        // if not the event we want, advance the reader
        reader.nextEvent()
      }
      false
    } catch {
      case NonFatal(e) if SparkErrorUtils.getRootCause(e).isInstanceOf[WstxEOFException] =>
        logWarning("Reached end of file while looking for next row start element.")
        false
    }
  }

  def validateXSDSchema(schema: Schema): Unit = {
    xsdValidationStreamReader match {
      case Some(p) =>
        try {
          // StAXSource requires the stream reader to start with the START_DOCUMENT OR START_ELEMENT
          // events.
          def rowTagStarted: Boolean =
            p.getEventType == XMLStreamConstants.START_ELEMENT &&
            StaxXmlParserUtils.getName(p.getName, options) == options.rowTag
          while (!rowTagStarted && p.hasNext) {
            p.next()
          }
          schema.newValidator().validate(new StAXSource(p))
        } catch {
          case NonFatal(e) =>
            try {
              // If the validation fails, we need to skip the current record in the primary reader
              // advancing the primary event reader so that the parser will continue to the
              // next record.
              primaryEventReader.next()
            } finally {
              throw e
            }
        }
      case None => throw new IllegalStateException("XSD validation parser is not initialized")
    }
  }

  def closeAllReaders(): Unit = {
    primaryEventReader.close()
    xsdValidationEventReader.foreach(_.close())
    xsdValidationStreamReader.foreach(_.close())
    in1.close()
    in2.foreach(_.close())
    hasMoreRecord = false
  }

  override def nextEvent(): XMLEvent = primaryEventReader.nextEvent()
  override def hasNext: Boolean = primaryEventReader.hasNext
  override def peek(): XMLEvent = primaryEventReader.peek()
  override def getElementText: String = primaryEventReader.getElementText
  override def nextTag(): XMLEvent = primaryEventReader.nextTag()
  override def getProperty(name: String): AnyRef = primaryEventReader.getProperty(name)
  override def close(): Unit = {}
  override def next(): AnyRef = primaryEventReader.next()

  private def filteredEventReader(
      inputStream: java.io.InputStream,
      options: XmlOptions): XMLEventReader = {
    val streamReader = filteredStreamReader(inputStream, options)
    filteredEventReader(streamReader)
  }

  private def filteredEventReader(streamReader: XMLStreamReader): XMLEventReader = {
    val filter = new EventFilter {
      override def accept(event: XMLEvent): Boolean =
        StaxXmlParserUtils.eventTypeFilter(event.getEventType)
    }
    val eventReader = StaxXmlParserUtils.factory.createXMLEventReader(streamReader)
    StaxXmlParserUtils.factory.createFilteredReader(eventReader, filter)
  }

  private def filteredStreamReader(
      inputStream: java.io.InputStream,
      options: XmlOptions): XMLStreamReader = {
    val filter = new StreamFilter {
      override def accept(event: XMLStreamReader): Boolean =
        StaxXmlParserUtils.eventTypeFilter(event.getEventType)
    }
    val bomInputStreamBuilder = new BOMInputStream.Builder
    bomInputStreamBuilder.setInputStream(inputStream)
    val streamReader =
      StaxXmlParserUtils.factory.createXMLStreamReader(bomInputStreamBuilder.get(), options.charset)
    StaxXmlParserUtils.factory.createFilteredReader(streamReader, filter)
  }
}
