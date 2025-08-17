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
import javax.xml.stream.{XMLEventReader, XMLStreamConstants}
import javax.xml.stream.events.{EndDocument, StartElement, XMLEvent}
import javax.xml.transform.stax.StAXSource

import scala.util.control.NonFatal

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
  private val primaryEventReader = StaxXmlParserUtils.filteredReader(in1)

  private val xsdSchemaValidator = Option(options.rowValidationXSDPath)
    .map(path => ValidatorUtil.getSchema(path).newValidator())
  // Reader for the XSD validation, if an XSD schema is provided.
  private val in2 = xsdSchemaValidator.map(_ => inputStream())
  // An XMLStreamReader used by StAXSource for XSD validation.
  private val xsdValidationStreamReader =
    in2.map(in => StaxXmlParserUtils.filteredStreamReader(in, options))

  final var hasMoreRecord: Boolean = true

  /**
   * Skip through the XML stream until we find the next row start element.
   * Returns true if a row start element is found, false if end of stream is reached.
   */
  def skipToNextRecord(): Boolean = {
    hasMoreRecord = skipToNextRowStart()
    if (hasMoreRecord) {
      validateXSDSchemaForNextRecordIfNecessary()
    } else {
      closeAllReaders()
    }
    hasMoreRecord
  }

  /**
   * Skip through the XML stream until we find the next row start element.
   */
  private def skipToNextRowStart(): Boolean = {
    val rowTagName = options.rowTag
    try {
      while (primaryEventReader.hasNext) {
        val event = primaryEventReader.peek()
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
        primaryEventReader.nextEvent()
      }
      false
    } catch {
      case NonFatal(e) if SparkErrorUtils.getRootCause(e).isInstanceOf[WstxEOFException] =>
        logWarning("Reached end of file while looking for next row start element.")
        false
    }
  }

  private def validateXSDSchemaForNextRecordIfNecessary(): Unit = {
    xsdValidationStreamReader foreach { p =>
      // StAXSource requires the stream reader to start with the START_DOCUMENT OR START_ELEMENT
      // events.
      def rowTagStarted: Boolean =
        p.getEventType == XMLStreamConstants.START_ELEMENT &&
        StaxXmlParserUtils.getName(p.getName, options) == options.rowTag
      while (!rowTagStarted && p.hasNext) {
        p.next()
      }
      xsdSchemaValidator.get.reset()
      xsdSchemaValidator.get.validate(new StAXSource(p))
    }
  }

  def closeAllReaders(): Unit = {
    primaryEventReader.close()
    xsdValidationStreamReader.foreach(_.close())
    in1.close()
    in2.foreach(_.close())
    hasMoreRecord = false
  }

  def getNextRecordString: String = {
    if (hasMoreRecord) {
      // Advance the parser to the start element of the record
      val start = primaryEventReader.next().asInstanceOf[StartElement]
      val rowTagName = StaxXmlParserUtils.getName(start.getName, options)
      val record = new StringBuilder()
      record.append("<").append(rowTagName).append(">")
      record.append(
        StaxXmlParserUtils.currentStructureAsString(primaryEventReader, options.rowTag, options)
      )
      record.append(s"</$rowTagName>\n")
      record.toString().trim
    } else {
      throw new IllegalStateException("No more records available to read.")
    }
  }

  override def nextEvent(): XMLEvent = primaryEventReader.nextEvent()
  override def hasNext: Boolean = primaryEventReader.hasNext
  override def peek(): XMLEvent = primaryEventReader.peek()
  override def getElementText: String = primaryEventReader.getElementText
  override def nextTag(): XMLEvent = primaryEventReader.nextTag()
  override def getProperty(name: String): AnyRef = primaryEventReader.getProperty(name)
  override def close(): Unit = {}
  override def next(): AnyRef = primaryEventReader.next()
}
