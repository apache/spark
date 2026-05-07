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
import javax.xml.stream.{XMLEventReader, XMLStreamConstants, XMLStreamReader}
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
  private lazy val in1 = inputStream()
  private lazy val primaryEventReader = StaxXmlParserUtils.filteredReader(in1, options)

  private val xsdSchemaValidator = Option(options.rowValidationXSDPath)
    .map(path => ValidatorUtil.getSchema(path).newValidator())
  // Reader for the XSD validation, if an XSD schema is provided.
  private lazy val in2 = xsdSchemaValidator.map(_ => inputStream())
  // An XMLStreamReader used by StAXSource for XSD validation.
  private lazy val xsdValidationStreamReader =
    in2.map(in => StaxXmlParserUtils.filteredStreamReader(in, options))

  final var hasMoreRecord: Boolean = true

  /**
   * Skip through the XML stream until we find the next row start element.
   * Returns true if a row start element is found, false if end of stream is reached.
   */
  def skipToNextRecord(): Boolean = {
    hasMoreRecord = skipToNextRowStart()
    if (hasMoreRecord) {
      xsdValidationStreamReader.foreach(validateXSDSchema)
    } else {
      close()
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

  private def validateXSDSchema(streamReader: XMLStreamReader): Unit = {
    // StAXSource requires the stream reader to start with the START_DOCUMENT OR START_ELEMENT
    // events.
    def rowTagStarted: Boolean =
      streamReader.getEventType == XMLStreamConstants.START_ELEMENT &&
      StaxXmlParserUtils.getName(streamReader.getName, options) == options.rowTag
    while (!rowTagStarted && streamReader.hasNext) {
      streamReader.next()
    }
    xsdSchemaValidator.get.reset()
    xsdSchemaValidator.get.validate(new StAXSource(streamReader))
  }

  override def close(): Unit = {
    hasMoreRecord = false
    try {
      in1.close()
      in2.foreach(_.close())
      primaryEventReader.close()
      xsdValidationStreamReader.foreach(_.close())
    } catch {
      case NonFatal(e) =>
        // If the file is corrupted/missing, we won't be able to close the input streams. We do a
        // best-effort to close the streams and log the error if closing fails.
        logWarning("Error closing XML stream", e)
    }
  }

  override def nextEvent(): XMLEvent = primaryEventReader.nextEvent()
  override def hasNext: Boolean = primaryEventReader.hasNext
  override def peek(): XMLEvent = primaryEventReader.peek()
  override def getElementText: String = primaryEventReader.getElementText
  override def nextTag(): XMLEvent = primaryEventReader.nextTag()
  override def getProperty(name: String): AnyRef = primaryEventReader.getProperty(name)
  override def next(): AnyRef = primaryEventReader.next()
}
