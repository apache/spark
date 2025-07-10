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

import java.io.{FileNotFoundException, InputStream, IOException}
import javax.xml.stream.{XMLEventReader, XMLStreamConstants, XMLStreamException, XMLStreamReader}
import javax.xml.stream.events.{EndDocument, StartElement, XMLEvent}
import javax.xml.transform.stax.StAXSource
import javax.xml.validation.Schema

import scala.util.control.NonFatal

import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.hadoop.hdfs.BlockMissingException
import org.apache.hadoop.security.AccessControlException
import org.apache.hadoop.shaded.com.ctc.wstx.exc.WstxEOFException

import org.apache.spark.internal.Logging

/**
 * XML tokenizer that never buffers complete XML records in memory. It uses XMLEventReader to parse
 * XML file stream directly and can move to the next XML record based on the rowTag option.
 *
 * The file stream will be closed by the XMLTokenizer if there is no more records available.
 */
class XmlTokenizer(inputStream: () => InputStream, options: XmlOptions) extends Logging {
  private var reader: StaxXMLRecordReader = StaxXMLRecordReader(
    () => StaxXmlParserUtils.filteredStreamReader(inputStream(), options),
    options
  )

  /**
   * Returns the next XML record as a positioned XMLEventReader.
   * This avoids creating intermediate string representations.
   */
  def next(): Option[StaxXMLRecordReader] = {
    var nextRecord: Option[StaxXMLRecordReader] = None
    try {
      // Skip to the next row start element
      if (reader.getAllValidEventReaders.forall(skipToNextRowStart)) {
        nextRecord = Some(reader)
      }
    } catch {
      case e: FileNotFoundException if options.ignoreMissingFiles =>
        logWarning("Skipping the rest of the content in the missing file", e)
      case NonFatal(e) =>
        ExceptionUtils.getRootCause(e) match {
          case _: AccessControlException | _: BlockMissingException =>
            throw e
          case _: RuntimeException | _: IOException | _: XMLStreamException
              if options.ignoreCorruptFiles =>
            logWarning("Skipping the rest of the content in the corrupted file", e)
          case e: Throwable =>
            throw e
        }
    } finally {
      if (nextRecord.isEmpty && reader != null) {
        close()
      }
    }
    nextRecord
  }

  def close(): Unit = {
    if (reader != null) {
      reader.closeAllReaders()
      reader = null
    }
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
      case NonFatal(e) if ExceptionUtils.getRootCause(e).isInstanceOf[WstxEOFException] =>
        logWarning("Reached end of file while looking for next row start element.")
        false
    }
  }
}

/**
 * XML record reader that reads the next XML record in the underlying XML stream. It can support XSD
 * schema validation by maintaining a separate XML reader and keep it in sync with the primary XML
 * reader.
 */
case class StaxXMLRecordReader(createStreamReader: () => XMLStreamReader, options: XmlOptions)
    extends XMLEventReader {
  // Reader for the XML record parsing.
  private val primaryEventReader = StaxXmlParserUtils.filteredReader(createStreamReader())
  // Reader for the XSD validation, if an XSD schema is provided.
  private val streamReaderForXSDValidation =
    Option(options.rowValidationXSDPath).map(_ => createStreamReader())
  private val eventReaderForXSDValidation =
    streamReaderForXSDValidation.map(StaxXmlParserUtils.filteredReader)

  def getAllValidEventReaders: Seq[XMLEventReader] =
    Seq(primaryEventReader) ++ eventReaderForXSDValidation

  def validateXSDSchema(schema: Schema): Unit = {
    streamReaderForXSDValidation match {
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
              // advancing the primary event reader so that the XMLTokenizer will continue to the
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
    streamReaderForXSDValidation.foreach(_.close())
    eventReaderForXSDValidation.foreach(_.close())
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

object StaxXMLRecordReader {
  def apply(xml: String, options: XmlOptions): StaxXMLRecordReader = {
    StaxXMLRecordReader(
      () => StaxXmlParserUtils.filteredStreamReader(xml),
      options
    )
  }
}
