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
import javax.xml.stream.{XMLEventReader, XMLStreamException}
import javax.xml.stream.events.{EndDocument, StartElement, XMLEvent}
import javax.xml.transform.stax.StAXSource
import javax.xml.validation.Schema

import scala.util.control.NonFatal
import scala.xml.SAXException

import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.hadoop.hdfs.BlockMissingException
import org.apache.hadoop.security.AccessControlException
import org.apache.hadoop.shaded.com.ctc.wstx.exc.WstxEOFException

import org.apache.spark.internal.Logging

/**
 * XML tokenizer that never buffers complete XML records in memory. It uses XMLEventReader to parse
 * XML file stream directly and can move to the next XML record based on the rowTag option.
 */
class XmlTokenizer(inputStream: () => InputStream, options: XmlOptions) extends Logging {
  // Primary XML event reader for parsing
  private val in1 = inputStream()
  private var reader = StaxXmlParserUtils.filteredReader(in1, options)

  // Optional XML event reader for XSD validation.
  private val in2 = Option(options.rowValidationXSDPath).map(_ => inputStream())
  private val readerForXSDValidation = in2.map(in => StaxXmlParserUtils.filteredReader(in, options))

  /**
   * Returns the next XML record as a positioned XMLEventReader.
   * This avoids creating intermediate string representations.
   */
  def next(): Option[XMLEventReaderWithXSDValidation] = {
    var nextRecord: Option[XMLEventReaderWithXSDValidation] = None
    try {
      // Skip to the next row start element
      if (skipToNextRowStart()) {
        nextRecord = Some(XMLEventReaderWithXSDValidation(reader, readerForXSDValidation, options))
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
      in1.close()
      in2.foreach(_.close())
      reader.close()
      reader = null
    }
  }

  /**
   * Skip through the XML stream until we find the next row start element.
   */
  private def skipToNextRowStart(): Boolean = {
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
        // advance the reader for XSD validation as well to keep them in sync
        readerForXSDValidation.foreach(_.nextEvent())
      }
      false
    } catch {
      case NonFatal(e) if ExceptionUtils.getRootCause(e).isInstanceOf[WstxEOFException] =>
        logWarning("Reached end of file while looking for next row start element.")
        false
    }
  }
}

case class XMLEventReaderWithXSDValidation(
    parser: XMLEventReader,
    parserForXSDValidation: Option[XMLEventReader] = None,
    options: XmlOptions)
    extends XMLEventReader {

  def validateXSDSchema(schema: Schema): Unit = {
    parserForXSDValidation match {
      case Some(p) =>
        try {
          schema.newValidator().validate(new StAXSource(DocumentWrappingXMLEventReader(p, options)))
        } catch {
          case e: SAXException =>
            try {
              // If the validation fails, try the same validation on the primary parser to keep
              // the two parsers in sync.
              schema.newValidator().validate(
                new StAXSource(DocumentWrappingXMLEventReader(parser, options))
              )
            } finally {
              throw e
            }
        }
      case None => throw new IllegalStateException("XSD validation parser is not initialized")
    }
  }

  override def nextEvent(): XMLEvent = parser.nextEvent()
  override def hasNext: Boolean = parser.hasNext
  override def peek(): XMLEvent = parser.peek()
  override def getElementText: String = parser.getElementText
  override def nextTag(): XMLEvent = parser.nextTag()
  override def getProperty(name: String): AnyRef = parser.getProperty(name)
  override def close(): Unit = {
    parser.close()
    parserForXSDValidation.foreach(_.close())
  }
  override def next(): AnyRef = parser.next()
}

object XMLEventReaderWithXSDValidation {
  def apply(xml: String, options: XmlOptions): XMLEventReaderWithXSDValidation = {
    XMLEventReaderWithXSDValidation(
      StaxXmlParserUtils.filteredReader(xml),
      Option(options.rowValidationXSDPath).map(_ => StaxXmlParserUtils.filteredReader(xml)),
      options
    )
  }
}

/**
 * XMLEventReader wrapper that injects StartDocument and EndDocument events around
 * an input XMLEventReader. This is required in XSD schema validation, as the Validator:validate
 * works only if the input StAXSource contains a complete XML document.
 */
case class DocumentWrappingXMLEventReader(
    wrappedReader: XMLEventReader,
    options: XmlOptions)
    extends XMLEventReader {
  import javax.xml.stream.{XMLEventFactory, XMLStreamException}
  import javax.xml.stream.events.{StartElement, EndElement}
  private val rowTag = options.rowTag

  private val eventFactory = XMLEventFactory.newInstance()
  eventFactory.setLocation(
    // Create a dummy location to avoid null pointer exceptions
    new javax.xml.stream.Location {
      override def getLineNumber: Int = -1
      override def getColumnNumber: Int = -1
      override def getCharacterOffset: Int = -1
      override def getPublicId: String = null
      override def getSystemId: String = ""
    }
  )
  private var state: DocumentWrapperState = StartDocumentState
  private var rowTagDepth = 0 // Track nesting depth of rowTag elements

  private sealed trait DocumentWrapperState
  private case object StartDocumentState extends DocumentWrapperState
  private case object DelegatingState extends DocumentWrapperState
  private case object EndDocumentState extends DocumentWrapperState
  private case object FinishedState extends DocumentWrapperState

  override def nextEvent(): XMLEvent = {
    state match {
      case StartDocumentState =>
        state = DelegatingState
        eventFactory.createStartDocument()

      case DelegatingState =>
        if (wrappedReader.hasNext) {
          val event = wrappedReader.nextEvent()

          // Track nesting depth of rowTag elements to handle nested elements with same name
          event match {
            case startElement: StartElement =>
              val elementName = StaxXmlParserUtils.getName(startElement.getName, options)
              if (elementName == rowTag) {
                rowTagDepth += 1 // Enter a rowTag element (could be nested)
              }

            case endElement: EndElement =>
              val elementName = StaxXmlParserUtils.getName(endElement.getName, options)
              if (elementName == rowTag && rowTagDepth > 0) {
                rowTagDepth -= 1 // Exit a rowTag element
                // Only transition to EndDocumentState when we've closed the top-level rowTag
                if (rowTagDepth == 0) {
                  state = EndDocumentState
                }
              }

            case _ => // Other events, just pass through
          }

          event
        } else {
          state = EndDocumentState
          nextEvent() // Recursively call to get the EndDocument event
        }

      case EndDocumentState =>
        state = FinishedState
        eventFactory.createEndDocument()

      case FinishedState =>
        throw new XMLStreamException("No more events available")
    }
  }

  override def hasNext: Boolean = {
    state match {
      case StartDocumentState => true
      case DelegatingState =>
        true // Either has wrapped events or will transition to EndDocumentState
      case EndDocumentState => true
      case FinishedState => false
    }
  }

  override def peek(): XMLEvent = {
    state match {
      case StartDocumentState =>
        eventFactory.createStartDocument()

      case DelegatingState =>
        if (wrappedReader.hasNext) {
          val nextEvent = wrappedReader.peek()

          // Check if the next event would cause us to transition to EndDocumentState
          nextEvent match {
            case endElement: EndElement =>
              val elementName = StaxXmlParserUtils.getName(endElement.getName, options)
              if (elementName == rowTag && rowTagDepth > 0) {
                // The next event would be the end of our row if it brings depth to 0
                // But we return the actual next event first
                nextEvent
              } else {
                nextEvent
              }
            case _ => nextEvent
          }
        } else {
          // Don't modify state in peek - just return what the next event would be
          eventFactory.createEndDocument()
        }

      case EndDocumentState =>
        eventFactory.createEndDocument()

      case FinishedState =>
        throw new XMLStreamException("No more events available")
    }
  }

  override def getElementText: String = {
    state match {
      case DelegatingState if wrappedReader.hasNext =>
        wrappedReader.getElementText
      case _ =>
        throw new XMLStreamException("getElementText() not supported in current state")
    }
  }

  override def nextTag(): XMLEvent = {
    state match {
      case DelegatingState if wrappedReader.hasNext =>
        wrappedReader.nextTag()
      case _ =>
        throw new XMLStreamException("nextTag() not supported in current state")
    }
  }

  override def getProperty(name: String): AnyRef = {
    wrappedReader.getProperty(name)
  }

  override def close(): Unit = {
    state = FinishedState
  }

  override def next(): AnyRef = nextEvent()
}
