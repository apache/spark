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
import javax.xml.stream.events.{Characters, EndDocument, EndElement, StartElement, XMLEvent}
import javax.xml.transform.stream.StreamSource
import javax.xml.validation.Schema

import scala.jdk.CollectionConverters._
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
          // Use StreamSource with a Reader that produces characters directly from XMLEventReader
          val streamingReader = XMLEventReaderToCharacterReader(p, options)
          schema.newValidator().validate(new StreamSource(streamingReader))
        } catch {
          case e: SAXException =>
            try {
              // If the validation fails, try the same validation on the primary parser to keep
              // the two parsers in sync.
              val streamingReader = XMLEventReaderToCharacterReader(parser, options)
              schema.newValidator().validate(new StreamSource(streamingReader))
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
 * A Reader that produces characters directly from XMLEventReader without any copying.
 * Characters are generated on-demand as the validator requests them, stopping at
 * the row tag boundary.
 */
case class XMLEventReaderToCharacterReader(parser: XMLEventReader, options: XmlOptions)
    extends java.io.Reader {
  private var currentEventChars: Iterator[Char] = Iterator.empty
  private var finished = false
  private var rowTagStarted = false
  private val rowTagName =
    StaxXmlParserUtils.getName(javax.xml.namespace.QName.valueOf(options.rowTag), options)

  override def read(): Int = {
    if (getNextChar()) {
      currentEventChars.next().toInt
    } else {
      -1
    }
  }

  override def read(cbuf: Array[Char], off: Int, len: Int): Int = {
    if (finished && !currentEventChars.hasNext) {
      return -1
    }

    var count = 0
    while (count < len && getNextChar()) {
      cbuf(off + count) = currentEventChars.next()
      count += 1
    }

    if (count == 0 && finished) -1 else count
  }

  private def getNextChar(): Boolean = {
    // If current event has more characters, return true
    if (currentEventChars.hasNext) {
      return true
    }

    // Need to get next event
    if (finished) {
      return false
    }

    // Get next XML event and create character iterator
    if (parser.hasNext) {
      val event = parser.nextEvent()

      // Check if this is the end of our row
      if (event.isEndElement) {
        val elementName = StaxXmlParserUtils.getName(event.asEndElement.getName, options)
        if (rowTagStarted && elementName == rowTagName) {
          finished = true
        }
      } else if (event.isStartElement && !rowTagStarted) {
        val elementName = StaxXmlParserUtils.getName(event.asStartElement.getName, options)
        if (elementName == rowTagName) {
          rowTagStarted = true
        }
      }

      // Create character iterator directly from event
      currentEventChars = createEventCharIterator(event)

      // Check if we got any characters, if not try next event
      if (currentEventChars.hasNext) {
        true
      } else {
        getNextChar() // Recursively try next event
      }
    } else {
      finished = true
      false
    }
  }

  private def createEventCharIterator(event: XMLEvent): Iterator[Char] = {
    event match {
      case se: StartElement =>
        val elementStr = createStartElementString(se)
        elementStr.iterator

      case ee: EndElement =>
        s"</${ee.getName}>".iterator

      case c: Characters =>
        escapeXmlIterator(c.getData)

      case _ =>
        Iterator.empty // Skip other event types
    }
  }

  private def createStartElementString(se: StartElement): String = {
    val sb = new StringBuilder()
    sb.append('<').append(se.getName)
    se.getAttributes.asScala.foreach { att =>
      sb.append(' ')
        .append(att.getName)
        .append("=\"")
        .append(escapeXml(att.getValue))
        .append('"')
    }
    sb.append('>')
    sb.toString()
  }

  private def escapeXmlIterator(text: String): Iterator[Char] = {
    new Iterator[Char] {
      private var pos = 0
      private var replacementChars: Iterator[Char] = Iterator.empty

      override def hasNext: Boolean = {
        replacementChars.hasNext || pos < text.length
      }

      override def next(): Char = {
        if (replacementChars.hasNext) {
          replacementChars.next()
        } else if (pos < text.length) {
          val char = text.charAt(pos)
          pos += 1
          char match {
            case '&' =>
              replacementChars = "amp;".iterator
              '&'
            case '<' =>
              replacementChars = "lt;".iterator
              '&'
            case '>' =>
              replacementChars = "gt;".iterator
              '&'
            case '"' =>
              replacementChars = "quot;".iterator
              '&'
            case '\'' =>
              replacementChars = "apos;".iterator
              '&'
            case c => c
          }
        } else {
          throw new NoSuchElementException()
        }
      }
    }
  }

  private def escapeXml(text: String): String = {
    text
      .replace("&", "&amp;")
      .replace("<", "&lt;")
      .replace(">", "&gt;")
      .replace("\"", "&quot;")
      .replace("'", "&apos;")
  }

  override def close(): Unit = {
    // XMLEventReader will be closed by the caller
  }
}
