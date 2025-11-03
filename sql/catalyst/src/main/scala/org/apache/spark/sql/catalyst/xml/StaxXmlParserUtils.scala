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

import java.io.StringReader
import javax.xml.namespace.QName
import javax.xml.stream.{
  EventFilter,
  StreamFilter,
  XMLEventReader,
  XMLInputFactory,
  XMLStreamConstants,
  XMLStreamReader
}
import javax.xml.stream.events._

import scala.annotation.tailrec
import scala.jdk.CollectionConverters._

import org.apache.commons.io.input.BOMInputStream

object StaxXmlParserUtils {

  private[sql] val factory: XMLInputFactory = {
    val factory = XMLInputFactory.newInstance()
    factory.setProperty(XMLInputFactory.IS_NAMESPACE_AWARE, false)
    factory.setProperty(XMLInputFactory.IS_COALESCING, true)
    factory.setProperty(XMLInputFactory.IS_SUPPORTING_EXTERNAL_ENTITIES, false)
    factory.setProperty(XMLInputFactory.SUPPORT_DTD, false)
    factory
  }

  private[sql] val eventTypeFilter: Int => Boolean = {
    // Ignore comments and processing instructions
    case XMLStreamConstants.COMMENT |
         XMLStreamConstants.PROCESSING_INSTRUCTION => false
    // unsupported events
    case XMLStreamConstants.DTD |
         XMLStreamConstants.ENTITY_DECLARATION |
         XMLStreamConstants.ENTITY_REFERENCE |
         XMLStreamConstants.NOTATION_DECLARATION => false
    case _ => true
  }

  def filteredReader(xml: String): XMLEventReader = {
    val filter = new EventFilter {
      override def accept(event: XMLEvent): Boolean = eventTypeFilter(event.getEventType)
    }
    // It does not have to skip for white space, since `XmlInputFormat`
    // always finds the root tag without a heading space.
    val eventReader = factory.createXMLEventReader(new StringReader(xml))
    factory.createFilteredReader(eventReader, filter)
  }

  def filteredReader(inputStream: java.io.InputStream, options: XmlOptions): XMLEventReader = {
    val filter = new EventFilter {
      override def accept(event: XMLEvent): Boolean = eventTypeFilter(event.getEventType)
    }
    val bomInputStreamBuilder = new BOMInputStream.Builder
    bomInputStreamBuilder.setInputStream(inputStream)
    bomInputStreamBuilder.setCharset(options.charset)
    val eventReader = factory.createXMLEventReader(bomInputStreamBuilder.get())
    factory.createFilteredReader(eventReader, filter)
  }

  def filteredReader(
      inputStream: () => java.io.InputStream,
      options: XmlOptions): StaxXMLRecordReader = {
    StaxXMLRecordReader(inputStream, options)
  }

  def filteredStreamReader(
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

  def gatherRootAttributes(parser: XMLEventReader): Array[Attribute] = {
    val rootEvent = StaxXmlParserUtils.skipUntil(parser, XMLStreamConstants.START_ELEMENT)
    rootEvent.asStartElement.getAttributes.asScala.toArray
  }

  /**
   * Skips elements until this meets the given type of a element
   */
  def skipUntil(parser: XMLEventReader, eventType: Int): XMLEvent = {
    var event = parser.peek
    while (parser.hasNext && event.getEventType != eventType) {
      event = parser.nextEvent
    }
    event
  }

  /**
   * Checks if current event points the EndElement.
   */
  @tailrec
  def checkEndElement(parser: XMLEventReader): Boolean = {
    parser.peek match {
      case _: EndElement | _: EndDocument => true
      case _: StartElement => false
      case _ =>
        // When other events are found here rather than `EndElement` or `StartElement`
        // , we need to look further to decide if this is the end because this can be
        // whitespace between `EndElement` and `StartElement`.
        parser.nextEvent
        checkEndElement(parser)
    }
  }

  /**
   * Produces values map from given attributes.
   */
  def convertAttributesToValuesMap(
      attributes: Array[Attribute],
      options: XmlOptions): Map[String, String] = {
    if (options.excludeAttributeFlag) {
      Map.empty[String, String]
    } else {
      attributes.map { attr =>
        val key = options.attributePrefix + getName(attr.getName, options)
        val data = if (options.ignoreSurroundingSpaces) attr.getValue.trim else attr.getValue
        val value = data match {
          case v if (v == options.nullValue) => null
          case v => v
        }
        key -> value
      }.toMap
    }
  }

  /**
   * Gets the local part of an XML name, optionally without namespace.
   */
  def getName(name: QName, options: XmlOptions): String = {
    val localPart = name.getLocalPart
    // Ignore namespace prefix up to last : if configured
    if (options.ignoreNamespace) {
      localPart.split(":").last
    } else {
      localPart
    }
  }

  /**
   * Convert the structure inside the target element to an XML string, **EXCLUDING** the target
   * element layer itself. The parser is expected to be positioned **AT** the start tag of the
   * target element.
   */
  def currentStructureAsString(
      parser: XMLEventReader,
      startElementName: String,
      options: XmlOptions): String = {
    val xmlString = new StringBuilder()
    var indent = 0
    do {
      val (str, ind) = nextEventToString(parser, indent)
      indent = ind
      xmlString.append(str)
    } while (parser.peek() match {
      case _: EndElement =>
        // until the unclosed end element for the whole parent is found
        indent > 0
      case _ => true
    })
    skipNextEndElement(parser, startElementName, options)
    xmlString.toString()
  }

  /**
   * Convert the element with the target element name to an XML string. The next event of the parser
   * is expected to be the start tag of the target element.
   */
  def currentElementAsString(
      parser: XMLEventReader,
      startElementName: String,
      options: XmlOptions): String = {
    assert(
      getName(parser.peek().asStartElement().getName, options) == startElementName,
      s"Expected StartElement <$startElementName>, but found ${parser.peek()}"
    )
    val xmlString = new StringBuilder()
    var indent = 0
    do {
      val (str, ind) = nextEventToString(parser, indent)
      indent = ind
      xmlString.append(str)
    } while (indent > 0)
    xmlString.toString()
  }

  private def nextEventToString(parser: XMLEventReader, currentIdent: Int): (String, Int) = {
    parser.nextEvent match {
      case e: StartElement =>
        val sb = new StringBuilder()
        sb.append('<').append(e.getName)
        e.getAttributes.asScala.foreach { att =>
          sb
            .append(' ')
            .append(att.getName)
            .append("=\"")
            .append(att.getValue)
            .append('"')
        }
        sb.append('>')
        (sb.toString(), currentIdent + 1)
      case e: EndElement =>
        (s"</${e.getName}>", currentIdent - 1)
      case c: Characters =>
        (c.getData, currentIdent)
      case _: XMLEvent => // do nothing
        ("", currentIdent)
    }
  }

  /**
   * Skip the children of the current XML element.
   * Before this function is called, the 'startElement' of the object has already been consumed.
   * Upon completion, this function consumes the 'endElement' of the object,
   * which effectively skipping the entire object enclosed within these elements.
   */
  def skipChildren(
      parser: XMLEventReader,
      expectedNextEndElementName: String,
      options: XmlOptions): Unit = {
    var shouldStop = false
    while (!shouldStop) {
      parser.nextEvent match {
        case startElement: StartElement =>
          val childField = StaxXmlParserUtils.getName(startElement.asStartElement.getName, options)
          skipChildren(parser, childField, options)
        case endElement: EndElement =>
          val endElementName = getName(endElement.getName, options)
          assert(
            endElementName == expectedNextEndElementName,
            s"Expected EndElement </$expectedNextEndElementName>, but found </$endElementName>"
          )
          shouldStop = true
        case _: XMLEvent => // do nothing
      }
    }
  }

  @tailrec
  def skipNextEndElement(
      parser: XMLEventReader,
      expectedNextEndElementName: String,
      options: XmlOptions): Unit = {
    parser.nextEvent() match {
      case c: Characters if c.isWhiteSpace =>
        skipNextEndElement(parser, expectedNextEndElementName, options)
      case endElement: EndElement =>
        val endElementName = getName(endElement.getName, options)
        assert(
          endElementName == expectedNextEndElementName,
          s"Expected EndElement </$expectedNextEndElementName>, but found </$endElementName>")
      case _ => throw new IllegalStateException(
        s"Expected EndElement </$expectedNextEndElementName>")
    }
  }
}
