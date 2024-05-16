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
import javax.xml.stream.{EventFilter, XMLEventReader, XMLInputFactory, XMLStreamConstants}
import javax.xml.stream.events._

import scala.annotation.tailrec
import scala.jdk.CollectionConverters._

object StaxXmlParserUtils {

  private[sql] val factory: XMLInputFactory = {
    val factory = XMLInputFactory.newInstance()
    factory.setProperty(XMLInputFactory.IS_NAMESPACE_AWARE, false)
    factory.setProperty(XMLInputFactory.IS_COALESCING, true)
    factory.setProperty(XMLInputFactory.IS_SUPPORTING_EXTERNAL_ENTITIES, false)
    factory.setProperty(XMLInputFactory.SUPPORT_DTD, false)
    factory
  }

  def filteredReader(xml: String): XMLEventReader = {
    val filter = new EventFilter {
      override def accept(event: XMLEvent): Boolean =
        event.getEventType match {
          // Ignore comments and processing instructions
          case XMLStreamConstants.COMMENT | XMLStreamConstants.PROCESSING_INSTRUCTION => false
          // unsupported events
          case XMLStreamConstants.DTD |
               XMLStreamConstants.ENTITY_DECLARATION |
               XMLStreamConstants.ENTITY_REFERENCE |
               XMLStreamConstants.NOTATION_DECLARATION => false
          case _ => true
        }
    }
    // It does not have to skip for white space, since `XmlInputFormat`
    // always finds the root tag without a heading space.
    val eventReader = factory.createXMLEventReader(new StringReader(xml))
    factory.createFilteredReader(eventReader, filter)
  }

  def gatherRootAttributes(parser: XMLEventReader): Array[Attribute] = {
    val rootEvent =
      StaxXmlParserUtils.skipUntil(parser, XMLStreamConstants.START_ELEMENT)
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
   * Convert the current structure of XML document to a XML string.
   */
  def currentStructureAsString(
      parser: XMLEventReader,
      startElementName: String,
      options: XmlOptions): String = {
    val xmlString = new StringBuilder()
    var indent = 0
    do {
      parser.nextEvent match {
        case e: StartElement =>
          xmlString.append('<').append(e.getName)
          e.getAttributes.asScala.foreach { att =>
            xmlString
              .append(' ')
              .append(att.getName)
              .append("=\"")
              .append(att.getValue)
              .append('"')
          }
          xmlString.append('>')
          indent += 1
        case e: EndElement =>
          xmlString.append("</").append(e.getName).append('>')
          indent -= 1
        case c: Characters =>
          xmlString.append(c.getData)
        case _: XMLEvent => // do nothing
      }
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
