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
package org.apache.spark.sql.execution.datasources.xml.parsers

import java.io.StringReader
import javax.xml.stream.{XMLInputFactory, XMLStreamConstants}

import scala.jdk.CollectionConverters._

import org.scalatest.BeforeAndAfterAll

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.xml.{StaxXmlParserUtils, XmlOptions}

final class StaxXmlParserUtilsSuite extends SparkFunSuite with BeforeAndAfterAll {

  private val factory = StaxXmlParserUtils.factory

  test("Test if elements are skipped until the given event type") {
    val input = <ROW><id>2</id><name>Sam Mad Dog Smith</name><amount>93</amount></ROW>
    val parser = factory.createXMLEventReader(new StringReader(input.toString))
    val event = StaxXmlParserUtils.skipUntil(parser, XMLStreamConstants.END_DOCUMENT)
    assert(event.isEndDocument)
  }

  test("Check the end of element") {
    val input = <ROW><id>2</id></ROW>
    val parser = factory.createXMLEventReader(new StringReader(input.toString))
    // Skip until </id>
    StaxXmlParserUtils.skipUntil(parser, XMLStreamConstants.END_ELEMENT)
    assert(StaxXmlParserUtils.checkEndElement(parser))
  }

  test("Convert attributes to a map with keys and values") {
    val input = <ROW id="2"></ROW>
    val parser = factory.createXMLEventReader(new StringReader(input.toString))
    val event =
      StaxXmlParserUtils.skipUntil(parser, XMLStreamConstants.START_ELEMENT)
    val attributes =
      event.asStartElement().getAttributes.asScala.toArray
    val valuesMap =
      StaxXmlParserUtils.convertAttributesToValuesMap(attributes, new XmlOptions())
    assert(valuesMap === Map(s"${XmlOptions.DEFAULT_ATTRIBUTE_PREFIX}id" -> "2"))
  }

  test("Convert current structure to string") {
    val input = <ROW><id>2</id><info>
      <name>Sam Mad Dog Smith</name><amount><small>1</small><large>9</large></amount></info></ROW>
    val parser = factory.createXMLEventReader(new StringReader(input.toString))
    // Skip until </id>
    StaxXmlParserUtils.skipUntil(parser, XMLStreamConstants.END_ELEMENT)
    val xmlString = StaxXmlParserUtils.currentStructureAsString(parser, "ROW", new XmlOptions())
    val expected = <info>
      <name>Sam Mad Dog Smith</name><amount><small>1</small><large>9</large></amount></info>
    assert(xmlString === expected.toString())
  }

  test("Skip XML children") {
    val input = <ROW><info>
      <name>Sam Mad Dog Smith</name><amount><small>1</small>
        <large>9</large></amount></info><abc>2</abc><test>2</test></ROW>
    val parser = factory.createXMLEventReader(new StringReader(input.toString))
    // We assume here it's reading the value within `id` field.
    StaxXmlParserUtils.skipUntil(parser, XMLStreamConstants.CHARACTERS)
    StaxXmlParserUtils.skipChildren(parser)
    assert(parser.nextEvent().asEndElement().getName.getLocalPart === "info")
    parser.next()
    StaxXmlParserUtils.skipChildren(parser)
    assert(parser.nextEvent().asEndElement().getName.getLocalPart === "abc")
    parser.next()
    StaxXmlParserUtils.skipChildren(parser)
    assert(parser.nextEvent().asEndElement().getName.getLocalPart === "test")
  }

  test("XML Input Factory disables DTD parsing") {
    assert(factory.getProperty(XMLInputFactory.IS_SUPPORTING_EXTERNAL_ENTITIES) === false)
    assert(factory.getProperty(XMLInputFactory.SUPPORT_DTD) === false)
  }
}
