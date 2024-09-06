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

import java.io.Writer
import java.sql.Timestamp
import javax.xml.stream.XMLOutputFactory

import scala.collection.Map

import org.apache.hadoop.shaded.com.ctc.wstx.api.WstxOutputProperties

import org.apache.spark.SparkIllegalArgumentException
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.{ArrayData, DateFormatter, DateTimeUtils, MapData, TimestampFormatter}
import org.apache.spark.sql.catalyst.util.LegacyDateFormats.FAST_DATE_FORMAT
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

class StaxXmlGenerator(
    schema: StructType,
    writer: Writer,
    options: XmlOptions,
    validateStructure: Boolean = true) {

  require(options.attributePrefix.nonEmpty,
    "'attributePrefix' option should not be empty string.")
  private val indentDisabled = options.indent == ""

  private val timestampFormatter = TimestampFormatter(
    options.timestampFormatInWrite,
    options.zoneId,
    options.locale,
    legacyFormat = FAST_DATE_FORMAT,
    isParsing = false)

  private val timestampNTZFormatter = TimestampFormatter(
    options.timestampNTZFormatInWrite,
    options.zoneId,
    legacyFormat = FAST_DATE_FORMAT,
    isParsing = false,
    forTimestampNTZ = true)

  private val dateFormatter = DateFormatter(
    options.dateFormatInWrite,
    options.locale,
    legacyFormat = FAST_DATE_FORMAT,
    isParsing = false)

  private val gen = {
    val factory = XMLOutputFactory.newInstance()
    // to_xml disables structure validation to allow multiple root tags
    factory.setProperty(WstxOutputProperties.P_OUTPUT_VALIDATE_STRUCTURE, validateStructure)
    factory.setProperty(WstxOutputProperties.P_OUTPUT_VALIDATE_NAMES, options.validateName)
    val xmlWriter = factory.createXMLStreamWriter(writer)
    if (!indentDisabled) {
      val indentingXmlWriter = new IndentingXMLStreamWriter(xmlWriter)
      indentingXmlWriter.setIndentStep(options.indent)
      indentingXmlWriter
    } else {
      xmlWriter
    }
  }

  private var rootElementWritten: Boolean = false
  def writeDeclaration(): Unit = {
    // Allow a root tag to be like "rootTag foo='bar'"
    // This is hacky; won't deal correctly with spaces in attributes, but want
    // to make this at least work for simple cases without much complication
    val rootTagTokens = options.rootTag.split(" ")
    val rootElementName = rootTagTokens.head
    val rootAttributes: Map[String, String] =
      if (rootTagTokens.length > 1) {
        rootTagTokens.tail.map { kv =>
          val Array(k, v) = kv.split("=")
          k -> v.replaceAll("['\"]", "")
        }.toMap
      } else {
        Map.empty
      }
    val declaration = options.declaration
    if (declaration != null && declaration.nonEmpty) {
      gen.writeProcessingInstruction("xml", declaration)
      gen.writeCharacters("\n")
    }
    gen.writeStartElement(rootElementName)
    rootAttributes.foreach { case (k, v) =>
      gen.writeAttribute(k, v)
    }
    if (indentDisabled) {
      gen.writeCharacters("\n")
    }
    rootElementWritten = true
  }

  def flush(): Unit = gen.flush()

  def close(): Unit = {
    if (rootElementWritten) {
      gen.writeEndElement()
      gen.close()
    }
    writer.close()
  }

  /**
   * Transforms a single Row to XML
   *
   * @param row
   * The row to convert
   */
  def write(row: InternalRow): Unit = {
    writeChildElement(options.rowTag, schema, row)
    if (indentDisabled) {
      gen.writeCharacters("\n")
    }
  }

  def writeChildElement(name: String, dt: DataType, v: Any): Unit = (name, dt, v) match {
    // If this is meant to be value but in no child, write only a value
    case (_, _, null) | (_, NullType, _) if options.nullValue == null =>
    // Because usually elements having `null` do not exist, just do not write
    // elements when given values are `null`.
    case (_, _, _) if name == options.valueTag =>
      // If this is meant to be value but in no child, write only a value
      writeElement(dt, v, options)
    case (_, _, _) =>
      gen.writeStartElement(name)
      writeElement(dt, v, options)
      gen.writeEndElement()
  }

  def writeChild(name: String, dt: DataType, v: Any): Unit = {
    (dt, v) match {
      // If this is meant to be attribute, write an attribute
      case (_, null) | (NullType, _)
        if name.startsWith(options.attributePrefix) && name != options.valueTag =>
        Option(options.nullValue).foreach {
          gen.writeAttribute(name.substring(options.attributePrefix.length), _)
        }
      case _ if name.startsWith(options.attributePrefix) && name != options.valueTag =>
        gen.writeAttribute(name.substring(options.attributePrefix.length), v.toString)

      // For ArrayType, we just need to write each as XML element.
      case (ArrayType(ty, _), v: ArrayData) =>
        (0 until v.numElements()).foreach { i =>
          writeChildElement(name, ty, v.get(i, ty))
        }
      // For other datatypes, we just write normal elements.
      case _ =>
        writeChildElement(name, dt, v)
    }
  }

  def writeElement(dt: DataType, v: Any, options: XmlOptions): Unit = (dt, v) match {
    case (_, null) | (NullType, _) => gen.writeCharacters(options.nullValue)
    case (_: StringType, v: UTF8String) => gen.writeCharacters(v.toString)
    case (_: StringType, v: String) => gen.writeCharacters(v)
    case (TimestampType, v: Timestamp) =>
      gen.writeCharacters(timestampFormatter.format(v.toInstant()))
    case (TimestampType, v: Long) =>
      gen.writeCharacters(timestampFormatter.format(v))
    case (TimestampNTZType, v: Long) =>
      gen.writeCharacters(timestampNTZFormatter.format(DateTimeUtils.microsToLocalDateTime(v)))
    case (DateType, v: Int) =>
      gen.writeCharacters(dateFormatter.format(v))
    case (IntegerType, v: Int) => gen.writeCharacters(v.toString)
    case (ShortType, v: Short) => gen.writeCharacters(v.toString)
    case (FloatType, v: Float) => gen.writeCharacters(v.toString)
    case (DoubleType, v: Double) => gen.writeCharacters(v.toString)
    case (LongType, v: Long) => gen.writeCharacters(v.toString)
    case (DecimalType(), v: java.math.BigDecimal) => gen.writeCharacters(v.toString)
    case (DecimalType(), v: Decimal) => gen.writeCharacters(v.toString)
    case (ByteType, v: Byte) => gen.writeCharacters(v.toString)
    case (BooleanType, v: Boolean) => gen.writeCharacters(v.toString)

    // For the case roundtrip in reading and writing XML files, [[ArrayType]] cannot have
    // [[ArrayType]] as element type. It always wraps the element with [[StructType]]. So,
    // this case only can happen when we convert a normal [[DataFrame]] to XML file.
    // When [[ArrayType]] has [[ArrayType]] as elements, it is confusing what is element name
    // for XML file.
    case (ArrayType(ty, _), v: ArrayData) =>
      (0 until v.numElements()).foreach { i =>
        writeChild(options.arrayElementName, ty, v.get(i, ty))
      }

    case (MapType(_, vt, _), mv: Map[_, _]) =>
      val (attributes, elements) = mv.toSeq.partition { case (f, _) =>
        f.toString.startsWith(options.attributePrefix) && f.toString != options.valueTag
      }
      // We need to write attributes first before the value.
      (attributes ++ elements).foreach { case (k, v) =>
        writeChild(k.toString, vt, v)
      }

    case (mt: MapType, mv: MapData) => writeMapData(mt, mv)

    case (st: StructType, r: InternalRow) =>
      val (attributes, elements) = st.zip(r.toSeq(st)).partition { case (f, _) =>
        f.name.startsWith(options.attributePrefix) && f.name != options.valueTag
      }
      // We need to write attributes first before the value.
      (attributes ++ elements).foreach { case (field, value) =>
        writeChild(field.name, field.dataType, value)
      }

    case (_, _) =>
      throw new SparkIllegalArgumentException(
        errorClass = "_LEGACY_ERROR_TEMP_3238",
        messageParameters = scala.collection.immutable.Map(
          "v" -> v.toString,
          "class" -> v.getClass.toString,
          "dt" -> dt.toString))
  }

  def writeMapData(mapType: MapType, map: MapData): Unit = {
    val keyArray = map.keyArray()
    val valueArray = map.valueArray()
    // write attributes first
    Seq(true, false).foreach { writeAttribute =>
      (0 until map.numElements()).foreach { i =>
        val key = keyArray.get(i, mapType.keyType).toString
        val isAttribute = key.startsWith(options.attributePrefix) && key != options.valueTag
        if (writeAttribute == isAttribute) {
          writeChild(key, mapType.valueType, valueArray.get(i, mapType.valueType))
        }
      }
    }
  }
}
