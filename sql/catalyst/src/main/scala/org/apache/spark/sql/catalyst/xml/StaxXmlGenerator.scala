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
import java.util.Base64
import javax.xml.stream.XMLOutputFactory

import scala.collection.Map

import org.apache.hadoop.shaded.com.ctc.wstx.api.WstxOutputProperties

import org.apache.spark.SparkIllegalArgumentException
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.{ArrayData, DateFormatter, DateTimeUtils, MapData, TimestampFormatter}
import org.apache.spark.sql.catalyst.util.LegacyDateFormats.FAST_DATE_FORMAT
import org.apache.spark.sql.types._
import org.apache.spark.types.variant.VariantUtil
import org.apache.spark.unsafe.types.{UTF8String, VariantVal}

class StaxXmlGenerator(
    schema: DataType,
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
    schema match {
      case st: StructType if st.fields.forall(f => options.singleVariantColumn.contains(f.name)) =>
        // If the top-level field is a StructType with only the single Variant column, we ignore
        // the single variant column layer and directly write the Variant value under the row tag
        writeChildElement(options.rowTag, VariantType, row.getVariant(0))
      case _ => writeChildElement(options.rowTag, schema, row)
    }
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
    case (_, VariantType, v: VariantVal) =>
      writeVariant(name, v, pos = 0)
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

  /**
   * Serialize the single Variant value to XML
   */
  def write(v: VariantVal): Unit = {
    writeVariant(options.rowTag, v, pos = 0)
  }

  /**
   * Write a Variant field to XML
   *
   * @param name The name of the field
   * @param v The original Variant entity
   * @param pos The position in the Variant data array where the field value starts
   */
  private def writeVariant(name: String, v: VariantVal, pos: Int): Unit = {
    VariantUtil.getType(v.getValue, pos) match {
      case VariantUtil.Type.OBJECT =>
        writeVariantObject(name, v, pos)
      case VariantUtil.Type.ARRAY =>
        writeVariantArray(name, v, pos)
      case _ =>
        writeVariantPrimitive(name, v, pos)
    }
  }

  /**
   * Write a Variant object to XML. A Variant object is serialized as an XML element, with the child
   * fields serialized as XML nodes recursively.
   *
   * @param name The name of the object field, which is used as the XML element name
   * @param v The original Variant entity
   * @param pos The position in the Variant data array where the object value starts
   */
  private def writeVariantObject(name: String, v: VariantVal, pos: Int): Unit = {
    gen.writeStartElement(name)
    VariantUtil.handleObject(
      v.getValue,
      pos,
      (size, idSize, offsetSize, idStart, offsetStart, dataStart) => {
        // Traverse the fields of the object and get their names and positions in the original
        // Variant
        val elementInfo = (0 until size).map { i =>
          val id = VariantUtil.readUnsigned(v.getValue, idStart + idSize * i, idSize)
          val offset =
            VariantUtil.readUnsigned(v.getValue, offsetStart + offsetSize * i, offsetSize)
          val elementPos = dataStart + offset
          val elementName = VariantUtil.getMetadataKey(v.getMetadata, id)
          (elementName, elementPos)
        }

        // Partition the fields of the object into XML attributes and elements
        val (attributes, elements) = elementInfo.partition {
          case (f, _) =>
            // Similar to the reader, we use attributePrefx option to determine whether the field is
            // an attribute or not.
            // In addition, we also check if the field is a value tag, in case the value tag also
            // starts with the attribute prefix.
            f.startsWith(options.attributePrefix) && f != options.valueTag
        }

        // We need to write attributes first before the elements.
        (attributes ++ elements).foreach {
          case (field, elementPos) =>
            writeVariant(field, v, elementPos)
        }
      }
    )
    gen.writeEndElement()
  }

  /**
   * Write a Variant array to XML. A Variant array is flattened and written as a sequence of
   * XML element with the same element name as the array field name.
   *
   * @param name The name of the array field
   * @param v The original Variant entity
   * @param pos The position in the Variant data array where the array value starts
   */
  private def writeVariantArray(name: String, v: VariantVal, pos: Int): Unit = {
    VariantUtil.handleArray(
      v.getValue,
      pos,
      (size, offsetSize, offsetStart, dataStart) => {
        // Traverse each item of the array and write each of them as an XML element
        (0 until size).foreach { i =>
          val offset =
            VariantUtil.readUnsigned(v.getValue, offsetStart + offsetSize * i, offsetSize)
          val elementPos = dataStart + offset
          // Check if the array element is also of type ARRAY
          if (VariantUtil.getType(v.getValue, elementPos) == VariantUtil.Type.ARRAY) {
            // For the case round trip in reading and writing XML files, [[ArrayType]] cannot have
            // [[ArrayType]] as element type. It always wraps the element with [[StructType]]. So,
            // this case only can happen when we convert a normal [[DataFrame]] to XML file.
            // When [[ArrayType]] has [[ArrayType]] as elements, it is confusing what is element
            // name for XML file.
            writeVariantArray(options.arrayElementName, v, elementPos)
          } else {
            writeVariant(name, v, elementPos)
          }
        }
      }
    )
  }

  /**
   * Write a Variant primitive field to XML
   *
   * @param name The name of the field
   * @param v The original Variant entity
   * @param pos The position in the Variant data array where the field value starts
   */
  private def writeVariantPrimitive(name: String, v: VariantVal, pos: Int): Unit = {
    val primitiveVal: String = VariantUtil.getType(v.getValue, pos) match {
      case VariantUtil.Type.NULL => Option(options.nullValue).orNull
      case VariantUtil.Type.BOOLEAN =>
        VariantUtil.getBoolean(v.getValue, pos).toString
      case VariantUtil.Type.LONG =>
        VariantUtil.getLong(v.getValue, pos).toString
      case VariantUtil.Type.STRING =>
        VariantUtil.getString(v.getValue, pos)
      case VariantUtil.Type.DOUBLE =>
        VariantUtil.getDouble(v.getValue, pos).toString
      case VariantUtil.Type.DECIMAL =>
        VariantUtil.getDecimal(v.getValue, pos).toString
      case VariantUtil.Type.DATE =>
        dateFormatter.format(VariantUtil.getLong(v.getValue, pos).toInt)
      case VariantUtil.Type.TIMESTAMP =>
        timestampFormatter.format(VariantUtil.getLong(v.getValue, pos))
      case VariantUtil.Type.TIMESTAMP_NTZ =>
        timestampNTZFormatter.format(
          DateTimeUtils.microsToLocalDateTime(VariantUtil.getLong(v.getValue, pos))
        )
      case VariantUtil.Type.FLOAT => VariantUtil.getFloat(v.getValue, pos).toString
      case VariantUtil.Type.BINARY =>
        Base64.getEncoder.encodeToString(VariantUtil.getBinary(v.getValue, pos))
      case VariantUtil.Type.UUID => VariantUtil.getUuid(v.getValue, pos).toString
      case _ =>
        throw new SparkIllegalArgumentException("invalid variant primitive type for XML")
    }

    val value = if (primitiveVal == null) options.nullValue else primitiveVal

    // Handle attributes first
    val isAttribute = name.startsWith(options.attributePrefix) && name != options.valueTag
    if (isAttribute && primitiveVal != null) {
      gen.writeAttribute(
        name.substring(options.attributePrefix.length),
        value
      )
      return
    }

    // Handle value tags
    if (name == options.valueTag && primitiveVal != null) {
      gen.writeCharacters(value)
      return
    }

    // Handle child elements
    gen.writeStartElement(name)
    if (primitiveVal != null) gen.writeCharacters(value)
    gen.writeEndElement()
  }
}
