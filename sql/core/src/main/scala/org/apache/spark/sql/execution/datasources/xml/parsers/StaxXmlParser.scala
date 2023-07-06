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
import javax.xml.stream.XMLEventReader
import javax.xml.stream.events.{Attribute, Characters, EndElement, StartElement, XMLEvent}
import javax.xml.transform.stream.StreamSource
import javax.xml.validation.Schema

import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._
import scala.util.Try
import scala.util.control.NonFatal

import org.slf4j.LoggerFactory

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.util.{DropMalformedMode, FailFastMode, ParseMode, PermissiveMode}
import org.apache.spark.sql.execution.datasources.xml.XmlOptions
import org.apache.spark.sql.execution.datasources.xml.util._
import org.apache.spark.sql.execution.datasources.xml.util.TypeCast._
import org.apache.spark.sql.types._

/**
 * Wraps parser to iteration process.
 */
private[xml] object StaxXmlParser extends Serializable {
  private val logger = LoggerFactory.getLogger(StaxXmlParser.getClass)

  def parse(
      xml: RDD[String],
      schema: StructType,
      options: XmlOptions): RDD[Row] = {
    xml.mapPartitions { iter =>
      val xsdSchema = Option(options.rowValidationXSDPath).map(ValidatorUtil.getSchema)
      iter.flatMap { xml =>
        doParseColumn(xml, schema, options, options.parseMode, xsdSchema)
      }
    }
  }

  def parseColumn(xml: String, schema: StructType, options: XmlOptions): Row = {
    // The user=specified schema from from_xml, etc will typically not include a
    // "corrupted record" column. In PERMISSIVE mode, which puts bad records in
    // such a column, this would cause an error. In this mode, if such a column
    // is not manually specified, then fall back to DROPMALFORMED, which will return
    // null column values where parsing fails.
    val parseMode =
      if (options.parseMode == PermissiveMode &&
          !schema.fields.exists(_.name == options.columnNameOfCorruptRecord)) {
        DropMalformedMode
      } else {
        options.parseMode
      }
    val xsdSchema = Option(options.rowValidationXSDPath).map(ValidatorUtil.getSchema)
    doParseColumn(xml, schema, options, parseMode, xsdSchema).orNull
  }

  private def doParseColumn(xml: String,
      schema: StructType,
      options: XmlOptions,
      parseMode: ParseMode,
      xsdSchema: Option[Schema]): Option[Row] = {
    try {
      xsdSchema.foreach { schema =>
        schema.newValidator().validate(new StreamSource(new StringReader(xml)))
      }
      val parser = StaxXmlParserUtils.filteredReader(xml)
      val rootAttributes = StaxXmlParserUtils.gatherRootAttributes(parser)
      Some(convertObject(parser, schema, options, rootAttributes))
    } catch {
      case e: PartialResultException =>
        failedRecord(xml, options, parseMode, schema,
          e.cause, Some(e.partialResult))
      case NonFatal(e) =>
        failedRecord(xml, options, parseMode, schema, e)
    }
  }

  private def failedRecord(record: String,
      options: XmlOptions,
      parseMode: ParseMode,
      schema: StructType,
      cause: Throwable = null,
      partialResult: Option[Row] = None): Option[Row] = {
    // create a row even if no corrupt record column is present
    val abbreviatedRecord =
      (if (record.length() > 1000) record.substring(0, 1000) + "..." else record).
        replaceAll("\n", "")
    parseMode match {
      case FailFastMode =>
        logger.info("Malformed line:", abbreviatedRecord)
        logger.debug("Caused by:", cause)
        throw new IllegalArgumentException("Malformed line in FAILFAST mode", cause)
      case DropMalformedMode =>
        logger.info("Malformed line:", abbreviatedRecord)
        logger.debug("Caused by:", cause)
        None
      case PermissiveMode =>
        logger.debug("Malformed line:", abbreviatedRecord)
        logger.debug("Caused by:", cause)
        // The logic below is borrowed from Apache Spark's FailureSafeParser.
        val resultRow = new Array[Any](schema.length)
        schema.filterNot(_.name == options.columnNameOfCorruptRecord).foreach { from =>
          val sourceIndex = schema.fieldIndex(from.name)
          resultRow(sourceIndex) = partialResult.map(_.get(sourceIndex)).orNull
        }
        val corruptFieldIndex = Try(schema.fieldIndex(options.columnNameOfCorruptRecord)).toOption
        corruptFieldIndex.foreach(resultRow(_) = record)
        Some(Row.fromSeq(resultRow.toIndexedSeq))
    }
  }

  /**
   * Parse the current token (and related children) according to a desired schema
   */
  private[xml] def convertField(
      parser: XMLEventReader,
      dataType: DataType,
      options: XmlOptions,
      attributes: Array[Attribute] = Array.empty): Any = {

    def convertComplicatedType(dt: DataType, attributes: Array[Attribute]): Any = dt match {
      case st: StructType => convertObject(parser, st, options)
      case MapType(StringType, vt, _) => convertMap(parser, vt, options, attributes)
      case ArrayType(st, _) => convertField(parser, st, options)
      case _: StringType =>
        convertTo(StaxXmlParserUtils.currentStructureAsString(parser), StringType, options)
    }

    (parser.peek, dataType) match {
      case (_: StartElement, dt: DataType) => convertComplicatedType(dt, attributes)
      case (_: EndElement, _: StringType) =>
        // Empty. It's null if these are explicitly treated as null, or "" is the null value
        if (options.treatEmptyValuesAsNulls || options.nullValue == "") {
          null
        } else {
          ""
        }
      case (_: EndElement, _: DataType) => null
      case (c: Characters, ArrayType(st, _)) =>
        // For `ArrayType`, it needs to return the type of element. The values are merged later.
        convertTo(c.getData, st, options)
      case (c: Characters, st: StructType) =>
        // If a value tag is present, this can be an attribute-only element whose values is in that
        // value tag field. Or, it can be a mixed-type element with both some character elements
        // and other complex structure. Character elements are ignored.
        val attributesOnly = st.fields.forall { f =>
          f.name == options.valueTag || f.name.startsWith(options.attributePrefix)
        }
        if (attributesOnly) {
          // If everything else is an attribute column, there's no complex structure.
          // Just return the value of the character element, or null if we don't have a value tag
          st.find(_.name == options.valueTag).map(
            valueTag => convertTo(c.getData, valueTag.dataType, options)).orNull
        } else {
          // Otherwise, ignore this character element, and continue parsing the following complex
          // structure
          parser.next
          parser.peek match {
            case _: EndElement => null // no struct here at all; done
            case _ => convertObject(parser, st, options)
          }
        }
      case (_: Characters, _: StringType) =>
        convertTo(StaxXmlParserUtils.currentStructureAsString(parser), StringType, options)
      case (c: Characters, _: DataType) if c.isWhiteSpace =>
        // When `Characters` is found, we need to look further to decide
        // if this is really data or space between other elements.
        val data = c.getData
        parser.next
        parser.peek match {
          case _: StartElement => convertComplicatedType(dataType, attributes)
          case _: EndElement if data.isEmpty => null
          case _: EndElement if options.treatEmptyValuesAsNulls => null
          case _: EndElement => convertTo(data, dataType, options)
          case _ => convertField(parser, dataType, options, attributes)
        }
      case (c: Characters, dt: DataType) =>
        convertTo(c.getData, dt, options)
      case (e: XMLEvent, dt: DataType) =>
        throw new IllegalArgumentException(
          s"Failed to parse a value for data type $dt with event ${e.toString}")
    }
  }

  /**
   * Parse an object as map.
   */
  private def convertMap(
      parser: XMLEventReader,
      valueType: DataType,
      options: XmlOptions,
      attributes: Array[Attribute]): Map[String, Any] = {
    val kvPairs = ArrayBuffer.empty[(String, Any)]
    attributes.foreach { attr =>
      kvPairs += (options.attributePrefix + attr.getName.getLocalPart -> attr.getValue)
    }
    var shouldStop = false
    while (!shouldStop) {
      parser.nextEvent match {
        case e: StartElement =>
          kvPairs +=
            (StaxXmlParserUtils.getName(e.asStartElement.getName, options) ->
             convertField(parser, valueType, options))
        case _: EndElement =>
          shouldStop = StaxXmlParserUtils.checkEndElement(parser)
        case _ => // do nothing
      }
    }
    kvPairs.toMap
  }

  /**
   * Convert XML attributes to a map with the given schema types.
   */
  private def convertAttributes(
      attributes: Array[Attribute],
      schema: StructType,
      options: XmlOptions): Map[String, Any] = {
    val convertedValuesMap = collection.mutable.Map.empty[String, Any]
    val valuesMap = StaxXmlParserUtils.convertAttributesToValuesMap(attributes, options)
    valuesMap.foreach { case (f, v) =>
      val nameToIndex = schema.map(_.name).zipWithIndex.toMap
      nameToIndex.get(f).foreach { i =>
        convertedValuesMap(f) = convertTo(v, schema(i).dataType, options)
      }
    }
    convertedValuesMap.toMap
  }

  /**
   * [[convertObject()]] calls this in order to convert the nested object to a row.
   * [[convertObject()]] contains some logic to find out which events are the start
   * and end of a nested row and this function converts the events to a row.
   */
  private def convertObjectWithAttributes(
      parser: XMLEventReader,
      schema: StructType,
      options: XmlOptions,
      attributes: Array[Attribute] = Array.empty): Row = {
    // TODO: This method might have to be removed. Some logics duplicate `convertObject()`
    val row = new Array[Any](schema.length)

    // Read attributes first.
    val attributesMap = convertAttributes(attributes, schema, options)

    // Then, we read elements here.
    val fieldsMap = convertField(parser, schema, options) match {
      case row: Row =>
        Map(schema.map(_.name).zip(row.toSeq): _*)
      case v if schema.fieldNames.contains(options.valueTag) =>
        // If this is the element having no children, then it wraps attributes
        // with a row So, we first need to find the field name that has the real
        // value and then push the value.
        val valuesMap = schema.fieldNames.map((_, null)).toMap
        valuesMap + (options.valueTag -> v)
      case _ => Map.empty
    }

    // Here we merge both to a row.
    val valuesMap = fieldsMap ++ attributesMap
    valuesMap.foreach { case (f, v) =>
      val nameToIndex = schema.map(_.name).zipWithIndex.toMap
      nameToIndex.get(f).foreach { row(_) = v }
    }

    if (valuesMap.isEmpty) {
      // Return an empty row with all nested elements by the schema set to null.
      Row.fromSeq(Seq.fill(schema.fieldNames.length)(null))
    } else {
      Row.fromSeq(row.toIndexedSeq)
    }
  }

  /**
   * Parse an object from the event stream into a new Row representing the schema.
   * Fields in the xml that are not defined in the requested schema will be dropped.
   */
  private def convertObject(
      parser: XMLEventReader,
      schema: StructType,
      options: XmlOptions,
      rootAttributes: Array[Attribute] = Array.empty): Row = {
    val row = new Array[Any](schema.length)
    val nameToIndex = schema.map(_.name).zipWithIndex.toMap
    // If there are attributes, then we process them first.
    convertAttributes(rootAttributes, schema, options).toSeq.foreach { case (f, v) =>
      nameToIndex.get(f).foreach { row(_) = v }
    }
    
    val wildcardColName = options.wildcardColName
    val hasWildcard = schema.exists(_.name == wildcardColName)
    
    var badRecordException: Option[Throwable] = None

    var shouldStop = false
    while (!shouldStop) {
      parser.nextEvent match {
        case e: StartElement => try {
          val attributes = e.getAttributes.asScala.map(_.asInstanceOf[Attribute]).toArray
          val field = StaxXmlParserUtils.getName(e.asStartElement.getName, options)

          nameToIndex.get(field) match {
            case Some(index) => schema(index).dataType match {
              case st: StructType =>
                row(index) = convertObjectWithAttributes(parser, st, options, attributes)

              case ArrayType(dt: DataType, _) =>
                val values = Option(row(index))
                  .map(_.asInstanceOf[ArrayBuffer[Any]])
                  .getOrElse(ArrayBuffer.empty[Any])
                val newValue = dt match {
                  case st: StructType =>
                    convertObjectWithAttributes(parser, st, options, attributes)
                  case dt: DataType =>
                    convertField(parser, dt, options)
                }
                row(index) = values :+ newValue

              case dt: DataType =>
                row(index) = convertField(parser, dt, options, attributes)
            }

            case None =>
              if (hasWildcard) {
                // Special case: there's an 'any' wildcard element that matches anything else
                // as a string (or array of strings, to parse multiple ones)
                val newValue = convertField(parser, StringType, options)
                val anyIndex = schema.fieldIndex(wildcardColName)
                schema(wildcardColName).dataType match {
                  case StringType =>
                    row(anyIndex) = newValue
                  case ArrayType(StringType, _) =>
                    val values = Option(row(anyIndex))
                      .map(_.asInstanceOf[ArrayBuffer[String]])
                      .getOrElse(ArrayBuffer.empty[String])
                    row(anyIndex) = values :+ newValue
                }
              } else {
                StaxXmlParserUtils.skipChildren(parser)
              }
          }
        } catch {
          case NonFatal(exception) if options.parseMode == PermissiveMode =>
            badRecordException = badRecordException.orElse(Some(exception))
        }

        case _: EndElement =>
          shouldStop = StaxXmlParserUtils.checkEndElement(parser)

        case _ => // do nothing
      }
    }

    if (badRecordException.isEmpty) {
      Row.fromSeq(row.toIndexedSeq)
    } else {
      throw PartialResultException(Row.fromSeq(row.toIndexedSeq), badRecordException.get)
    }
  }
}
