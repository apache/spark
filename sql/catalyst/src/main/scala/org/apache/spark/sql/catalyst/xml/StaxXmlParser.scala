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

import java.io.{CharConversionException, InputStream, InputStreamReader, StringReader}
import java.nio.charset.{Charset, MalformedInputException}
import javax.xml.stream.{XMLEventReader, XMLStreamException}
import javax.xml.stream.events._
import javax.xml.transform.stream.StreamSource
import javax.xml.validation.Schema

import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal
import scala.xml.SAXException

import org.apache.spark.SparkUpgradeException
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, BadRecordException, DropMalformedMode, FailureSafeParser, GenericArrayData, MapData, ParseMode, PartialResultArrayException, PartialResultException, PermissiveMode}
import org.apache.spark.sql.catalyst.xml.StaxXmlParser.convertStream
import org.apache.spark.sql.catalyst.xml.TypeCast._
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

class StaxXmlParser(
    schema: StructType,
    val options: XmlOptions,
    filters: Seq[Filter] = Seq.empty) extends Logging {

  private val factory = options.buildXmlFactory()

  /**
   * Parses a single XML string and turns it into either one resulting row or no row (if the
   * the record is malformed).
   */
  val parse: String => Option[InternalRow] = {
    // This is intentionally a val to create a function once and reuse.
    if (schema.isEmpty) {
      (_: String) => Some(InternalRow.empty)
    } else {
      val xsdSchema = Option(options.rowValidationXSDPath).map(ValidatorUtil.getSchema)
      (input: String) => doParseColumn(input, options.parseMode, xsdSchema)
    }
  }

  def parseStream(
      inputStream: InputStream,
      schema: StructType): Iterator[InternalRow] = {
    val xsdSchema = Option(options.rowValidationXSDPath).map(ValidatorUtil.getSchema)
    val safeParser = new FailureSafeParser[String](
      input => doParseColumn(input, options.parseMode, xsdSchema),
      options.parseMode,
      schema,
      options.columnNameOfCorruptRecord)

    val xmlTokenizer = new XmlTokenizer(inputStream, options)
    convertStream(xmlTokenizer) { tokens =>
      safeParser.parse(tokens)
    }.flatten
  }

  def parseColumn(xml: String, schema: StructType): InternalRow = {
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
    doParseColumn(xml, parseMode, xsdSchema).orNull
  }

  def doParseColumn(xml: String,
      parseMode: ParseMode,
      xsdSchema: Option[Schema]): Option[InternalRow] = {
    val xmlRecord = UTF8String.fromString(xml)
    try {
      xsdSchema.foreach { schema =>
        schema.newValidator().validate(new StreamSource(new StringReader(xml)))
      }
      val parser = StaxXmlParserUtils.filteredReader(xml)
      val rootAttributes = StaxXmlParserUtils.gatherRootAttributes(parser)
      Some(convertObject(parser, schema, options, rootAttributes))
    } catch {
      case e: SparkUpgradeException => throw e
      case e@(_: RuntimeException | _: XMLStreamException | _: MalformedInputException
              | _: SAXException) =>
        // XML parser currently doesn't support partial results for corrupted records.
        // For such records, all fields other than the field configured by
        // `columnNameOfCorruptRecord` are set to `null`.
        throw BadRecordException(() => xmlRecord, () => Array.empty, e)
      case e: CharConversionException if options.charset.isEmpty =>
        val msg =
          """JSON parser cannot handle a character in its input.
            |Specifying encoding as an input option explicitly might help to resolve the issue.
            |""".stripMargin + e.getMessage
        val wrappedCharException = new CharConversionException(msg)
        wrappedCharException.initCause(e)
        throw BadRecordException(() => xmlRecord, () => Array.empty,
          wrappedCharException)
      case PartialResultException(row, cause) =>
        throw BadRecordException(
          record = () => xmlRecord,
          partialResults = () => Array(row),
          cause)
      case PartialResultArrayException(rows, cause) =>
        throw BadRecordException(
          record = () => xmlRecord,
          partialResults = () => rows,
          cause)
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
          UTF8String.fromString("")
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
      attributes: Array[Attribute]): MapData = {
    val kvPairs = ArrayBuffer.empty[(UTF8String, Any)]
    attributes.foreach { attr =>
      kvPairs += (UTF8String.fromString(options.attributePrefix + attr.getName.getLocalPart)
        -> convertTo(attr.getValue, valueType, options))
    }
    var shouldStop = false
    while (!shouldStop) {
      parser.nextEvent match {
        case e: StartElement =>
          kvPairs +=
            (UTF8String.fromString(StaxXmlParserUtils.getName(e.asStartElement.getName, options)) ->
             convertField(parser, valueType, options))
        case _: EndElement =>
          shouldStop = StaxXmlParserUtils.checkEndElement(parser)
        case _ => // do nothing
      }
    }
    ArrayBasedMapData(kvPairs.toMap)
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
      attributes: Array[Attribute] = Array.empty): InternalRow = {
    // TODO: This method might have to be removed. Some logics duplicate `convertObject()`
    val row = new Array[Any](schema.length)

    // Read attributes first.
    val attributesMap = convertAttributes(attributes, schema, options)

    // Then, we read elements here.
    val fieldsMap = convertField(parser, schema, options) match {
      case internalRow: InternalRow =>
        Map(schema.map(_.name).zip(internalRow.toSeq(schema)): _*)
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
      InternalRow.fromSeq(Seq.fill(schema.fieldNames.length)(null))
    } else {
      InternalRow.fromSeq(row.toIndexedSeq)
    }
  }

  /**
   * Parse an object from the event stream into a new InternalRow representing the schema.
   * Fields in the xml that are not defined in the requested schema will be dropped.
   */
  private def convertObject(
      parser: XMLEventReader,
      schema: StructType,
      options: XmlOptions,
      rootAttributes: Array[Attribute] = Array.empty): InternalRow = {
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
          case e: SparkUpgradeException => throw e
          case NonFatal(e) =>
            badRecordException = badRecordException.orElse(Some(e))
        }

        case _: EndElement =>
          shouldStop = StaxXmlParserUtils.checkEndElement(parser)

        case _ => // do nothing
      }
    }

    // TODO: find a more efficient way to convert ArrayBuffer to GenericArrayData
    val newRow = new Array[Any](schema.length)
    var i = 0
    while (i < schema.length) {
      if (row(i).isInstanceOf[ArrayBuffer[_]]) {
        newRow(i) = new GenericArrayData(row(i).asInstanceOf[ArrayBuffer[Any]])
      } else {
        newRow(i) = row(i)
      }
      i += 1;
    }

    if (badRecordException.isEmpty) {
      InternalRow.fromSeq(newRow.toIndexedSeq)
    } else {
      throw PartialResultException(InternalRow.fromSeq(newRow.toIndexedSeq),
        badRecordException.get)
    }
  }
}

/**
 * XMLRecordReader class to read through a given xml document to output xml blocks as records
 * as specified by the start tag and end tag.
 *
 * This implementation is ultimately loosely based on LineRecordReader in Hadoop.
 */
private[xml] class XmlTokenizer(
  inputStream: InputStream,
  options: XmlOptions) {
  private val reader = new InputStreamReader(inputStream, Charset.forName(options.charset))
  private var currentStartTag: String = _
  private var buffer = new StringBuilder()
  private val startTag = s"<${options.rowTag}>"
  private val endTag = s"</${options.rowTag}>"

    /**
   * Finds the start of the next record.
   * It treats data from `startTag` and `endTag` as a record.
   *
   * @param key the current key that will be written
   * @param value  the object that will be written
   * @return whether it reads successfully
   */
  def next(): Option[String] = {
    if (readUntilStartElement()) {
      try {
        buffer.append(currentStartTag)
        // Don't check whether the end element was found. Even if not, return everything
        // that was read, which will invariably cause a parse error later
        readUntilEndElement(currentStartTag.endsWith(">"))
        return Some(buffer.toString())
      } finally {
        buffer = new StringBuilder()
      }
    }
    None
  }

  private def readUntilStartElement(): Boolean = {
    currentStartTag = startTag
    var i = 0
    while (true) {
      val cOrEOF = reader.read()
      if (cOrEOF == -1) { // || (i == 0 && getFilePosition() > end)) {
        // End of file or end of split.
        return false
      }
      val c = cOrEOF.toChar
      if (c == startTag(i)) {
        if (i >= startTag.length - 1) {
          // Found start tag.
          return true
        }
        // else in start tag
        i += 1
      } else {
        // if doesn't match the closing angle bracket, check if followed by attributes
        if (i == (startTag.length - 1) && Character.isWhitespace(c)) {
          // Found start tag with attributes. Remember to write with following whitespace
          // char, not angle bracket
          currentStartTag = startTag.dropRight(1) + c
          return true
        }
        // else not in start tag
        i = 0
      }
    }
    // Unreachable.
    false
  }

  private def readUntilEndElement(startTagClosed: Boolean): Boolean = {
    // Index into the start or end tag that has matched so far
    var si = 0
    var ei = 0
    // How many other start tags enclose the one that's started already?
    var depth = 0
    // Previously read character
    var prevC = '\u0000'

    // The current start tag already found may or may not have terminated with
    // a '>' as it may have attributes we read here. If not, we search for
    // a self-close tag, but only until a non-self-closing end to the start
    // tag is found
    var canSelfClose = !startTagClosed

    while (true) {

      val cOrEOF = reader.read()
      if (cOrEOF == -1) {
        // End of file (ignore end of split).
        return false
      }

      val c = cOrEOF.toChar
      buffer.append(c)

      if (c == '>' && prevC != '/') {
        canSelfClose = false
      }

      // Still matching a start tag?
      if (c == startTag(si)) {
        // Still also matching an end tag?
        if (c == endTag(ei)) {
          // In start tag or end tag.
          si += 1
          ei += 1
        } else {
          if (si >= startTag.length - 1) {
            // Found start tag.
            si = 0
            ei = 0
            depth += 1
          } else {
            // In start tag.
            si += 1
            ei = 0
          }
        }
      } else if (c == endTag(ei)) {
        if (ei >= endTag.length - 1) {
          if (depth == 0) {
            // Found closing end tag.
            return true
          }
          // else found nested end tag.
          si = 0
          ei = 0
          depth -= 1
        } else {
          // In end tag.
          si = 0
          ei += 1
        }
      } else if (c == '>' && prevC == '/' && canSelfClose) {
        if (depth == 0) {
          // found a self-closing tag (end tag)
          return true
        }
        // else found self-closing nested tag (end tag)
        si = 0
        ei = 0
        depth -= 1
      } else if (si == (startTag.length - 1) && Character.isWhitespace(c)) {
        // found a start tag with attributes
        si = 0
        ei = 0
        depth += 1
      } else {
        // Not in start tag or end tag.
        si = 0
        ei = 0
      }
      prevC = c
    }
    // Unreachable.
    false
  }
}

private[sql] object StaxXmlParser {
  /**
   * Parses a stream that contains CSV strings and turns it into an iterator of tokens.
   */
  def tokenizeStream(inputStream: InputStream, options: XmlOptions): Iterator[String] = {
    val xmlTokenizer = new XmlTokenizer(inputStream, options)
    convertStream(xmlTokenizer)(tokens => tokens)
  }

  private def convertStream[T](
    xmlTokenizer: XmlTokenizer)(
    convert: String => T) = new Iterator[T] {

    private var nextRecord = xmlTokenizer.next()

    override def hasNext: Boolean = nextRecord.nonEmpty

    override def next(): T = {
      if (!hasNext) {
        throw QueryExecutionErrors.endOfStreamError()
      }
      val curRecord = convert(nextRecord.get)
      nextRecord = xmlTokenizer.next()
      curRecord
    }
  }
}
