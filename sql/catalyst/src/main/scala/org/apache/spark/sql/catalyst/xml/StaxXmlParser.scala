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

import java.io.{CharConversionException, FileNotFoundException, InputStream, IOException, StringReader}
import java.nio.charset.MalformedInputException
import java.text.NumberFormat
import java.util
import java.util.Locale
import javax.xml.stream.{XMLEventReader, XMLStreamException}
import javax.xml.stream.events._
import javax.xml.transform.stream.StreamSource
import javax.xml.validation.Schema

import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._
import scala.util.Try
import scala.util.control.Exception.allCatch
import scala.util.control.NonFatal
import scala.xml.SAXException

import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.hadoop.hdfs.BlockMissingException
import org.apache.hadoop.security.AccessControlException

import org.apache.spark.{SparkIllegalArgumentException, SparkUpgradeException}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{ExprUtils, GenericInternalRow}
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, BadRecordException, DateFormatter, FailureSafeParser, GenericArrayData, MapData, PartialResultArrayException, PartialResultException, TimestampFormatter}
import org.apache.spark.sql.catalyst.util.LegacyDateFormats.FAST_DATE_FORMAT
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.types.variant.{Variant, VariantBuilder}
import org.apache.spark.types.variant.VariantBuilder.FieldEntry
import org.apache.spark.types.variant.VariantUtil
import org.apache.spark.unsafe.types.{UTF8String, VariantVal}

class StaxXmlParser(
    schema: StructType,
    val options: XmlOptions) extends Logging {

  private lazy val timestampFormatter = TimestampFormatter(
    options.timestampFormatInRead,
    options.zoneId,
    options.locale,
    legacyFormat = FAST_DATE_FORMAT,
    isParsing = true)

  private lazy val timestampNTZFormatter = TimestampFormatter(
    options.timestampNTZFormatInRead,
    options.zoneId,
    legacyFormat = FAST_DATE_FORMAT,
    isParsing = true,
    forTimestampNTZ = true)

  private lazy val dateFormatter = DateFormatter(
    options.dateFormatInRead,
    options.locale,
    legacyFormat = FAST_DATE_FORMAT,
    isParsing = true)

  private val decimalParser = ExprUtils.getDecimalParser(options.locale)

  private val caseSensitive = SQLConf.get.caseSensitiveAnalysis

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
      (input: String) => doParseColumn(input, xsdSchema)
    }
  }

  private def getFieldIndex(schema: StructType, fieldName: String): Option[Int] = {
    if (caseSensitive) {
      schema.getFieldIndex(fieldName)
    } else {
      schema.getFieldIndexCaseInsensitive(fieldName)
    }
  }

  /**
   * XML stream parser that reads XML records from the input file stream sequentially without
   * loading each individual XML record string into memory.
   */
  def parseStream(
      inputStream: InputStream,
      schema: StructType,
      streamLiteral: () => UTF8String): Iterator[InternalRow] = {
    val xsdSchema = Option(options.rowValidationXSDPath).map(ValidatorUtil.getSchema)
    val safeParser = new FailureSafeParser[XMLEventReader](
      input => doParseColumn(input, xsdSchema, streamLiteral),
      options.parseMode,
      schema,
      options.columnNameOfCorruptRecord
    )

    val xmlTokenizer = new XmlTokenizer(inputStream, options)
    StaxXmlParser.convertStream(xmlTokenizer) { tokens =>
      safeParser.parse(tokens)
    }.flatten
  }

  /**
   * Parse a single XML record string and return an InternalRow.
   */
  def doParseColumn(xml: String, xsdSchema: Option[Schema]): Option[InternalRow] = {
    val parser = StaxXmlParserUtils.filteredReader(xml)
    try {
      doParseColumn(parser, xsdSchema, () => UTF8String.fromString(xml))
    } finally {
      parser.close()
    }
  }

  /**
   * Parse the next XML record from the XML event stream.
   * Note that the method will **NOT** close the XML event stream as there could have more XML
   * records to parse. It's the caller's responsibility to close the stream.
   *
   * @param parser The XML event reader.
   * @param xmlLiteral A function that returns the entire XML file content as a UTF8String. Used
   *                   to create a BadRecordException in case of parsing errors.
   *                   TODO: Only include the file content starting with the current record.
   */
  def doParseColumn(
      parser: XMLEventReader,
      xsdSchema: Option[Schema] = None,
      xmlLiteral: () => UTF8String): Option[InternalRow] = {
    try {
      // TODO: support XSD validation
      xsdSchema.foreach { schema =>
        schema.newValidator().validate(new StreamSource(new StringReader(xmlLiteral().toString)))
      }
      options.singleVariantColumn match {
        case Some(_) =>
          // If the singleVariantColumn is specified, parse the entire xml record as a Variant
          val v = StaxXmlParser.parseVariant(parser, options)
          Some(InternalRow(v))
        case _ =>
          // Otherwise, parse the xml record as Structs
          val rootAttributes = StaxXmlParserUtils.gatherRootAttributes(parser)
          val result = Some(convertObject(parser, schema, rootAttributes))
          result
      }
    } catch {
      case e: SparkUpgradeException => throw e
      case e@(_: RuntimeException | _: XMLStreamException | _: MalformedInputException
              | _: SAXException) =>
        // Skip rest of the content in the parser and put the whole XML file in the
        // BadRecordException.
        parser.close()
        // XML parser currently doesn't support partial results for corrupted records.
        // For such records, all fields other than the field configured by
        // `columnNameOfCorruptRecord` are set to `null`.
        throw BadRecordException(xmlLiteral, () => Array.empty, e)
      case e: CharConversionException if options.charset.isEmpty =>
        val msg =
          """XML parser cannot handle a character in its input.
            |Specifying encoding as an input option explicitly might help to resolve the issue.
            |""".stripMargin + e.getMessage
        val wrappedCharException = new CharConversionException(msg)
        wrappedCharException.initCause(e)
        throw BadRecordException(xmlLiteral, () => Array.empty,
          wrappedCharException)
      case PartialResultException(row, cause) =>
        throw BadRecordException(
          record = xmlLiteral,
          partialResults = () => Array(row),
          cause)
      case PartialResultArrayException(rows, cause) =>
        throw BadRecordException(
          record = xmlLiteral,
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
      startElementName: String,
      attributes: Array[Attribute] = Array.empty): Any = {

    def convertComplicatedType(
        dt: DataType,
        startElementName: String,
        attributes: Array[Attribute]): Any = dt match {
      case st: StructType => convertObject(parser, st)
      case MapType(StringType, vt, _) => convertMap(parser, vt, attributes)
      case ArrayType(st, _) => convertField(parser, st, startElementName)
      case VariantType =>
        StaxXmlParser.convertVariant(parser, attributes, options)
      case _: StringType =>
        convertTo(
          StaxXmlParserUtils.currentStructureAsString(
            parser, startElementName, options),
          StringType)
    }

    (parser.peek, dataType) match {
      case (_: StartElement, dt: DataType) =>
        convertComplicatedType(dt, startElementName, attributes)
      case (_: EndElement, _: StringType) =>
        StaxXmlParserUtils.skipNextEndElement(parser, startElementName, options)
        // Empty. It's null if "" is the null value
        if (options.nullValue == "") {
          null
        } else {
          UTF8String.fromString("")
        }
      case (_: EndElement, _: DataType) =>
        StaxXmlParserUtils.skipNextEndElement(parser, startElementName, options)
        null
      case (c: Characters, ArrayType(st, _)) =>
        // For `ArrayType`, it needs to return the type of element. The values are merged later.
        parser.next
        val value = convertTo(c.getData, st)
        StaxXmlParserUtils.skipNextEndElement(parser, startElementName, options)
        value
      case (_: Characters, st: StructType) =>
        convertObject(parser, st)
      case (_: Characters, VariantType) =>
        StaxXmlParser.convertVariant(parser, Array.empty, options)
      case (_: Characters, _: StringType) =>
        convertTo(
          StaxXmlParserUtils.currentStructureAsString(
            parser, startElementName, options),
          StringType)
      case (c: Characters, _: DataType) if c.isWhiteSpace =>
        // When `Characters` is found, we need to look further to decide
        // if this is really data or space between other elements.
        parser.next
        convertField(parser, dataType, startElementName, attributes)
      case (c: Characters, dt: DataType) =>
        val value = convertTo(c.getData, dt)
        parser.next
        StaxXmlParserUtils.skipNextEndElement(parser, startElementName, options)
        value
      case (e: XMLEvent, dt: DataType) =>
        throw new SparkIllegalArgumentException(
          errorClass = "_LEGACY_ERROR_TEMP_3240",
          messageParameters = Map(
            "dt" -> dt.toString,
            "e" -> e.toString))
    }
  }

  /**
   * Parse an object as map.
   */
  private def convertMap(
      parser: XMLEventReader,
      valueType: DataType,
      attributes: Array[Attribute]): MapData = {
    val kvPairs = ArrayBuffer.empty[(UTF8String, Any)]
    attributes.foreach { attr =>
      kvPairs += (UTF8String.fromString(options.attributePrefix + attr.getName.getLocalPart)
        -> convertTo(attr.getValue, valueType))
    }
    var shouldStop = false
    while (!shouldStop) {
      parser.nextEvent match {
        case e: StartElement =>
          val key = StaxXmlParserUtils.getName(e.asStartElement.getName, options)
          kvPairs +=
          (UTF8String.fromString(key) -> convertField(parser, valueType, key))
        case c: Characters if !c.isWhiteSpace =>
          // Create a value tag field for it
          kvPairs +=
          // TODO: We don't support an array value tags in map yet.
          (UTF8String.fromString(options.valueTag) -> convertTo(c.getData, valueType))
        case _: EndElement | _: EndDocument =>
          shouldStop = true
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
      schema: StructType): Map[String, Any] = {
    val convertedValuesMap = collection.mutable.Map.empty[String, Any]
    val valuesMap = StaxXmlParserUtils.convertAttributesToValuesMap(attributes, options)
    valuesMap.foreach { case (f, v) =>
      val indexOpt = getFieldIndex(schema, f)
      indexOpt.foreach { i =>
        convertedValuesMap(f) = convertTo(v, schema(i).dataType)
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
      startElementName: String,
      attributes: Array[Attribute] = Array.empty): InternalRow = {
    // TODO: This method might have to be removed. Some logics duplicate `convertObject()`
    val row = new Array[Any](schema.length)

    // Read attributes first.
    val attributesMap = convertAttributes(attributes, schema)

    // Then, we read elements here.
    val fieldsMap = convertField(parser, schema, startElementName) match {
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
      val indexOpt = getFieldIndex(schema, f)
      indexOpt.foreach { row(_) = v }
    }

    if (valuesMap.isEmpty) {
      // Return an empty row with all nested elements by the schema set to null.
      new GenericInternalRow(Array.fill[Any](schema.length)(null))
    } else {
      new GenericInternalRow(row)
    }
  }

  /**
   * Parse an object from the event stream into a new InternalRow representing the schema.
   * Fields in the xml that are not defined in the requested schema will be dropped.
   */
  private def convertObject(
      parser: XMLEventReader,
      schema: StructType,
      rootAttributes: Array[Attribute] = Array.empty): InternalRow = {
    val row = new Array[Any](schema.length)
    // If there are attributes, then we process them first.
    convertAttributes(rootAttributes, schema).toSeq.foreach {
      case (f, v) =>
        getFieldIndex(schema, f).foreach { row(_) = v }
    }

    val wildcardColName = options.wildcardColName
    val hasWildcard = schema.exists(_.name == wildcardColName)

    var badRecordException: Option[Throwable] = None

    var shouldStop = false
    while (!shouldStop) {
      parser.nextEvent match {
        case e: StartElement => try {
          val attributes = e.getAttributes.asScala.toArray
          val field = StaxXmlParserUtils.getName(e.asStartElement.getName, options)

          getFieldIndex(schema, field) match {
            case Some(index) => schema(index).dataType match {
              case st: StructType =>
                row(index) = convertObjectWithAttributes(parser, st, field, attributes)

              case ArrayType(dt: DataType, _) =>
                val values = Option(row(index))
                  .map(_.asInstanceOf[ArrayBuffer[Any]])
                  .getOrElse(ArrayBuffer.empty[Any])
                val newValue = dt match {
                  case st: StructType =>
                    convertObjectWithAttributes(parser, st, field, attributes)
                  case VariantType =>
                    StaxXmlParser.convertVariant(parser, attributes, options)
                  case dt: DataType =>
                    convertField(parser, dt, field)
                }
                row(index) = values :+ newValue

              case VariantType =>
                row(index) = StaxXmlParser.convertVariant(parser, attributes, options)

              case dt: DataType =>
                row(index) = convertField(parser, dt, field, attributes)
            }

            case None =>
              if (hasWildcard) {
                // Special case: there's an 'any' wildcard element that matches anything else
                // as a string (or array of strings, to parse multiple ones)
                val newValue = convertField(parser, StringType, field)
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
                StaxXmlParserUtils.skipChildren(parser, field, options)
              }
          }
        } catch {
          case e: SparkUpgradeException => throw e
          case NonFatal(e) =>
            // TODO: we don't support partial results now
            badRecordException = badRecordException.orElse(Some(e))
        }

        case c: Characters if !c.isWhiteSpace =>
          addOrUpdate(row, schema, options.valueTag, c.getData)

        case _: EndElement | _: EndDocument =>
          shouldStop = true

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
      new GenericInternalRow(newRow)
    } else {
      throw PartialResultException(new GenericInternalRow(newRow),
        badRecordException.get)
    }
  }

  /**
   * Casts given string datum to specified type.
   *
   * For string types, this is simply the datum.
   * For other nullable types, returns null if it is null or equals to the value specified
   * in `nullValue` option.
   *
   * @param datum    string value
   * @param castType SparkSQL type
   */
  private def castTo(
      datum: String,
      castType: DataType): Any = {
    if (datum == options.nullValue || datum == null) {
      null
    } else {
      castType match {
        case _: ByteType => datum.toByte
        case _: ShortType => datum.toShort
        case _: IntegerType => datum.toInt
        case _: LongType => datum.toLong
        case _: FloatType => Try(datum.toFloat)
          .getOrElse(NumberFormat.getInstance(Locale.getDefault).parse(datum).floatValue())
        case _: DoubleType => Try(datum.toDouble)
          .getOrElse(NumberFormat.getInstance(Locale.getDefault).parse(datum).doubleValue())
        case _: BooleanType => parseXmlBoolean(datum)
        case dt: DecimalType =>
          Decimal(decimalParser(datum), dt.precision, dt.scale)
        case _: TimestampType => parseXmlTimestamp(datum, options)
        case _: TimestampNTZType => timestampNTZFormatter.parseWithoutTimeZone(datum, false)
        case _: DateType => parseXmlDate(datum, options)
        case _: StringType => UTF8String.fromString(datum)
        case _ => throw new SparkIllegalArgumentException(
          errorClass = "_LEGACY_ERROR_TEMP_3244",
          messageParameters = Map("castType" -> "castType.typeName"))
      }
    }
  }

  private def parseXmlBoolean(s: String): Boolean = {
    s.toLowerCase(Locale.ROOT) match {
      case "true" | "1" => true
      case "false" | "0" => false
      case _ => throw new SparkIllegalArgumentException(
        errorClass = "_LEGACY_ERROR_TEMP_3245",
        messageParameters = Map("s" -> s))
    }
  }

  private def parseXmlDate(value: String, options: XmlOptions): Int = {
    dateFormatter.parse(value)
  }

  private def parseXmlTimestamp(value: String, options: XmlOptions): Long = {
    timestampFormatter.parse(value)
  }

  // TODO: This function unnecessarily does type dispatch. Should merge it with `castTo`.
  private def convertTo(
      datum: String,
      dataType: DataType): Any = {
    val value = if (datum != null && options.ignoreSurroundingSpaces) {
      datum.trim()
    } else {
      datum
    }
    if (value == options.nullValue || value == null) {
      null
    } else {
      dataType match {
        case NullType => castTo(value, StringType)
        case LongType => signSafeToLong(value)
        case DoubleType => signSafeToDouble(value)
        case BooleanType => castTo(value, BooleanType)
        case StringType => castTo(value, StringType)
        case DateType => castTo(value, DateType)
        case TimestampType => castTo(value, TimestampType)
        case TimestampNTZType => castTo(value, TimestampNTZType)
        case FloatType => signSafeToFloat(value)
        case ByteType => castTo(value, ByteType)
        case ShortType => castTo(value, ShortType)
        case IntegerType => signSafeToInt(value)
        case dt: DecimalType => castTo(value, dt)
        case VariantType =>
          val builder = new VariantBuilder(false)
          StaxXmlParser.appendXMLCharacterToVariant(builder, value, options)
          val v = builder.result()
          new VariantVal(v.getValue, v.getMetadata)
        case _ => throw new SparkIllegalArgumentException(
          errorClass = "_LEGACY_ERROR_TEMP_3246",
          messageParameters = Map("dataType" -> dataType.toString))
      }
    }
  }


  private def signSafeToLong(value: String): Long = {
    if (value.startsWith("+")) {
      val data = value.substring(1)
      castTo(data, LongType).asInstanceOf[Long]
    } else if (value.startsWith("-")) {
      val data = value.substring(1)
      -castTo(data, LongType).asInstanceOf[Long]
    } else {
      val data = value
      castTo(data, LongType).asInstanceOf[Long]
    }
  }

  private def signSafeToDouble(value: String): Double = {
    if (value.startsWith("+")) {
      val data = value.substring(1)
      castTo(data, DoubleType).asInstanceOf[Double]
    } else if (value.startsWith("-")) {
      val data = value.substring(1)
      -castTo(data, DoubleType).asInstanceOf[Double]
    } else {
      val data = value
      castTo(data, DoubleType).asInstanceOf[Double]
    }
  }

  private def signSafeToInt(value: String): Int = {
    if (value.startsWith("+")) {
      val data = value.substring(1)
      castTo(data, IntegerType).asInstanceOf[Int]
    } else if (value.startsWith("-")) {
      val data = value.substring(1)
      -castTo(data, IntegerType).asInstanceOf[Int]
    } else {
      val data = value
      castTo(data, IntegerType).asInstanceOf[Int]
    }
  }

  private def signSafeToFloat(value: String): Float = {
    if (value.startsWith("+")) {
      val data = value.substring(1)
      castTo(data, FloatType).asInstanceOf[Float]
    } else if (value.startsWith("-")) {
      val data = value.substring(1)
      -castTo(data, FloatType).asInstanceOf[Float]
    } else {
      val data = value
      castTo(data, FloatType).asInstanceOf[Float]
    }
  }

  private def addOrUpdate(
      row: Array[Any],
      schema: StructType,
      name: String,
      data: String,
      addToTail: Boolean = true): InternalRow = {
    schema.getFieldIndex(name) match {
      case Some(index) =>
        schema(index).dataType match {
          case ArrayType(elementType, _) =>
            val value = convertTo(data, elementType)
            val values = Option(row(index))
              .map(_.asInstanceOf[ArrayBuffer[Any]])
              .getOrElse(ArrayBuffer.empty[Any])
            row(index) = if (addToTail) {
                values :+ value
              } else {
                value +: values
              }
          case dataType =>
            row(index) = convertTo(data, dataType)
        }
      case None => // do nothing
    }
    new GenericInternalRow(row)
  }
}

/**
 * XML tokenizer that never buffers complete XML records in memory. It uses XMLEventReader to parse
 * XML file stream directly and can move to the next XML record based on the rowTag option.
 */
class XmlTokenizer(inputStream: InputStream, options: XmlOptions) extends Logging {
  private var reader = StaxXmlParserUtils.filteredReader(inputStream, options)

  /**
   * Returns the next XML record as a positioned XMLEventReader.
   * This avoids creating intermediate string representations.
   */
  def next(): Option[XMLEventReader] = {
    var nextRecord: Option[XMLEventReader] = None
    try {
      // Skip to the next row start element
      if (skipToNextRowStart()) {
        nextRecord = Some(reader)
      }
    } catch {
      case e: FileNotFoundException if options.ignoreMissingFiles =>
        logWarning("Skipping the rest of the content in the missing file", e)
      case NonFatal(e) =>
        ExceptionUtils.getRootCause(e) match {
          case _: AccessControlException | _: BlockMissingException =>
            close()
            throw e
          case _: RuntimeException | _: IOException if options.ignoreCorruptFiles =>
            logWarning("Skipping the rest of the content in the corrupted file", e)
          case _: XMLStreamException =>
            logWarning("Skipping the rest of the content in the corrupted file", e)
          case e: Throwable =>
            close()
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
      reader.close()
      inputStream.close()
      reader = null
    }
  }

  /**
   * Skip through the XML stream until we find the next row start element.
   */
  private def skipToNextRowStart(): Boolean = {
    val rowTagName = options.rowTag
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
  }
}

object StaxXmlParser {
  /**
   * Parses a stream that contains CSV strings and turns it into an iterator of tokens.
   */
  def tokenizeStream(inputStream: InputStream, options: XmlOptions): Iterator[XMLEventReader] = {
    val xmlTokenizer = new XmlTokenizer(inputStream, options)
    convertStream(xmlTokenizer)(tokens => tokens)
  }

  def convertStream[T](
    xmlTokenizer: XmlTokenizer)(
    convert: XMLEventReader => T): Iterator[T] = new Iterator[T] {

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

  /**
   * Parse the input XML string as a Variant value
   */
  def parseVariant(xml: String, options: XmlOptions): VariantVal = {
    val parser = StaxXmlParserUtils.filteredReader(xml)
    try {
      parseVariant(parser, options)
    } finally {
      parser.close()
    }
  }

  def parseVariant(parser: XMLEventReader, options: XmlOptions): VariantVal = {
    val rootAttributes = StaxXmlParserUtils.gatherRootAttributes(parser)
    val v = convertVariant(parser, rootAttributes, options)
    new VariantVal(v.getValue, v.getMetadata)
  }

  /**
   * Parse an XML element from the XML event stream into a Variant.
   * This method transforms the XML element along with its attributes and child elements
   * into a hierarchical Variant data structure that preserves the XML structure.
   *
   * @param parser The XML event stream reader positioned after the start element
   * @param attributes The attributes of the current XML element to be included in the Variant
   * @param options Configuration options that control how XML is parsed into Variants
   * @return A Variant representing the XML element with its attributes and child content
   */
  def convertVariant(
      parser: XMLEventReader,
      attributes: Array[Attribute],
      options: XmlOptions): VariantVal = {
    val v = convertVariantInternal(parser, attributes, options)
    new VariantVal(v.getValue, v.getMetadata)
  }

  private def convertVariantInternal(
      parser: XMLEventReader,
      attributes: Array[Attribute],
      options: XmlOptions): Variant = {
    // The variant builder for the root startElement
    val rootBuilder = new VariantBuilder(false)
    val start = rootBuilder.getWritePos

    // Map to store the variant values of all child fields
    // Each field could have multiple entries, which means it's an array
    // The map is sorted by field name, and the ordering is based on the case sensitivity
    val caseSensitivityOrdering: Ordering[String] = if (SQLConf.get.caseSensitiveAnalysis) {
      (x: String, y: String) => x.compareTo(y)
    } else {
      (x: String, y: String) => x.compareToIgnoreCase(y)
    }
    val fieldToVariants = collection.mutable.TreeMap.empty[String, java.util.ArrayList[Variant]](
      caseSensitivityOrdering
    )

    // Handle attributes first
    StaxXmlParserUtils.convertAttributesToValuesMap(attributes, options).foreach {
      case (f, v) =>
        val builder = new VariantBuilder(false)
        appendXMLCharacterToVariant(builder, v, options)
        val variants = fieldToVariants.getOrElseUpdate(f, new java.util.ArrayList[Variant]())
        variants.add(builder.result())
    }

    var shouldStop = false
    while (!shouldStop) {
      parser.nextEvent() match {
        case s: StartElement =>
          // For each child element, convert it to a variant and keep track of it in
          // fieldsToVariants
          val attributes = s.getAttributes.asScala.map(_.asInstanceOf[Attribute]).toArray
          val field = StaxXmlParserUtils.getName(s.asStartElement.getName, options)
          val variants = fieldToVariants.getOrElseUpdate(field, new java.util.ArrayList[Variant]())
          variants.add(convertVariantInternal(parser, attributes, options))

        case c: Characters if !c.isWhiteSpace =>
          // Treat the character as a value tag field, where we use the [[XMLOptions.valueTag]] as
          // the field key
          val builder = new VariantBuilder(false)
          appendXMLCharacterToVariant(builder, c.getData, options)
          val variants = fieldToVariants.getOrElseUpdate(
            options.valueTag,
            new java.util.ArrayList[Variant]()
          )
          variants.add(builder.result())

        case _: EndElement =>
          if (fieldToVariants.nonEmpty) {
            val onlyValueTagField = fieldToVariants.keySet.forall(_ == options.valueTag)
            if (onlyValueTagField) {
              // If the element only has value tag field, parse the element as a variant primitive
              rootBuilder.appendVariant(fieldToVariants(options.valueTag).get(0))
            } else {
              writeVariantObject(rootBuilder, fieldToVariants)
            }
          }
          shouldStop = true

        case _: EndDocument => shouldStop = true

        case _ => // do nothing
      }
    }

    // If the element is empty, we treat it as a Variant null
    if (rootBuilder.getWritePos == start) {
      rootBuilder.appendNull()
    }

    rootBuilder.result()
  }

  /**
   * Write a variant object to the variant builder.
   *
   * @param builder The variant builder to write to
   * @param fieldToVariants A map of field names to their corresponding variant values of the object
   */
  private def writeVariantObject(
      builder: VariantBuilder,
      fieldToVariants: collection.mutable.TreeMap[String, java.util.ArrayList[Variant]]): Unit = {
    val start = builder.getWritePos
    val objectFieldEntries = new java.util.ArrayList[FieldEntry]()

    val (lastFieldKey, lastFieldValue) =
      fieldToVariants.tail.foldLeft(fieldToVariants.head._1, fieldToVariants.head._2) {
        case ((key, variantVals), (k, v)) =>
          if (!SQLConf.get.caseSensitiveAnalysis && k.equalsIgnoreCase(key)) {
            variantVals.addAll(v)
            (key, variantVals)
          } else {
            writeVariantObjectField(key, variantVals, builder, start, objectFieldEntries)
            (k, v)
          }
      }

    writeVariantObjectField(lastFieldKey, lastFieldValue, builder, start, objectFieldEntries)

    // Finish writing the variant object
    builder.finishWritingObject(start, objectFieldEntries)
  }

  /**
   * Write a single field to a variant object
   *
   * @param fieldName the name of the object field
   * @param fieldVariants the variant value of the field. A field could have multiple variant value,
   *                      which means it's an array field
   * @param builder the variant builder
   * @param objectStart the start position of the variant object in the builder
   * @param objectFieldEntries a list tracking all fields of the variant object
   */
  private def writeVariantObjectField(
      fieldName: String,
      fieldVariants: java.util.ArrayList[Variant],
      builder: VariantBuilder,
      objectStart: Int,
      objectFieldEntries: java.util.ArrayList[FieldEntry]): Unit = {
    val start = builder.getWritePos
    val fieldId = builder.addKey(fieldName)
    objectFieldEntries.add(
      new FieldEntry(fieldName, fieldId, builder.getWritePos - objectStart)
    )

    val fieldValue = if (fieldVariants.size() > 1) {
      // If the field has more than one entry, it's an array field. Build a Variant
      // array as the field value
      val arrayBuilder = new VariantBuilder(false)
      val arrayStart = arrayBuilder.getWritePos
      val offsets = new util.ArrayList[Integer]()
      fieldVariants.asScala.foreach { v =>
        offsets.add(arrayBuilder.getWritePos - arrayStart)
        arrayBuilder.appendVariant(v)
      }
      arrayBuilder.finishWritingArray(arrayStart, offsets)
      arrayBuilder.result()
    } else {
      // Otherwise, just use the first variant as the field value
      fieldVariants.get(0)
    }

    // Append the field value to the variant builder
    builder.appendVariant(fieldValue)
  }

  /**
   * Convert an XML Character value `s` into a variant value and append the result to `builder`.
   * The result can only be one of a variant boolean/long/decimal/string. Anything other than
   * the supported types will be appended to the Variant builder as a string.
   *
   * Floating point types (double, float) are not considered to avoid precision loss.
   */
  private def appendXMLCharacterToVariant(
      builder: VariantBuilder,
      s: String,
      options: XmlOptions): Unit = {
    if (s == null || s == options.nullValue) {
      builder.appendNull()
      return
    }

    val value = if (options.ignoreSurroundingSpaces) s.trim() else s

    // Exit early for empty strings
    if (value.isEmpty) {
      builder.appendString(value)
      return
    }

    // Try parsing the value as boolean first
    if (value.toLowerCase(Locale.ROOT) == "true") {
      builder.appendBoolean(true)
      return
    }
    if (value.toLowerCase(Locale.ROOT) == "false") {
      builder.appendBoolean(false)
      return
    }

    // Try parsing the value as a long
    allCatch opt value.toLong match {
      case Some(l) =>
        builder.appendLong(l)
        return
      case _ =>
    }

    // Try parsing the value as decimal
    val decimalParser = ExprUtils.getDecimalParser(options.locale)
    allCatch opt decimalParser(value) match {
      case Some(decimalValue) =>
        var d = decimalValue
        if (d.scale() < 0) {
          d = d.setScale(0)
        }
        if (d.scale <= VariantUtil.MAX_DECIMAL16_PRECISION &&
            d.precision <= VariantUtil.MAX_DECIMAL16_PRECISION) {
          builder.appendDecimal(d)
          return
        }
      case _ =>
    }

    // If the character is of other primitive types, parse it as a string
    builder.appendString(value)
  }
}
