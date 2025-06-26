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

import java.io.{BufferedReader, CharConversionException, FileNotFoundException, InputStream, InputStreamReader, IOException, StringReader}
import java.nio.charset.{Charset, MalformedInputException}
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
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, BadRecordException, DateFormatter, DropMalformedMode, FailureSafeParser, GenericArrayData, MapData, ParseMode, PartialResultArrayException, PartialResultException, PermissiveMode, TimestampFormatter}
import org.apache.spark.sql.catalyst.util.LegacyDateFormats.FAST_DATE_FORMAT
import org.apache.spark.sql.catalyst.xml.StaxXmlParser.convertStream
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
      (input: String) => doParseColumn(input, options.parseMode, xsdSchema)
    }
  }

  private def getFieldIndex(schema: StructType, fieldName: String): Option[Int] = {
    if (caseSensitive) {
      schema.getFieldIndex(fieldName)
    } else {
      schema.getFieldIndexCaseInsensitive(fieldName)
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
    lazy val xmlRecord = UTF8String.fromString(xml)
    try {
      xsdSchema.foreach { schema =>
        schema.newValidator().validate(new StreamSource(new StringReader(xml)))
      }
      options.singleVariantColumn match {
        case Some(_) =>
          // If the singleVariantColumn is specified, parse the entire xml string as a Variant
          val v = StaxXmlParser.parseVariant(xml, options)
          Some(InternalRow(v))
        case _ =>
          // Otherwise, parse the xml string as Structs
          val parser = StaxXmlParserUtils.filteredReader(xml)
          val rootAttributes = StaxXmlParserUtils.gatherRootAttributes(parser)
          val result = Some(convertObject(parser, schema, rootAttributes))
          parser.close()
          result
      }
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
          """XML parser cannot handle a character in its input.
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
 * XMLRecordReader class to read through a given xml document to output xml blocks as records
 * as specified by the start tag and end tag.
 *
 * This implementation is ultimately loosely based on LineRecordReader in Hadoop.
 */
class XmlTokenizer(
  inputStream: InputStream,
  options: XmlOptions) extends Logging {
  private var reader = new BufferedReader(
    new InputStreamReader(inputStream, Charset.forName(options.charset)))
  private var currentStartTag: String = _
  private var buffer = new StringBuilder()
  private val startTag = s"<${options.rowTag}>"
  private val endTag = s"</${options.rowTag}>"
  private val commentStart = s"<!--"
  private val commentEnd = s"-->"
  private val cdataStart = s"<![CDATA["
  private val cdataEnd = s"]]>"

    /**
   * Finds the start of the next record.
   * It treats data from `startTag` and `endTag` as a record.
   *
   * @param key the current key that will be written
   * @param value  the object that will be written
   * @return whether it reads successfully
   */
    def next(): Option[String] = {
      var nextString: Option[String] = None
      try {
        if (readUntilStartElement()) {
          buffer.append(currentStartTag)
          // Don't check whether the end element was found. Even if not, return everything
          // that was read, which will invariably cause a parse error later
          readUntilEndElement(currentStartTag.endsWith(">"))
          nextString = Some(buffer.toString())
          buffer = new StringBuilder()
        }
      } catch {
        case e: FileNotFoundException if options.ignoreMissingFiles =>
          logWarning(
            "Skipping the rest of" +
              " the content in the missing file during schema inference",
            e)
        case NonFatal(e) =>
          ExceptionUtils.getRootCause(e) match {
            case _: AccessControlException | _: BlockMissingException =>
              reader.close()
              reader = null
              throw e
            case _: RuntimeException | _: IOException if options.ignoreCorruptFiles =>
              logWarning(
                "Skipping the rest of" +
                  " the content in the corrupted file during schema inference",
                e)
            case e: Throwable =>
              reader.close()
              reader = null
              throw e
          }
      } finally {
        if (nextString.isEmpty && reader != null) {
          reader.close()
          reader = null
        }
      }
      nextString
    }

  private def readUntilMatch(end: String): Boolean = {
    var i = 0
    while (true) {
      val cOrEOF = reader.read()
      if (cOrEOF == -1) {
        // End of file.
        return false
      }
      val c = cOrEOF.toChar
      if (c == end(i)) {
        i += 1
        if (i >= end.length) {
          // Found the end string.
          return true
        }
      } else {
        i = 0
      }
    }
    // Unreachable.
    false
  }

  private def readUntilStartElement(): Boolean = {
    currentStartTag = startTag
    var i = 0
    var commentIdx = 0
    var cdataIdx = 0

    while (true) {
      val cOrEOF = reader.read()
      if (cOrEOF == -1) { // || (i == 0 && getFilePosition() > end)) {
        // End of file or end of split.
        return false
      }
      val c = cOrEOF.toChar

      if (c == commentStart(commentIdx)) {
        if (commentIdx >= commentStart.length - 1) {
          //  If a comment beigns we must ignore all character until its end
          commentIdx = 0
          readUntilMatch(commentEnd)
        } else {
          commentIdx += 1
        }
      } else {
        commentIdx = 0
      }

      if (c == cdataStart(cdataIdx)) {
        if (cdataIdx >= cdataStart.length - 1) {
          //  If a CDATA beigns we must ignore all character until its end
          cdataIdx = 0
          readUntilMatch(cdataEnd)
        } else {
          cdataIdx += 1
        }
      } else {
        cdataIdx = 0
      }

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
    // Index into the start of a comment tag that matched so far
    var commentIdx = 0
    // Index into the start of a CDATA tag that matched so far
    var cdataIdx = 0
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

      if (c == commentStart(commentIdx)) {
        if (commentIdx >= commentStart.length - 1) {
          //  If a comment beigns we must ignore everything until its end
          buffer.setLength(buffer.length - commentStart.length)
          commentIdx = 0
          readUntilMatch(commentEnd)
        } else {
          commentIdx += 1
        }
      } else {
        commentIdx = 0
      }

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

object StaxXmlParser {
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

  /**
   * Parse the input XML string as a Variant value
   */
  def parseVariant(xml: String, options: XmlOptions): VariantVal = {
    val parser = StaxXmlParserUtils.filteredReader(xml)
    val rootAttributes = StaxXmlParserUtils.gatherRootAttributes(parser)
    val v = convertVariant(parser, rootAttributes, options)
    parser.close()
    v
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
