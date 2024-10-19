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

import java.io.{CharConversionException, FileNotFoundException, IOException, StringReader}
import java.nio.charset.MalformedInputException
import java.util.Locale
import javax.xml.stream.{XMLEventReader, XMLStreamException}
import javax.xml.stream.events._
import javax.xml.transform.stream.StreamSource
import javax.xml.validation.Schema

import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._
import scala.util.control.Exception._
import scala.util.control.NonFatal
import scala.xml.SAXException

import org.apache.spark.SparkIllegalArgumentException
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.analysis.TypeCoercion
import org.apache.spark.sql.catalyst.expressions.ExprUtils
import org.apache.spark.sql.catalyst.util.{DateFormatter, DropMalformedMode, FailFastMode, ParseMode, PermissiveMode, TimestampFormatter}
import org.apache.spark.sql.catalyst.util.LegacyDateFormats.FAST_DATE_FORMAT
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.internal.{LegacyBehaviorPolicy, SQLConf}
import org.apache.spark.sql.types._

class XmlInferSchema(options: XmlOptions, caseSensitive: Boolean)
    extends Serializable
    with Logging {

  import org.apache.spark.sql.catalyst.xml.XmlInferSchema._

  private val decimalParser = ExprUtils.getDecimalParser(options.locale)

  private val timestampFormatter = TimestampFormatter(
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

  private def handleXmlErrorsByParseMode(
      parseMode: ParseMode,
      columnNameOfCorruptRecord: String,
      e: Throwable): Option[StructType] = {
    parseMode match {
      case PermissiveMode =>
        Some(StructType(Array(StructField(columnNameOfCorruptRecord, StringType))))
      case DropMalformedMode =>
        None
      case FailFastMode =>
        throw QueryExecutionErrors.malformedRecordsDetectedInSchemaInferenceError(
          e, columnNameOfCorruptRecord)
    }
  }

  /**
   * Infer the type of a collection of XML records in three stages:
   *   1. Infer the type of each record
   *   2. Merge types by choosing the lowest type necessary to cover equal keys
   *   3. Replace any remaining null fields with string, the top type
   */
  def infer(xml: RDD[String]): StructType = {
    val schemaData = if (options.samplingRatio < 1.0) {
      xml.sample(withReplacement = false, options.samplingRatio, 1)
    } else {
      xml
    }
    // perform schema inference on each row and merge afterwards
    val mergedTypesFromPartitions = schemaData.mapPartitions { iter =>
      val xsdSchema = Option(options.rowValidationXSDPath).map(ValidatorUtil.getSchema)

      iter.flatMap { xml =>
        infer(xml, xsdSchema)
      }.reduceOption(compatibleType(caseSensitive, options.valueTag)).iterator
    }

    // Here we manually submit a fold-like Spark job, so that we can set the SQLConf when running
    // the fold functions in the scheduler event loop thread.
    val existingConf = SQLConf.get
    var rootType: DataType = StructType(Nil)
    val foldPartition = (iter: Iterator[DataType]) =>
      iter.fold(StructType(Nil))(compatibleType(caseSensitive, options.valueTag))
    val mergeResult = (index: Int, taskResult: DataType) => {
      rootType = SQLConf.withExistingConf(existingConf) {
        compatibleType(caseSensitive, options.valueTag)(rootType, taskResult)
      }
    }
    xml.sparkContext.runJob(mergedTypesFromPartitions, foldPartition, mergeResult)

    canonicalizeType(rootType) match {
      case Some(st: StructType) => st
      case _ =>
        // canonicalizeType erases all empty structs, including the only one we want to keep
        // XML shouldn't run into this line
        StructType(Seq())
    }
  }

  def infer(xml: String, xsdSchema: Option[Schema] = None): Option[DataType] = {
    var parser: XMLEventReader = null
    try {
      val xsd = xsdSchema.orElse(Option(options.rowValidationXSDPath).map(ValidatorUtil.getSchema))
      xsd.foreach { schema =>
        schema.newValidator().validate(new StreamSource(new StringReader(xml)))
      }
      parser = StaxXmlParserUtils.filteredReader(xml)
      val rootAttributes = StaxXmlParserUtils.gatherRootAttributes(parser)
      val schema = Some(inferObject(parser, rootAttributes))
      parser.close()
      schema
    } catch {
      case e @ (_: XMLStreamException | _: MalformedInputException | _: SAXException) =>
        handleXmlErrorsByParseMode(options.parseMode, options.columnNameOfCorruptRecord, e)
      case e: CharConversionException if options.charset.isEmpty =>
        val msg =
          """XML parser cannot handle a character in its input.
            |Specifying encoding as an input option explicitly might help to resolve the issue.
            |""".stripMargin + e.getMessage
        val wrappedCharException = new CharConversionException(msg)
        wrappedCharException.initCause(e)
        handleXmlErrorsByParseMode(
          options.parseMode,
          options.columnNameOfCorruptRecord,
          wrappedCharException)
      case e: FileNotFoundException if options.ignoreMissingFiles =>
        logWarning("Skipped missing file", e)
        Some(StructType(Nil))
      case e: FileNotFoundException if !options.ignoreMissingFiles => throw e
      case e @ (_: IOException | _: RuntimeException) if options.ignoreCorruptFiles =>
        logWarning("Skipped the rest of the content in the corrupted file", e)
        Some(StructType(Nil))
      case NonFatal(e) =>
        handleXmlErrorsByParseMode(options.parseMode, options.columnNameOfCorruptRecord, e)
    } finally {
      if (parser != null) {
        parser.close()
      }
    }
  }

  private def inferFrom(datum: String): DataType = {
    val value = if (datum != null && options.ignoreSurroundingSpaces) {
      datum.trim()
    } else {
      datum
    }

    if (options.inferSchema) {
      lazy val decimalTry = tryParseDecimal(value)
      lazy val timestampNTZTry = tryParseTimestampNTZ(value)
      value match {
        case null => NullType
        case v if v.isEmpty => NullType
        case v if isLong(v) => LongType
        case v if options.prefersDecimal && decimalTry.isDefined => decimalTry.get
        case v if isDouble(v) => DoubleType
        case v if isBoolean(v) => BooleanType
        case v if isDate(v) => DateType
        case v if timestampNTZTry.isDefined => timestampNTZTry.get
        case v if isTimestamp(v) => TimestampType
        case _ => StringType
      }
    } else {
      StringType
    }
  }

  private def inferField(parser: XMLEventReader): DataType = {
    parser.peek match {
      case _: EndElement =>
        parser.nextEvent()
        NullType
      case _: StartElement => inferObject(parser)
      case _: Characters =>
        val structType = inferObject(parser).asInstanceOf[StructType]
        structType match {
          case _ if structType.fields.isEmpty =>
            NullType
          case simpleType
              if structType.fields.length == 1
              && isPrimitiveType(structType.fields.head.dataType)
              && isValueTagField(structType.fields.head, caseSensitive) =>
            simpleType.fields.head.dataType
          case _ => structType
        }
      case e: XMLEvent =>
        throw new SparkIllegalArgumentException(
          errorClass = "_LEGACY_ERROR_TEMP_3239",
          messageParameters = Map("e" -> e.toString))
    }
  }

  /**
   * Infer the type of a xml document from the parser's token stream
   */
  private def inferObject(
      parser: XMLEventReader,
      rootAttributes: Array[Attribute] = Array.empty): DataType = {
    /**
     * Retrieves the field name with respect to the case sensitivity setting.
     * We pick the first name we encountered.
     *
     * If case sensitivity is enabled, the original field name is returned.
     * If not, the field name is managed in a case-insensitive map.
     *
     * For instance, if we encounter the following field names:
     * foo, Foo, FOO
     *
     * In case-sensitive mode: we will infer three fields: foo, Foo, FOO
     * In case-insensitive mode, we will infer an array named by foo
     * (as it's the first one we encounter)
     */
    val caseSensitivityOrdering: Ordering[String] = if (caseSensitive) {
      (x: String, y: String) => x.compareTo(y)
    } else {
      (x: String, y: String) => x.compareToIgnoreCase(y)
    }

    val nameToDataType =
      collection.mutable.TreeMap.empty[String, DataType](caseSensitivityOrdering)

    // If there are attributes, then we should process them first.
    val rootValuesMap =
      StaxXmlParserUtils.convertAttributesToValuesMap(rootAttributes, options)
    rootValuesMap.foreach {
      case (f, v) =>
        addOrUpdateType(nameToDataType, f, inferFrom(v))
    }
    var shouldStop = false
    while (!shouldStop) {
      parser.nextEvent match {
        case e: StartElement =>
          val attributes = e.getAttributes.asScala.toArray
          val valuesMap = StaxXmlParserUtils.convertAttributesToValuesMap(attributes, options)
          val field = StaxXmlParserUtils.getName(e.asStartElement.getName, options)
          val inferredType = inferField(parser) match {
            case st: StructType if valuesMap.nonEmpty =>
              // Merge attributes to the field
              val nestedBuilder = ArrayBuffer[StructField]()
              nestedBuilder ++= st.fields
              valuesMap.foreach {
                case (f, v) =>
                  nestedBuilder += StructField(f, inferFrom(v), nullable = true)
              }
              StructType(nestedBuilder.sortBy(_.name).toArray)

            case dt: DataType if valuesMap.nonEmpty =>
              // We need to manually add the field for value.
              val nestedBuilder = ArrayBuffer[StructField]()
              if (!dt.isInstanceOf[NullType]) {
                nestedBuilder += StructField(options.valueTag, dt, nullable = true)
              }
              valuesMap.foreach {
                case (f, v) =>
                  nestedBuilder += StructField(f, inferFrom(v), nullable = true)
              }
              StructType(nestedBuilder.sortBy(_.name).toArray)

            case dt: DataType => dt
          }
          // Add the field and datatypes so that we can check if this is ArrayType.
          addOrUpdateType(nameToDataType, field, inferredType)

        case c: Characters if !c.isWhiteSpace =>
          // This is a value tag
          val valueTagType = inferFrom(c.getData)
          addOrUpdateType(nameToDataType, options.valueTag, valueTagType)

        case _: EndElement | _: EndDocument =>
          shouldStop = true

        case _ => // do nothing
      }
    }

    // Note: other code relies on this sorting for correctness, so don't remove it!
    StructType(nameToDataType.map{
      case (name, dataType) => StructField(name, dataType)
    }.toList.sortBy(_.name))
  }

  /**
   * Helper method that checks and cast string representation of a numeric types.
   */
  private def isBoolean(value: String): Boolean = {
    value.toLowerCase(Locale.ROOT) match {
      case "true" | "false" => true
      case _ => false
    }
  }

  private def tryParseDecimal(value: String): Option[DataType] = {
    val signSafeValue = if (value.startsWith("+") || value.startsWith("-")) {
      value.substring(1)
    } else {
      value
    }
    // A little shortcut to avoid trying many formatters in the common case that
    // the input isn't a decimal. All built-in formats will start with a digit or period.
    if (signSafeValue.isEmpty ||
      !(Character.isDigit(signSafeValue.head) || signSafeValue.head == '.')) {
      return None
    }
    // Rule out strings ending in D or F, as they will parse as double but should be disallowed
    if (signSafeValue.last match {
      case 'd' | 'D' | 'f' | 'F' => true
      case _ => false
    }) {
      return None
    }

    allCatch opt {
      val bigDecimal = decimalParser(value)
      DecimalType(Math.max(bigDecimal.precision, bigDecimal.scale), bigDecimal.scale)
    }
  }

  private def isDouble(value: String): Boolean = {
    val signSafeValue = if (value.startsWith("+") || value.startsWith("-")) {
      value.substring(1)
    } else {
      value
    }
    // A little shortcut to avoid trying many formatters in the common case that
    // the input isn't a double. All built-in formats will start with a digit or period.
    if (signSafeValue.isEmpty ||
      !(Character.isDigit(signSafeValue.head) || signSafeValue.head == '.')) {
      return false
    }
    // Rule out strings ending in D or F, as they will parse as double but should be disallowed
    if (signSafeValue.last match {
      case 'd' | 'D' | 'f' | 'F' => true
      case _ => false
    }) {
      return false
    }
    (allCatch opt signSafeValue.toDouble).isDefined
  }

  private def isLong(value: String): Boolean = {
    val signSafeValue = if (value.startsWith("+") || value.startsWith("-")) {
      value.substring(1)
    } else {
      value
    }
    // A little shortcut to avoid trying many formatters in the common case that
    // the input isn't a number. All built-in formats will start with a digit.
    if (signSafeValue.isEmpty || !Character.isDigit(signSafeValue.head)) {
      return false
    }
    (allCatch opt signSafeValue.toLong).isDefined
  }

  private def isTimestamp(value: String): Boolean = {
    try {
      timestampFormatter.parseOptional(value).isDefined
    } catch {
      case _: IllegalArgumentException => false
    }
  }

  private def tryParseTimestampNTZ(field: String): Option[DataType] = {
    // We can only parse the value as TimestampNTZType if it does not have zone-offset or
    // time-zone component and can be parsed with the timestamp formatter.
    // Otherwise, it is likely to be a timestamp with timezone.
    val timestampType = SQLConf.get.timestampType
    try {
      if ((SQLConf.get.legacyTimeParserPolicy == LegacyBehaviorPolicy.LEGACY ||
        timestampType == TimestampNTZType) &&
        timestampNTZFormatter.parseWithoutTimeZoneOptional(field, false).isDefined) {
        return Some(timestampType)
      }
    } catch {
      case _: Exception =>
    }
    None
  }

  private def isDate(value: String): Boolean = {
    (allCatch opt dateFormatter.parse(value)).isDefined
  }

  /**
   * Convert NullType to StringType and remove StructTypes with no fields
   */
  def canonicalizeType(dt: DataType): Option[DataType] = dt match {
    case at @ ArrayType(elementType, _) =>
      for {
        canonicalType <- canonicalizeType(elementType)
      } yield {
        at.copy(canonicalType)
      }

    case StructType(fields) =>
      val canonicalFields = for {
        field <- fields if field.name.nonEmpty
        canonicalType <- canonicalizeType(field.dataType)
      } yield {
        field.copy(dataType = canonicalType)
      }

      if (canonicalFields.nonEmpty) {
        Some(StructType(canonicalFields))
      } else {
        // per SPARK-8093: empty structs should be deleted
        None
      }

    case NullType => Some(StringType)
    case other => Some(other)
  }


  private def addOrUpdateType(
      nameToDataType: collection.mutable.TreeMap[String, DataType],
      fieldName: String,
      newType: DataType): Unit = {
    val oldTypeOpt = nameToDataType.get(fieldName)
    val mergedType = addOrUpdateType(oldTypeOpt, newType)
    nameToDataType.put(fieldName, mergedType)
  }

  private def addOrUpdateType(oldTypeOpt: Option[DataType], newType: DataType): DataType = {
    oldTypeOpt match {
      // If the field name already exists,
      // merge the type and infer the combined field as an array type if necessary
      case Some(oldType) if !oldType.isInstanceOf[ArrayType] =>
        ArrayType(compatibleType(caseSensitive, options.valueTag)(oldType, newType))
      case Some(oldType) =>
        compatibleType(caseSensitive, options.valueTag)(oldType, newType)
      case None =>
        newType
    }
  }

  private[xml] def isPrimitiveType(dataType: DataType): Boolean = {
    dataType match {
      case _: StructType => false
      case _: ArrayType => false
      case _: MapType => false
      case _ => true
    }
  }

  private[xml] def isValueTagField(structField: StructField, caseSensitive: Boolean): Boolean = {
    if (!caseSensitive) {
      structField.name.toLowerCase(Locale.ROOT) == options.valueTag.toLowerCase(Locale.ROOT)
    } else {
      structField.name == options.valueTag
    }
  }
}

object XmlInferSchema {
  def normalize(name: String, caseSensitive: Boolean): String = {
    if (caseSensitive) name else name.toLowerCase(Locale.ROOT)
  }

  /**
   * Returns the most general data type for two given data types.
   */
  private[xml] def compatibleType(caseSensitive: Boolean, valueTag: String)
    (t1: DataType, t2: DataType): DataType = {

    // TODO: Optimise this logic.
    TypeCoercion.findTightestCommonType(t1, t2).getOrElse {
      // t1 or t2 is a StructType, ArrayType, or an unexpected type.
      (t1, t2) match {
        // Double support larger range than fixed decimal, DecimalType.Maximum should be enough
        // in most case, also have better precision.
        case (DoubleType, _: DecimalType) | (_: DecimalType, DoubleType) =>
          DoubleType

        case (t1: DecimalType, t2: DecimalType) =>
          val scale = math.max(t1.scale, t2.scale)
          val range = math.max(t1.precision - t1.scale, t2.precision - t2.scale)
          if (range + scale > 38) {
            // DecimalType can't support precision > 38
            DoubleType
          } else {
            DecimalType(range + scale, scale)
          }
        case (TimestampNTZType, TimestampType) | (TimestampType, TimestampNTZType) =>
          TimestampType

        case (StructType(fields1), StructType(fields2)) =>
          val newFields = (fields1 ++ fields2)
           // normalize field name and pair it with original field
           .map(field => (normalize(field.name, caseSensitive), field))
           .groupBy(_._1) // group by normalized field name
           .map { case (_: String, fields: Array[(String, StructField)]) =>
             val fieldTypes = fields.map(_._2)
             val dataType = fieldTypes.map(_.dataType)
               .reduce(compatibleType(caseSensitive, valueTag))
             // we pick up the first field name that we've encountered for the field
             StructField(fields.head._2.name, dataType)
           }
           StructType(newFields.toArray.sortBy(_.name))

        case (ArrayType(elementType1, containsNull1), ArrayType(elementType2, containsNull2)) =>
          ArrayType(
            compatibleType(caseSensitive, valueTag)(
              elementType1, elementType2), containsNull1 || containsNull2)

        // In XML datasource, since StructType can be compared with ArrayType.
        // In this case, ArrayType wraps the StructType.
        case (ArrayType(ty1, _), ty2) =>
          ArrayType(compatibleType(caseSensitive, valueTag)(ty1, ty2))

        case (ty1, ArrayType(ty2, _)) =>
          ArrayType(compatibleType(caseSensitive, valueTag)(ty1, ty2))

        // As this library can infer an element with attributes as StructType whereas
        // some can be inferred as other non-structural data types, this case should be
        // treated.
        // 1. Without value tags, combining structs and primitive types defaults to string type
        // 2. With value tags, combining structs and primitive types defaults to
        //    a struct with value tags of compatible type
        // This behavior keeps aligned with JSON
        case (st: StructType, dt: DataType) if st.fieldNames.contains(valueTag) =>
          val valueIndex = st.fieldNames.indexOf(valueTag)
          val valueField = st.fields(valueIndex)
          val valueDataType = compatibleType(caseSensitive, valueTag)(valueField.dataType, dt)
          st.fields(valueIndex) = StructField(valueTag, valueDataType, nullable = true)
          st

        case (dt: DataType, st: StructType) if st.fieldNames.contains(valueTag) =>
          val valueIndex = st.fieldNames.indexOf(valueTag)
          val valueField = st.fields(valueIndex)
          val valueDataType = compatibleType(caseSensitive, valueTag)(dt, valueField.dataType)
          st.fields(valueIndex) = StructField(valueTag, valueDataType, nullable = true)
          st

        // The case that given `DecimalType` is capable of given `IntegralType` is handled in
        // `findTightestCommonType`. Both cases below will be executed only when the given
        // `DecimalType` is not capable of the given `IntegralType`.
        case (t1: IntegralType, t2: DecimalType) =>
          compatibleType(caseSensitive, valueTag)(DecimalType.forType(t1), t2)
        case (t1: DecimalType, t2: IntegralType) =>
          compatibleType(caseSensitive, valueTag)(t1, DecimalType.forType(t2))

        // strings and every string is a XML object.
        case (_, _) => StringType
      }
    }
  }
}
