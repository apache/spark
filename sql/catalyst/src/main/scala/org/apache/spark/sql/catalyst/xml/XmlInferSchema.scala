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
import java.util.Locale
import javax.xml.stream.XMLEventReader
import javax.xml.stream.events._
import javax.xml.transform.stream.StreamSource
import javax.xml.validation.Schema

import scala.annotation.tailrec
import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._
import scala.util.control.Exception._
import scala.util.control.NonFatal

import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.ExprUtils
import org.apache.spark.sql.catalyst.util.{DateFormatter, PermissiveMode, TimestampFormatter}
import org.apache.spark.sql.catalyst.util.LegacyDateFormats.FAST_DATE_FORMAT
import org.apache.spark.sql.internal.{LegacyBehaviorPolicy, SQLConf}
import org.apache.spark.sql.types._

class XmlInferSchema(options: XmlOptions, caseSensitive: Boolean)
    extends Serializable
    with Logging {

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

  /**
   * Copied from internal Spark api
   * [[org.apache.spark.sql.catalyst.analysis.TypeCoercion]]
   */
  private val numericPrecedence: IndexedSeq[DataType] =
    IndexedSeq[DataType](
      ByteType,
      ShortType,
      IntegerType,
      LongType,
      FloatType,
      DoubleType,
      TimestampType,
      DecimalType.SYSTEM_DEFAULT)

  private val findTightestCommonTypeOfTwo: (DataType, DataType) => Option[DataType] = {
    case (t1, t2) if t1 == t2 => Some(t1)

    // Promote numeric types to the highest of the two
    case (t1, t2) if Seq(t1, t2).forall(numericPrecedence.contains) =>
      val index = numericPrecedence.lastIndexWhere(t => t == t1 || t == t2)
      Some(numericPrecedence(index))

    case _ => None
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
    val rootType = schemaData.mapPartitions { iter =>
      val xsdSchema = Option(options.rowValidationXSDPath).map(ValidatorUtil.getSchema)

      iter.flatMap { xml =>
        infer(xml, xsdSchema)
      }
    }.fold(StructType(Seq()))(compatibleType)

    canonicalizeType(rootType) match {
      case Some(st: StructType) => st
      case _ =>
        // canonicalizeType erases all empty structs, including the only one we want to keep
        StructType(Seq())
    }
  }

  def infer(xml: String, xsdSchema: Option[Schema] = None): Option[DataType] = {
    try {
      val xsd = xsdSchema.orElse(Option(options.rowValidationXSDPath).map(ValidatorUtil.getSchema))
      xsd.foreach { schema =>
        schema.newValidator().validate(new StreamSource(new StringReader(xml)))
      }
      val parser = StaxXmlParserUtils.filteredReader(xml)
      val rootAttributes = StaxXmlParserUtils.gatherRootAttributes(parser)
      val schema = Some(inferObject(parser, rootAttributes))
      parser.close()
      schema
    } catch {
      case NonFatal(_) if options.parseMode == PermissiveMode =>
        Some(StructType(Seq(StructField(options.columnNameOfCorruptRecord, StringType))))
      case NonFatal(_) =>
        None
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
      case _: EndElement => NullType
      case _: StartElement => inferObject(parser)
      case c: Characters if c.isWhiteSpace =>
        // When `Characters` is found, we need to look further to decide
        // if this is really data or space between other elements.
        val data = c.getData
        parser.nextEvent()
        parser.peek match {
          case _: StartElement => inferObject(parser)
          case _: EndElement if data.isEmpty => NullType
          case _: EndElement if options.nullValue == "" => NullType
          case _: EndElement => StringType
          case _ => inferField(parser)
        }
      case c: Characters if !c.isWhiteSpace =>
        val characterType = inferFrom(c.getData)
        parser.nextEvent()
        parser.peek match {
          case _: StartElement =>
            // Some more elements follow;
            // This is a mix of values and other elements
            val innerType = inferObject(parser).asInstanceOf[StructType]
            addOrUpdateValueTagType(innerType, characterType)
          case _ =>
            val fieldType = inferField(parser)
            fieldType match {
              case st: StructType => addOrUpdateValueTagType(st, characterType)
              case _: NullType => characterType
              case _: DataType =>
                // The field type couldn't be an array type
                new StructType()
                .add(options.valueTag, addOrUpdateType(Some(characterType), fieldType))

            }
        }
      case e: XMLEvent =>
        throw new IllegalArgumentException(s"Failed to parse data with unexpected event $e")
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

    @tailrec
    def inferAndCheckEndElement(parser: XMLEventReader): Boolean = {
      parser.peek match {
        case _: EndElement | _: EndDocument => true
        case _: StartElement => false
        case c: Characters if !c.isWhiteSpace =>
          val characterType = inferFrom(c.getData)
          parser.nextEvent()
          addOrUpdateType(nameToDataType, options.valueTag, characterType)
          inferAndCheckEndElement(parser)
        case _ =>
          parser.nextEvent()
          inferAndCheckEndElement(parser)
      }
    }

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
              nestedBuilder += StructField(options.valueTag, dt, nullable = true)
              valuesMap.foreach {
                case (f, v) =>
                  nestedBuilder += StructField(f, inferFrom(v), nullable = true)
              }
              StructType(nestedBuilder.sortBy(_.name).toArray)

            case dt: DataType => dt
          }
          // Add the field and datatypes so that we can check if this is ArrayType.
          val field = StaxXmlParserUtils.getName(e.asStartElement.getName, options)
          addOrUpdateType(nameToDataType, field, inferredType)

        case c: Characters if !c.isWhiteSpace =>
          // This can be an attribute-only object
          val valueTagType = inferFrom(c.getData)
          addOrUpdateType(nameToDataType, options.valueTag, valueTagType)

        case _: EndElement =>
          shouldStop = inferAndCheckEndElement(parser)

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

    try {
      // The conversion can fail when the `field` is not a form of number.
      val bigDecimal = decimalParser(signSafeValue)
      // Because many other formats do not support decimal, it reduces the cases for
      // decimals by disallowing values having scale (e.g. `1.1`).
      if (bigDecimal.scale <= 0) {
        // `DecimalType` conversion can fail when
        //   1. The precision is bigger than 38.
        //   2. scale is bigger than precision.
        return Some(DecimalType(bigDecimal.precision, bigDecimal.scale))
      }
    } catch {
      case _ : Exception =>
    }
    None
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

  /**
   * Returns the most general data type for two given data types.
   */
  private[xml] def compatibleType(t1: DataType, t2: DataType): DataType = {

    def normalize(name: String): String = {
      if (caseSensitive) name else name.toLowerCase(Locale.ROOT)
    }

    // TODO: Optimise this logic.
    findTightestCommonTypeOfTwo(t1, t2).getOrElse {
      // t1 or t2 is a StructType, ArrayType, or an unexpected type.
      (t1, t2) match {
        // Double support larger range than fixed decimal, DecimalType.Maximum should be enough
        // in most case, also have better precision.
        case (DoubleType, _: DecimalType) =>
          DoubleType
        case (_: DecimalType, DoubleType) =>
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
            .map(field => (normalize(field.name), field))
            .groupBy(_._1) // group by normalized field name
            .map { case (_: String, fields: Array[(String, StructField)]) =>
              val fieldTypes = fields.map(_._2)
              val dataType = fieldTypes.map(_.dataType).reduce(compatibleType)
              // we pick up the first field name that we've encountered for the field
              StructField(fields.head._2.name, dataType)
          }
          StructType(newFields.toArray.sortBy(_.name))

        case (ArrayType(elementType1, containsNull1), ArrayType(elementType2, containsNull2)) =>
          ArrayType(
            compatibleType(elementType1, elementType2), containsNull1 || containsNull2)

        // In XML datasource, since StructType can be compared with ArrayType.
        // In this case, ArrayType wraps the StructType.
        case (ArrayType(ty1, _), ty2) =>
          ArrayType(compatibleType(ty1, ty2))

        case (ty1, ArrayType(ty2, _)) =>
          ArrayType(compatibleType(ty1, ty2))

        // As this library can infer an element with attributes as StructType whereas
        // some can be inferred as other non-structural data types, this case should be
        // treated.
        case (st: StructType, dt: DataType) if st.fieldNames.contains(options.valueTag) =>
          val valueIndex = st.fieldNames.indexOf(options.valueTag)
          val valueField = st.fields(valueIndex)
          val valueDataType = compatibleType(valueField.dataType, dt)
          st.fields(valueIndex) = StructField(options.valueTag, valueDataType, nullable = true)
          st

        case (dt: DataType, st: StructType) if st.fieldNames.contains(options.valueTag) =>
          val valueIndex = st.fieldNames.indexOf(options.valueTag)
          val valueField = st.fields(valueIndex)
          val valueDataType = compatibleType(dt, valueField.dataType)
          st.fields(valueIndex) = StructField(options.valueTag, valueDataType, nullable = true)
          st

        // TODO: These null type checks should be in `findTightestCommonTypeOfTwo`.
        case (_, NullType) => t1
        case (NullType, _) => t2
        // strings and every string is a XML object.
        case (_, _) => StringType
      }
    }
  }

  /**
   * This helper function merges the data type of value tags and inner elements.
   * It could only be structure data. Consider the following case,
   * <a>
   *   value1
   *   <b>1</b>
   *   value2
   * </a>
   * Input: ''a struct<b int, _VALUE string>'' and ''_VALUE string''
   * Return: ''a struct<b int, _VALUE array<string>>''
   * @param objectType inner elements' type
   * @param valueTagType value tag's type
   */
  private[xml] def addOrUpdateValueTagType(
      objectType: DataType,
      valueTagType: DataType): DataType = {
    (objectType, valueTagType) match {
      case (st: StructType, _) =>
        val valueTagIndexOpt = st.getFieldIndex(options.valueTag)

        valueTagIndexOpt match {
          // If the field name exists in the inner elements,
          // merge the type and infer the combined field as an array type if necessary
          case Some(index) if !st(index).dataType.isInstanceOf[ArrayType] =>
            updateStructField(
              st,
              index,
              ArrayType(compatibleType(st(index).dataType, valueTagType)))
          case Some(index) =>
            updateStructField(st, index, compatibleType(st(index).dataType, valueTagType))
          case None =>
            st.add(options.valueTag, valueTagType)
        }
      case _ =>
        throw new IllegalStateException(
          "illegal state when merging value tags types in schema inference"
        )
    }
  }

  private def updateStructField(
      structType: StructType,
      index: Int,
      newType: DataType): StructType = {
    val newFields: Array[StructField] =
      structType.fields.updated(index, structType.fields(index).copy(dataType = newType))
    StructType(newFields)
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
      case Some(oldType) if !oldType.isInstanceOf[ArrayType] && !newType.isInstanceOf[NullType] =>
        ArrayType(compatibleType(oldType, newType))
      case Some(oldType) =>
        compatibleType(oldType, newType)
      case None =>
        newType
    }
  }
}
