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

import org.apache.hadoop.hdfs.BlockMissingException
import org.apache.hadoop.security.AccessControlException

import org.apache.spark.{SparkException, SparkIllegalArgumentException}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.analysis.TypeCoercion
import org.apache.spark.sql.catalyst.expressions.ExprUtils
import org.apache.spark.sql.catalyst.util.{DateFormatter, DropMalformedMode, FailFastMode, ParseMode, PermissiveMode, TimeFormatter, TimestampFormatter}
import org.apache.spark.sql.catalyst.util.LegacyDateFormats.FAST_DATE_FORMAT
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.internal.{LegacyBehaviorPolicy, SQLConf}
import org.apache.spark.sql.types._
import org.apache.spark.util.SparkErrorUtils

class XmlInferSchema(private val options: XmlOptions, private val caseSensitive: Boolean)
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

  private lazy val timeFormatter = TimeFormatter(options.timeFormatInRead, isParsing = true)

  private val isTimeTypeEnabled = SQLConf.get.isTimeTypeEnabled

  private val incrementalTypeCasting = SQLConf.get.xmlSchemaInferenceIncrementalTypeCasting

  override def equals(obj: Any): Boolean = obj match {
    case other: XmlInferSchema =>
      options == other.options &&
      caseSensitive == other.caseSensitive
    case _ => false
  }

  override def hashCode(): Int = {
    var result = options.hashCode()
    result = 31 * result + (if (caseSensitive) 1 else 0)
    result
  }

  private def handleXmlErrorsByParseMode(
      parser: XMLEventReader,
      parseMode: ParseMode,
      columnNameOfCorruptRecord: String,
      e: Throwable): Option[StructType] = {
    parseMode match {
      case PermissiveMode =>
        Some(StructType(Array(StructField(columnNameOfCorruptRecord, StringType))))
      case DropMalformedMode =>
        None
      case FailFastMode =>
        parser.close()
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

      if (incrementalTypeCasting) {
        // Incremental inference: carry the schema merged so far in this partition and feed it
        // back as a hint into the next record's inference, so each field's type is refined
        // rather than re-derived from scratch. The final partition type is the last (fully
        // merged) schema, matching the batch path's fold up to `compatibleType`'s associativity.
        var schemaSoFar: DataType = StructType(Nil)
        iter.flatMap { xml =>
          val schemaHints = schemaSoFar match {
            case st: StructType => st
            case _ => StructType(Nil)
          }
          infer(xml, xsdSchema, schemaHints).map { dataType =>
            schemaSoFar = compatibleType(caseSensitive, options.valueTag)(schemaSoFar, dataType)
            schemaSoFar
          }
        }.reduceOption((_, latest) => latest).iterator
      } else {
        iter.flatMap { xml =>
          infer(xml, xsdSchema)
        }.reduceOption(compatibleType(caseSensitive, options.valueTag)).iterator
      }
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

  def infer(
      xml: String,
      xsdSchema: Option[Schema],
      schemaHints: StructType): Option[DataType] = {
    var parser: XMLEventReader = null
    try {
      val xsd = xsdSchema.orElse(Option(options.rowValidationXSDPath).map(ValidatorUtil.getSchema))
      xsd.foreach { schema =>
        schema.newValidator().validate(new StreamSource(new StringReader(xml)))
      }
      parser = StaxXmlParserUtils.filteredReader(xml)
      val rootAttributes = StaxXmlParserUtils.gatherRootAttributes(parser)
      val schema = Some(inferObject(parser, rootAttributes, Some(schemaHints)))
      parser.close()
      schema
    } catch {
      case e @ (_: XMLStreamException | _: MalformedInputException | _: SAXException) =>
        handleXmlErrorsByParseMode(parser, options.parseMode, options.columnNameOfCorruptRecord, e)
      case e: CharConversionException if options.charset.isEmpty =>
        val msg =
          """XML parser cannot handle a character in its input.
            |Specifying encoding as an input option explicitly might help to resolve the issue.
            |""".stripMargin + e.getMessage
        val wrappedCharException = new CharConversionException(msg)
        wrappedCharException.initCause(e)
        handleXmlErrorsByParseMode(
          parser,
          options.parseMode,
          options.columnNameOfCorruptRecord,
          wrappedCharException)
      case e: FileNotFoundException if options.ignoreMissingFiles =>
        logWarning("Skipped missing file", e)
        Some(StructType(Nil))
      case e: FileNotFoundException if !options.ignoreMissingFiles => throw e
      case e @ (_ : AccessControlException | _ : BlockMissingException) => throw e
      case e @ (_: IOException | _: RuntimeException) if options.ignoreCorruptFiles =>
        logWarning("Skipped the rest of the content in the corrupted file", e)
        Some(StructType(Nil))
      case NonFatal(e) =>
        handleXmlErrorsByParseMode(parser, options.parseMode, options.columnNameOfCorruptRecord, e)
    } finally {
      if (parser != null) {
        parser.close()
      }
    }
  }

  def infer(xml: String, xsdSchema: Option[Schema] = None): Option[DataType] =
    infer(xml, xsdSchema, StructType(Nil))

  def inferFromReaders(recordReader: RDD[StaxXMLRecordReader]): StructType = {
    val sampledRecordReader = if (options.samplingRatio < 1.0) {
      recordReader.sample(withReplacement = false, options.samplingRatio, 1)
    } else {
      recordReader
    }
    // perform schema inference on each row and merge afterwards
    val mergedTypesFromPartitions = sampledRecordReader.mapPartitions { iter =>
      iter.flatMap { xmlReader =>
        infer(xmlReader)
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
    recordReader.sparkContext.runJob(mergedTypesFromPartitions, foldPartition, mergeResult)

    canonicalizeType(rootType) match {
      case Some(st: StructType) => st
      case _ =>
        // canonicalizeType erases all empty structs, including the only one we want to keep
        // XML shouldn't run into this line
        StructType(Seq())
    }
  }

  /**
   * Infer the schema of the next XML record in the XML event stream.
   * Note that the method will **NOT** close the XML event stream as there could have more XML
   * records to parse. The StaxXMLRecordReader will automatically close the stream when there are
   * no more XML records to parse.
   */
  def infer(parser: StaxXMLRecordReader): Option[DataType] = {
    try {
      if (!parser.skipToNextRecord()) {
        return None
      }

      val rootAttributes = parser.nextEvent().asStartElement.getAttributes.asScala.toArray
      val schema = Some(inferObject(parser, rootAttributes))
      schema
    } catch {
      case e: CharConversionException if options.charset.isEmpty =>
        val msg =
          """XML parser cannot handle a character in its input.
            |Specifying encoding as an input option explicitly might help to resolve the issue.
            |""".stripMargin + e.getMessage
        val wrappedCharException = new CharConversionException(msg)
        wrappedCharException.initCause(e)
        handleXmlErrorsByParseMode(
          parser,
          options.parseMode,
          options.columnNameOfCorruptRecord,
          wrappedCharException)
      case e: FileNotFoundException if options.ignoreMissingFiles =>
        logWarning("Skipped missing file", e)
        parser.close()
        Some(StructType(Nil))
      case e: FileNotFoundException if !options.ignoreMissingFiles =>
        parser.close()
        throw e
      case NonFatal(e) =>
        SparkErrorUtils.getRootCause(e) match {
          case _: XMLStreamException | _: MalformedInputException =>
            logWarning("Malformed XML record found", e)
            // Close the parser from the first malformed XML record
            parser.close()
            handleXmlErrorsByParseMode(
              parser = parser,
              parseMode = options.parseMode,
              columnNameOfCorruptRecord = options.columnNameOfCorruptRecord,
              e = e
            )
          case _: SAXException =>
            // For XSD validation errors, don't close the parser as there might be more valid
            // records to parse.
            // Advance the parser so that the next record can be parsed.
            parser.nextEvent()
            handleXmlErrorsByParseMode(
              parser = parser,
              parseMode = options.parseMode,
              columnNameOfCorruptRecord = options.columnNameOfCorruptRecord,
              e = e
            )
          case _: AccessControlException | _: BlockMissingException =>
            parser.close()
            throw e
          case _: IOException | _: RuntimeException | _: InternalError | _: AssertionError
              if options.ignoreCorruptFiles =>
            logWarning("Skipped the rest of the content in the corrupted file", e)
            parser.close()
            Some(StructType(Nil))
          case _: IOException | _: RuntimeException | _: InternalError
              if !options.ignoreCorruptFiles =>
            parser.close()
            throw e
          case _ =>
            logWarning("Failed to infer schema from XML record", e)
            handleXmlErrorsByParseMode(
              parser = parser,
              parseMode = options.parseMode,
              columnNameOfCorruptRecord = options.columnNameOfCorruptRecord,
              e = e
            )
        }
    }
  }

  private def inferFrom(datum: String): DataType = {
    // Start type inference from scratch, without any prior knowledge of the field's type.
    inferFrom(datum, NullType)
  }

  /**
   * Infer the type of a primitive value, refining a type already inferred for the same field
   * from previous values.
   *
   * Rather than probing every candidate type from scratch on every value (the historical
   * approach), this dispatches on `typeSoFar` -- the type accumulated for this field so far --
   * and only attempts to widen it. Given a value already known to be `Double`, for example,
   * there is no point checking whether it is a `Long`, as the merged type can only be `Double`
   * or wider. This mirrors the schema inference used by the CSV datasource
   * (`CSVInferSchema.inferField`) and keeps the type lattice monotonic: each value can only
   * hold the type where it is or widen it, never narrow it. `compatibleType` reconciles the
   * newly inferred type with `typeSoFar`, falling back to `StringType` (the top type) when they
   * are incompatible.
   */
  private[xml] def inferFrom(datum: String, typeSoFar: DataType): DataType = {
    val value = if (datum != null && options.ignoreSurroundingSpaces) {
      datum.trim()
    } else {
      datum
    }

    if (!options.inferSchema) {
      return StringType
    }
    if (value == null || value.isEmpty) {
      return typeSoFar
    }

    val inferredType = typeSoFar match {
      // For a numeric type inferred so far, re-enter the cascade at its top (`tryParseLong`)
      // rather than at the narrower numeric parser. Entering at `tryParseDecimal` would infer an
      // integer value directly as a narrow `Decimal` (e.g. "5" -> Decimal(1,0)) instead of the
      // `Long` that from-scratch inference produces; `compatibleType` then merges those to
      // different Decimal precisions (Decimal(5,2) vs Decimal(22,2)), which would make the
      // inferred type differ from the legacy path and depend on row order. Re-entering at the top
      // yields the same representative as from-scratch inference, so the merged type matches.
      case NullType | LongType | DoubleType | _: DecimalType => tryParseLong(value)
      case BooleanType => tryParseBoolean(value)
      // For a temporal type inferred so far, re-enter the cascade at the top of the temporal
      // sub-cascade (`tryParseTime`, which flows Time -> Date -> TimestampNTZ -> Timestamp)
      // rather than at the narrower temporal parser. Entering at `tryParseTimestamp` for a
      // `Timestamp` type-so-far, for example, would fail to parse a date-only value (when
      // `timestampFormat` requires a time part) and fall through to `String`; `compatibleType`
      // then merges `Timestamp` with `String` to `String`, whereas from-scratch inference yields
      // `Date` which widens with `Timestamp` to `Timestamp` via `findWiderDateTimeType`. That
      // would make the inferred type differ from the legacy path and depend on row order.
      // Re-entering at the top yields the same representative as from-scratch inference (which
      // reaches the temporal parsers through `tryParseDouble`), so the merged type matches.
      case _: TimeType | DateType | TimestampNTZType | _: TimestampNTZNanosType | TimestampType =>
        tryParseTime(value)
      case StringType => StringType
      case other: DataType =>
        throw SparkException.internalError(s"Unexpected data type $other")
    }
    compatibleType(caseSensitive, options.valueTag)(typeSoFar, inferredType)
  }

  private def inferField(
      parser: XMLEventReader,
      dataTypeHintOpt: Option[DataType] = None): DataType = {
    parser.peek match {
      case _: EndElement =>
        parser.nextEvent()
        NullType
      case _: StartElement =>
        inferObject(parser, schemaHintsOpt = structTypeHint(dataTypeHintOpt))
      case _: Characters =>
        val structType =
          inferObject(parser, schemaHintsOpt = structTypeHintWithValueTag(dataTypeHintOpt))
            .asInstanceOf[StructType]
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
      rootAttributes: Array[Attribute] = Array.empty,
      schemaHintsOpt: Option[StructType] = None): DataType = {
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

    // When incremental inference is on, `schemaHintsOpt` carries the schema inferred for this
    // object from previous records. `fieldToHint` maps each field name to its already-inferred
    // type so it can be threaded into `inferFrom`/`inferField` as the starting point, refining
    // it rather than re-deriving from scratch. Empty for the batch path (`schemaHintsOpt` None).
    val fieldToHint: Map[String, DataType] = schemaHintsOpt match {
      case Some(schema) if caseSensitive => schema.nameToDataType
      case Some(schema) => schema.nameToDataTypeCaseInsensitive
      case None => Map.empty
    }

    // If there are attributes, then we should process them first.
    val rootValuesMap =
      StaxXmlParserUtils.convertAttributesToValuesMap(rootAttributes, options)
    rootValuesMap.foreach {
      case (f, v) =>
        addOrUpdateType(nameToDataType, f, inferFrom(v, matchingPrimitiveHint(fieldToHint.get(f))))
    }
    var shouldStop = false
    while (!shouldStop) {
      parser.nextEvent match {
        case e: StartElement =>
          val attributes = e.getAttributes.asScala.toArray
          val valuesMap = StaxXmlParserUtils.convertAttributesToValuesMap(attributes, options)
          val field = StaxXmlParserUtils.getName(e.asStartElement.getName, options)
          val fieldHintOpt = fieldToHint.get(field)
          val inferredType = inferField(parser, fieldHintOpt) match {
            case st: StructType if valuesMap.nonEmpty =>
              // Merge attributes to the field
              val nestedBuilder = ArrayBuffer[StructField]()
              nestedBuilder ++= st.fields
              valuesMap.foreach {
                case (f, v) =>
                  val attrHint = matchingAttributeHint(fieldHintOpt, f)
                  nestedBuilder += StructField(f, inferFrom(v, attrHint), nullable = true)
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
                  val attrHint = matchingAttributeHint(fieldHintOpt, f)
                  nestedBuilder += StructField(f, inferFrom(v, attrHint), nullable = true)
              }
              StructType(nestedBuilder.sortBy(_.name).toArray)

            case dt: DataType => dt
          }
          // Add the field and datatypes so that we can check if this is ArrayType.
          addOrUpdateType(nameToDataType, field, inferredType)

        case c: Characters if !c.isWhiteSpace =>
          // This is a value tag
          val valueTagType =
            inferFrom(c.getData, matchingPrimitiveHint(fieldToHint.get(options.valueTag)))
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

  // --- Helpers for incremental inference: extracting the type hint for a nested field ---
  //
  // When incremental inference threads a previously-inferred schema back in, a field's hint may
  // be wrapped (a leaf value gets represented as a single-`valueTag` struct, and repeated elements
  // as an array). These helpers unwrap the hint to the shape the next inference step expects.

  /**
   * Reduce a field's hint to the primitive type to feed into `inferFrom` for a leaf value.
   * A leaf value may have been inferred previously either as a bare primitive, or -- when the
   * element also carried attributes -- as a struct with a `valueTag` field, possibly wrapped in
   * an array for repeated elements. Anything else (a struct without a value tag) offers no usable
   * primitive hint, so we fall back to `NullType` (i.e. infer from scratch).
   */
  private def matchingPrimitiveHint(dataTypeHintOpt: Option[DataType]): DataType = {
    def unwrap(dt: DataType): DataType = dt match {
      case ArrayType(elementType, _) => unwrap(elementType)
      case st: StructType if st.fieldNames.contains(options.valueTag) =>
        st(options.valueTag).dataType match {
          case ArrayType(elementType, _) => elementType
          case other => other
        }
      case _: StructType => NullType
      case primitive => primitive
    }
    dataTypeHintOpt.map(unwrap).map {
      // A value tag can only hold a primitive; a nested complex type here means the hint is not
      // usable as a primitive starting point, so infer from scratch.
      case _: StructType | _: MapType => NullType
      case primitive => primitive
    }.getOrElse(NullType)
  }

  /**
   * Extract the hint for a named attribute from a field's hint. Attributes live as fields of the
   * struct (or the struct element of an array) the field was inferred as.
   */
  private def matchingAttributeHint(
      dataTypeHintOpt: Option[DataType],
      attributeName: String): DataType = {
    val structOpt = dataTypeHintOpt match {
      case Some(st: StructType) => Some(st)
      case Some(ArrayType(st: StructType, _)) => Some(st)
      case _ => None
    }
    matchingPrimitiveHint(structOpt.flatMap(_.fields.find(_.name == attributeName).map(_.dataType)))
  }

  /** Reduce a field's hint to a struct hint for a nested object (an array uses its element). */
  private def structTypeHint(dataTypeHintOpt: Option[DataType]): Option[StructType] =
    dataTypeHintOpt match {
      case Some(st: StructType) => Some(st)
      case Some(ArrayType(st: StructType, _)) => Some(st)
      case _ => None
    }

  /**
   * Like `structTypeHint`, but for a leaf-or-mixed element whose previous inference may have been a
   * bare primitive: wrap it as a single-`valueTag` struct so the value tag's type is still hinted.
   */
  private def structTypeHintWithValueTag(dataTypeHintOpt: Option[DataType]): Option[StructType] =
    dataTypeHintOpt match {
      case Some(st: StructType) => Some(st)
      case Some(ArrayType(st: StructType, _)) => Some(st)
      case Some(primitive) => Some(new StructType().add(options.valueTag, primitive))
      case None => None
    }

  /**
   * The `tryParse*` methods below form a cascade: each attempts to parse the value as its own
   * type and, on failure, delegates to the next-wider type in the inference lattice
   * (Long -> Decimal -> Double -> Time/Date -> TimestampNTZ -> Timestamp -> Boolean -> String).
   * Entering the cascade at the parser matching `typeSoFar` (see `inferFrom`) therefore only ever
   * widens the type, never narrows it. This mirrors `CSVInferSchema`'s `tryParse*` chain.
   */
  private def tryParseLong(field: String): DataType = {
    val signSafeValue = if (field.startsWith("+") || field.startsWith("-")) {
      field.substring(1)
    } else {
      field
    }
    // A little shortcut to avoid trying many formatters in the common case that
    // the input isn't a number. All built-in formats will start with a digit.
    if (signSafeValue.isEmpty || !Character.isDigit(signSafeValue.head)) {
      return tryParseDecimal(field)
    }
    if ((allCatch opt signSafeValue.toLong).isDefined) {
      LongType
    } else {
      tryParseDecimal(field)
    }
  }

  private def tryParseDecimal(field: String): DataType = {
    val signSafeValue = if (field.startsWith("+") || field.startsWith("-")) {
      field.substring(1)
    } else {
      field
    }
    // A little shortcut to avoid trying many formatters in the common case that
    // the input isn't a decimal. All built-in formats will start with a digit or period.
    if (signSafeValue.isEmpty ||
      !(Character.isDigit(signSafeValue.head) || signSafeValue.head == '.')) {
      return tryParseDouble(field)
    }
    // Rule out strings ending in D or F, as they will parse as double but should be disallowed
    if (signSafeValue.last match {
      case 'd' | 'D' | 'f' | 'F' => true
      case _ => false
    }) {
      return tryParseDouble(field)
    }
    val decimalTry = if (options.prefersDecimal) {
      allCatch opt {
        val bigDecimal = decimalParser(field)
        DecimalType(Math.max(bigDecimal.precision, bigDecimal.scale), bigDecimal.scale)
      }
    } else {
      None
    }
    decimalTry.getOrElse(tryParseDouble(field))
  }

  private def tryParseDouble(field: String): DataType = {
    val signSafeValue = if (field.startsWith("+") || field.startsWith("-")) {
      field.substring(1)
    } else {
      field
    }
    // A little shortcut to avoid trying many formatters in the common case that
    // the input isn't a double. All built-in formats will start with a digit or period.
    val isDouble =
      if (signSafeValue.isEmpty ||
        !(Character.isDigit(signSafeValue.head) || signSafeValue.head == '.')) {
        false
      } else if (signSafeValue.last match {
          // Rule out strings ending in D or F,
          // as they will parse as double but should be disallowed
          case 'd' | 'D' | 'f' | 'F' => true
          case _ => false
        }) {
        false
      } else {
        (allCatch opt signSafeValue.toDouble).isDefined
      }
    if (isDouble) {
      DoubleType
    } else if (isTimeTypeEnabled && isTime(field)) {
      // TIME is tried ahead of date/timestamp, matching the ordering of the previous cascade.
      TimeType(TimeType.DEFAULT_PRECISION)
    } else {
      tryParseDate(field)
    }
  }

  private def tryParseTime(field: String): DataType = {
    if (isTimeTypeEnabled && isTime(field)) {
      TimeType(TimeType.DEFAULT_PRECISION)
    } else {
      tryParseDate(field)
    }
  }

  private def tryParseDate(field: String): DataType = {
    if ((allCatch opt dateFormatter.parse(field)).isDefined) {
      DateType
    } else {
      tryParseTimestampNTZ(field)
    }
  }

  private def tryParseTimestampNTZ(field: String): DataType = {
    // We can only parse the value as TimestampNTZType if it does not have zone-offset or
    // time-zone component and can be parsed with the timestamp formatter.
    // Otherwise, it is likely to be a timestamp with timezone.
    val timestampType = SQLConf.get.timestampType
    try {
      if ((SQLConf.get.legacyTimeParserPolicy == LegacyBehaviorPolicy.LEGACY ||
        timestampType == TimestampNTZType) &&
        timestampNTZFormatter.parseWithoutTimeZoneOptional(field, false).isDefined) {
        // Prefer nanosecond type when there is a nonzero sub-microsecond component
        // (nanosWithinMicro != 0) that TimestampNTZType cannot represent.
        val hasSubMicro = SQLConf.get.timestampNanosTypesEnabled &&
          timestampNTZFormatter.parseWithoutTimeZoneNanosOptional(field, 9, false)
            .exists(_.nanosWithinMicro != 0)
        if (hasSubMicro) {
          return TimestampNTZNanosType(9)
        }
        return timestampType
      }
    } catch {
      case _: Exception =>
    }
    tryParseTimestamp(field)
  }

  private def tryParseTimestamp(field: String): DataType = {
    val isTimestamp =
      try {
        timestampFormatter.parseOptional(field).isDefined
      } catch {
        case _: IllegalArgumentException => false
      }
    if (isTimestamp) {
      TimestampType
    } else {
      tryParseBoolean(field)
    }
  }

  private def tryParseBoolean(field: String): DataType = {
    field.toLowerCase(Locale.ROOT) match {
      case "true" | "false" => BooleanType
      case _ => StringType
    }
  }

  private def isTime(value: String): Boolean = {
    (allCatch opt timeFormatter.parse(value)).isDefined
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
    // AnyTimestampNanoType extends DatetimeType but is not covered by findWiderDateTimeType;
    // handle it first to avoid a MatchError inside TypeCoercion.findTightestCommonType.
    // StructType and ArrayType are also handled here so that compatibleType is used recursively
    // for nested field types, preserving the nano-timestamp downgrade logic at all nesting levels.
    // (TypeCoercion.findTightestCommonType handles same-structure StructType/ArrayType via
    // findTypeForComplex, which calls findWiderDateTimeType and would bypass the custom logic.)
    (t1, t2) match {
      case (n1: TimestampNTZNanosType, n2: TimestampNTZNanosType) =>
        return TimestampNTZNanosType(math.max(n1.precision, n2.precision))
      case (n1: TimestampLTZNanosType, n2: TimestampLTZNanosType) =>
        return TimestampLTZNanosType(math.max(n1.precision, n2.precision))
      case (_: TimestampNTZNanosType, TimestampNTZType) |
          (TimestampNTZType, _: TimestampNTZNanosType) =>
        return TimestampNTZType
      case (_: AnyTimestampNanoType, _: DatetimeType) |
          (_: DatetimeType, _: AnyTimestampNanoType) =>
        return TimestampType
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
        return StructType(newFields.toArray.sortBy(_.name))
      case (ArrayType(elementType1, containsNull1), ArrayType(elementType2, containsNull2)) =>
        return ArrayType(
          compatibleType(caseSensitive, valueTag)(elementType1, elementType2),
          containsNull1 || containsNull2)
      case _ =>
    }

    TypeCoercion.findTightestCommonType(t1, t2).getOrElse {
      // t1 or t2 is an unexpected type combination (DecimalType variants, valueTag structs, etc.)
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
