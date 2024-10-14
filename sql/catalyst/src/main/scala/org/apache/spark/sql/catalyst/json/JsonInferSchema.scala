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

package org.apache.spark.sql.catalyst.json

import java.io.{CharConversionException, FileNotFoundException, IOException}
import java.nio.charset.MalformedInputException
import java.util.Comparator

import scala.util.control.Exception.allCatch

import com.fasterxml.jackson.core._

import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.analysis.TypeCoercion
import org.apache.spark.sql.catalyst.expressions.ExprUtils
import org.apache.spark.sql.catalyst.json.JacksonUtils.nextUntil
import org.apache.spark.sql.catalyst.util._
import org.apache.spark.sql.catalyst.util.LegacyDateFormats.FAST_DATE_FORMAT
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.internal.{LegacyBehaviorPolicy, SQLConf}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.ArrayImplicits._
import org.apache.spark.util.Utils

class JsonInferSchema(options: JSONOptions) extends Serializable with Logging {

  private val decimalParser = ExprUtils.getDecimalParser(options.locale)

  private val timestampFormatter = TimestampFormatter(
    options.timestampFormatInRead,
    options.zoneId,
    options.locale,
    legacyFormat = FAST_DATE_FORMAT,
    isParsing = true)
  private val timestampNTZFormatter = TimestampFormatter(
    options.timestampNTZFormatInRead,
    options.zoneId,
    legacyFormat = FAST_DATE_FORMAT,
    isParsing = true,
    forTimestampNTZ = true)

  private val ignoreCorruptFiles = options.ignoreCorruptFiles
  private val ignoreMissingFiles = options.ignoreMissingFiles
  private val isDefaultNTZ = SQLConf.get.timestampType == TimestampNTZType
  private val legacyMode = SQLConf.get.legacyTimeParserPolicy == LegacyBehaviorPolicy.LEGACY

  private def handleJsonErrorsByParseMode(parseMode: ParseMode,
      columnNameOfCorruptRecord: String, e: Throwable): Option[StructType] = {
    parseMode match {
      case PermissiveMode =>
        Some(StructType(Array(StructField(columnNameOfCorruptRecord, StringType))))
      case DropMalformedMode =>
        None
      case FailFastMode =>
        throw QueryExecutionErrors.malformedRecordsDetectedInSchemaInferenceError(e)
    }
  }

  /**
   * Infer the type of a collection of json records in three stages:
   *   1. Infer the type of each record
   *   2. Merge types by choosing the lowest type necessary to cover equal keys
   *   3. Replace any remaining null fields with string, the top type
   */
  def infer[T](
      json: RDD[T],
      createParser: (JsonFactory, T) => JsonParser,
      isReadFile: Boolean = false): StructType = {
    val parseMode = options.parseMode
    val columnNameOfCorruptRecord = options.columnNameOfCorruptRecord

    // In each RDD partition, perform schema inference on each row and merge afterwards.
    val typeMerger = JsonInferSchema.compatibleRootType(columnNameOfCorruptRecord, parseMode)
    val mergedTypesFromPartitions = json.mapPartitions { iter =>
      val factory = options.buildJsonFactory()
      iter.flatMap { row =>
        try {
          Utils.tryWithResource(createParser(factory, row)) { parser =>
            parser.nextToken()
            Some(inferField(parser))
          }
        } catch {
          // If we are not reading from files but hit `RuntimeException`, it means corrupted record.
          case e: RuntimeException if !isReadFile =>
            handleJsonErrorsByParseMode(parseMode, columnNameOfCorruptRecord, e)
          case e @ (_: JsonProcessingException | _: MalformedInputException) =>
            handleJsonErrorsByParseMode(parseMode, columnNameOfCorruptRecord, e)
          case e: CharConversionException if options.encoding.isEmpty =>
            val msg =
              """JSON parser cannot handle a character in its input.
                |Specifying encoding as an input option explicitly might help to resolve the issue.
                |""".stripMargin + e.getMessage
            val wrappedCharException = new CharConversionException(msg)
            wrappedCharException.initCause(e)
            handleJsonErrorsByParseMode(parseMode, columnNameOfCorruptRecord, wrappedCharException)
          case e: FileNotFoundException if ignoreMissingFiles =>
            logWarning("Skipped missing file", e)
            Some(StructType(Nil))
          case e: FileNotFoundException if !ignoreMissingFiles => throw e
          case e @ (_: IOException | _: RuntimeException) if ignoreCorruptFiles =>
            logWarning("Skipped the rest of the content in the corrupted file", e)
            Some(StructType(Nil))
        }
      }.reduceOption(typeMerger).iterator
    }

    // Here we manually submit a fold-like Spark job, so that we can set the SQLConf when running
    // the fold functions in the scheduler event loop thread.
    val existingConf = SQLConf.get
    var rootType: DataType = StructType(Nil)
    val foldPartition = (iter: Iterator[DataType]) => iter.fold(StructType(Nil))(typeMerger)
    val mergeResult = (index: Int, taskResult: DataType) => {
      rootType = SQLConf.withExistingConf(existingConf) {
        typeMerger(rootType, taskResult)
      }
    }
    json.sparkContext.runJob(mergedTypesFromPartitions, foldPartition, mergeResult)

    canonicalizeType(rootType, options)
      .find(_.isInstanceOf[StructType])
      // canonicalizeType erases all empty structs, including the only one we want to keep
      .getOrElse(StructType(Nil)).asInstanceOf[StructType]
  }

  /**
   * Infer the type of a json document from the parser's token stream
   */
  def inferField(parser: JsonParser): DataType = {
    import com.fasterxml.jackson.core.JsonToken._
    parser.getCurrentToken match {
      case null | VALUE_NULL => NullType

      case FIELD_NAME =>
        parser.nextToken()
        inferField(parser)

      case VALUE_STRING if parser.getTextLength < 1 =>
        // Zero length strings and nulls have special handling to deal
        // with JSON generators that do not distinguish between the two.
        // To accurately infer types for empty strings that are really
        // meant to represent nulls we assume that the two are isomorphic
        // but will defer treating null fields as strings until all the
        // record fields' types have been combined.
        NullType

      case VALUE_STRING =>
        val field = parser.getText
        lazy val decimalTry = allCatch opt {
          val bigDecimal = decimalParser(field)
            DecimalType(bigDecimal.precision, bigDecimal.scale)
        }
        if (options.prefersDecimal && decimalTry.isDefined) {
          decimalTry.get
        } else if (options.inferTimestamp) {
          // For text-based format, it's ambiguous to infer a timestamp string without timezone, as
          // it can be both TIMESTAMP LTZ and NTZ. To avoid behavior changes with the new support
          // of NTZ, here we only try to infer NTZ if the config is set to use NTZ by default.
          if (isDefaultNTZ &&
            timestampNTZFormatter.parseWithoutTimeZoneOptional(field, false).isDefined) {
            TimestampNTZType
          } else if (timestampFormatter.parseOptional(field).isDefined) {
            TimestampType
          } else if (legacyMode) {
            val utf8Value = UTF8String.fromString(field)
            // There was a mistake that we use TIMESTAMP NTZ parser to infer LTZ type with legacy
            // mode. The mistake makes it easier to infer TIMESTAMP LTZ type and we have to keep
            // this behavior now. See SPARK-46769 for more details.
            if (SparkDateTimeUtils.stringToTimestampWithoutTimeZone(utf8Value, false).isDefined) {
              TimestampType
            } else {
              StringType
            }
          } else {
            StringType
          }
        } else {
          StringType
        }

      case START_OBJECT =>
        val builder = Array.newBuilder[StructField]
        while (nextUntil(parser, END_OBJECT)) {
          builder += StructField(
            parser.currentName,
            inferField(parser),
            nullable = true)
        }
        val fields: Array[StructField] = builder.result()
        // Note: other code relies on this sorting for correctness, so don't remove it!
        java.util.Arrays.sort(fields, JsonInferSchema.structFieldComparator)
        StructType(fields)

      case START_ARRAY =>
        // If this JSON array is empty, we use NullType as a placeholder.
        // If this array is not empty in other JSON objects, we can resolve
        // the type as we pass through all JSON objects.
        var elementType: DataType = NullType
        while (nextUntil(parser, END_ARRAY)) {
          elementType = JsonInferSchema.compatibleType(
            elementType, inferField(parser))
        }

        ArrayType(elementType)

      case (VALUE_NUMBER_INT | VALUE_NUMBER_FLOAT) if options.primitivesAsString => StringType

      case (VALUE_TRUE | VALUE_FALSE) if options.primitivesAsString => StringType

      case VALUE_NUMBER_INT | VALUE_NUMBER_FLOAT =>
        import JsonParser.NumberType._
        parser.getNumberType match {
          // For Integer values, use LongType by default.
          case INT | LONG => LongType
          // Since we do not have a data type backed by BigInteger,
          // when we see a Java BigInteger, we use DecimalType.
          case BIG_INTEGER | BIG_DECIMAL =>
            val v = parser.getDecimalValue
            if (Math.max(v.precision(), v.scale()) <= DecimalType.MAX_PRECISION) {
              DecimalType(Math.max(v.precision(), v.scale()), v.scale())
            } else {
              DoubleType
            }
          case FLOAT | DOUBLE if options.prefersDecimal =>
            val v = parser.getDecimalValue
            if (Math.max(v.precision(), v.scale()) <= DecimalType.MAX_PRECISION) {
              DecimalType(Math.max(v.precision(), v.scale()), v.scale())
            } else {
              DoubleType
            }
          case FLOAT | DOUBLE =>
            DoubleType
        }

      case VALUE_TRUE | VALUE_FALSE => BooleanType

      case _ =>
        throw QueryExecutionErrors.malformedJSONError()
    }
  }

  /**
   * Recursively canonicalizes inferred types, e.g., removes StructTypes with no fields,
   * drops NullTypes or converts them to StringType based on provided options.
   */
  private[catalyst] def canonicalizeType(
      tpe: DataType, options: JSONOptions): Option[DataType] = tpe match {
    case at: ArrayType =>
      canonicalizeType(at.elementType, options)
        .map(t => at.copy(elementType = t))

    case StructType(fields) =>
      val canonicalFields = fields.filter(_.name.nonEmpty).flatMap { f =>
        canonicalizeType(f.dataType, options)
          .map(t => f.copy(dataType = t))
      }
      // SPARK-8093: empty structs should be deleted
      if (canonicalFields.isEmpty) {
        None
      } else {
        Some(StructType(canonicalFields))
      }

    case NullType =>
      if (options.dropFieldIfAllNull) {
        None
      } else {
        Some(StringType)
      }

    case other => Some(other)
  }
}

object JsonInferSchema {
  val structFieldComparator = new Comparator[StructField] {
    override def compare(o1: StructField, o2: StructField): Int = {
      o1.name.compareTo(o2.name)
    }
  }

  def isSorted(arr: Array[StructField]): Boolean = {
    var i: Int = 0
    while (i < arr.length - 1) {
      if (structFieldComparator.compare(arr(i), arr(i + 1)) > 0) {
        return false
      }
      i += 1
    }
    true
  }

  def withCorruptField(
      struct: StructType,
      other: DataType,
      columnNameOfCorruptRecords: String,
      parseMode: ParseMode): StructType = parseMode match {
    case PermissiveMode =>
      // If we see any other data type at the root level, we get records that cannot be
      // parsed. So, we use the struct as the data type and add the corrupt field to the schema.
      if (!struct.fieldNames.contains(columnNameOfCorruptRecords)) {
        // If this given struct does not have a column used for corrupt records,
        // add this field.
        val newFields: Array[StructField] =
        StructField(columnNameOfCorruptRecords, StringType, nullable = true) +: struct.fields
        // Note: other code relies on this sorting for correctness, so don't remove it!
        java.util.Arrays.sort(newFields, structFieldComparator)
        StructType(newFields)
      } else {
        // Otherwise, just return this struct.
        struct
      }

    case DropMalformedMode =>
      // If corrupt record handling is disabled we retain the valid schema and discard the other.
      struct

    case FailFastMode =>
      // If `other` is not struct type, consider it as malformed one and throws an exception.
      throw QueryExecutionErrors.malformedRecordsDetectedInSchemaInferenceError(other)
  }

  /**
   * Remove top-level ArrayType wrappers and merge the remaining schemas
   */
  def compatibleRootType(
      columnNameOfCorruptRecords: String,
      parseMode: ParseMode): (DataType, DataType) => DataType = {
    // Since we support array of json objects at the top level,
    // we need to check the element type and find the root level data type.
    case (ArrayType(ty1, _), ty2) =>
      compatibleRootType(columnNameOfCorruptRecords, parseMode)(ty1, ty2)
    case (ty1, ArrayType(ty2, _)) =>
      compatibleRootType(columnNameOfCorruptRecords, parseMode)(ty1, ty2)
    // Discard null/empty documents
    case (struct: StructType, NullType) => struct
    case (NullType, struct: StructType) => struct
    case (struct: StructType, o) if !o.isInstanceOf[StructType] =>
      withCorruptField(struct, o, columnNameOfCorruptRecords, parseMode)
    case (o, struct: StructType) if !o.isInstanceOf[StructType] =>
      withCorruptField(struct, o, columnNameOfCorruptRecords, parseMode)
    // If we get anything else, we call compatibleType.
    // Usually, when we reach here, ty1 and ty2 are two StructTypes.
    case (ty1, ty2) => compatibleType(ty1, ty2)
  }

  private[this] val emptyStructFieldArray = Array.empty[StructField]

  /**
   * Returns the most general data type for two given data types.
   * When the two types are incompatible, return `defaultDataType` as a fallback result.
   */
  def compatibleType(
      t1: DataType, t2: DataType, defaultDataType: DataType = StringType): DataType = {
    TypeCoercion.findTightestCommonType(t1, t2).getOrElse {
      // t1 or t2 is a StructType, ArrayType, or an unexpected type.
      (t1, t2) match {
        // Double support larger range than fixed decimal, DecimalType.Maximum should be enough
        // in most case, also have better precision.
        case (DoubleType, _: DecimalType) | (_: DecimalType, DoubleType) =>
          DoubleType

        // This branch is only used by `SchemaOfVariant.mergeSchema` because `JsonInferSchema` never
        // produces `FloatType`.
        case (FloatType, _: DecimalType) | (_: DecimalType, FloatType) =>
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

        case (StructType(fields1), StructType(fields2)) =>
          // Both fields1 and fields2 should be sorted by name, since inferField performs sorting.
          // Therefore, we can take advantage of the fact that we're merging sorted lists and skip
          // building a hash map or performing additional sorting.
          assert(isSorted(fields1),
            s"${StructType.simpleString}'s fields were not sorted: ${fields1.toImmutableArraySeq}")
          assert(isSorted(fields2),
            s"${StructType.simpleString}'s fields were not sorted: ${fields2.toImmutableArraySeq}")

          val newFields = new java.util.ArrayList[StructField]()

          var f1Idx = 0
          var f2Idx = 0

          while (f1Idx < fields1.length && f2Idx < fields2.length) {
            val f1Name = fields1(f1Idx).name
            val f2Name = fields2(f2Idx).name
            val comp = f1Name.compareTo(f2Name)
            if (comp == 0) {
              val dataType = compatibleType(
                fields1(f1Idx).dataType, fields2(f2Idx).dataType, defaultDataType)
              newFields.add(StructField(f1Name, dataType, nullable = true))
              f1Idx += 1
              f2Idx += 1
            } else if (comp < 0) { // f1Name < f2Name
              newFields.add(fields1(f1Idx))
              f1Idx += 1
            } else { // f1Name > f2Name
              newFields.add(fields2(f2Idx))
              f2Idx += 1
            }
          }
          while (f1Idx < fields1.length) {
            newFields.add(fields1(f1Idx))
            f1Idx += 1
          }
          while (f2Idx < fields2.length) {
            newFields.add(fields2(f2Idx))
            f2Idx += 1
          }
          StructType(newFields.toArray(emptyStructFieldArray))

        case (ArrayType(elementType1, containsNull1), ArrayType(elementType2, containsNull2)) =>
          ArrayType(
            compatibleType(elementType1, elementType2, defaultDataType),
            containsNull1 || containsNull2)

        // The case that given `DecimalType` is capable of given `IntegralType` is handled in
        // `findTightestCommonType`. Both cases below will be executed only when the given
        // `DecimalType` is not capable of the given `IntegralType`.
        case (t1: IntegralType, t2: DecimalType) =>
          compatibleType(DecimalType.forType(t1), t2, defaultDataType)
        case (t1: DecimalType, t2: IntegralType) =>
          compatibleType(t1, DecimalType.forType(t2), defaultDataType)

        case (TimestampNTZType, TimestampType) | (TimestampType, TimestampNTZType) =>
          TimestampType

        case (_, _) => defaultDataType
      }
    }
  }
}
