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

package org.apache.spark.sql.execution.datasources.parquet

import java.math.{BigDecimal, BigInteger}
import java.nio.ByteOrder
import java.nio.charset.StandardCharsets
import java.time.{ZoneId, ZoneOffset}

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

import org.apache.parquet.column.Dictionary
import org.apache.parquet.io.api.{Binary, Converter, GroupConverter, PrimitiveConverter}
import org.apache.parquet.schema.{DecimalMetadata, GroupType, MessageType, OriginalType, Type}
import org.apache.parquet.schema.OriginalType._
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName._

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.util._
import org.apache.spark.sql.execution.datasources.DataSourceUtils
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.SQLConf.LegacyBehaviorPolicy
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

/**
 * A [[ParentContainerUpdater]] is used by a Parquet converter to set converted values to some
 * corresponding parent container. For example, a converter for a `StructType` field may set
 * converted values to a [[InternalRow]]; or a converter for array elements may append converted
 * values to an [[ArrayBuffer]].
 */
private[parquet] trait ParentContainerUpdater {
  /** Called before a record field is being converted */
  def start(): Unit = ()

  /** Called after a record field is being converted */
  def end(): Unit = ()

  def set(value: Any): Unit = ()
  def setBoolean(value: Boolean): Unit = set(value)
  def setByte(value: Byte): Unit = set(value)
  def setShort(value: Short): Unit = set(value)
  def setInt(value: Int): Unit = set(value)
  def setLong(value: Long): Unit = set(value)
  def setFloat(value: Float): Unit = set(value)
  def setDouble(value: Double): Unit = set(value)
}

/** A no-op updater used for root converter (who doesn't have a parent). */
private[parquet] object NoopUpdater extends ParentContainerUpdater

private[parquet] trait HasParentContainerUpdater {
  def updater: ParentContainerUpdater
}

/**
 * A convenient converter class for Parquet group types with a [[HasParentContainerUpdater]].
 */
private[parquet] abstract class ParquetGroupConverter(val updater: ParentContainerUpdater)
  extends GroupConverter with HasParentContainerUpdater

/**
 * Parquet converter for Parquet primitive types.  Note that not all Spark SQL atomic types
 * are handled by this converter.  Parquet primitive types are only a subset of those of Spark
 * SQL.  For example, BYTE, SHORT, and INT in Spark SQL are all covered by INT32 in Parquet.
 */
private[parquet] class ParquetPrimitiveConverter(val updater: ParentContainerUpdater)
  extends PrimitiveConverter with HasParentContainerUpdater {

  override def addBoolean(value: Boolean): Unit = updater.setBoolean(value)
  override def addInt(value: Int): Unit = updater.setInt(value)
  override def addLong(value: Long): Unit = updater.setLong(value)
  override def addFloat(value: Float): Unit = updater.setFloat(value)
  override def addDouble(value: Double): Unit = updater.setDouble(value)
  override def addBinary(value: Binary): Unit = updater.set(value.getBytes)
}

/**
 * A [[ParquetRowConverter]] is used to convert Parquet records into Catalyst [[InternalRow]]s.
 * Since Catalyst `StructType` is also a Parquet record, this converter can be used as root
 * converter.  Take the following Parquet type as an example:
 * {{{
 *   message root {
 *     required int32 f1;
 *     optional group f2 {
 *       required double f21;
 *       optional binary f22 (utf8);
 *     }
 *   }
 * }}}
 * 5 converters will be created:
 *
 * - a root [[ParquetRowConverter]] for [[MessageType]] `root`, which contains:
 *   - a [[ToSparkIntegerConverter]] for required [[INT_32]] field `f1`, and
 *   - a nested [[ParquetRowConverter]] for optional [[GroupType]] `f2`, which contains:
 *     - a [[ToSparkDoubleConverter]] for required [[DOUBLE]] field `f21`, and
 *     - a [[ToSparkStringConverter]] for optional [[UTF8]] string field `f22`
 *
 * When used as a root converter, [[NoopUpdater]] should be used since root converters don't have
 * any "parent" container.
 *
 * @param schemaConverter A utility converter used to convert Parquet types to Catalyst types.
 * @param parquetType Parquet schema of Parquet records
 * @param catalystType Spark SQL schema that corresponds to the Parquet record type. User-defined
 *        types should have been expanded.
 * @param convertTz Timezone ID of the session
 * @param datetimeRebaseMode the mode of rebasing date/timestamp from Julian to Proleptic Gregorian
 *                           calendar
 * @param updater An updater which propagates converted field values to the parent container
 * @param convertInt96Timestamp Whether to use session timezone to convert to int96 data
 */
private[parquet] class ParquetRowConverter(
    schemaConverter: ParquetToSparkSchemaConverter,
    parquetType: GroupType,
    catalystType: StructType,
    convertTz: ZoneId,
    datetimeRebaseMode: LegacyBehaviorPolicy.Value,
    updater: ParentContainerUpdater,
    convertInt96Timestamp: Boolean)
  extends ParquetGroupConverter(updater) with Logging {

  assert(
    parquetType.getFieldCount <= catalystType.length,
    s"""Field count of the Parquet schema is greater than the field count of the Catalyst schema:
       |
       |Parquet schema:
       |$parquetType
       |Catalyst schema:
       |${catalystType.prettyJson}
     """.stripMargin)

  assert(
    !catalystType.existsRecursively(_.isInstanceOf[UserDefinedType[_]]),
    s"""User-defined types in Catalyst schema should have already been expanded:
       |${catalystType.prettyJson}
     """.stripMargin)

  logDebug(
    s"""Building row converter for the following schema:
       |
       |Parquet form:
       |$parquetType
       |Catalyst form:
       |${catalystType.prettyJson}
     """.stripMargin)

  /**
   * Updater used together with field converters within a [[ParquetRowConverter]].  It propagates
   * converted filed values to the `ordinal`-th cell in `currentRow`.
   */
  private final class RowUpdater(row: InternalRow, ordinal: Int) extends ParentContainerUpdater {
    override def set(value: Any): Unit = row(ordinal) = value
    override def setBoolean(value: Boolean): Unit = row.setBoolean(ordinal, value)
    override def setByte(value: Byte): Unit = row.setByte(ordinal, value)
    override def setShort(value: Short): Unit = row.setShort(ordinal, value)
    override def setInt(value: Int): Unit = row.setInt(ordinal, value)
    override def setLong(value: Long): Unit = row.setLong(ordinal, value)
    override def setDouble(value: Double): Unit = row.setDouble(ordinal, value)
    override def setFloat(value: Float): Unit = row.setFloat(ordinal, value)
  }

  private[this] val currentRow = new SpecificInternalRow(catalystType.map(_.dataType))

  /**
   * The [[InternalRow]] converted from an entire Parquet record.
   */
  def currentRecord: InternalRow = currentRow

  private val dateRebaseFunc = DataSourceUtils.creteDateRebaseFuncInRead(
    datetimeRebaseMode, "Parquet")

  private val timestampRebaseFunc = DataSourceUtils.creteTimestampRebaseFuncInRead(
    datetimeRebaseMode, "Parquet")

  private lazy val dateFormatter = DateFormatter(convertTz)
  private lazy val timestampFormatter = TimestampFormatter.getFractionFormatter(convertTz)

  // Converters for each field.
  private[this] val fieldConverters: Array[Converter with HasParentContainerUpdater] = {
    // (SPARK-31116) Use case insensitive map if spark.sql.caseSensitive is false
    // to prevent throwing IllegalArgumentException when searching catalyst type's field index
    val catalystFieldNameToIndex = if (SQLConf.get.caseSensitiveAnalysis) {
      catalystType.fieldNames.zipWithIndex.toMap
    } else {
      CaseInsensitiveMap(catalystType.fieldNames.zipWithIndex.toMap)
    }
    parquetType.getFields.asScala.map { parquetField =>
      val fieldIndex = catalystFieldNameToIndex(parquetField.getName)
      val catalystField = catalystType(fieldIndex)
      // Converted field value should be set to the `fieldIndex`-th cell of `currentRow`
      newConverter(parquetField, catalystField.dataType, new RowUpdater(currentRow, fieldIndex))
    }.toArray
  }

  // Updaters for each field.
  private[this] val fieldUpdaters: Array[ParentContainerUpdater] = fieldConverters.map(_.updater)

  override def getConverter(fieldIndex: Int): Converter = fieldConverters(fieldIndex)

  override def end(): Unit = {
    var i = 0
    while (i < fieldUpdaters.length) {
      fieldUpdaters(i).end()
      i += 1
    }
    updater.set(currentRow)
  }

  override def start(): Unit = {
    var i = 0
    val numFields = currentRow.numFields
    while (i < numFields) {
      currentRow.setNullAt(i)
      i += 1
    }
    i = 0
    while (i < fieldUpdaters.length) {
      fieldUpdaters(i).start()
      i += 1
    }
  }

  /**
   * Creates a converter for the given Parquet type `parquetType` and Spark SQL data type
   * `catalystType`. Converted values are handled by `updater`.
   */
  private def newConverter(
      parquetType: Type,
      catalystType: DataType,
      updater: ParentContainerUpdater): Converter with HasParentContainerUpdater = {

    catalystType match {
      case BooleanType | BinaryType =>
        new ParquetPrimitiveConverter(updater)

      case FloatType =>
        new ToSparkFloatConverter(parquetType, updater)

      case DoubleType =>
        new ToSparkDoubleConverter(parquetType, updater)

      case ByteType =>
        new ToSparkByteConverter(parquetType, updater)

      case ShortType =>
        new ToSparkShortConverter(parquetType, updater)

      case IntegerType =>
        new ToSparkIntegerConverter(parquetType, updater)

      case LongType =>
        new ToSparkLongConverter(parquetType, updater)

      case t: DecimalType =>
        new ToSparkDecimalConverter(parquetType, updater, t)

      case StringType =>
        new ToSparkStringConverter(parquetType, updater)

      case DateType =>
        new ToSparkDateConverter(parquetType, updater)

      case TimestampType =>
        new ToSparkTimestampConverter(parquetType, updater)

      // A repeated field that is neither contained by a `LIST`- or `MAP`-annotated group nor
      // annotated by `LIST` or `MAP` should be interpreted as a required list of required
      // elements where the element type is the type of the field.
      case t: ArrayType if parquetType.getOriginalType != LIST =>
        if (parquetType.isPrimitive) {
          new RepeatedPrimitiveConverter(parquetType, t.elementType, updater)
        } else {
          new RepeatedGroupConverter(parquetType, t.elementType, updater)
        }

      case t: ArrayType =>
        new ParquetArrayConverter(parquetType.asGroupType(), t, updater)

      case t: MapType =>
        new ParquetMapConverter(parquetType.asGroupType(), t, updater)

      case t: StructType =>
        val wrappedUpdater = {
          // SPARK-30338: avoid unnecessary InternalRow copying for nested structs:
          // There are two cases to handle here:
          //
          //  1. Parent container is a map or array: we must make a deep copy of the mutable row
          //     because this converter may be invoked multiple times per Parquet input record
          //     (if the map or array contains multiple elements).
          //
          //  2. Parent container is a struct: we don't need to copy the row here because either:
          //
          //     (a) all ancestors are structs and therefore no copying is required because this
          //         converter will only be invoked once per Parquet input record, or
          //     (b) some ancestor is struct that is nested in a map or array and that ancestor's
          //         converter will perform deep-copying (which will recursively copy this row).
          if (updater.isInstanceOf[RowUpdater]) {
            // `updater` is a RowUpdater, implying that the parent container is a struct.
            updater
          } else {
            // `updater` is NOT a RowUpdater, implying that the parent container a map or array.
            new ParentContainerUpdater {
              override def set(value: Any): Unit = {
                updater.set(value.asInstanceOf[SpecificInternalRow].copy())  // deep copy
              }
            }
          }
        }
        new ParquetRowConverter(
          schemaConverter,
          parquetType.asGroupType(),
          t,
          convertTz,
          datetimeRebaseMode,
          wrappedUpdater,
          convertInt96Timestamp)

      case t =>
        throw new RuntimeException(
          s"Unable to create Parquet converter for data type ${t.json} " +
            s"whose Parquet type is $parquetType")
    }
  }

  /**
   * Converter to spark [[NumericType]].
   * Possible parquet primitive types:
   * 1.FLOAT
   * 2.DOUBLE
   * 3.INT32(INT_8 INT_16 INT_32 DECIMAL)
   * 4.INT64(INT_64 DECIMAL)
   * 5.BINARY(UTF8 DECIMAL)
   * 6.FIXED_LEN_BYTE_ARRAY(DECIMAL)
   */
  private abstract class ToSparkNumericConverter[T: ClassTag](
      parquetType: Type, updater: ParentContainerUpdater)
    extends ParquetPrimitiveConverter(updater) {

    protected val isDecimal: Boolean = parquetType.asPrimitiveType().getOriginalType == DECIMAL
    protected val parquetDecimal: DecimalMetadata = parquetType.asPrimitiveType().getDecimalMetadata
    protected var expandedDictionary: Array[T] = null

    override def hasDictionarySupport: Boolean = {
      parquetType.asPrimitiveType().getPrimitiveTypeName match {
        case BINARY | FIXED_LEN_BYTE_ARRAY => true
        case INT32 | INT64 if isDecimal => true
        case _ => false
      }
    }
    override def setDictionary(dictionary: Dictionary): Unit = {
      this.expandedDictionary = parquetType.asPrimitiveType().getPrimitiveTypeName match {
        case INT32 if isDecimal =>
          Array.tabulate(dictionary.getMaxId + 1) { i =>
            val d = ParquetRowConverter.decimalFromLong(dictionary.decodeToInt(i), parquetDecimal)
            decimalToType(d)
          }
        case INT64 if isDecimal =>
          Array.tabulate(dictionary.getMaxId + 1) { i =>
            val d = ParquetRowConverter.decimalFromLong(dictionary.decodeToLong(i), parquetDecimal)
            decimalToType(d)
          }
        case BINARY | FIXED_LEN_BYTE_ARRAY if isDecimal =>
          Array.tabulate(dictionary.getMaxId + 1) { i =>
            val d = ParquetRowConverter
              .decimalFromBinary(dictionary.decodeToBinary(i), parquetDecimal)
            decimalToType(d)
          }
        case BINARY | FIXED_LEN_BYTE_ARRAY =>
          Array.tabulate(dictionary.getMaxId + 1) { i =>
            utf8BinaryToType(dictionary.decodeToBinary(i))
          }
        case _ =>
          throw new RuntimeException(
          s"Unable to create Parquet dictionary converter for spark NumericType " +
            s"whose Parquet type is $parquetType")
      }
    }

    protected def add(value: T): Unit
    protected def decimalToType(decimal: Decimal): T
    protected def utf8BinaryToType(binary: Binary): T
    protected def doubleToType(doubleValue: Double): T
    protected def longToType(longValue: Long): T

    override def addValueFromDictionary(dictionaryId: Int): Unit = {
      add(expandedDictionary(dictionaryId))
    }

    // parquet primitive types: FLOAT
    override def addFloat(value: Float): Unit = {
      addDouble(value: Double)
    }

    // parquet primitive types: DOUBLE
    override def addDouble(value: Double): Unit = {
      add(doubleToType(value))
    }

    // parquet primitive types: INT32(INT_8 INT_16 INT_32 DECIMAL)
    override def addInt(value: Int): Unit = {
      addLong(value)
    }

    // parquet primitive types: INT64(INT_64 DECIMAL)
    override def addLong(value: Long): Unit = {
      if (isDecimal) {
        val decimal = ParquetRowConverter.decimalFromLong(value, parquetDecimal)
        add(decimalToType(decimal))
      } else {
        add(longToType(value))
      }
    }

    // parquet primitive types: (UTF8 DECIMAL)
    override def addBinary(value: Binary): Unit = {
      if (isDecimal) {
        val decimal = ParquetRowConverter.decimalFromBinary(value, parquetDecimal)
        add(decimalToType(decimal))
      } else {
        add(utf8BinaryToType(value))
      }
    }

    protected def isFiniteFloat(floatValue: Float): Boolean =
      Float.NegativeInfinity < floatValue && floatValue < Float.PositiveInfinity

    protected def isFiniteDouble(doubleValue: Double): Boolean =
      Double.NegativeInfinity < doubleValue && doubleValue < Double.PositiveInfinity

    protected def checkToIntegralOverflow(x: Double, upperBound: Long, lowerBound: Long): Unit = {
      if (Math.floor(x) > upperBound || Math.ceil(x) < lowerBound) {
        throw new ArithmeticException(
          s"Casting Fractional($x) to Integral causes overflow, $parquetType")
      }
    }
  }

  /**
   * Converter to spark [[FloatType]].
   * Possible parquet primitive types:
   * 1.FLOAT
   * 2.DOUBLE - may overflow
   * 3.INT32(INT_8 INT_16 INT_32 DECIMAL)
   * 4.INT64(INT_64 DECIMAL)
   * 5.BINARY(UTF8 DECIMAL) - may overflow
   * 6.FIXED_LEN_BYTE_ARRAY(DECIMAL) - may overflow
   */
  private final class ToSparkFloatConverter(parquetType: Type, updater: ParentContainerUpdater)
    extends ToSparkNumericConverter[Float](parquetType, updater) {
    private val decimalMayOverflow = isDecimal && parquetDecimal.getPrecision > 38

    override protected def add(value: Float): Unit = {
      updater.setFloat(value)
    }

    override def addFloat(value: Float): Unit = {
      updater.setFloat(value)
    }

    override protected def decimalToType(decimal: Decimal): Float = {
      val floatValue = decimal.toFloat
      if (decimalMayOverflow && !isFiniteFloat(floatValue)) {
        throw new ArithmeticException(
          s"Casting Decimal(${decimal.toString}) to Float causes overflow $parquetType")
      }
      floatValue
    }

    override protected def utf8BinaryToType(binary: Binary): Float = {
      val string = ParquetRowConverter.binaryToJString(binary).trim()
      // Infinity, -Infinity, NaN
      if (string.length < 2 || string.charAt(1) <= '9') {
        val floatValue = string.toFloat
        if (!isFiniteFloat(floatValue)) {
          throw new ArithmeticException(
            s"Casting String($string) to Float causes overflow, $parquetType")
        }
        floatValue
      } else {
        val floatValue = Cast.processFloatingPointSpecialLiterals(string, isFloat = true)
        if (floatValue == null) {
          throw new NumberFormatException(
            s"This String($string) cannot be converted to Float, $parquetType")
        }
        floatValue.asInstanceOf[Float]
      }
    }

    override protected def doubleToType(doubleValue: Double): Float = {
      doubleToFloat(doubleValue)
    }

    override protected def longToType(longValue: Long): Float = longValue.toFloat

    private def doubleToFloat(doubleValue: Double): Float = {
      if ((doubleValue < Float.MinValue || doubleValue > Float.MaxValue)
        && isFiniteDouble(doubleValue)) {
        throw new ArithmeticException(
          s"Casting Double($doubleValue) to Float causes overflow, $parquetType")
      } else {
        doubleValue.toFloat
      }
    }
  }

  /**
   * Converter to spark [[DoubleType]].
   * Possible parquet primitive types:
   * 1.FLOAT
   * 2.DOUBLE
   * 3.INT32(INT_8 INT_16 INT_32 DECIMAL)
   * 4.INT64(INT_64 DECIMAL)
   * 5.BINARY(UTF8 DECIMAL) - may overflow
   * 6.FIXED_LEN_BYTE_ARRAY(DECIMAL) - may overflow
   */
  private final class ToSparkDoubleConverter(parquetType: Type, updater: ParentContainerUpdater)
    extends ToSparkNumericConverter[Double](parquetType, updater) {
    private val decimalMayOverflow = isDecimal && parquetDecimal.getPrecision > 308

    override protected def add(value: Double): Unit = {
      updater.setDouble(value)
    }

    override protected def decimalToType(decimal: Decimal): Double = {
      val doubleValue = decimal.toDouble
      if (decimalMayOverflow && !isFiniteDouble(doubleValue)) {
        throw new ArithmeticException(
          s"Casting Decimal($decimal) to Double causes overflow, $parquetType")
      }
      doubleValue
    }

    override protected def utf8BinaryToType(binary: Binary): Double = {
      val string = ParquetRowConverter.binaryToJString(binary).trim()
      // Infinity, -Infinity, NaN
      if (string.length < 2 || string.charAt(1) <= '9') {
        val doubleValue = string.toDouble
        if (!isFiniteDouble(doubleValue)) {
          throw new ArithmeticException(
            s"Casting String($string) to Double causes overflow, $parquetType")
        }
        doubleValue
      } else {
        val doubleValue = Cast.processFloatingPointSpecialLiterals(string, isFloat = false)
        if (doubleValue == null) {
          throw new NumberFormatException(
            s"This String($string) cannot be converted to Double, $parquetType")
        }
        doubleValue.asInstanceOf[Double]
      }
    }

    override protected def doubleToType(doubleValue: Double): Double = doubleValue

    override protected def longToType(longValue: Long): Double = longValue.toDouble
  }

  /**
   * Converter to spark [[ByteType]].
   * Possible parquet primitive types:
   * 1.FLOAT - may overflow
   * 2.DOUBLE - may overflow
   * 3.INT32(INT_8)
   *   INT32(INT_16 INT_32 DECIMAL) - may overflow
   * 4.INT64(INT_64 DECIMAL) - may overflow
   * 5.BINARY(UTF8 DECIMAL) - may overflow
   * 6.FIXED_LEN_BYTE_ARRAY(DECIMAL) - may overflow
   */
  private final class ToSparkByteConverter(parquetType: Type, updater: ParentContainerUpdater)
    extends ToSparkNumericConverter[Byte](parquetType, updater) {
    private val upperBound = Byte.MaxValue
    private val lowerBound = Byte.MinValue

    override protected def add(value: Byte): Unit = {
      updater.setByte(value)
    }

    override protected def decimalToType(decimal: Decimal): Byte = {
      decimal.roundToByte()
    }

    override protected def utf8BinaryToType(binary: Binary): Byte = {
      val utf8 = ParquetRowConverter.utf8StringFromBinary(binary)
      utf8.toByteExact
    }

    override protected def doubleToType(doubleValue: Double): Byte = {
      checkToIntegralOverflow(doubleValue, upperBound, lowerBound)
      doubleValue.toByte
    }

    override protected def longToType(longValue: Long): Byte = {
      if (longValue == longValue.toByte) {
        longValue.toByte
      } else {
        throw new ArithmeticException(
          s"Casting Long($longValue) to Byte causes overflow, $parquetType")
      }
    }
  }

  /**
   * Converter to spark [[ShortType]].
   * Possible parquet primitive types:
   * 1.FLOAT - may overflow
   * 2.DOUBLE - may overflow
   * 3.INT32(INT_8 INT_16)
   *   INT32(INT_32 DECIMAL) - may overflow
   * 4.INT64(INT_64 DECIMAL) - may overflow
   * 5.BINARY(UTF8 DECIMAL) - may overflow
   * 6.FIXED_LEN_BYTE_ARRAY(DECIMAL) - may overflow
   */
  private final class ToSparkShortConverter(parquetType: Type, updater: ParentContainerUpdater)
    extends ToSparkNumericConverter[Short](parquetType, updater) {
    private val upperBound = Short.MaxValue
    private val lowerBound = Short.MinValue

    override protected def add(value: Short): Unit = {
      updater.setShort(value)
    }

    override protected def decimalToType(decimal: Decimal): Short = {
      decimal.roundToShort()
    }

    override protected def utf8BinaryToType(binary: Binary): Short = {
      val utf8 = ParquetRowConverter.utf8StringFromBinary(binary)
      utf8.toShortExact
    }

    override protected def doubleToType(doubleValue: Double): Short = {
      checkToIntegralOverflow(doubleValue, upperBound, lowerBound)
      doubleValue.toShort
    }

    override protected def longToType(longValue: Long): Short = {
      if (longValue == longValue.toShort) {
        longValue.toShort
      } else {
        throw new ArithmeticException(
          s"Casting Long($longValue) to Short causes overflow, $parquetType")
      }
    }
  }

  /**
   * Converter to spark [[IntegerType]].
   * Possible parquet primitive types:
   * 1.FLOAT - may overflow
   * 2.DOUBLE - may overflow
   * 3.INT32(INT_8 INT_16 INT_32 DECIMAL)
   * 4.INT64(INT_64 DECIMAL) - may overflow
   * 5.BINARY(UTF8 DECIMAL) - may overflow
   * 6.FIXED_LEN_BYTE_ARRAY(DECIMAL) - may overflow
   */
  private final class ToSparkIntegerConverter(parquetType: Type, updater: ParentContainerUpdater)
    extends ToSparkNumericConverter[Int](parquetType, updater) {
    private val upperBound = Int.MaxValue
    private val lowerBound = Int.MinValue

    override protected def add(value: Int): Unit = {
      updater.setInt(value)
    }

    override def addInt(value: Int): Unit = {
      if (isDecimal) {
        val decimal = ParquetRowConverter.decimalFromLong(value, parquetDecimal)
        add(decimalToType(decimal))
      } else {
        add(value)
      }
    }

    override protected def decimalToType(decimal: Decimal): Int = {
      decimal.roundToInt()
    }

    override protected def utf8BinaryToType(binary: Binary): Int = {
      val utf8 = ParquetRowConverter.utf8StringFromBinary(binary)
      utf8.toIntExact
    }

    override protected def doubleToType(doubleValue: Double): Int = {
      checkToIntegralOverflow(doubleValue, upperBound, lowerBound)
      doubleValue.toInt
    }

    override protected def longToType(longValue: Long): Int = {
      if (longValue == longValue.toInt) {
        longValue.toInt
      } else {
        throw new ArithmeticException(
          s"Casting Long($longValue) to Int causes overflow, $parquetType")
      }
    }
  }

  /**
   * Converter to spark [[LongType]].
   * Possible parquet primitive types:
   * 1.FLOAT - may overflow
   * 2.DOUBLE - may overflow
   * 3.INT32(INT_8 INT_16 INT_32 DECIMAL)
   * 4.INT64(INT_64 DECIMAL)
   * 5.BINARY(UTF8 DECIMAL) - may overflow
   * 6.FIXED_LEN_BYTE_ARRAY(DECIMAL) - may overflow
   */
  private final class ToSparkLongConverter(parquetType: Type, updater: ParentContainerUpdater)
    extends ToSparkNumericConverter[Long](parquetType, updater) {
    private val upperBound = Long.MaxValue
    private val lowerBound = Long.MinValue

    override protected def add(value: Long): Unit = {
      updater.setLong(value)
    }

    override protected def decimalToType(decimal: Decimal): Long = {
      decimal.roundToLong()
    }

    override protected def utf8BinaryToType(binary: Binary): Long = {
      val utf8 = ParquetRowConverter.utf8StringFromBinary(binary)
      utf8.toLongExact
    }

    override protected def doubleToType(doubleValue: Double): Long = {
      checkToIntegralOverflow(doubleValue, upperBound, lowerBound)
      doubleValue.toLong
    }

    override protected def longToType(longValue: Long): Long = longValue
  }

  /**
   * Converter to spark [[DecimalType]].
   * Possible parquet primitive types:
   * 1.FLOAT - may overflow
   * 2.DOUBLE - may overflow
   * 3.INT32(INT_8 INT_16 INT_32 DECIMAL) - may overflow
   * 4.INT64(INT_64 DECIMAL) - may overflow
   * 5.BINARY(UTF8 DECIMAL) - may overflow
   * 6.FIXED_LEN_BYTE_ARRAY(DECIMAL) - may overflow
   */
  private final class ToSparkDecimalConverter(
    parquetType: Type, updater: ParentContainerUpdater, sparkDecimal: DecimalType)
    extends ToSparkNumericConverter[Decimal](parquetType, updater) {
    private val sparkPrecision = sparkDecimal.precision
    private val sparkScale = sparkDecimal.scale

    override def hasDictionarySupport: Boolean = {
      parquetType.asPrimitiveType().getPrimitiveTypeName match {
        case FLOAT | DOUBLE => true
        case _ => super.hasDictionarySupport
      }
    }

    override def setDictionary(dictionary: Dictionary): Unit = {
      this.expandedDictionary = parquetType.asPrimitiveType().getPrimitiveTypeName match {
        case FLOAT =>
          Array.tabulate(dictionary.getMaxId + 1) { i =>
            doubleToType(dictionary.decodeToFloat(i))
          }
        case DOUBLE =>
          Array.tabulate(dictionary.getMaxId + 1) { i =>
            doubleToType(dictionary.decodeToDouble(i))
          }
        case _ => null
      }
      super.setDictionary(dictionary)
    }

    override protected def add(value: Decimal): Unit = {
      updater.set(value)
    }

    override protected def decimalToType(decimal: Decimal): Decimal = {
      decimalToPrecision(decimal)
    }

    override protected def utf8BinaryToType(binary: Binary): Decimal = {
      val string = ParquetRowConverter.binaryToJString(binary)
      val decimal = Decimal(new BigDecimal(string.trim))
      decimalToPrecision(decimal)
    }

    override protected def doubleToType(doubleValue: Double): Decimal = {
      decimalToPrecision(Decimal(doubleValue))
    }

    override protected def longToType(longValue: Long): Decimal = {
      decimalToPrecision(Decimal(longValue))
    }

    private def decimalToPrecision(decimal: Decimal): Decimal = {
      if (!decimal.changePrecision(sparkPrecision, sparkScale, Decimal.ROUND_DOWN)) {
        throw new ArithmeticException(s"Decimal($decimal) cannot be represented as " +
          s"Decimal($sparkPrecision, $sparkScale), $parquetType")
      }
      decimal
    }
  }

  /**
   * Converter to spark [[DateType]].
   * Possible parquet primitive types:
   * 1.INT32(DATE)
   * 2.INT64(TIMESTAMP_MILLIS TIMESTAMP_MICROS)
   * 3.INT96
   * 4.BINARY(UTF8)
   */
  private final class ToSparkDateConverter(
    parquetType: Type, updater: ParentContainerUpdater)
    extends ParquetPrimitiveConverter(updater) {
    private val isInt96 = parquetType.asPrimitiveType().getPrimitiveTypeName == INT96
    private val isMillis = parquetType.getOriginalType == OriginalType.TIMESTAMP_MILLIS

    protected var expandedDictionary: Array[Int] = null
    override def hasDictionarySupport: Boolean = true
    override def setDictionary(dictionary: Dictionary): Unit = {
      this.expandedDictionary = parquetType.asPrimitiveType().getPrimitiveTypeName match {
        case  INT32 =>
          Array.tabulate(dictionary.getMaxId + 1) { i =>
            dateRebaseFunc(dictionary.decodeToInt(i))
          }
        case INT64 =>
          Array.tabulate(dictionary.getMaxId + 1) { i =>
            longTimeToDate(dictionary.decodeToLong(i))
          }
        case INT96 | BINARY =>
          Array.tabulate(dictionary.getMaxId + 1) { i =>
            binaryToDate(dictionary.decodeToBinary(i))
          }
        case _ =>
          throw new RuntimeException(
            s"Unable to create Parquet dictionary converter for spark DateType " +
              s"whose Parquet type is $parquetType")
      }
    }
    override def addValueFromDictionary(dictionaryId: Int): Unit = {
      updater.set(expandedDictionary(dictionaryId))
    }

    // parquet primitive types: DINT32(DATE)
    override def addInt(value: Int): Unit = {
      updater.set(dateRebaseFunc(value))
    }

    // parquet primitive types: INT64(TIMESTAMP_MILLIS TIMESTAMP_MICROS)
    override def addLong(value: Long): Unit = {
      updater.set(longTimeToDate(value))
    }

    private def longTimeToDate(value: Long): Int = {
      val micros = if (isMillis) {
        DateTimeUtils.millisToMicros(value)
      } else {
        value
      }
      DateTimeUtils.microsToDays(timestampRebaseFunc(micros), convertTz)
    }

    // parquet primitive types: INT96 BINARY(UTF8)
    override def addBinary(value: Binary): Unit = {
      updater.set(binaryToDate(value))
    }

    private def binaryToDate(value: Binary): Int = {
      if (isInt96) {
        val rawTime = ParquetRowConverter.binaryToSQLTimestamp(value)
        val micros = if (convertInt96Timestamp) {
          DateTimeUtils.convertTz(rawTime, convertTz, ZoneOffset.UTC)
        } else {
          rawTime
        }
        DateTimeUtils.microsToDays(micros, convertTz)
      } else {
        val utf8 = ParquetRowConverter.utf8StringFromBinary(value)
        DateTimeUtils.stringToDate(utf8, convertTz).getOrElse {
          throw new IllegalArgumentException(s"This String(${utf8.toString})" +
            s" cannot be converted to Date, $parquetType")
        }
      }
    }
  }

  /**
   * Converter to spark [[TimestampType]].
   * Possible parquet primitive types:
   * 1.INT32(DATE)
   * 2.INT64(TIMESTAMP_MILLIS TIMESTAMP_MICROS)
   * 3.INT96
   * 4.BINARY(UTF8)
   */
  private final class ToSparkTimestampConverter(
    parquetType: Type, updater: ParentContainerUpdater)
    extends ParquetPrimitiveConverter(updater) {
    private val isInt96 = parquetType.asPrimitiveType().getPrimitiveTypeName == INT96
    private val isMillis = parquetType.getOriginalType == OriginalType.TIMESTAMP_MILLIS

    protected var expandedDictionary: Array[Long] = null
    override def hasDictionarySupport: Boolean = true
    override def setDictionary(dictionary: Dictionary): Unit = {
      this.expandedDictionary = parquetType.asPrimitiveType().getPrimitiveTypeName match {
        case  INT32 =>
          Array.tabulate(dictionary.getMaxId + 1) { i =>
            intDateToMicros(dictionary.decodeToInt(i))
          }
        case INT64 =>
          Array.tabulate(dictionary.getMaxId + 1) { i =>
            longTimeToMicros(dictionary.decodeToLong(i))
          }
        case INT96 | BINARY =>
          Array.tabulate(dictionary.getMaxId + 1) { i =>
            binaryToMicros(dictionary.decodeToBinary(i))
          }
        case _ =>
          throw new RuntimeException(
            s"Unable to create Parquet dictionary converter for spark TimestampType " +
              s"whose Parquet type is $parquetType")
      }
    }
    override def addValueFromDictionary(dictionaryId: Int): Unit = {
      updater.setLong(expandedDictionary(dictionaryId))
    }

    // parquet primitive types: DINT32(DATE)
    override def addInt(value: Int): Unit = {
      updater.setLong(intDateToMicros(value))
    }

    private def intDateToMicros(value: Int): Long = {
      val date = dateRebaseFunc(value)
      DateTimeUtils.daysToMicros(date, convertTz)
    }

    // parquet primitive types: INT64(TIMESTAMP_MILLIS TIMESTAMP_MICROS)
    override def addLong(value: Long): Unit = {
      updater.setLong(longTimeToMicros(value))
    }

    private def longTimeToMicros(value: Long): Long = {
      val micros = if (isMillis) {
        DateTimeUtils.millisToMicros(value)
      } else {
        value
      }
      timestampRebaseFunc(micros)
    }

    // parquet primitive types: INT96 BINARY(UTF8)
    override def addBinary(value: Binary): Unit = {
      updater.setLong(binaryToMicros(value))
    }

    private def binaryToMicros(value: Binary): Long = {
      if (isInt96) {
        val rawTime = ParquetRowConverter.binaryToSQLTimestamp(value)
        if (convertInt96Timestamp) {
          DateTimeUtils.convertTz(rawTime, convertTz, ZoneOffset.UTC)
        } else {
          rawTime
        }
      } else {
        val utf8 = ParquetRowConverter.utf8StringFromBinary(value)
        DateTimeUtils.stringToTimestamp(utf8, convertTz).getOrElse {
          throw new IllegalArgumentException(s"This String(${utf8.toString})" +
            s" cannot be converted to Timestamp, $parquetType")
        }
      }
    }
  }


  /**
   * Converter to spark [[StringType]].
   * Possible parquet primitive types:
   * 1.BOOLEAN
   * 2.FLOAT
   * 3.DOUBLE
   * 4.INT32(INT_8 INT_16 INT_32 DECIMAL DATE)
   * 5.INT64(INT_64 DECIMAL TIMESTAMP_MILLIS TIMESTAMP_MICROS)
   * 6.INT96
   * 7.BINARY(UTF8 DECIMAL)
   * 8.FIXED_LEN_BYTE_ARRAY(DECIMAL)
   */
  private final class ToSparkStringConverter(
    parquetType: Type, updater: ParentContainerUpdater)
    extends ParquetPrimitiveConverter(updater) {
    // TODO: It may be possible to cache the created utf8string to
    //  avoid duplicate creation of utf8string objects with the same value.
    //  This is a space for time optimization.

    private val parquetDecimal = parquetType.asPrimitiveType().getDecimalMetadata
    protected var expandedDictionary: Array[UTF8String] = null
    override def hasDictionarySupport: Boolean =
      parquetType.asPrimitiveType().getPrimitiveTypeName != BOOLEAN
    override def setDictionary(dictionary: Dictionary): Unit = {
      this.expandedDictionary = parquetType.asPrimitiveType().getPrimitiveTypeName match {
        case FLOAT =>
          Array.tabulate(dictionary.getMaxId + 1) { i =>
            UTF8String.fromString(dictionary.decodeToFloat(i).toString)
          }
        case DOUBLE =>
          Array.tabulate(dictionary.getMaxId + 1) { i =>
            UTF8String.fromString(dictionary.decodeToDouble(i).toString)
          }
        case INT32 =>
          Array.tabulate(dictionary.getMaxId + 1) { i =>
            intToUtf8String(dictionary.decodeToInt(i))
          }
        case INT64 =>
          Array.tabulate(dictionary.getMaxId + 1) { i =>
            longToUtf8String(dictionary.decodeToLong(i))
          }
        case INT96 | BINARY | FIXED_LEN_BYTE_ARRAY =>
          Array.tabulate(dictionary.getMaxId + 1) { i =>
            binaryToUtf8String(dictionary.decodeToBinary(i))
          }
        case _ =>
          throw new RuntimeException(
            s"Unable to create Parquet dictionary converter for spark StringType " +
              s"whose Parquet type is $parquetType")
      }
    }
    override def addValueFromDictionary(dictionaryId: Int): Unit = {
      updater.set(expandedDictionary(dictionaryId))
    }

    // parquet primitive types: BOOLEAN
    override def addBoolean(value: Boolean): Unit = {
      updater.set {
        if (value) ParquetRowConverter.trueUtf8String
        else ParquetRowConverter.falseUtf8String
      }
    }

    // parquet primitive types: FLOAT
    override def addFloat(value: Float): Unit = {
      val utf8 = UTF8String.fromString(value.toString)
      updater.set(utf8)
    }

    // parquet primitive types: DOUBLE
    override def addDouble(value: Double): Unit = {
      val utf8 = UTF8String.fromString(value.toString)
      updater.set(utf8)
    }

    // parquet primitive types: INT32(INT_8 INT_16 INT_32 DECIMAL DATE)
    override def addInt(value: Int): Unit = {
      updater.set(intToUtf8String(value))
    }

    private val intToUtf8String: Int => UTF8String = {
      parquetType.asPrimitiveType().getOriginalType match {
        case null | INT_8 | INT_16 | INT_32 =>
          intValue => {
            UTF8String.fromString(intValue.toString)
          }
        case DATE =>
          dateIntValue => {
            val date = dateRebaseFunc(dateIntValue)
            UTF8String.fromString(dateFormatter.format(date))
          }
        case DECIMAL =>
          decimalIntValue => {
            val decimal = ParquetRowConverter.decimalFromLong(decimalIntValue, parquetDecimal)
            UTF8String.fromString(decimal.toString)
          }
        case _ =>
          _ => throw new RuntimeException(
            s"Unsupported int value from parquet type: $parquetType")
      }
    }

    // parquet primitive types: INT64(INT_64 DECIMAL TIMESTAMP_MILLIS TIMESTAMP_MICROS)
    override def addLong(value: Long): Unit = {
      updater.set(longToUtf8String(value))
    }

    private val longToUtf8String: Long => UTF8String = {
      parquetType.asPrimitiveType().getOriginalType match {
        case null | INT_64 =>
          longValue => {
            UTF8String.fromString(longValue.toString)
          }
        case DECIMAL =>
          decimalLongValue => {
            val decimal = ParquetRowConverter.decimalFromLong(decimalLongValue, parquetDecimal)
            UTF8String.fromString(decimal.toString)
          }
        case TIMESTAMP_MILLIS =>
          millisLongValue => {
            val micros = timestampRebaseFunc(DateTimeUtils.millisToMicros(millisLongValue))
            UTF8String.fromString(timestampFormatter.format(micros))
          }
        case TIMESTAMP_MICROS =>
          microsLongValue => {
            val micros = timestampRebaseFunc(microsLongValue)
            UTF8String.fromString(timestampFormatter.format(micros))
          }
        case _ =>
          _ => throw new RuntimeException(s"" +
            s"Unsupported long value from parquet type: $parquetType")
      }
    }

    // parquet primitive types: (INT96 UTF8 DECIMAL)
    override def addBinary(value: Binary): Unit = {
      updater.set(binaryToUtf8String(value))
    }

    private val binaryToUtf8String: Binary => UTF8String = {
      parquetType.asPrimitiveType().getPrimitiveTypeName match {
        case INT96 =>
          int96BinaryValue => {
            val rawTime = ParquetRowConverter.binaryToSQLTimestamp(int96BinaryValue)
            val micros = if (convertInt96Timestamp) {
              DateTimeUtils.convertTz(rawTime, convertTz, ZoneOffset.UTC)
            } else {
              rawTime
            }
            UTF8String.fromString(timestampFormatter.format(micros))
          }
        case BINARY | FIXED_LEN_BYTE_ARRAY =>
          parquetType.asPrimitiveType().getOriginalType match {
            case DECIMAL =>
              decimalBinaryValue => {
                val decimal =
                  ParquetRowConverter.decimalFromBinary(decimalBinaryValue, parquetDecimal)
                UTF8String.fromString(decimal.toString)
              }
            case null | UTF8 | ENUM | JSON => value => {
              ParquetRowConverter.utf8StringFromBinary(value)
            }
            case _ =>
              _ => throw new RuntimeException(s"" +
                s"Unsupported binary value from parquet type: $parquetType")
          }
        case _ =>
          _ => throw new RuntimeException(s"" +
            s"Unsupported binary value from parquet type: $parquetType")
      }
    }
  }

  /**
   * Parquet converter for arrays.  Spark SQL arrays are represented as Parquet lists.  Standard
   * Parquet lists are represented as a 3-level group annotated by `LIST`:
   * {{{
   *   <list-repetition> group <name> (LIST) {            <-- parquetSchema points here
   *     repeated group list {
   *       <element-repetition> <element-type> element;
   *     }
   *   }
   * }}}
   * The `parquetSchema` constructor argument points to the outermost group.
   *
   * However, before this representation is standardized, some Parquet libraries/tools also use some
   * non-standard formats to represent list-like structures.  Backwards-compatibility rules for
   * handling these cases are described in Parquet format spec.
   *
   * @see https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#lists
   */
  private final class ParquetArrayConverter(
      parquetSchema: GroupType,
      catalystSchema: ArrayType,
      updater: ParentContainerUpdater)
    extends ParquetGroupConverter(updater) {

    private[this] val currentArray = ArrayBuffer.empty[Any]

    private[this] val elementConverter: Converter = {
      val repeatedType = parquetSchema.getType(0)
      val elementType = catalystSchema.elementType

      // At this stage, we're not sure whether the repeated field maps to the element type or is
      // just the syntactic repeated group of the 3-level standard LIST layout. Take the following
      // Parquet LIST-annotated group type as an example:
      //
      //    optional group f (LIST) {
      //      repeated group list {
      //        optional group element {
      //          optional int32 element;
      //        }
      //      }
      //    }
      //
      // This type is ambiguous:
      //
      // 1. When interpreted as a standard 3-level layout, the `list` field is just the syntactic
      //    group, and the entire type should be translated to:
      //
      //      ARRAY<STRUCT<element: INT>>
      //
      // 2. On the other hand, when interpreted as a non-standard 2-level layout, the `list` field
      //    represents the element type, and the entire type should be translated to:
      //
      //      ARRAY<STRUCT<element: STRUCT<element: INT>>>
      //
      // Here we try to convert field `list` into a Catalyst type to see whether the converted type
      // matches the Catalyst array element type. If it doesn't match, then it's case 1; otherwise,
      // it's case 2.
      val guessedElementType = schemaConverter.convertField(repeatedType)

      if (DataType.equalsIgnoreCompatibleNullability(guessedElementType, elementType)) {
        // If the repeated field corresponds to the element type, creates a new converter using the
        // type of the repeated field.
        newConverter(repeatedType, elementType, new ParentContainerUpdater {
          override def set(value: Any): Unit = currentArray += value
        })
      } else {
        // If the repeated field corresponds to the syntactic group in the standard 3-level Parquet
        // LIST layout, creates a new converter using the only child field of the repeated field.
        assert(!repeatedType.isPrimitive && repeatedType.asGroupType().getFieldCount == 1)
        new ElementConverter(repeatedType.asGroupType().getType(0), elementType)
      }
    }

    override def getConverter(fieldIndex: Int): Converter = elementConverter

    override def end(): Unit = updater.set(new GenericArrayData(currentArray.toArray))

    override def start(): Unit = currentArray.clear()

    /** Array element converter */
    private final class ElementConverter(parquetType: Type, catalystType: DataType)
      extends GroupConverter {

      private var currentElement: Any = _

      private[this] val converter =
        newConverter(parquetType, catalystType, new ParentContainerUpdater {
          override def set(value: Any): Unit = currentElement = value
        })

      override def getConverter(fieldIndex: Int): Converter = converter

      override def end(): Unit = currentArray += currentElement

      override def start(): Unit = currentElement = null
    }
  }

  /** Parquet converter for maps */
  private final class ParquetMapConverter(
      parquetType: GroupType,
      catalystType: MapType,
      updater: ParentContainerUpdater)
    extends ParquetGroupConverter(updater) {

    private[this] val currentKeys = ArrayBuffer.empty[Any]
    private[this] val currentValues = ArrayBuffer.empty[Any]

    private[this] val keyValueConverter = {
      val repeatedType = parquetType.getType(0).asGroupType()
      new KeyValueConverter(
        repeatedType.getType(0),
        repeatedType.getType(1),
        catalystType.keyType,
        catalystType.valueType)
    }

    override def getConverter(fieldIndex: Int): Converter = keyValueConverter

    override def end(): Unit = {
      // The parquet map may contains null or duplicated map keys. When it happens, the behavior is
      // undefined.
      // TODO (SPARK-26174): disallow it with a config.
      updater.set(
        new ArrayBasedMapData(
          new GenericArrayData(currentKeys.toArray),
          new GenericArrayData(currentValues.toArray)))
    }

    override def start(): Unit = {
      currentKeys.clear()
      currentValues.clear()
    }

    /** Parquet converter for key-value pairs within the map. */
    private final class KeyValueConverter(
        parquetKeyType: Type,
        parquetValueType: Type,
        catalystKeyType: DataType,
        catalystValueType: DataType)
      extends GroupConverter {

      private var currentKey: Any = _

      private var currentValue: Any = _

      private[this] val converters = Array(
        // Converter for keys
        newConverter(parquetKeyType, catalystKeyType, new ParentContainerUpdater {
          override def set(value: Any): Unit = currentKey = value
        }),

        // Converter for values
        newConverter(parquetValueType, catalystValueType, new ParentContainerUpdater {
          override def set(value: Any): Unit = currentValue = value
        }))

      override def getConverter(fieldIndex: Int): Converter = converters(fieldIndex)

      override def end(): Unit = {
        currentKeys += currentKey
        currentValues += currentValue
      }

      override def start(): Unit = {
        currentKey = null
        currentValue = null
      }
    }
  }

  private trait RepeatedConverter {
    private[this] val currentArray = ArrayBuffer.empty[Any]

    protected def newArrayUpdater(updater: ParentContainerUpdater) = new ParentContainerUpdater {
      override def start(): Unit = currentArray.clear()
      override def end(): Unit = updater.set(new GenericArrayData(currentArray.toArray))
      override def set(value: Any): Unit = currentArray += value
    }
  }

  /**
   * A primitive converter for converting unannotated repeated primitive values to required arrays
   * of required primitives values.
   */
  private final class RepeatedPrimitiveConverter(
      parquetType: Type,
      catalystType: DataType,
      parentUpdater: ParentContainerUpdater)
    extends PrimitiveConverter with RepeatedConverter with HasParentContainerUpdater {

    val updater: ParentContainerUpdater = newArrayUpdater(parentUpdater)

    private[this] val elementConverter: PrimitiveConverter =
      newConverter(parquetType, catalystType, updater).asPrimitiveConverter()

    override def addBoolean(value: Boolean): Unit = elementConverter.addBoolean(value)
    override def addInt(value: Int): Unit = elementConverter.addInt(value)
    override def addLong(value: Long): Unit = elementConverter.addLong(value)
    override def addFloat(value: Float): Unit = elementConverter.addFloat(value)
    override def addDouble(value: Double): Unit = elementConverter.addDouble(value)
    override def addBinary(value: Binary): Unit = elementConverter.addBinary(value)

    override def setDictionary(dict: Dictionary): Unit = elementConverter.setDictionary(dict)
    override def hasDictionarySupport: Boolean = elementConverter.hasDictionarySupport
    override def addValueFromDictionary(id: Int): Unit = elementConverter.addValueFromDictionary(id)
  }

  /**
   * A group converter for converting unannotated repeated group values to required arrays of
   * required struct values.
   */
  private final class RepeatedGroupConverter(
      parquetType: Type,
      catalystType: DataType,
      parentUpdater: ParentContainerUpdater)
    extends GroupConverter with HasParentContainerUpdater with RepeatedConverter {

    val updater: ParentContainerUpdater = newArrayUpdater(parentUpdater)

    private[this] val elementConverter: GroupConverter =
      newConverter(parquetType, catalystType, updater).asGroupConverter()

    override def getConverter(field: Int): Converter = elementConverter.getConverter(field)
    override def end(): Unit = elementConverter.end()
    override def start(): Unit = elementConverter.start()
  }
}

private[parquet] object ParquetRowConverter {
  val trueUtf8String: UTF8String = UTF8String.fromString(true.toString)
  val falseUtf8String: UTF8String = UTF8String.fromString(false.toString)

  def binaryToUnscaledLong(binary: Binary): Long = {
    // The underlying `ByteBuffer` implementation is guaranteed to be `HeapByteBuffer`, so here
    // we are using `Binary.toByteBuffer.array()` to steal the underlying byte array without
    // copying it.
    val buffer = binary.toByteBuffer
    val bytes = buffer.array()
    val start = buffer.arrayOffset() + buffer.position()
    val end = buffer.arrayOffset() + buffer.limit()

    var unscaled = 0L
    var i = start

    while (i < end) {
      unscaled = (unscaled << 8) | (bytes(i) & 0xff)
      i += 1
    }

    val bits = 8 * (end - start)
    unscaled = (unscaled << (64 - bits)) >> (64 - bits)
    unscaled
  }

  def binaryToSQLTimestamp(binary: Binary): Long = {
    assert(binary.length() == 12, s"Timestamps (with nanoseconds) are expected to be stored in" +
      s" 12-byte long binaries. Found a ${binary.length()}-byte binary instead.")
    val buffer = binary.toByteBuffer.order(ByteOrder.LITTLE_ENDIAN)
    val timeOfDayNanos = buffer.getLong
    val julianDay = buffer.getInt
    DateTimeUtils.fromJulianDay(julianDay, timeOfDayNanos)
  }

  def binaryToJString(binary: Binary): String = {
    val buffer = binary.toByteBuffer
    val offset = buffer.arrayOffset() + buffer.position()
    val numBytes = buffer.remaining()
    new String(buffer.array(), offset, numBytes, StandardCharsets.UTF_8)
  }

  def utf8StringFromBinary(binary: Binary): UTF8String = {
    val buffer = binary.toByteBuffer
    val offset = buffer.arrayOffset() + buffer.position()
    val numBytes = buffer.remaining()
    UTF8String.fromBytes(buffer.array(), offset, numBytes)
  }

  def decimalFromLong(longValue: Long, parquetDecimal: DecimalMetadata): Decimal = {
    Decimal(longValue, parquetDecimal.getPrecision, parquetDecimal.getScale)
  }

  def decimalFromBinary(binary: Binary, parquetDecimal: DecimalMetadata): Decimal = {
    if (parquetDecimal.getPrecision <= Decimal.MAX_LONG_DIGITS) {
      // Constructs a `Decimal` with an unscaled `Long` value if possible.
      val unscaled = ParquetRowConverter.binaryToUnscaledLong(binary)
      decimalFromLong(unscaled, parquetDecimal)
    } else {
      // Otherwise, resorts to an unscaled `BigInteger` instead.
      Decimal(new BigDecimal(new BigInteger(binary.getBytes),
        parquetDecimal.getScale), parquetDecimal.getPrecision, parquetDecimal.getScale)
    }
  }
}
