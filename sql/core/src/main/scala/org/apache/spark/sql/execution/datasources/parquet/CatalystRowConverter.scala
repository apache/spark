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

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

import org.apache.parquet.column.Dictionary
import org.apache.parquet.io.api.{Binary, Converter, GroupConverter, PrimitiveConverter}
import org.apache.parquet.schema.OriginalType.{INT_32, LIST, UTF8}
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.DOUBLE
import org.apache.parquet.schema.Type.Repetition
import org.apache.parquet.schema.{GroupType, MessageType, PrimitiveType, Type}

import org.apache.spark.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

/**
 * A [[ParentContainerUpdater]] is used by a Parquet converter to set converted values to some
 * corresponding parent container. For example, a converter for a `StructType` field may set
 * converted values to a [[MutableRow]]; or a converter for array elements may append converted
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
 * A convenient converter class for Parquet group types with an [[HasParentContainerUpdater]].
 */
private[parquet] abstract class CatalystGroupConverter(val updater: ParentContainerUpdater)
  extends GroupConverter with HasParentContainerUpdater

/**
 * Parquet converter for Parquet primitive types.  Note that not all Spark SQL atomic types
 * are handled by this converter.  Parquet primitive types are only a subset of those of Spark
 * SQL.  For example, BYTE, SHORT, and INT in Spark SQL are all covered by INT32 in Parquet.
 */
private[parquet] class CatalystPrimitiveConverter(val updater: ParentContainerUpdater)
  extends PrimitiveConverter with HasParentContainerUpdater {

  override def addBoolean(value: Boolean): Unit = updater.setBoolean(value)
  override def addInt(value: Int): Unit = updater.setInt(value)
  override def addLong(value: Long): Unit = updater.setLong(value)
  override def addFloat(value: Float): Unit = updater.setFloat(value)
  override def addDouble(value: Double): Unit = updater.setDouble(value)
  override def addBinary(value: Binary): Unit = updater.set(value.getBytes)
}

/**
 * A [[CatalystRowConverter]] is used to convert Parquet records into Catalyst [[InternalRow]]s.
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
 * - a root [[CatalystRowConverter]] for [[MessageType]] `root`, which contains:
 *   - a [[CatalystPrimitiveConverter]] for required [[INT_32]] field `f1`, and
 *   - a nested [[CatalystRowConverter]] for optional [[GroupType]] `f2`, which contains:
 *     - a [[CatalystPrimitiveConverter]] for required [[DOUBLE]] field `f21`, and
 *     - a [[CatalystStringConverter]] for optional [[UTF8]] string field `f22`
 *
 * When used as a root converter, [[NoopUpdater]] should be used since root converters don't have
 * any "parent" container.
 *
 * @note Constructor argument [[parquetType]] refers to requested fields of the actual schema of the
 *       Parquet file being read, while constructor argument [[catalystType]] refers to requested
 *       fields of the global schema.  The key difference is that, in case of schema merging,
 *       [[parquetType]] can be a subset of [[catalystType]].  For example, it's possible to have
 *       the following [[catalystType]]:
 *       {{{
 *         new StructType()
 *           .add("f1", IntegerType, nullable = false)
 *           .add("f2", StringType, nullable = true)
 *           .add("f3", new StructType()
 *             .add("f31", DoubleType, nullable = false)
 *             .add("f32", IntegerType, nullable = true)
 *             .add("f33", StringType, nullable = true), nullable = false)
 *       }}}
 *       and the following [[parquetType]] (`f2` and `f32` are missing):
 *       {{{
 *         message root {
 *           required int32 f1;
 *           required group f3 {
 *             required double f31;
 *             optional binary f33 (utf8);
 *           }
 *         }
 *       }}}
 *
 * @param parquetType Parquet schema of Parquet records
 * @param catalystType Spark SQL schema that corresponds to the Parquet record type
 * @param updater An updater which propagates converted field values to the parent container
 */
private[parquet] class CatalystRowConverter(
    parquetType: GroupType,
    catalystType: StructType,
    updater: ParentContainerUpdater)
  extends CatalystGroupConverter(updater) with Logging {

  logDebug(
    s"""Building row converter for the following schema:
       |
       |Parquet form:
       |$parquetType
       |Catalyst form:
       |${catalystType.prettyJson}
     """.stripMargin)

  /**
   * Updater used together with field converters within a [[CatalystRowConverter]].  It propagates
   * converted filed values to the `ordinal`-th cell in `currentRow`.
   */
  private final class RowUpdater(row: MutableRow, ordinal: Int) extends ParentContainerUpdater {
    override def set(value: Any): Unit = row(ordinal) = value
    override def setBoolean(value: Boolean): Unit = row.setBoolean(ordinal, value)
    override def setByte(value: Byte): Unit = row.setByte(ordinal, value)
    override def setShort(value: Short): Unit = row.setShort(ordinal, value)
    override def setInt(value: Int): Unit = row.setInt(ordinal, value)
    override def setLong(value: Long): Unit = row.setLong(ordinal, value)
    override def setDouble(value: Double): Unit = row.setDouble(ordinal, value)
    override def setFloat(value: Float): Unit = row.setFloat(ordinal, value)
  }

  /**
   * Represents the converted row object once an entire Parquet record is converted.
   */
  val currentRow = new SpecificMutableRow(catalystType.map(_.dataType))

  // Converters for each field.
  private val fieldConverters: Array[Converter with HasParentContainerUpdater] = {
    // In case of schema merging, `parquetType` can be a subset of `catalystType`.  We need to pad
    // those missing fields and create converters for them, although values of these fields are
    // always null.
    val paddedParquetFields = {
      val parquetFields = parquetType.getFields
      val parquetFieldNames = parquetFields.map(_.getName).toSet
      val missingFields = catalystType.filterNot(f => parquetFieldNames.contains(f.name))

      // We don't need to worry about feature flag arguments like `assumeBinaryIsString` when
      // creating the schema converter here, since values of missing fields are always null.
      val toParquet = new CatalystSchemaConverter()

      (parquetFields ++ missingFields.map(toParquet.convertField)).sortBy { f =>
        catalystType.indexWhere(_.name == f.getName)
      }
    }

    if (paddedParquetFields.length != catalystType.length) {
      throw new UnsupportedOperationException(
        "A Parquet file's schema has different number of fields with the table schema. " +
          "Please enable schema merging by setting \"mergeSchema\" to true when load " +
          "a Parquet dataset or set spark.sql.parquet.mergeSchema to true in SQLConf.")
    }

    paddedParquetFields.zip(catalystType).zipWithIndex.map {
      case ((parquetFieldType, catalystField), ordinal) =>
        // Converted field value should be set to the `ordinal`-th cell of `currentRow`
        newConverter(parquetFieldType, catalystField.dataType, new RowUpdater(currentRow, ordinal))
    }.toArray
  }

  override def getConverter(fieldIndex: Int): Converter = fieldConverters(fieldIndex)

  override def end(): Unit = {
    var i = 0
    while (i < currentRow.numFields) {
      fieldConverters(i).updater.end()
      i += 1
    }
    updater.set(currentRow)
  }

  override def start(): Unit = {
    var i = 0
    while (i < currentRow.numFields) {
      fieldConverters(i).updater.start()
      currentRow.setNullAt(i)
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
      case BooleanType | IntegerType | LongType | FloatType | DoubleType | BinaryType =>
        new CatalystPrimitiveConverter(updater)

      case ByteType =>
        new CatalystPrimitiveConverter(updater) {
          override def addInt(value: Int): Unit =
            updater.setByte(value.asInstanceOf[ByteType#InternalType])
        }

      case ShortType =>
        new CatalystPrimitiveConverter(updater) {
          override def addInt(value: Int): Unit =
            updater.setShort(value.asInstanceOf[ShortType#InternalType])
        }

      case t: DecimalType =>
        new CatalystDecimalConverter(t, updater)

      case StringType =>
        new CatalystStringConverter(updater)

      case TimestampType =>
        // TODO Implements `TIMESTAMP_MICROS` once parquet-mr has that.
        new CatalystPrimitiveConverter(updater) {
          // Converts nanosecond timestamps stored as INT96
          override def addBinary(value: Binary): Unit = {
            assert(
              value.length() == 12,
              "Timestamps (with nanoseconds) are expected to be stored in 12-byte long binaries, " +
              s"but got a ${value.length()}-byte binary.")

            val buf = value.toByteBuffer.order(ByteOrder.LITTLE_ENDIAN)
            val timeOfDayNanos = buf.getLong
            val julianDay = buf.getInt
            updater.setLong(DateTimeUtils.fromJulianDay(julianDay, timeOfDayNanos))
          }
        }

      case DateType =>
        new CatalystPrimitiveConverter(updater) {
          override def addInt(value: Int): Unit = {
            // DateType is not specialized in `SpecificMutableRow`, have to box it here.
            updater.set(value.asInstanceOf[DateType#InternalType])
          }
        }

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
        new CatalystArrayConverter(parquetType.asGroupType(), t, updater)

      case t: MapType =>
        new CatalystMapConverter(parquetType.asGroupType(), t, updater)

      case t: StructType =>
        new CatalystRowConverter(parquetType.asGroupType(), t, new ParentContainerUpdater {
          override def set(value: Any): Unit = updater.set(value.asInstanceOf[InternalRow].copy())
        })

      case t: UserDefinedType[_] =>
        val catalystTypeForUDT = t.sqlType
        val nullable = parquetType.isRepetition(Repetition.OPTIONAL)
        val field = StructField("udt", catalystTypeForUDT, nullable)
        val parquetTypeForUDT = new CatalystSchemaConverter().convertField(field)
        newConverter(parquetTypeForUDT, catalystTypeForUDT, updater)

      case _ =>
        throw new RuntimeException(
          s"Unable to create Parquet converter for data type ${catalystType.json}")
    }
  }

  /**
   * Parquet converter for strings. A dictionary is used to minimize string decoding cost.
   */
  private final class CatalystStringConverter(updater: ParentContainerUpdater)
    extends CatalystPrimitiveConverter(updater) {

    private var expandedDictionary: Array[UTF8String] = null

    override def hasDictionarySupport: Boolean = true

    override def setDictionary(dictionary: Dictionary): Unit = {
      this.expandedDictionary = Array.tabulate(dictionary.getMaxId + 1) { i =>
        UTF8String.fromBytes(dictionary.decodeToBinary(i).getBytes)
      }
    }

    override def addValueFromDictionary(dictionaryId: Int): Unit = {
      updater.set(expandedDictionary(dictionaryId))
    }

    override def addBinary(value: Binary): Unit = {
      updater.set(UTF8String.fromBytes(value.getBytes))
    }
  }

  /**
   * Parquet converter for fixed-precision decimals.
   */
  private final class CatalystDecimalConverter(
      decimalType: DecimalType,
      updater: ParentContainerUpdater)
    extends CatalystPrimitiveConverter(updater) {

    // Converts decimals stored as INT32
    override def addInt(value: Int): Unit = {
      addLong(value: Long)
    }

    // Converts decimals stored as INT64
    override def addLong(value: Long): Unit = {
      updater.set(Decimal(value, decimalType.precision, decimalType.scale))
    }

    // Converts decimals stored as either FIXED_LENGTH_BYTE_ARRAY or BINARY
    override def addBinary(value: Binary): Unit = {
      updater.set(toDecimal(value))
    }

    private def toDecimal(value: Binary): Decimal = {
      val precision = decimalType.precision
      val scale = decimalType.scale
      val bytes = value.getBytes

      if (precision <= CatalystSchemaConverter.MAX_PRECISION_FOR_INT64) {
        // Constructs a `Decimal` with an unscaled `Long` value if possible.
        var unscaled = 0L
        var i = 0

        while (i < bytes.length) {
          unscaled = (unscaled << 8) | (bytes(i) & 0xff)
          i += 1
        }

        val bits = 8 * bytes.length
        unscaled = (unscaled << (64 - bits)) >> (64 - bits)
        Decimal(unscaled, precision, scale)
      } else {
        // Otherwise, resorts to an unscaled `BigInteger` instead.
        Decimal(new BigDecimal(new BigInteger(bytes), scale), precision, scale)
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
  private final class CatalystArrayConverter(
      parquetSchema: GroupType,
      catalystSchema: ArrayType,
      updater: ParentContainerUpdater)
    extends CatalystGroupConverter(updater) {

    private var currentArray: ArrayBuffer[Any] = _

    private val elementConverter: Converter = {
      val repeatedType = parquetSchema.getType(0)
      val elementType = catalystSchema.elementType
      val parentName = parquetSchema.getName

      if (isElementType(repeatedType, elementType, parentName)) {
        newConverter(repeatedType, elementType, new ParentContainerUpdater {
          override def set(value: Any): Unit = currentArray += value
        })
      } else {
        new ElementConverter(repeatedType.asGroupType().getType(0), elementType)
      }
    }

    override def getConverter(fieldIndex: Int): Converter = elementConverter

    override def end(): Unit = updater.set(new GenericArrayData(currentArray.toArray))

    // NOTE: We can't reuse the mutable `ArrayBuffer` here and must instantiate a new buffer for the
    // next value.  `Row.copy()` only copies row cells, it doesn't do deep copy to objects stored
    // in row cells.
    override def start(): Unit = currentArray = ArrayBuffer.empty[Any]

    // scalastyle:off
    /**
     * Returns whether the given type is the element type of a list or is a syntactic group with
     * one field that is the element type.  This is determined by checking whether the type can be
     * a syntactic group and by checking whether a potential syntactic group matches the expected
     * schema.
     * {{{
     *   <list-repetition> group <name> (LIST) {
     *     repeated group list {                          <-- repeatedType points here
     *       <element-repetition> <element-type> element;
     *     }
     *   }
     * }}}
     * In short, here we handle Parquet list backwards-compatibility rules on the read path.  This
     * method is based on `AvroIndexedRecordConverter.isElementType`.
     *
     * @see https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#backward-compatibility-rules
     */
    // scalastyle:on
    private def isElementType(
        parquetRepeatedType: Type, catalystElementType: DataType, parentName: String): Boolean = {
      (parquetRepeatedType, catalystElementType) match {
        case (t: PrimitiveType, _) => true
        case (t: GroupType, _) if t.getFieldCount > 1 => true
        case (t: GroupType, _) if t.getFieldCount == 1 && t.getName == "array" => true
        case (t: GroupType, _) if t.getFieldCount == 1 && t.getName == parentName + "_tuple" => true
        case (t: GroupType, StructType(Array(f))) if f.name == t.getFieldName(0) => true
        case _ => false
      }
    }

    /** Array element converter */
    private final class ElementConverter(parquetType: Type, catalystType: DataType)
      extends GroupConverter {

      private var currentElement: Any = _

      private val converter = newConverter(parquetType, catalystType, new ParentContainerUpdater {
        override def set(value: Any): Unit = currentElement = value
      })

      override def getConverter(fieldIndex: Int): Converter = converter

      override def end(): Unit = currentArray += currentElement

      override def start(): Unit = currentElement = null
    }
  }

  /** Parquet converter for maps */
  private final class CatalystMapConverter(
      parquetType: GroupType,
      catalystType: MapType,
      updater: ParentContainerUpdater)
    extends CatalystGroupConverter(updater) {

    private var currentKeys: ArrayBuffer[Any] = _
    private var currentValues: ArrayBuffer[Any] = _

    private val keyValueConverter = {
      val repeatedType = parquetType.getType(0).asGroupType()
      new KeyValueConverter(
        repeatedType.getType(0),
        repeatedType.getType(1),
        catalystType.keyType,
        catalystType.valueType)
    }

    override def getConverter(fieldIndex: Int): Converter = keyValueConverter

    override def end(): Unit =
      updater.set(ArrayBasedMapData(currentKeys.toArray, currentValues.toArray))

    // NOTE: We can't reuse the mutable Map here and must instantiate a new `Map` for the next
    // value.  `Row.copy()` only copies row cells, it doesn't do deep copy to objects stored in row
    // cells.
    override def start(): Unit = {
      currentKeys = ArrayBuffer.empty[Any]
      currentValues = ArrayBuffer.empty[Any]
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

      private val converters = Array(
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
    private var currentArray: ArrayBuffer[Any] = _

    protected def newArrayUpdater(updater: ParentContainerUpdater) = new ParentContainerUpdater {
      override def start(): Unit = currentArray = ArrayBuffer.empty[Any]
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

    private val elementConverter: PrimitiveConverter =
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

    private val elementConverter: GroupConverter =
      newConverter(parquetType, catalystType, updater).asGroupConverter()

    override def getConverter(field: Int): Converter = elementConverter.getConverter(field)
    override def end(): Unit = elementConverter.end()
    override def start(): Unit = elementConverter.start()
  }
}
