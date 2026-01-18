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

package org.apache.spark.sql.util

import java.util.concurrent.atomic.AtomicInteger

import scala.jdk.CollectionConverters._

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.complex.MapVector
import org.apache.arrow.vector.types.{DateUnit, FloatingPointPrecision, IntervalUnit, TimeUnit}
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType, Schema}

import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.types.ops.TypeApiOps
import org.apache.spark.sql.errors.ExecutionErrors
import org.apache.spark.sql.types._
import org.apache.spark.util.ArrayImplicits._

private[sql] object ArrowUtils {

  val rootAllocator = new RootAllocator(Long.MaxValue)

  // todo: support more types.

  /**
   * Check if a Spark DataType is supported by Arrow.
   * This recursively checks complex types (Array, Struct, Map).
   */
  def isSupportedByArrow(dt: DataType): Boolean = {
    dt match {
      // Primitive types
      case BooleanType | ByteType | ShortType | IntegerType | LongType |
           FloatType | DoubleType | _: StringType | BinaryType | NullType => true

      // Decimal
      case _: DecimalType => true

      // Temporal types
      case DateType | TimestampType | TimestampNTZType | _: TimeType => true

      // Interval types
      case _: YearMonthIntervalType | _: DayTimeIntervalType | CalendarIntervalType => true

      // Complex types - recursively check element types
      case ArrayType(elementType, _) => isSupportedByArrow(elementType)
      case StructType(fields) => fields.forall(f => isSupportedByArrow(f.dataType))
      case MapType(keyType, valueType, _) =>
        isSupportedByArrow(keyType) && isSupportedByArrow(valueType)

      // Special types
      case _: UserDefinedType[_] => true  // UDTs are converted to their sqlType
      case _: GeometryType => true
      case _: GeographyType => true
      case _: VariantType => true

      // Unsupported types
      case _ => false
    }
  }

  /** Maps data type from Spark to Arrow. NOTE: timeZoneId required for TimestampTypes */
  def toArrowType(dt: DataType, timeZoneId: String, largeVarTypes: Boolean = false): ArrowType =
    TypeApiOps(dt)
      .flatMap(_.toArrowType(timeZoneId))
      .getOrElse(toArrowTypeDefault(dt, timeZoneId, largeVarTypes))

  private def toArrowTypeDefault(
      dt: DataType,
      timeZoneId: String,
      largeVarTypes: Boolean): ArrowType =
    dt match {
      case BooleanType => ArrowType.Bool.INSTANCE
      case ByteType => new ArrowType.Int(8, true)
      case ShortType => new ArrowType.Int(8 * 2, true)
      case IntegerType => new ArrowType.Int(8 * 4, true)
      case LongType => new ArrowType.Int(8 * 8, true)
      case FloatType => new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)
      case DoubleType => new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)
      case _: StringType if !largeVarTypes => ArrowType.Utf8.INSTANCE
      case BinaryType if !largeVarTypes => ArrowType.Binary.INSTANCE
      case _: StringType if largeVarTypes => ArrowType.LargeUtf8.INSTANCE
      case BinaryType if largeVarTypes => ArrowType.LargeBinary.INSTANCE
      case DecimalType.Fixed(precision, scale) => new ArrowType.Decimal(precision, scale, 8 * 16)
      case DateType => new ArrowType.Date(DateUnit.DAY)
      case TimestampType if timeZoneId == null =>
        throw SparkException.internalError("Missing timezoneId where it is mandatory.")
      case TimestampType => new ArrowType.Timestamp(TimeUnit.MICROSECOND, timeZoneId)
      case TimestampNTZType =>
        new ArrowType.Timestamp(TimeUnit.MICROSECOND, null)
      case NullType => ArrowType.Null.INSTANCE
      case _: YearMonthIntervalType => new ArrowType.Interval(IntervalUnit.YEAR_MONTH)
      case _: DayTimeIntervalType => new ArrowType.Duration(TimeUnit.MICROSECOND)
      case CalendarIntervalType => new ArrowType.Interval(IntervalUnit.MONTH_DAY_NANO)
      case _ =>
        throw ExecutionErrors.unsupportedDataTypeError(dt)
    }

  def fromArrowType(dt: ArrowType): DataType =
    TypeApiOps.fromArrowType(dt).getOrElse(fromArrowTypeDefault(dt))

  private def fromArrowTypeDefault(dt: ArrowType): DataType = dt match {
    case ArrowType.Bool.INSTANCE => BooleanType
    case int: ArrowType.Int if int.getIsSigned && int.getBitWidth == 8 => ByteType
    case int: ArrowType.Int if int.getIsSigned && int.getBitWidth == 8 * 2 => ShortType
    case int: ArrowType.Int if int.getIsSigned && int.getBitWidth == 8 * 4 => IntegerType
    case int: ArrowType.Int if int.getIsSigned && int.getBitWidth == 8 * 8 => LongType
    case float: ArrowType.FloatingPoint
        if float.getPrecision() == FloatingPointPrecision.SINGLE =>
      FloatType
    case float: ArrowType.FloatingPoint
        if float.getPrecision() == FloatingPointPrecision.DOUBLE =>
      DoubleType
    case ArrowType.Utf8.INSTANCE => StringType
    case ArrowType.Binary.INSTANCE => BinaryType
    case ArrowType.LargeUtf8.INSTANCE => StringType
    case ArrowType.LargeBinary.INSTANCE => BinaryType
    case d: ArrowType.Decimal => DecimalType(d.getPrecision, d.getScale)
    case date: ArrowType.Date if date.getUnit == DateUnit.DAY => DateType
    case ts: ArrowType.Timestamp
        if ts.getUnit == TimeUnit.MICROSECOND && ts.getTimezone == null =>
      TimestampNTZType
    case ts: ArrowType.Timestamp if ts.getUnit == TimeUnit.MICROSECOND => TimestampType
    case ArrowType.Null.INSTANCE => NullType
    case yi: ArrowType.Interval if yi.getUnit == IntervalUnit.YEAR_MONTH =>
      YearMonthIntervalType()
    case di: ArrowType.Duration if di.getUnit == TimeUnit.MICROSECOND => DayTimeIntervalType()
    case ci: ArrowType.Interval if ci.getUnit == IntervalUnit.MONTH_DAY_NANO =>
      CalendarIntervalType
    case _ => throw ExecutionErrors.unsupportedArrowTypeError(dt)
  }

  private val metadataKey = "SPARK::metadata::json"
  // Arrow's Timestamp type carries only (unit, timezone) and has no fractional-second precision
  // field, so the precision of the nanosecond timestamp types is stored in the Arrow field
  // metadata under this dedicated key (namespaced like `metadataKey`, separate from the user
  // metadata blob so user metadata is untouched) and recovered on read in `fromArrowField`.
  private val timestampNanosPrecisionKey = "SPARK::timestampNanos::precision"
  // Arrow's Time type carries only (unit, bitWidth) and has no fractional-second precision field,
  // so the precision of TimeType is stored in the Arrow field metadata under this dedicated key
  // (namespaced like `metadataKey`, separate from the user metadata blob so user metadata is
  // untouched) and recovered on read in `fromArrowField`.
  private val timePrecisionKey = "SPARK::time::precision"
  // Marks the epochMicros child of the lossless struct representation of a nanosecond timestamp
  // (see `toArrowField` with `losslessInternalTypes = true`). The value is "ntz" or "ltz" and
  // distinguishes TimestampNTZNanosType from TimestampLTZNanosType on read; the precision is
  // stored alongside under `timestampNanosPrecisionKey`. The tag lives on a child field (like the
  // geometry/variant struct tags) so it cannot collide with user metadata on the struct itself.
  private val timestampNanosStructKey = "SPARK::timestampNanos::struct"
  // Marks the months child of the lossless struct representation of a CalendarInterval (see
  // `toArrowField` with `losslessInternalTypes = true`). The value is "true"; like
  // `timestampNanosStructKey`, the tag lives on a child field so it cannot collide with user
  // metadata on the struct itself.
  private val calendarIntervalStructKey = "SPARK::calendarInterval::struct"
  private def toArrowMetaData(metadata: Metadata) = {
    if (metadata != null && !metadata.isEmpty) {
      Map(metadataKey -> metadata.json).asJava
    } else {
      null // Arrow metadata defaults to NULL
    }
  }
  private def fromArrowMetaData(map: java.util.Map[String, String]) = {
    if (map != null && map.containsKey(metadataKey)) {
      Metadata.fromJson(map.get(metadataKey))
    } else {
      Metadata.empty // Spark metadata defaults to Metadata.empty
    }
  }

  /**
   * Builds an Arrow field for a type whose Arrow representation cannot encode its
   * fractional-second precision (nanosecond timestamps, TIME), stashing the column precision in
   * the field metadata under `precisionKey` (alongside the user metadata) so it can be recovered
   * in `fromArrowField`.
   */
  private def toPrecisionTaggedArrowField(
      name: String,
      dt: DataType,
      precision: Int,
      precisionKey: String,
      nullable: Boolean,
      timeZoneId: String,
      largeVarTypes: Boolean,
      metadata: Metadata): Field = {
    val base = Option(toArrowMetaData(metadata)).map(_.asScala.toMap).getOrElse(Map.empty)
    val md = (base + (precisionKey -> precision.toString)).asJava
    val fieldType = new FieldType(nullable, toArrowType(dt, timeZoneId, largeVarTypes), null, md)
    new Field(name, fieldType, Seq.empty[Field].asJava)
  }

  /**
   * Builds the lossless Arrow struct representation of a nanosecond timestamp: a struct of
   * (epochMicros: int64, nanosWithinMicro: int16), mirroring TimestampNanosVal's own layout with
   * no unit conversion. Unlike the default Timestamp(NANOSECOND) mapping, which packs the value
   * into a single int64 of epoch-nanoseconds and therefore only covers roughly years 1677-2262,
   * this representation covers the full domain of the Spark types (years 0001-9999).
   *
   * Why two representations exist, permanently: the mismatch is structural. Arrow's timestamp
   * physical type is fixed at int64 by the Arrow format spec, while the Spark types are defined
   * over years 0001-9999, so no single Arrow timestamp encoding can serve both goals.
   *   - Interchange paths (pandas conversion, Arrow UDFs, Connect result sets) must keep the
   *     standard Timestamp(NANOSECOND) encoding: their consumers only understand that encoding,
   *     and those consumers' own timestamp domains are equally int64-bound (e.g. pandas
   *     datetime64[ns]), so the reduced domain is inherent to the destination -- failing loudly
   *     at write (DATETIME_OVERFLOW) is the correct behavior there, not a limitation of the
   *     mapping.
   *   - Internal storage (e.g. the Arrow-based Dataset cache) is a closed write-then-read-back
   *     loop with no external consumer, where the only requirement is fidelity to Spark
   *     semantics, hence this struct.
   * The choice is made per call site via `losslessInternalTypes` on `toArrowSchema` /
   * `toArrowField` (the same pattern as `largeVarTypes`: one Spark type, two Arrow encodings,
   * selected by the consumer's needs). Only schema construction needs the flag: the struct is
   * self-describing through its child-field tag, so `fromArrowField`, `ArrowWriter`, and
   * `ArrowColumnVector` recognize both shapes unconditionally and no mode mismatch is possible.
   */
  private def toTimestampNanosStructField(
      name: String,
      isNtz: Boolean,
      precision: Int,
      nullable: Boolean,
      metadata: Metadata): Field = {
    val fieldType =
      new FieldType(nullable, ArrowType.Struct.INSTANCE, null, toArrowMetaData(metadata))
    // Tag the epochMicros child so `fromArrowField` (and ArrowColumnVector) can recognize that
    // this struct represents a nanosecond timestamp, following the geometry/variant tag pattern.
    val microsFieldType = new FieldType(
      false,
      new ArrowType.Int(8 * 8, true),
      null,
      Map(
        timestampNanosStructKey -> (if (isNtz) "ntz" else "ltz"),
        timestampNanosPrecisionKey -> precision.toString).asJava)
    val nanosFieldType = new FieldType(false, new ArrowType.Int(8 * 2, true), null, null)
    new Field(
      name,
      fieldType,
      Seq(
        new Field("epochMicros", microsFieldType, Seq.empty[Field].asJava),
        new Field("nanosWithinMicro", nanosFieldType, Seq.empty[Field].asJava)).asJava)
  }

  /**
   * Builds the lossless Arrow struct representation of a CalendarInterval: a struct of (months:
   * int32, days: int32, microseconds: int64) -- the type's own field layout, mirroring the
   * default in-memory cache's CALENDAR_INTERVAL ColumnType. The default Interval(MONTH_DAY_NANO)
   * mapping multiplies microseconds by 1000 into Arrow's int64 nanosecond field, so any
   * |microseconds| > Long.MaxValue / 1000 (roughly +/-292 years) cannot be represented; this
   * struct stores the components as-is, so the full Long microsecond domain round-trips. See
   * `toTimestampNanosStructField` for why the default interchange mapping must stay unchanged
   * and the lossless shape is a per-call-site opt-in for internal storage.
   */
  private def toCalendarIntervalStructField(
      name: String,
      nullable: Boolean,
      metadata: Metadata): Field = {
    val fieldType =
      new FieldType(nullable, ArrowType.Struct.INSTANCE, null, toArrowMetaData(metadata))
    // Tag the months child so `fromArrowField` (and ArrowColumnVector) can recognize that this
    // struct represents a CalendarInterval, following the geometry/variant tag pattern.
    val monthsFieldType = new FieldType(
      false,
      new ArrowType.Int(8 * 4, true),
      null,
      Map(calendarIntervalStructKey -> "true").asJava)
    val daysFieldType = new FieldType(false, new ArrowType.Int(8 * 4, true), null, null)
    val microsFieldType = new FieldType(false, new ArrowType.Int(8 * 8, true), null, null)
    new Field(
      name,
      fieldType,
      Seq(
        new Field("months", monthsFieldType, Seq.empty[Field].asJava),
        new Field("days", daysFieldType, Seq.empty[Field].asJava),
        new Field("microseconds", microsFieldType, Seq.empty[Field].asJava)).asJava)
  }

  /**
   * Maps field from Spark to Arrow. NOTE: timeZoneId required for TimestampType
   *
   * @param losslessInternalTypes
   *   when true, types whose standard Arrow encoding cannot cover their full Spark value domain
   *   (nanosecond timestamps, CalendarInterval) map to lossless struct representations instead.
   *   Only internal-storage callers with no external Arrow consumer (e.g. the Arrow-based Dataset
   *   cache) should pass true; interchange paths must keep the default. See
   *   `toTimestampNanosStructField` for the full rationale.
   */
  def toArrowField(
      name: String,
      dt: DataType,
      nullable: Boolean,
      timeZoneId: String,
      largeVarTypes: Boolean = false,
      metadata: Metadata = Metadata.empty,
      losslessInternalTypes: Boolean = false): Field = {
    dt match {
      case ArrayType(elementType, containsNull) =>
        val fieldType =
          new FieldType(nullable, ArrowType.List.INSTANCE, null, toArrowMetaData(metadata))
        new Field(
          name,
          fieldType,
          Seq(
            toArrowField(
              "element",
              elementType,
              containsNull,
              timeZoneId,
              largeVarTypes,
              Metadata.empty,
              losslessInternalTypes)).asJava)
      case StructType(fields) =>
        val fieldType =
          new FieldType(nullable, ArrowType.Struct.INSTANCE, null, toArrowMetaData(metadata))
        new Field(
          name,
          fieldType,
          fields
            .map { field =>
              toArrowField(
                field.name,
                field.dataType,
                field.nullable,
                timeZoneId,
                largeVarTypes,
                field.metadata,
                losslessInternalTypes)
            }
            .toImmutableArraySeq
            .asJava)
      case MapType(keyType, valueType, valueContainsNull) =>
        val fieldType =
          new FieldType(nullable, new ArrowType.Map(false), null, toArrowMetaData(metadata))
        // Note: Map Type struct can not be null, Struct Type key field can not be null
        new Field(
          name,
          fieldType,
          Seq(
            toArrowField(
              MapVector.DATA_VECTOR_NAME,
              new StructType()
                .add(MapVector.KEY_NAME, keyType, nullable = false)
                .add(MapVector.VALUE_NAME, valueType, nullable = valueContainsNull),
              nullable = false,
              timeZoneId,
              largeVarTypes,
              Metadata.empty,
              losslessInternalTypes)).asJava)
      case udt: UserDefinedType[_] =>
        toArrowField(
          name,
          udt.sqlType,
          nullable,
          timeZoneId,
          largeVarTypes,
          metadata,
          losslessInternalTypes)
      case g: GeometryType =>
        val fieldType =
          new FieldType(nullable, ArrowType.Struct.INSTANCE, null, toArrowMetaData(metadata))

        // WKB field is tagged with additional metadata so we can identify that the arrow
        // struct actually represents a geometry schema.
        val wkbFieldType = new FieldType(
          false,
          toArrowType(BinaryType, timeZoneId, largeVarTypes),
          null,
          Map("geometry" -> "true", "srid" -> g.srid.toString).asJava)

        new Field(
          name,
          fieldType,
          Seq(
            toArrowField("srid", IntegerType, false, timeZoneId, largeVarTypes),
            new Field("wkb", wkbFieldType, Seq.empty[Field].asJava)).asJava)

      case g: GeographyType =>
        val fieldType =
          new FieldType(nullable, ArrowType.Struct.INSTANCE, null, toArrowMetaData(metadata))

        // WKB field is tagged with additional metadata so we can identify that the arrow
        // struct actually represents a geography schema.
        val wkbFieldType = new FieldType(
          false,
          toArrowType(BinaryType, timeZoneId, largeVarTypes),
          null,
          Map("geography" -> "true", "srid" -> g.srid.toString).asJava)

        new Field(
          name,
          fieldType,
          Seq(
            toArrowField("srid", IntegerType, false, timeZoneId, largeVarTypes),
            new Field("wkb", wkbFieldType, Seq.empty[Field].asJava)).asJava)
      case _: VariantType =>
        val fieldType =
          new FieldType(nullable, ArrowType.Struct.INSTANCE, null, toArrowMetaData(metadata))
        // The metadata field is tagged with additional metadata so we can identify that the arrow
        // struct actually represents a variant schema.
        val metadataFieldType = new FieldType(
          false,
          toArrowType(BinaryType, timeZoneId, largeVarTypes),
          null,
          Map("variant" -> "true").asJava)
        new Field(
          name,
          fieldType,
          Seq(
            toArrowField("value", BinaryType, false, timeZoneId, largeVarTypes),
            new Field("metadata", metadataFieldType, Seq.empty[Field].asJava)).asJava)
      case CalendarIntervalType if losslessInternalTypes =>
        toCalendarIntervalStructField(name, nullable, metadata)
      case t: TimestampNTZNanosType if losslessInternalTypes =>
        toTimestampNanosStructField(name, isNtz = true, t.precision, nullable, metadata)
      case t: TimestampLTZNanosType if losslessInternalTypes =>
        toTimestampNanosStructField(name, isNtz = false, t.precision, nullable, metadata)
      case t: TimestampNTZNanosType =>
        toPrecisionTaggedArrowField(
          name,
          t,
          t.precision,
          timestampNanosPrecisionKey,
          nullable,
          timeZoneId,
          largeVarTypes,
          metadata)
      case t: TimestampLTZNanosType =>
        toPrecisionTaggedArrowField(
          name,
          t,
          t.precision,
          timestampNanosPrecisionKey,
          nullable,
          timeZoneId,
          largeVarTypes,
          metadata)
      case t: TimeType =>
        toPrecisionTaggedArrowField(
          name,
          t,
          t.precision,
          timePrecisionKey,
          nullable,
          timeZoneId,
          largeVarTypes,
          metadata)
      case dataType =>
        val fieldType = new FieldType(
          nullable,
          toArrowType(dataType, timeZoneId, largeVarTypes),
          null,
          toArrowMetaData(metadata))
        new Field(name, fieldType, Seq.empty[Field].asJava)
    }
  }

  def isVariantField(field: Field): Boolean = {
    assert(field.getType.isInstanceOf[ArrowType.Struct])
    field.getChildren.asScala
      .map(_.getName)
      .asJava
      .containsAll(Seq("value", "metadata").asJava) && field.getChildren.asScala.exists { child =>
      child.getName == "metadata" && child.getMetadata.getOrDefault("variant", "false") == "true"
    }
  }

  def isGeometryField(field: Field): Boolean = {
    assert(field.getType.isInstanceOf[ArrowType.Struct])
    field.getChildren.asScala
      .map(_.getName)
      .asJava
      .containsAll(Seq("wkb", "srid").asJava) && field.getChildren.asScala.exists { child =>
      child.getName == "wkb" && child.getMetadata.getOrDefault("geometry", "false") == "true"
    }
  }

  def isGeographyField(field: Field): Boolean = {
    assert(field.getType.isInstanceOf[ArrowType.Struct])
    field.getChildren.asScala
      .map(_.getName)
      .asJava
      .containsAll(Seq("wkb", "srid").asJava) && field.getChildren.asScala.exists { child =>
      child.getName == "wkb" && child.getMetadata.getOrDefault("geography", "false") == "true"
    }
  }

  // Both lossless-struct recognizers below accept only the exact canonical shape built by
  // `toArrowField` (child count, order, types, and nullability), not merely the presence of the
  // tag and child names. The struct writers fill children positionally while ArrowColumnVector's
  // accessors read them by name, so a permissive match on, say, a tagged but reordered schema
  // would silently swap component values. Anything non-canonical falls back to the generic
  // struct handling, which is order-faithful.
  private def isCanonicalStructChild(
      child: Field,
      name: String,
      arrowType: ArrowType): Boolean = {
    child.getName == name && child.getType == arrowType && !child.isNullable
  }

  /**
   * Whether the Arrow struct field is the lossless representation of a nanosecond timestamp built
   * by `toArrowField` with `losslessInternalTypes = true`. Also callable from Java
   * (ArrowColumnVector) to select the timestamp accessor for such structs.
   */
  def isTimestampNanosStructField(field: Field): Boolean = {
    field.getType.isInstanceOf[ArrowType.Struct] && {
      val children = field.getChildren
      children.size == 2 &&
      isCanonicalStructChild(children.get(0), "epochMicros", new ArrowType.Int(8 * 8, true)) &&
      isCanonicalStructChild(
        children.get(1),
        "nanosWithinMicro",
        new ArrowType.Int(8 * 2, true)) &&
      Set("ntz", "ltz").contains(
        children.get(0).getMetadata.getOrDefault(timestampNanosStructKey, ""))
    }
  }

  /**
   * Whether the Arrow struct field is the lossless representation of a CalendarInterval built by
   * `toArrowField` with `losslessInternalTypes = true`. Also callable from Java
   * (ArrowColumnVector) to select the interval accessor for such structs.
   */
  def isCalendarIntervalStructField(field: Field): Boolean = {
    field.getType.isInstanceOf[ArrowType.Struct] && {
      val children = field.getChildren
      children.size == 3 &&
      isCanonicalStructChild(children.get(0), "months", new ArrowType.Int(8 * 4, true)) &&
      isCanonicalStructChild(children.get(1), "days", new ArrowType.Int(8 * 4, true)) &&
      isCanonicalStructChild(children.get(2), "microseconds", new ArrowType.Int(8 * 8, true)) &&
      children.get(0).getMetadata.getOrDefault(calendarIntervalStructKey, "false") == "true"
    }
  }

  def fromArrowField(field: Field): DataType = {
    field.getType match {
      case _: ArrowType.Map =>
        val elementField = field.getChildren.get(0)
        val keyType = fromArrowField(elementField.getChildren.get(0))
        val valueType = fromArrowField(elementField.getChildren.get(1))
        MapType(keyType, valueType, elementField.getChildren.get(1).isNullable)
      case ArrowType.List.INSTANCE =>
        val elementField = field.getChildren().get(0)
        val elementType = fromArrowField(elementField)
        ArrayType(elementType, containsNull = elementField.isNullable)
      case ArrowType.Struct.INSTANCE if isCalendarIntervalStructField(field) =>
        CalendarIntervalType
      case ArrowType.Struct.INSTANCE if isTimestampNanosStructField(field) =>
        val microsChild = field.getChildren.asScala.find(_.getName == "epochMicros").get
        val isNtz = microsChild.getMetadata.get(timestampNanosStructKey) == "ntz"
        // Recover the precision like the Timestamp(NANOSECOND) case below: a missing or invalid
        // precision key falls back to the canonical maximum precision.
        val precision = Option(microsChild.getMetadata.get(timestampNanosPrecisionKey))
          .flatMap(s => scala.util.Try(s.toInt).toOption)
          .filter { p =>
            p >= TimestampNTZNanosType.MIN_PRECISION && p <= TimestampNTZNanosType.MAX_PRECISION
          }
          .getOrElse(TimestampNTZNanosType.MAX_PRECISION)
        if (isNtz) TimestampNTZNanosType(precision) else TimestampLTZNanosType(precision)
      case ArrowType.Struct.INSTANCE if isVariantField(field) =>
        VariantType
      case ArrowType.Struct.INSTANCE if isGeometryField(field) =>
        // We expect that type metadata is associated with wkb field.
        val metadataField =
          field.getChildren.asScala.filter { child => child.getName == "wkb" }.head
        val srid = metadataField.getMetadata.get("srid").toInt
        if (srid == GeometryType.MIXED_SRID) {
          GeometryType("ANY")
        } else {
          GeometryType(srid)
        }
      case ArrowType.Struct.INSTANCE if isGeographyField(field) =>
        // We expect that type metadata is associated with wkb field.
        val metadataField =
          field.getChildren.asScala.filter { child => child.getName == "wkb" }.head
        val srid = metadataField.getMetadata.get("srid").toInt
        if (srid == GeographyType.MIXED_SRID) {
          GeographyType("ANY")
        } else {
          GeographyType(srid)
        }
      case ArrowType.Struct.INSTANCE =>
        val fields = field.getChildren().asScala.map { child =>
          val dt = fromArrowField(child)
          StructField(child.getName, dt, child.isNullable, fromArrowMetaData(child.getMetadata))
        }
        StructType(fields.toArray)
      // Recover the exact precision of nanosecond timestamps from the field metadata written by
      // `toPrecisionTaggedArrowField`. Foreign Arrow data (or an out-of-range value) has no usable
      // key, so fall back to the canonical maximum precision via `fromArrowType`.
      case ts: ArrowType.Timestamp if ts.getUnit == TimeUnit.NANOSECOND =>
        val precision = Option(field.getMetadata.get(timestampNanosPrecisionKey))
          .flatMap(s => scala.util.Try(s.toInt).toOption)
          .filter { p =>
            p >= TimestampNTZNanosType.MIN_PRECISION && p <= TimestampNTZNanosType.MAX_PRECISION
          }
        precision match {
          case Some(p) if ts.getTimezone == null => TimestampNTZNanosType(p)
          case Some(p) => TimestampLTZNanosType(p)
          case None => fromArrowType(ts)
        }
      // Recover the exact precision of TIME from the field metadata written by `toArrowField`.
      // Foreign Arrow data has no precision key, and a present-but-invalid value (out of [0, 9] or
      // non-numeric) is unusable, so either way fall back to the canonical microsecond precision
      // via `fromArrowType`.
      case t: ArrowType.Time if t.getUnit == TimeUnit.NANOSECOND =>
        Option(field.getMetadata.get(timePrecisionKey))
          .flatMap(s => scala.util.Try(s.toInt).toOption)
          .filter(p => p >= TimeType.MIN_PRECISION && p <= TimeType.MAX_PRECISION)
          .map(TimeType(_))
          .getOrElse(fromArrowType(t))
      case arrowType => fromArrowType(arrowType)
    }
  }

  /**
   * Maps schema from Spark to Arrow. NOTE: timeZoneId required for TimestampType in StructType
   *
   * @param losslessInternalTypes
   *   see `toArrowField`: opt-in full-domain struct encoding of nanosecond timestamps and
   *   CalendarInterval for internal storage; interchange paths must keep the default.
   */
  def toArrowSchema(
      schema: StructType,
      timeZoneId: String,
      errorOnDuplicatedFieldNames: Boolean,
      largeVarTypes: Boolean,
      losslessInternalTypes: Boolean = false): Schema = {
    new Schema(schema.map { field =>
      toArrowField(
        field.name,
        deduplicateFieldNames(field.dataType, errorOnDuplicatedFieldNames),
        field.nullable,
        timeZoneId,
        largeVarTypes,
        field.metadata,
        losslessInternalTypes)
    }.asJava)
  }

  /**
   * Maps schema from Spark to Arrow. NOTE: timeZoneId required for TimestampType in StructType
   */
  def toArrowSchema(schema: StructType, timeZoneId: String, largeVarTypes: Boolean): Schema = {
    new Schema(schema.map { field =>
      toArrowField(
        field.name,
        field.dataType,
        field.nullable,
        timeZoneId,
        largeVarTypes,
        field.metadata)
    }.asJava)
  }

  /**
   * Check the schema and fail once an insider struct type contains duplicated field names. Note
   * that the first level accepts duplicated names.
   */
  def failDuplicatedFieldNames(schema: StructType): Unit = {
    schema.fields.foreach { field => failDuplicatedFieldNamesImpl(field.dataType) }
  }

  /**
   * Check the schema and fail once a struct type contains duplicated field names.
   */
  private def failDuplicatedFieldNamesImpl(dt: DataType): Unit = {
    dt match {
      case st: StructType =>
        if (st.names.toSet.size != st.names.length) {
          throw ExecutionErrors.duplicatedFieldNameInArrowStructError(
            st.names.toImmutableArraySeq)
        }
        st.fields.foreach { field => failDuplicatedFieldNamesImpl(field.dataType) }
      case arr: ArrayType =>
        failDuplicatedFieldNamesImpl(arr.elementType)
      case map: MapType =>
        failDuplicatedFieldNamesImpl(map.keyType)
        failDuplicatedFieldNamesImpl(map.valueType)
      case udt: UserDefinedType[_] =>
        failDuplicatedFieldNamesImpl(udt.sqlType)
      case _ =>
    }
  }

  def fromArrowSchema(schema: Schema): StructType = {
    StructType(schema.getFields.asScala.map { field =>
      StructField(
        field.getName,
        fromArrowField(field),
        field.isNullable,
        fromArrowMetaData(field.getMetadata))
    }.toArray)
  }

  private def deduplicateFieldNames(
      dt: DataType,
      errorOnDuplicatedFieldNames: Boolean): DataType = dt match {
    case udt: UserDefinedType[_] =>
      deduplicateFieldNames(udt.sqlType, errorOnDuplicatedFieldNames)
    case st @ StructType(fields) =>
      val newNames = if (st.names.toSet.size == st.names.length) {
        st.names
      } else {
        if (errorOnDuplicatedFieldNames) {
          throw ExecutionErrors.duplicatedFieldNameInArrowStructError(
            st.names.toImmutableArraySeq)
        }
        val genNawName = st.names.groupBy(identity).map {
          case (name, names) if names.length > 1 =>
            val i = new AtomicInteger()
            name -> { () => s"${name}_${i.getAndIncrement()}" }
          case (name, _) => name -> { () => name }
        }
        st.names.map(genNawName(_)())
      }
      val newFields =
        fields.zip(newNames).map { case (StructField(_, dataType, nullable, metadata), name) =>
          StructField(
            name,
            deduplicateFieldNames(dataType, errorOnDuplicatedFieldNames),
            nullable,
            metadata)
        }
      StructType(newFields)
    case ArrayType(elementType, containsNull) =>
      ArrayType(deduplicateFieldNames(elementType, errorOnDuplicatedFieldNames), containsNull)
    case MapType(keyType, valueType, valueContainsNull) =>
      MapType(
        deduplicateFieldNames(keyType, errorOnDuplicatedFieldNames),
        deduplicateFieldNames(valueType, errorOnDuplicatedFieldNames),
        valueContainsNull)
    case _ => dt
  }
}
