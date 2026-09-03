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

import java.lang.{Boolean => JBoolean, Byte => JByte, Double => JDouble, Float => JFloat, Long => JLong, Short => JShort}
import java.math.{BigDecimal => JBigDecimal}
import java.nio.charset.StandardCharsets.UTF_8
import java.sql.{Date, Timestamp}
import java.time.{Duration, Instant, LocalDate, LocalTime, Period}
import java.time.temporal.ChronoField.MICRO_OF_DAY
import java.util.HashSet
import java.util.Locale

import scala.jdk.CollectionConverters._

import org.apache.parquet.filter2.predicate._
import org.apache.parquet.filter2.predicate.SparkFilterApi._
import org.apache.parquet.io.api.Binary
import org.apache.parquet.schema.{GroupType, LogicalTypeAnnotation, MessageType, PrimitiveComparator, PrimitiveType, Type}
import org.apache.parquet.schema.LogicalTypeAnnotation.{DecimalLogicalTypeAnnotation, IntLogicalTypeAnnotation, TimeUnit}
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName._
import org.apache.parquet.schema.Type.Repetition

import org.apache.spark.sql.catalyst.expressions.variant.{ObjectExtraction, VariantPathParser}
import org.apache.spark.sql.catalyst.util.{CaseInsensitiveMap, DateTimeUtils, IntervalUtils}
import org.apache.spark.sql.catalyst.util.RebaseDateTime.{rebaseGregorianToJulianDays, rebaseGregorianToJulianMicros, RebaseSpec}
import org.apache.spark.sql.execution.datasources.VariantMetadata
import org.apache.spark.sql.execution.datasources.parquet.types.ops.{ParquetFilterOps, ParquetTypeOps}
import org.apache.spark.sql.internal.LegacyBehaviorPolicy
import org.apache.spark.sql.sources
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.ArrayImplicits._

/**
 * Some utility function to convert Spark data source filters to Parquet filters.
 */
class ParquetFilters(
    schema: MessageType,
    pushDownDate: Boolean,
    pushDownTimestamp: Boolean,
    pushDownDecimal: Boolean,
    pushDownStringPredicate: Boolean,
    pushDownInFilterThreshold: Int,
    caseSensitive: Boolean,
    datetimeRebaseSpec: RebaseSpec,
    variantExtractionSchema: Option[StructType] = None) {
  // Shredded-variant physical field-name constants.
  private val TYPED_VALUE = "typed_value"
  private val VALUE = "value"

  // A map which contains parquet field name and data type, if predicate push down applies.
  //
  // Each key in `nameToParquetField` represents a column; `dots` are used as separators for
  // nested columns. If any part of the names contains `dots`, it is quoted to avoid confusion.
  // See `org.apache.spark.sql.connector.catalog.quote` for implementation details.
  private val nameToParquetField : Map[String, ParquetPrimitiveField] = {
    // Recursively traverse the parquet schema to get primitive fields that can be pushed-down.
    // `parentFieldNames` is used to keep track of the current nested level when traversing.
    def getPrimitiveFields(
        fields: Seq[Type],
        parentFieldNames: Array[String] = Array.empty): Seq[ParquetPrimitiveField] = {
      fields.flatMap {
        // Parquet only supports predicate push-down for non-repeated primitive types.
        // TODO(SPARK-39393): Remove extra condition when parquet added filter predicate support for
        //                    repeated columns (https://issues.apache.org/jira/browse/PARQUET-34)
        case p: PrimitiveType if p.getRepetition != Repetition.REPEATED =>
          Some(ParquetPrimitiveField(fieldNames = parentFieldNames :+ p.getName,
            fieldType = ParquetSchemaType(getNormalizedLogicalType(p),
              p.getPrimitiveTypeName, p.getTypeLength)))
        // Note that when g is a `Struct`, `g.getOriginalType` is `null`.
        // When g is a `Map`, `g.getOriginalType` is `MAP`.
        // When g is a `List`, `g.getOriginalType` is `LIST`.
        case g: GroupType if g.getOriginalType == null =>
          getPrimitiveFields(g.getFields.asScala.toSeq, parentFieldNames :+ g.getName)
        // Parquet only supports push-down for primitive types; as a result, Map and List types
        // are removed.
        case _ => None
      }
    }

    val primitiveFields = getPrimitiveFields(schema.getFields.asScala.toSeq).map { field =>
      import org.apache.spark.sql.connector.catalog.CatalogV2Implicits.MultipartIdentifierHelper
      (field.fieldNames.toImmutableArraySeq.quoted, field)
    }
    if (caseSensitive) {
      primitiveFields.toMap
    } else {
      // Don't consider ambiguity here, i.e. more than one field is matched in case insensitive
      // mode, just skip pushdown for these fields, they will trigger Exception when reading,
      // See: SPARK-25132.
      val dedupPrimitiveFields =
      primitiveFields
        .groupBy(_._1.toLowerCase(Locale.ROOT))
        .filter(_._2.size == 1)
        .transform((_, v) => v.head._2)
      CaseInsensitiveMap(dedupPrimitiveFields)
    }
  }

  /**
   * Holds a single primitive field information stored in the underlying parquet file.
   *
   * @param fieldNames a field name as an array of string multi-identifier in parquet file
   * @param fieldType field type related info in parquet file
   */
  private case class ParquetPrimitiveField(
      fieldNames: Array[String],
      fieldType: ParquetSchemaType)

  /**
   * Holds the mapping from a logical shredded-variant path (e.g. "v.`0`") to the physical
   * shredded columns needed to push a sound row-group-skipping predicate.
   *
   * @param leaf the physical `typed_value` scalar leaf carrying min/max statistics
   * @param residualFieldNames the untyped `value` residual columns along the path, from the
   *                           top-level residual down to the leaf's own-level sibling. Each is a
   *                           physical field-name array. Only residuals that exist in this file's
   *                           schema are included; a value for the path can only be hiding in one
   *                           of these residuals when the typed leaf is NULL, which is what the
   *                           pushed predicate's guard checks (see `makeShreddedFilter`).
   *                           Spark's writer (`VariantShreddingWriter.castShredded`) always shreds
   *                           an object field that is in the shredding schema and routes only
   *                           non-schema keys into a level's own `value`, so with Spark-written
   *                           files a value can only fall back to the leaf's own-level residual;
   *                           the ancestor-level residuals guard against writers that legitimately
   *                           decline to shred an intermediate level.
   */
  private case class ShreddedVariantField(
      leaf: ParquetPrimitiveField,
      residualFieldNames: Seq[Array[String]])

  // Maps logical shredded-variant paths produced by PushVariantIntoScan (e.g. "v.`0`") to the
  // physical shredded columns. Populated only when `variantExtractionSchema` is provided and the
  // physical file schema actually shreds the requested path.
  //
  // Soundness: shredding is per-row and per-file best-effort. A row whose value does not fit the
  // shredded type (type mismatch or overflow), or whose field is not shredded in this file, is
  // stored in an untyped `value` residual with `typed_value` NULL. Parquet min/max excludes NULLs,
  // so pushing the predicate on the typed leaf alone could skip a row group that still holds a
  // matching row in a residual. To stay sound we push a guarded predicate that skips a row group
  // only when the leaf cannot match AND every value for the path is provably in the typed leaf.
  // See `makeShreddedFilter`.
  //
  // Lazy so it is computed after the `Parquet*Type` vals below are initialized (resolution reads
  // them via `expectedLeafType`); a strict val here would see them as null under Scala's
  // declaration-order initialization.
  private lazy val nameToShreddedVariantField: Map[String, ShreddedVariantField] = {
    variantExtractionSchema match {
      case Some(variantSchema) =>
        val entries = shreddedVariantEntries(
          variantSchema.fields.toSeq, schema.asGroupType(), Array.empty, Array.empty)
        if (caseSensitive) {
          entries.toMap
        } else {
          // Mirror `nameToParquetField`: drop names that are ambiguous under case-insensitive
          // matching rather than risk pushing a filter on the wrong physical column.
          val dedup = entries
            .groupBy(_._1.toLowerCase(Locale.ROOT))
            .filter(_._2.size == 1)
            .transform((_, v) => v.head._2)
          CaseInsensitiveMap(dedup)
        }
      case None => Map.empty
    }
  }

  // Look up a child of `group` by name, always case-sensitively. Returns the child type together
  // with its actual physical name so callers build paths from the on-disk names.
  //
  // The match is always exact-case because every caller resolves either a variant object key or a
  // structural `typed_value`/`value` name. Variant object keys are data, not Spark identifiers, and
  // the reader resolves them case-sensitively (VariantSchema.objectSchemaMap and
  // Variant.getFieldByKey use exact equals). A file may legally shred sibling keys differing only
  // in case (e.g. `A` and `a`), so a case-insensitive first-match could bind the predicate to the
  // wrong physical subtree and skip a row group that holds matching rows -- silent data loss. The
  // top-level variant column name is a Spark identifier and is matched by `caseSensitive`
  // separately, in `shreddedVariantEntries`.
  private def findChild(group: GroupType, name: String): Option[Type] = {
    group.getFields.asScala.find(_.getName == name)
  }

  // Look up the untyped `value` residual sibling in `group`, if it exists as a non-REPEATED
  // primitive. Returns the physical field name. `value` is a fixed structural name; matched exact.
  private def residualIn(group: GroupType): Option[String] =
    findChild(group, VALUE).collect {
      case p: PrimitiveType if p.getRepetition != Repetition.REPEATED => p.getName
    }

  // Shared by both the `nameToParquetField` traversal and the shredded leaf resolution so the two
  // pushdown paths normalize physical types identically.
  private def getNormalizedLogicalType(p: PrimitiveType): LogicalTypeAnnotation = {
    // SPARK-40280: Signed 64 bits on an INT64 and signed 32 bits on an INT32 are optional, but
    // the rest of the code here assumes they are not set, so normalize them to not being set.
    (p.getPrimitiveTypeName, p.getLogicalTypeAnnotation) match {
      case (INT32, intType: IntLogicalTypeAnnotation)
        if intType.getBitWidth() == 32 && intType.isSigned() => null
      case (INT64, intType: IntLogicalTypeAnnotation)
        if intType.getBitWidth() == 64 && intType.isSigned() => null
      case (_, otherType) => otherType
    }
  }

  // Navigate the regular shredding layout from a variant column's physical group, resolving both
  // the typed leaf and the residual `value` columns along the path. The layout is:
  //   <col> / typed_value / k0 / typed_value / ... / kN / typed_value   (leaf)
  //   <col> / value                                                     (L0 residual)
  //   <col> / typed_value / k0 / value                                  (L1 residual)
  //   ...
  //   <col> / typed_value / k0 / ... / kN / value                       (leaf-level residual)
  // Paths are built from the on-disk field names (via `findChild`). Object keys and the structural
  // typed_value/value names are matched case-sensitively (variant keys are data; see `findChild`).
  // A value for the path can only be hiding in one of these residual `value` columns when the typed
  // leaf is NULL, so IS NOT NULL on all of them is the soundness guard.
  // Residuals absent in this file's schema are skipped (that level cannot hold a fallback here).
  // Returns None if the file does not shred this path down to a non-REPEATED scalar leaf (nothing
  // is pushed and the row group is simply read).
  private def resolveShredded(
      physCol: GroupType,
      physColPath: Array[String],
      keys: Array[String],
      targetType: DataType): Option[ShreddedVariantField] = {
    if (keys.isEmpty) return None
    val residuals = scala.collection.mutable.ArrayBuffer.empty[Array[String]]
    // L0: the variant column's own residual.
    residualIn(physCol).foreach(r => residuals += (physColPath :+ r))
    // Descend key by key: <group>/typed_value/<key>. Collect each level's residual sibling.
    var group = physCol
    var namePath = physColPath
    var idx = 0
    while (idx < keys.length) {
      val typedChild = findChild(group, TYPED_VALUE) match {
        case Some(g: GroupType) => g
        case _ => return None
      }
      val typedName = typedChild.getName
      // Variant object keys are data, matched case-sensitively (see `findChild`).
      val keyChild = findChild(typedChild, keys(idx)) match {
        case Some(g: GroupType) => g
        case _ => return None
      }
      namePath = namePath ++ Array(typedName, keyChild.getName)
      group = keyChild
      residualIn(group).foreach(r => residuals += (namePath :+ r))
      idx += 1
    }
    // The leaf is the typed_value of the last key group.
    findChild(group, TYPED_VALUE) match {
      case Some(p: PrimitiveType) if p.getRepetition != Repetition.REPEATED =>
        val leafType =
          ParquetSchemaType(getNormalizedLogicalType(p), p.getPrimitiveTypeName, p.getTypeLength)
        // Accept the leaf when the extraction's target type maps to it exactly, or when the leaf is
        // a narrower signed integer than the target (safe widening). Narrowing must be rejected:
        // for a narrower extraction such as smallint over an int leaf, the leaf min/max is over int
        // values, so a row group holding only out-of-int16-range values (residuals null) would be
        // skipped, changing an eager INVALID_VARIANT_CAST into an empty result. Widening is sound:
        // every value in a narrower leaf casts to the wider target losslessly and ordering is
        // preserved, so the leaf stats bound the target predicate; `valueMatchesParquetType` still
        // rejects a literal outside the leaf's representable range at push time.
        if (!expectedLeafType(targetType).contains(leafType) &&
            !isSafeIntegerWidening(leafType, targetType)) {
          None
        } else {
          val leaf = ParquetPrimitiveField(namePath :+ p.getName, leafType)
          Some(ShreddedVariantField(leaf, residuals.toSeq))
        }
      case _ => None
    }
  }

  // Whether an extraction of `targetType` may be pushed against a physical `leafType` that is a
  // narrower signed integer (e.g. bigint extraction over an int/smallint/tinyint leaf). Only the
  // integer family widens soundly here.
  private def isSafeIntegerWidening(
      leafType: ParquetSchemaType, targetType: DataType): Boolean = {
    def intRank(t: ParquetSchemaType): Option[Int] = t match {
      case ParquetByteType => Some(0)
      case ParquetShortType => Some(1)
      case ParquetIntegerType => Some(2)
      case ParquetLongType => Some(3)
      case _ => None
    }
    (intRank(leafType), expectedLeafType(targetType).flatMap(intRank)) match {
      case (Some(leafRank), Some(targetRank)) => leafRank < targetRank
      case _ => false
    }
  }

  // The physical Parquet leaf type a shredded scalar of `targetType` is written as, matching
  // `SparkShreddingUtils.variantShreddingSchema` (which writes the scalar's natural type) and the
  // `Parquet*Type` normalization used for the leaf. Returns None for types that are not shredded as
  // a comparable scalar leaf (or that this pushdown does not handle), so the path is not pushed.
  private def expectedLeafType(targetType: DataType): Option[ParquetSchemaType] = targetType match {
    case BooleanType => Some(ParquetBooleanType)
    case ByteType => Some(ParquetByteType)
    case ShortType => Some(ParquetShortType)
    case IntegerType => Some(ParquetIntegerType)
    case LongType => Some(ParquetLongType)
    case FloatType => Some(ParquetFloatType)
    case DoubleType => Some(ParquetDoubleType)
    case _: StringType => Some(ParquetStringType)
    case BinaryType => Some(ParquetBinaryType)
    case DateType => Some(ParquetDateType)
    case d: DecimalType if DecimalType.is32BitDecimalType(d) =>
      Some(ParquetSchemaType(LogicalTypeAnnotation.decimalType(d.scale, d.precision), INT32, 0))
    case d: DecimalType if DecimalType.is64BitDecimalType(d) =>
      Some(ParquetSchemaType(LogicalTypeAnnotation.decimalType(d.scale, d.precision), INT64, 0))
    case d: DecimalType =>
      Some(ParquetSchemaType(LogicalTypeAnnotation.decimalType(d.scale, d.precision),
        FIXED_LEN_BYTE_ARRAY, Decimal.minBytesForPrecision(d.precision)))
    case _ => None
  }

  // Walk the variant-extraction schema alongside the physical Parquet group, collecting
  // logicalName -> ShreddedVariantField entries for shredded scalar object paths that this file
  // actually shreds. Only object-extraction, scalar-leaf paths are eligible; array-index paths and
  // synthetic (empty / placeholder / companion / full-variant passthrough) paths resolve to None.
  //
  // `logicalParentNames` accumulates the logical field names (used to build the map key that the
  // pushed filter references); `physParentNames` accumulates the on-disk field names (used to build
  // the physical Parquet column paths). They differ only in case under case-insensitive matching.
  private def shreddedVariantEntries(
      variantFields: Seq[StructField],
      physGroup: GroupType,
      logicalParentNames: Array[String],
      physParentNames: Array[String]): Seq[(String, ShreddedVariantField)] = {
    import org.apache.spark.sql.connector.catalog.CatalogV2Implicits.MultipartIdentifierHelper
    variantFields.flatMap { field =>
      val physChildOpt = physGroup.getFields.asScala.collectFirst {
        case g: GroupType if
          (if (caseSensitive) g.getName == field.name
           else g.getName.equalsIgnoreCase(field.name)) => g
      }
      physChildOpt match {
        case None => Nil
        case Some(physChild) =>
          val logicalColPath = logicalParentNames :+ field.name
          val physColPath = physParentNames :+ physChild.getName
          field.dataType match {
            // Variant struct: each child is a requested extraction carrying VariantMetadata.
            case s: StructType if VariantMetadata.isVariantStruct(s) =>
              s.fields.toSeq.flatMap { extraction =>
                if (!extraction.metadata.contains(VariantMetadata.METADATA_KEY)) {
                  Nil
                } else {
                  val meta = VariantMetadata.fromMetadata(extraction.metadata)
                  // `VariantPathParser.parse` returns None for an unparseable path; use it directly
                  // rather than `parsedPath()` (which throws) so a bad path is a clean no-push and
                  // we don't swallow unrelated exceptions.
                  VariantPathParser.parse(meta.path) match {
                    case None => Nil
                    case Some(segments) =>
                      // Only scalar object-extraction paths are eligible. Reject array-index paths
                      // (a mix of ObjectExtraction and ArrayExtraction fails this check) and empty
                      // paths ("$" / passthrough / companion), which yield no keys.
                      val keys = segments.collect { case o: ObjectExtraction => o.key }
                      if (keys.isEmpty || keys.length != segments.length) {
                        Nil
                      } else {
                        // `physChild` is this variant column's physical group; `physColPath` holds
                        // its on-disk name path. Navigate the shredding layout from there. The
                        // extraction's target type must match the physical leaf type exactly.
                        resolveShredded(physChild, physColPath, keys, extraction.dataType) match {
                          case None => Nil
                          case Some(shredded) =>
                            val logicalName =
                              (logicalColPath :+ extraction.name).toImmutableArraySeq.quoted
                            Seq(logicalName -> shredded)
                        }
                      }
                  }
                }
              }
            // Ordinary struct: recurse into nested fields.
            case s: StructType if !VariantMetadata.isVariantStruct(s) =>
              shreddedVariantEntries(s.fields.toSeq, physChild, logicalColPath, physColPath)
            case _ => Nil
          }
      }
    }
  }

  private case class ParquetSchemaType(
      logicalTypeAnnotation: LogicalTypeAnnotation,
      primitiveTypeName: PrimitiveTypeName,
      length: Int)

  private val ParquetBooleanType = ParquetSchemaType(null, BOOLEAN, 0)
  private val ParquetByteType =
    ParquetSchemaType(LogicalTypeAnnotation.intType(8, true), INT32, 0)
  private val ParquetShortType =
    ParquetSchemaType(LogicalTypeAnnotation.intType(16, true), INT32, 0)
  private val ParquetIntegerType = ParquetSchemaType(null, INT32, 0)
  private val ParquetLongType = ParquetSchemaType(null, INT64, 0)
  private val ParquetFloatType = ParquetSchemaType(null, FLOAT, 0)
  private val ParquetDoubleType = ParquetSchemaType(null, DOUBLE, 0)
  private val ParquetStringType =
    ParquetSchemaType(LogicalTypeAnnotation.stringType(), BINARY, 0)
  private val ParquetBinaryType = ParquetSchemaType(null, BINARY, 0)
  private val ParquetDateType =
    ParquetSchemaType(LogicalTypeAnnotation.dateType(), INT32, 0)
  private val ParquetTimestampMicrosType =
    ParquetSchemaType(LogicalTypeAnnotation.timestampType(true, TimeUnit.MICROS), INT64, 0)
  private val ParquetTimestampMillisType =
    ParquetSchemaType(LogicalTypeAnnotation.timestampType(true, TimeUnit.MILLIS), INT64, 0)

  /**
   * Extractor that maps a Parquet field's schema to its Types Framework filter ops, if the
   * field's on-disk encoding belongs to a framework-managed type. Defined here, not in the
   * ops package, because it pattern-matches on the private [[ParquetSchemaType]]. A `Some`
   * routes the field's predicates through the framework ops; `None` falls through to the
   * built-in cases below. Framework types use Parquet encodings distinct from the built-in
   * cases, so the extractor never shadows them. This replaces the inline TimeType handling
   * (TIME(MICROS) -> micros Long), which now lives in TimeTypeParquetOps.filterOps.
   */
  private object FrameworkFilterOps {
    def unapply(parquetSchemaType: ParquetSchemaType): Option[ParquetFilterOps] =
      ParquetTypeOps.filterOpsFor(
        parquetSchemaType.logicalTypeAnnotation, parquetSchemaType.primitiveTypeName)
  }

  // SPARK-53368: TIME(MICROS, isAdjustedToUTC=true) is not registered in the framework's
  // filterOps (which only covers isAdjustedToUTC=false), so it is matched explicitly here.
  private val ParquetTimeMicrosTypeAdjToUTC =
    ParquetSchemaType(LogicalTypeAnnotation.timeType(true, TimeUnit.MICROS), INT64, 0)

  private def dateToDays(date: Any): Int = {
    val gregorianDays = date match {
      case d: Date => DateTimeUtils.fromJavaDate(d)
      case ld: LocalDate => DateTimeUtils.localDateToDays(ld)
    }
    datetimeRebaseSpec.mode match {
      case LegacyBehaviorPolicy.LEGACY => rebaseGregorianToJulianDays(gregorianDays)
      case _ => gregorianDays
    }
  }

  private def timestampToMicros(v: Any): JLong = {
    val gregorianMicros = v match {
      case i: Instant => DateTimeUtils.instantToMicros(i)
      case t: Timestamp => DateTimeUtils.fromJavaTimestamp(t)
    }
    datetimeRebaseSpec.mode match {
      case LegacyBehaviorPolicy.LEGACY =>
        rebaseGregorianToJulianMicros(datetimeRebaseSpec.timeZone, gregorianMicros)
      case _ => gregorianMicros
    }
  }

  private def localTimeToMicros(v: Any): JLong = {
    v.asInstanceOf[LocalTime].getLong(MICRO_OF_DAY)
  }

  // A LocalTime filter literal is only pushable against a TIME(MICROS) column when it carries no
  // sub-microsecond (nanosecond) component. TimeType is held internally as nanos-of-day, so a
  // literal can be finer-grained than the on-disk MICROS unit; localTimeToMicros would then
  // truncate it and push a bound that skips matching rows (e.g. `t < 12:00:00.000000001` truncates
  // to `t < 12:00:00`, wrongly pruning a row at exactly 12:00:00; `!=` has the symmetric
  // false-negative). Sub-microsecond literals are therefore not pushed down; the read falls back
  // to a full scan, which is always correct.
  private def isMicrosResolution(v: Any): Boolean =
    v.asInstanceOf[LocalTime].getNano % 1000 == 0

  private def decimalToInt32(decimal: JBigDecimal): Integer = decimal.unscaledValue().intValue()

  private def decimalToInt64(decimal: JBigDecimal): JLong = decimal.unscaledValue().longValue()

  private def decimalToByteArray(decimal: JBigDecimal, numBytes: Int): Binary = {
    val decimalBuffer = new Array[Byte](numBytes)
    val bytes = decimal.unscaledValue().toByteArray

    val fixedLengthBytes = if (bytes.length == numBytes) {
      bytes
    } else {
      val signByte = if (bytes.head < 0) -1: Byte else 0: Byte
      java.util.Arrays.fill(decimalBuffer, 0, numBytes - bytes.length, signByte)
      System.arraycopy(bytes, 0, decimalBuffer, numBytes - bytes.length, bytes.length)
      decimalBuffer
    }
    Binary.fromConstantByteArray(fixedLengthBytes, 0, numBytes)
  }

  private def timestampToMillis(v: Any): JLong = {
    val micros = timestampToMicros(v)
    val millis = DateTimeUtils.microsToMillis(micros)
    millis.asInstanceOf[JLong]
  }

  private def toIntValue(v: Any): Integer = {
    Option(v).map {
      case p: Period => IntervalUtils.periodToMonths(p)
      case n => n.asInstanceOf[Number].intValue
    }.map(_.asInstanceOf[Integer]).orNull
  }

  private def toLongValue(v: Any): JLong = v match {
    case d: Duration => IntervalUtils.durationToMicros(d)
    case lt: LocalTime => localTimeToMicros(lt)
    case l => l.asInstanceOf[JLong]
  }

  private val makeEq:
    PartialFunction[ParquetSchemaType, (Array[String], Any) => FilterPredicate] = {
    case ParquetBooleanType =>
      (n: Array[String], v: Any) => FilterApi.eq(booleanColumn(n), v.asInstanceOf[JBoolean])
    case ParquetByteType | ParquetShortType | ParquetIntegerType =>
      (n: Array[String], v: Any) => FilterApi.eq(intColumn(n), toIntValue(v))
    case ParquetLongType =>
      (n: Array[String], v: Any) => FilterApi.eq(longColumn(n), toLongValue(v))
    case ParquetFloatType =>
      (n: Array[String], v: Any) => FilterApi.eq(floatColumn(n), v.asInstanceOf[JFloat])
    case ParquetDoubleType =>
      (n: Array[String], v: Any) => FilterApi.eq(doubleColumn(n), v.asInstanceOf[JDouble])

    // Binary.fromString and Binary.fromByteArray don't accept null values
    case ParquetStringType =>
      (n: Array[String], v: Any) => FilterApi.eq(
        binaryColumn(n),
        Option(v).map(s => Binary.fromString(s.asInstanceOf[String])).orNull)
    case ParquetBinaryType =>
      (n: Array[String], v: Any) => FilterApi.eq(
        binaryColumn(n),
        Option(v).map(b => Binary.fromReusedByteArray(v.asInstanceOf[Array[Byte]])).orNull)
    case ParquetDateType if pushDownDate =>
      (n: Array[String], v: Any) => FilterApi.eq(
        intColumn(n),
        Option(v).map(date => dateToDays(date).asInstanceOf[Integer]).orNull)
    case ParquetTimestampMicrosType if pushDownTimestamp =>
      (n: Array[String], v: Any) => FilterApi.eq(
        longColumn(n),
        Option(v).map(timestampToMicros).orNull)
    case ParquetTimestampMillisType if pushDownTimestamp =>
      (n: Array[String], v: Any) => FilterApi.eq(
        longColumn(n),
        Option(v).map(timestampToMillis).orNull)
    case FrameworkFilterOps(ops) =>
      (n: Array[String], v: Any) => ops.makeEq(n, v)
    case ParquetTimeMicrosTypeAdjToUTC =>
      (n: Array[String], v: Any) => FilterApi.eq(
        longColumn(n),
        Option(v).map(localTimeToMicros).orNull)

    case ParquetSchemaType(_: DecimalLogicalTypeAnnotation, INT32, _) if pushDownDecimal =>
      (n: Array[String], v: Any) => FilterApi.eq(
        intColumn(n),
        Option(v).map(d => decimalToInt32(d.asInstanceOf[JBigDecimal])).orNull)
    case ParquetSchemaType(_: DecimalLogicalTypeAnnotation, INT64, _) if pushDownDecimal =>
      (n: Array[String], v: Any) => FilterApi.eq(
        longColumn(n),
        Option(v).map(d => decimalToInt64(d.asInstanceOf[JBigDecimal])).orNull)
    case ParquetSchemaType(_: DecimalLogicalTypeAnnotation, FIXED_LEN_BYTE_ARRAY, length)
        if pushDownDecimal =>
      (n: Array[String], v: Any) => FilterApi.eq(
        binaryColumn(n),
        Option(v).map(d => decimalToByteArray(d.asInstanceOf[JBigDecimal], length)).orNull)
  }

  private val makeNotEq:
    PartialFunction[ParquetSchemaType, (Array[String], Any) => FilterPredicate] = {
    case ParquetBooleanType =>
      (n: Array[String], v: Any) => FilterApi.notEq(booleanColumn(n), v.asInstanceOf[JBoolean])
    case ParquetByteType | ParquetShortType | ParquetIntegerType =>
      (n: Array[String], v: Any) => FilterApi.notEq(intColumn(n), toIntValue(v))
    case ParquetLongType =>
      (n: Array[String], v: Any) => FilterApi.notEq(longColumn(n), toLongValue(v))
    case ParquetFloatType =>
      (n: Array[String], v: Any) => FilterApi.notEq(floatColumn(n), v.asInstanceOf[JFloat])
    case ParquetDoubleType =>
      (n: Array[String], v: Any) => FilterApi.notEq(doubleColumn(n), v.asInstanceOf[JDouble])

    case ParquetStringType =>
      (n: Array[String], v: Any) => FilterApi.notEq(
        binaryColumn(n),
        Option(v).map(s => Binary.fromString(s.asInstanceOf[String])).orNull)
    case ParquetBinaryType =>
      (n: Array[String], v: Any) => FilterApi.notEq(
        binaryColumn(n),
        Option(v).map(b => Binary.fromReusedByteArray(v.asInstanceOf[Array[Byte]])).orNull)
    case ParquetDateType if pushDownDate =>
      (n: Array[String], v: Any) => FilterApi.notEq(
        intColumn(n),
        Option(v).map(date => dateToDays(date).asInstanceOf[Integer]).orNull)
    case ParquetTimestampMicrosType if pushDownTimestamp =>
      (n: Array[String], v: Any) => FilterApi.notEq(
        longColumn(n),
        Option(v).map(timestampToMicros).orNull)
    case ParquetTimestampMillisType if pushDownTimestamp =>
      (n: Array[String], v: Any) => FilterApi.notEq(
        longColumn(n),
        Option(v).map(timestampToMillis).orNull)
    case FrameworkFilterOps(ops) =>
      (n: Array[String], v: Any) => ops.makeNotEq(n, v)
    case ParquetTimeMicrosTypeAdjToUTC =>
      (n: Array[String], v: Any) => FilterApi.notEq(
        longColumn(n),
        Option(v).map(localTimeToMicros).orNull)

    case ParquetSchemaType(_: DecimalLogicalTypeAnnotation, INT32, _) if pushDownDecimal =>
      (n: Array[String], v: Any) => FilterApi.notEq(
        intColumn(n),
        Option(v).map(d => decimalToInt32(d.asInstanceOf[JBigDecimal])).orNull)
    case ParquetSchemaType(_: DecimalLogicalTypeAnnotation, INT64, _) if pushDownDecimal =>
      (n: Array[String], v: Any) => FilterApi.notEq(
        longColumn(n),
        Option(v).map(d => decimalToInt64(d.asInstanceOf[JBigDecimal])).orNull)
    case ParquetSchemaType(_: DecimalLogicalTypeAnnotation, FIXED_LEN_BYTE_ARRAY, length)
        if pushDownDecimal =>
      (n: Array[String], v: Any) => FilterApi.notEq(
        binaryColumn(n),
        Option(v).map(d => decimalToByteArray(d.asInstanceOf[JBigDecimal], length)).orNull)
  }

  private val makeLt:
    PartialFunction[ParquetSchemaType, (Array[String], Any) => FilterPredicate] = {
    case ParquetByteType | ParquetShortType | ParquetIntegerType =>
      (n: Array[String], v: Any) => FilterApi.lt(intColumn(n), toIntValue(v))
    case ParquetLongType =>
      (n: Array[String], v: Any) => FilterApi.lt(longColumn(n), toLongValue(v))
    case ParquetFloatType =>
      (n: Array[String], v: Any) => FilterApi.lt(floatColumn(n), v.asInstanceOf[JFloat])
    case ParquetDoubleType =>
      (n: Array[String], v: Any) => FilterApi.lt(doubleColumn(n), v.asInstanceOf[JDouble])

    case ParquetStringType =>
      (n: Array[String], v: Any) =>
        FilterApi.lt(binaryColumn(n), Binary.fromString(v.asInstanceOf[String]))
    case ParquetBinaryType =>
      (n: Array[String], v: Any) =>
        FilterApi.lt(binaryColumn(n), Binary.fromReusedByteArray(v.asInstanceOf[Array[Byte]]))
    case ParquetDateType if pushDownDate =>
      (n: Array[String], v: Any) =>
        FilterApi.lt(intColumn(n), dateToDays(v).asInstanceOf[Integer])
    case ParquetTimestampMicrosType if pushDownTimestamp =>
      (n: Array[String], v: Any) => FilterApi.lt(longColumn(n), timestampToMicros(v))
    case ParquetTimestampMillisType if pushDownTimestamp =>
      (n: Array[String], v: Any) => FilterApi.lt(longColumn(n), timestampToMillis(v))
    case FrameworkFilterOps(ops) =>
      (n: Array[String], v: Any) => ops.makeLt(n, v)
    case ParquetTimeMicrosTypeAdjToUTC =>
      (n: Array[String], v: Any) => FilterApi.lt(longColumn(n), localTimeToMicros(v))

    case ParquetSchemaType(_: DecimalLogicalTypeAnnotation, INT32, _) if pushDownDecimal =>
      (n: Array[String], v: Any) =>
        FilterApi.lt(intColumn(n), decimalToInt32(v.asInstanceOf[JBigDecimal]))
    case ParquetSchemaType(_: DecimalLogicalTypeAnnotation, INT64, _) if pushDownDecimal =>
      (n: Array[String], v: Any) =>
        FilterApi.lt(longColumn(n), decimalToInt64(v.asInstanceOf[JBigDecimal]))
    case ParquetSchemaType(_: DecimalLogicalTypeAnnotation, FIXED_LEN_BYTE_ARRAY, length)
        if pushDownDecimal =>
      (n: Array[String], v: Any) =>
        FilterApi.lt(binaryColumn(n), decimalToByteArray(v.asInstanceOf[JBigDecimal], length))
  }

  private val makeLtEq:
    PartialFunction[ParquetSchemaType, (Array[String], Any) => FilterPredicate] = {
    case ParquetByteType | ParquetShortType | ParquetIntegerType =>
      (n: Array[String], v: Any) => FilterApi.ltEq(intColumn(n), toIntValue(v))
    case ParquetLongType =>
      (n: Array[String], v: Any) => FilterApi.ltEq(longColumn(n), toLongValue(v))
    case ParquetFloatType =>
      (n: Array[String], v: Any) => FilterApi.ltEq(floatColumn(n), v.asInstanceOf[JFloat])
    case ParquetDoubleType =>
      (n: Array[String], v: Any) => FilterApi.ltEq(doubleColumn(n), v.asInstanceOf[JDouble])

    case ParquetStringType =>
      (n: Array[String], v: Any) =>
        FilterApi.ltEq(binaryColumn(n), Binary.fromString(v.asInstanceOf[String]))
    case ParquetBinaryType =>
      (n: Array[String], v: Any) =>
        FilterApi.ltEq(binaryColumn(n), Binary.fromReusedByteArray(v.asInstanceOf[Array[Byte]]))
    case ParquetDateType if pushDownDate =>
      (n: Array[String], v: Any) =>
        FilterApi.ltEq(intColumn(n), dateToDays(v).asInstanceOf[Integer])
    case ParquetTimestampMicrosType if pushDownTimestamp =>
      (n: Array[String], v: Any) => FilterApi.ltEq(longColumn(n), timestampToMicros(v))
    case ParquetTimestampMillisType if pushDownTimestamp =>
      (n: Array[String], v: Any) => FilterApi.ltEq(longColumn(n), timestampToMillis(v))
    case FrameworkFilterOps(ops) =>
      (n: Array[String], v: Any) => ops.makeLtEq(n, v)
    case ParquetTimeMicrosTypeAdjToUTC =>
      (n: Array[String], v: Any) => FilterApi.ltEq(longColumn(n), localTimeToMicros(v))

    case ParquetSchemaType(_: DecimalLogicalTypeAnnotation, INT32, _) if pushDownDecimal =>
      (n: Array[String], v: Any) =>
        FilterApi.ltEq(intColumn(n), decimalToInt32(v.asInstanceOf[JBigDecimal]))
    case ParquetSchemaType(_: DecimalLogicalTypeAnnotation, INT64, _) if pushDownDecimal =>
      (n: Array[String], v: Any) =>
        FilterApi.ltEq(longColumn(n), decimalToInt64(v.asInstanceOf[JBigDecimal]))
    case ParquetSchemaType(_: DecimalLogicalTypeAnnotation, FIXED_LEN_BYTE_ARRAY, length)
        if pushDownDecimal =>
      (n: Array[String], v: Any) =>
        FilterApi.ltEq(binaryColumn(n), decimalToByteArray(v.asInstanceOf[JBigDecimal], length))
  }

  private val makeGt:
    PartialFunction[ParquetSchemaType, (Array[String], Any) => FilterPredicate] = {
    case ParquetByteType | ParquetShortType | ParquetIntegerType =>
      (n: Array[String], v: Any) => FilterApi.gt(intColumn(n), toIntValue(v))
    case ParquetLongType =>
      (n: Array[String], v: Any) => FilterApi.gt(longColumn(n), toLongValue(v))
    case ParquetFloatType =>
      (n: Array[String], v: Any) => FilterApi.gt(floatColumn(n), v.asInstanceOf[JFloat])
    case ParquetDoubleType =>
      (n: Array[String], v: Any) => FilterApi.gt(doubleColumn(n), v.asInstanceOf[JDouble])

    case ParquetStringType =>
      (n: Array[String], v: Any) =>
        FilterApi.gt(binaryColumn(n), Binary.fromString(v.asInstanceOf[String]))
    case ParquetBinaryType =>
      (n: Array[String], v: Any) =>
        FilterApi.gt(binaryColumn(n), Binary.fromReusedByteArray(v.asInstanceOf[Array[Byte]]))
    case ParquetDateType if pushDownDate =>
      (n: Array[String], v: Any) =>
        FilterApi.gt(intColumn(n), dateToDays(v).asInstanceOf[Integer])
    case ParquetTimestampMicrosType if pushDownTimestamp =>
      (n: Array[String], v: Any) => FilterApi.gt(longColumn(n), timestampToMicros(v))
    case ParquetTimestampMillisType if pushDownTimestamp =>
      (n: Array[String], v: Any) => FilterApi.gt(longColumn(n), timestampToMillis(v))
    case FrameworkFilterOps(ops) =>
      (n: Array[String], v: Any) => ops.makeGt(n, v)
    case ParquetTimeMicrosTypeAdjToUTC =>
      (n: Array[String], v: Any) => FilterApi.gt(longColumn(n), localTimeToMicros(v))

    case ParquetSchemaType(_: DecimalLogicalTypeAnnotation, INT32, _) if pushDownDecimal =>
      (n: Array[String], v: Any) =>
        FilterApi.gt(intColumn(n), decimalToInt32(v.asInstanceOf[JBigDecimal]))
    case ParquetSchemaType(_: DecimalLogicalTypeAnnotation, INT64, _) if pushDownDecimal =>
      (n: Array[String], v: Any) =>
        FilterApi.gt(longColumn(n), decimalToInt64(v.asInstanceOf[JBigDecimal]))
    case ParquetSchemaType(_: DecimalLogicalTypeAnnotation, FIXED_LEN_BYTE_ARRAY, length)
        if pushDownDecimal =>
      (n: Array[String], v: Any) =>
        FilterApi.gt(binaryColumn(n), decimalToByteArray(v.asInstanceOf[JBigDecimal], length))
  }

  private val makeGtEq:
    PartialFunction[ParquetSchemaType, (Array[String], Any) => FilterPredicate] = {
    case ParquetByteType | ParquetShortType | ParquetIntegerType =>
      (n: Array[String], v: Any) => FilterApi.gtEq(intColumn(n), toIntValue(v))
    case ParquetLongType =>
      (n: Array[String], v: Any) => FilterApi.gtEq(longColumn(n), toLongValue(v))
    case ParquetFloatType =>
      (n: Array[String], v: Any) => FilterApi.gtEq(floatColumn(n), v.asInstanceOf[JFloat])
    case ParquetDoubleType =>
      (n: Array[String], v: Any) => FilterApi.gtEq(doubleColumn(n), v.asInstanceOf[JDouble])

    case ParquetStringType =>
      (n: Array[String], v: Any) =>
        FilterApi.gtEq(binaryColumn(n), Binary.fromString(v.asInstanceOf[String]))
    case ParquetBinaryType =>
      (n: Array[String], v: Any) =>
        FilterApi.gtEq(binaryColumn(n), Binary.fromReusedByteArray(v.asInstanceOf[Array[Byte]]))
    case ParquetDateType if pushDownDate =>
      (n: Array[String], v: Any) =>
        FilterApi.gtEq(intColumn(n), dateToDays(v).asInstanceOf[Integer])
    case ParquetTimestampMicrosType if pushDownTimestamp =>
      (n: Array[String], v: Any) => FilterApi.gtEq(longColumn(n), timestampToMicros(v))
    case ParquetTimestampMillisType if pushDownTimestamp =>
      (n: Array[String], v: Any) => FilterApi.gtEq(longColumn(n), timestampToMillis(v))
    case FrameworkFilterOps(ops) =>
      (n: Array[String], v: Any) => ops.makeGtEq(n, v)
    case ParquetTimeMicrosTypeAdjToUTC =>
      (n: Array[String], v: Any) => FilterApi.gtEq(longColumn(n), localTimeToMicros(v))

    case ParquetSchemaType(_: DecimalLogicalTypeAnnotation, INT32, _) if pushDownDecimal =>
      (n: Array[String], v: Any) =>
        FilterApi.gtEq(intColumn(n), decimalToInt32(v.asInstanceOf[JBigDecimal]))
    case ParquetSchemaType(_: DecimalLogicalTypeAnnotation, INT64, _) if pushDownDecimal =>
      (n: Array[String], v: Any) =>
        FilterApi.gtEq(longColumn(n), decimalToInt64(v.asInstanceOf[JBigDecimal]))
    case ParquetSchemaType(_: DecimalLogicalTypeAnnotation, FIXED_LEN_BYTE_ARRAY, length)
        if pushDownDecimal =>
      (n: Array[String], v: Any) =>
        FilterApi.gtEq(binaryColumn(n), decimalToByteArray(v.asInstanceOf[JBigDecimal], length))
  }

  private val makeInPredicate:
    PartialFunction[ParquetSchemaType, (Array[String], Array[Any]) => FilterPredicate] = {

    case ParquetByteType | ParquetShortType | ParquetIntegerType =>
      (n: Array[String], values: Array[Any]) =>
        val set = new HashSet[Integer]()
        for (value <- values) {
          set.add(toIntValue(value))
        }
        FilterApi.in(intColumn(n), set)

    case ParquetLongType =>
      (n: Array[String], values: Array[Any]) =>
        val set = new HashSet[JLong]()
        for (value <- values) {
          set.add(toLongValue(value))
        }
        FilterApi.in(longColumn(n), set)

    case ParquetFloatType =>
      (n: Array[String], values: Array[Any]) =>
        val set = new HashSet[JFloat]()
        for (value <- values) {
          set.add(value.asInstanceOf[JFloat])
        }
        FilterApi.in(floatColumn(n), set)

    case ParquetDoubleType =>
      (n: Array[String], values: Array[Any]) =>
        val set = new HashSet[JDouble]()
        for (value <- values) {
          set.add(value.asInstanceOf[JDouble])
        }
        FilterApi.in(doubleColumn(n), set)

    case ParquetStringType =>
      (n: Array[String], values: Array[Any]) =>
        val set = new HashSet[Binary]()
        for (value <- values) {
          set.add(Option(value).map(s => Binary.fromString(s.asInstanceOf[String])).orNull)
        }
        FilterApi.in(binaryColumn(n), set)

    case ParquetBinaryType =>
      (n: Array[String], values: Array[Any]) =>
        val set = new HashSet[Binary]()
        for (value <- values) {
          set.add(Option(value)
            .map(b => Binary.fromReusedByteArray(b.asInstanceOf[Array[Byte]])).orNull)
        }
        FilterApi.in(binaryColumn(n), set)

    case ParquetDateType if pushDownDate =>
      (n: Array[String], values: Array[Any]) =>
        val set = new HashSet[Integer]()
        for (value <- values) {
          set.add(Option(value).map(date => dateToDays(date).asInstanceOf[Integer]).orNull)
        }
        FilterApi.in(intColumn(n), set)

    case ParquetTimestampMicrosType if pushDownTimestamp =>
      (n: Array[String], values: Array[Any]) =>
        val set = new HashSet[JLong]()
        for (value <- values) {
          set.add(Option(value).map(timestampToMicros).orNull)
        }
        FilterApi.in(longColumn(n), set)

    case ParquetTimestampMillisType if pushDownTimestamp =>
      (n: Array[String], values: Array[Any]) =>
        val set = new HashSet[JLong]()
        for (value <- values) {
          set.add(Option(value).map(timestampToMillis).orNull)
        }
        FilterApi.in(longColumn(n), set)

    case FrameworkFilterOps(ops) =>
      (n: Array[String], values: Array[Any]) => ops.makeIn(n, values)
    case ParquetTimeMicrosTypeAdjToUTC =>
      (n: Array[String], values: Array[Any]) =>
        val set = new HashSet[JLong]()
        for (value <- values) {
          set.add(Option(value).map(localTimeToMicros).orNull)
        }
        FilterApi.in(longColumn(n), set)

    case ParquetSchemaType(_: DecimalLogicalTypeAnnotation, INT32, _) if pushDownDecimal =>
      (n: Array[String], values: Array[Any]) =>
        val set = new HashSet[Integer]()
        for (value <- values) {
          set.add(Option(value).map(d => decimalToInt32(d.asInstanceOf[JBigDecimal])).orNull)
        }
        FilterApi.in(intColumn(n), set)

    case ParquetSchemaType(_: DecimalLogicalTypeAnnotation, INT64, _) if pushDownDecimal =>
      (n: Array[String], values: Array[Any]) =>
        val set = new HashSet[JLong]()
        for (value <- values) {
          set.add(Option(value).map(d => decimalToInt64(d.asInstanceOf[JBigDecimal])).orNull)
        }
        FilterApi.in(longColumn(n), set)

    case ParquetSchemaType(_: DecimalLogicalTypeAnnotation, FIXED_LEN_BYTE_ARRAY, length)
      if pushDownDecimal =>
      (n: Array[String], values: Array[Any]) =>
        val set = new HashSet[Binary]()
        for (value <- values) {
          set.add(Option(value)
            .map(d => decimalToByteArray(d.asInstanceOf[JBigDecimal], length)).orNull)
        }
        FilterApi.in(binaryColumn(n), set)
  }

  // Returns filters that can be pushed down when reading Parquet files.
  def convertibleFilters(filters: Seq[sources.Filter]): Seq[sources.Filter] = {
    filters.flatMap(convertibleFiltersHelper(_, canPartialPushDown = true))
  }

  private def convertibleFiltersHelper(
      predicate: sources.Filter,
      canPartialPushDown: Boolean): Option[sources.Filter] = {
    predicate match {
      case sources.And(left, right) =>
        val leftResultOptional = convertibleFiltersHelper(left, canPartialPushDown)
        val rightResultOptional = convertibleFiltersHelper(right, canPartialPushDown)
        (leftResultOptional, rightResultOptional) match {
          case (Some(leftResult), Some(rightResult)) => Some(sources.And(leftResult, rightResult))
          case (Some(leftResult), None) if canPartialPushDown => Some(leftResult)
          case (None, Some(rightResult)) if canPartialPushDown => Some(rightResult)
          case _ => None
        }

      case sources.Or(left, right) =>
        val leftResultOptional = convertibleFiltersHelper(left, canPartialPushDown)
        val rightResultOptional = convertibleFiltersHelper(right, canPartialPushDown)
        if (leftResultOptional.isEmpty || rightResultOptional.isEmpty) {
          None
        } else {
          Some(sources.Or(leftResultOptional.get, rightResultOptional.get))
        }
      // A negated shredded-variant predicate cannot be pushed soundly (see the matching guard in
      // `createFilterHelper`), so it is not convertible either.
      case sources.Not(pred) if referencesShreddedName(pred) => None
      case sources.Not(pred) =>
        val resultOptional = convertibleFiltersHelper(pred, canPartialPushDown = false)
        resultOptional.map(sources.Not)

      case other =>
        if (createFilter(other).isDefined) {
          Some(other)
        } else {
          None
        }
    }
  }

  /**
   * Converts data sources filters to Parquet filter predicates.
   */
  def createFilter(predicate: sources.Filter): Option[FilterPredicate] = {
    createFilterHelper(predicate, canPartialPushDownConjuncts = true)
  }

  // Parquet's type in the given file should be matched to the value's type
  // in the pushed filter in order to push down the filter to Parquet.
  private def valueCanMakeFilterOn(name: String, value: Any): Boolean = {
    valueMatchesParquetType(nameToParquetField(name).fieldType, value)
  }

  // The value's type must match the field's on-disk Parquet type for a filter to be pushed.
  private def valueMatchesParquetType(fieldType: ParquetSchemaType, value: Any): Boolean = {
    value == null || (fieldType match {
      case ParquetBooleanType => value.isInstanceOf[JBoolean]
      case ParquetIntegerType if value.isInstanceOf[Period] => true
      case ParquetByteType | ParquetShortType | ParquetIntegerType => value match {
        // Byte/Short/Int are all stored as INT32 in Parquet so filters are built using type Int.
        // We don't create a filter if the value would overflow.
        case _: JByte | _: JShort | _: Integer => true
        case v: JLong => v.longValue() >= Int.MinValue && v.longValue() <= Int.MaxValue
        case _ => false
      }
      case ParquetLongType =>
        value.isInstanceOf[JLong] || value.isInstanceOf[Duration]
      case ParquetFloatType => value.isInstanceOf[JFloat]
      case ParquetDoubleType => value.isInstanceOf[JDouble]
      case ParquetStringType => value.isInstanceOf[String]
      case ParquetBinaryType => value.isInstanceOf[Array[Byte]]
      case ParquetDateType =>
        value.isInstanceOf[Date] || value.isInstanceOf[LocalDate]
      case ParquetTimestampMicrosType | ParquetTimestampMillisType =>
        value.isInstanceOf[Timestamp] || value.isInstanceOf[Instant]
      case FrameworkFilterOps(ops) => ops.acceptsValue(value)
      case ParquetTimeMicrosTypeAdjToUTC =>
        value.isInstanceOf[LocalTime] && isMicrosResolution(value)
      case ParquetSchemaType(decimalType: DecimalLogicalTypeAnnotation, INT32, _) =>
        isDecimalMatched(value, decimalType)
      case ParquetSchemaType(decimalType: DecimalLogicalTypeAnnotation, INT64, _) =>
        isDecimalMatched(value, decimalType)
      case
        ParquetSchemaType(decimalType: DecimalLogicalTypeAnnotation, FIXED_LEN_BYTE_ARRAY, _) =>
        isDecimalMatched(value, decimalType)
      case _ => false
    })
  }

  // Decimal type must make sure that filter value's scale matched the file.
  // If doesn't matched, which would cause data corruption.
  private def isDecimalMatched(value: Any,
      decimalLogicalType: DecimalLogicalTypeAnnotation): Boolean = value match {
    case decimal: JBigDecimal =>
      decimal.scale == decimalLogicalType.getScale
    case _ => false
  }

  private def canMakeFilterOn(name: String, value: Any): Boolean = {
    nameToParquetField.contains(name) && valueCanMakeFilterOn(name, value)
  }

  // Whether `name` is a shredded-variant logical path whose typed leaf accepts `value`. `value`
  // must be non-null: shredded pushdown only handles comparison predicates.
  private def canMakeShreddedFilterOn(name: String, value: Any): Boolean = {
    value != null && nameToShreddedVariantField.get(name).exists { f =>
      valueMatchesParquetType(f.leaf.fieldType, value)
    }
  }

  // Whether `predicate` references a shredded-variant logical path anywhere. Used to refuse
  // conversion under negation: the shredded predicate is `or(leaf, and(anyResidualNotNull,
  // isNull(leaf)))`, and parquet-mr's LogicalInverseRewriter pushes `not(...)` inside to
  // `and(not(leaf), or(and(eq(residual, null)...), notEq(leaf, null)))`, which is row-group-
  // droppable as soon as some residual has no nulls AND the leaf is entirely NULL -- exactly an
  // all-fallback row group, whose matching values are all in a residual. Since a negated shredded
  // predicate cannot be expressed soundly with row-group statistics, we do not push it at all.
  //
  // `sources.Filter.references` already recurses through And/Or/Not and every leaf filter, so this
  // stays correct if new Filter subtypes are added.
  private def referencesShreddedName(predicate: sources.Filter): Boolean =
    predicate.references.exists(nameToShreddedVariantField.contains)

  // Build the sound shredded-variant predicate:
  //   or(leafPredicate, and(anyResidualNotNull, isNull(leaf)))
  // where `anyResidualNotNull` is `or(notEq(residual_0, null), ..., notEq(residual_n, null))` and
  // `isNull(leaf)` is `eq(leaf, null)`.
  //
  // Parquet's statistics drop logic: `or(a, b)` is row-group-droppable iff BOTH `a` and `b` are
  // droppable; `and(a, b)` iff EITHER is; `notEq(col, null)` (IS NOT NULL) iff the column is
  // entirely NULL (no non-nulls); `eq(col, null)` (IS NULL) iff the column has no nulls. So the
  // whole `or` drops the row group iff the leaf min/max cannot match AND (every residual is
  // entirely NULL OR the leaf column has no nulls). The second arm is what makes this sound and
  // still effective: a value for the path can be outside the typed leaf only on a row where the
  // leaf is NULL, so a leaf with zero nulls means every value is provably in the typed leaf and the
  // leaf min/max is a complete summary -- regardless of what the residual columns hold (they may be
  // non-null because a sibling key outside the shredding schema landed in the level's `value`,
  // which is the normal layout for real Variant data). Per record it still keeps every row that
  // could match: a row whose value fell back to a residual has a NULL leaf and a non-null residual,
  // so `and(anyResidualNotNull, isNull(leaf))` holds for it.
  //
  // The naive `and(leafPredicate, isNull(residual))` is UNSOUND: `and` drops iff EITHER conjunct is
  // droppable, so the leaf predicate alone would drop the row group regardless of the residual. The
  // earlier flat `or(leaf, isNotNull(residual)...)` was sound but could never drop a row group once
  // any residual was non-null (e.g. a partial object), i.e. it paid the pushdown cost without ever
  // skipping on that common layout.
  //
  // `makeLeaf` produces the leaf predicate from the leaf's field-name array; it returns None if the
  // leaf type has no comparison encoding.
  private def makeShreddedFilter(
      name: String,
      makeLeaf: (ParquetSchemaType, Array[String]) => Option[FilterPredicate]
      ): Option[FilterPredicate] = {
    val field = nameToShreddedVariantField(name)
    makeLeaf(field.leaf.fieldType, field.leaf.fieldNames).map { leafPredicate =>
      field.residualFieldNames
        .map(n => FilterApi.notEq(binaryColumn(n), null.asInstanceOf[Binary]))
        .reduceLeftOption[FilterPredicate](FilterApi.or) match {
        case None =>
          // No residuals exist in this file for the path, so the leaf is a complete summary.
          leafPredicate
        case Some(anyResidualNotNull) =>
          // `makeLeaf` returned Some, so the leaf type is one `makeEq` also covers (its case list
          // is a superset of the comparison ops), hence `makeEq.lift` is defined here.
          val leafIsNull = makeEq.lift(field.leaf.fieldType).get(field.leaf.fieldNames, null)
          FilterApi.or(leafPredicate, FilterApi.and(anyResidualNotNull, leafIsNull))
      }
    }
  }

  /**
   * @param predicate the input filter predicates. Not all the predicates can be pushed down.
   * @param canPartialPushDownConjuncts whether a subset of conjuncts of predicates can be pushed
   *                                    down safely. Pushing ONLY one side of AND down is safe to
   *                                    do at the top level or none of its ancestors is NOT and OR.
   * @return the Parquet-native filter predicates that are eligible for pushdown.
   */
  private def createFilterHelper(
      predicate: sources.Filter,
      canPartialPushDownConjuncts: Boolean): Option[FilterPredicate] = {
    // NOTE:
    //
    // For any comparison operator `cmp`, both `a cmp NULL` and `NULL cmp a` evaluate to `NULL`,
    // which can be casted to `false` implicitly. Please refer to the `eval` method of these
    // operators and the `PruneFilters` rule for details.

    // Hyukjin:
    // I added [[EqualNullSafe]] with [[org.apache.parquet.filter2.predicate.Operators.Eq]].
    // So, it performs equality comparison identically when given [[sources.Filter]] is [[EqualTo]].
    // The reason why I did this is, that the actual Parquet filter checks null-safe equality
    // comparison.
    // So I added this and maybe [[EqualTo]] should be changed. It still seems fine though, because
    // physical planning does not set `NULL` to [[EqualTo]] but changes it to [[IsNull]] and etc.
    // Probably I missed something and obviously this should be changed.

    predicate match {
      // Shredded-variant paths (e.g. "v.`0`"). Only comparison predicates that use min/max
      // statistics are eligible. Each pushes the guarded predicate built by `makeShreddedFilter`.
      // IS NULL / IS NOT NULL on the logical variant field are intentionally out of scope: "the
      // extracted field is null" is not the same as "typed_value is null", so we must not conflate
      // them.
      case sources.EqualTo(name, value) if canMakeShreddedFilterOn(name, value) =>
        makeShreddedFilter(name, (t, n) => makeEq.lift(t).map(_(n, value)))
      case sources.EqualNullSafe(name, value) if canMakeShreddedFilterOn(name, value) =>
        makeShreddedFilter(name, (t, n) => makeEq.lift(t).map(_(n, value)))
      case sources.LessThan(name, value) if canMakeShreddedFilterOn(name, value) =>
        makeShreddedFilter(name, (t, n) => makeLt.lift(t).map(_(n, value)))
      case sources.LessThanOrEqual(name, value) if canMakeShreddedFilterOn(name, value) =>
        makeShreddedFilter(name, (t, n) => makeLtEq.lift(t).map(_(n, value)))
      case sources.GreaterThan(name, value) if canMakeShreddedFilterOn(name, value) =>
        makeShreddedFilter(name, (t, n) => makeGt.lift(t).map(_(n, value)))
      case sources.GreaterThanOrEqual(name, value) if canMakeShreddedFilterOn(name, value) =>
        makeShreddedFilter(name, (t, n) => makeGtEq.lift(t).map(_(n, value)))
      case sources.In(name, values) if pushDownInFilterThreshold > 0 && values.nonEmpty &&
          values.forall(v => canMakeShreddedFilterOn(name, v)) =>
        // Build the leaf predicate once (an OR of per-value equalities under the threshold, or a
        // single FilterApi.in above it), then OR the residual isNotNull guards on once via
        // `makeShreddedFilter`. Mirrors the regular `In` path below: threshold on `values.length`,
        // and `makeInPredicate`/`FilterApi.in` for large lists so those still get skipping.
        makeShreddedFilter(name, (t, n) =>
          if (values.length <= pushDownInFilterThreshold) {
            values.distinct.flatMap(v => makeEq.lift(t).map(_(n, v))).reduceLeftOption(FilterApi.or)
          } else {
            makeInPredicate.lift(t).map(_(n, values))
          })

      case sources.IsNull(name) if canMakeFilterOn(name, null) =>
        makeEq.lift(nameToParquetField(name).fieldType)
          .map(_(nameToParquetField(name).fieldNames, null))
      case sources.IsNotNull(name) if canMakeFilterOn(name, null) =>
        makeNotEq.lift(nameToParquetField(name).fieldType)
          .map(_(nameToParquetField(name).fieldNames, null))

      case sources.EqualTo(name, value) if canMakeFilterOn(name, value) =>
        makeEq.lift(nameToParquetField(name).fieldType)
          .map(_(nameToParquetField(name).fieldNames, value))
      case sources.Not(sources.EqualTo(name, value)) if canMakeFilterOn(name, value) =>
        makeNotEq.lift(nameToParquetField(name).fieldType)
          .map(_(nameToParquetField(name).fieldNames, value))

      case sources.EqualNullSafe(name, value) if canMakeFilterOn(name, value) =>
        makeEq.lift(nameToParquetField(name).fieldType)
          .map(_(nameToParquetField(name).fieldNames, value))
      case sources.Not(sources.EqualNullSafe(name, value)) if canMakeFilterOn(name, value) =>
        makeNotEq.lift(nameToParquetField(name).fieldType)
          .map(_(nameToParquetField(name).fieldNames, value))

      case sources.LessThan(name, value) if (value != null) && canMakeFilterOn(name, value) =>
        makeLt.lift(nameToParquetField(name).fieldType)
          .map(_(nameToParquetField(name).fieldNames, value))
      case sources.LessThanOrEqual(name, value) if (value != null) &&
        canMakeFilterOn(name, value) =>
        makeLtEq.lift(nameToParquetField(name).fieldType)
          .map(_(nameToParquetField(name).fieldNames, value))

      case sources.GreaterThan(name, value) if (value != null) && canMakeFilterOn(name, value) =>
        makeGt.lift(nameToParquetField(name).fieldType)
          .map(_(nameToParquetField(name).fieldNames, value))
      case sources.GreaterThanOrEqual(name, value) if (value != null) &&
        canMakeFilterOn(name, value) =>
        makeGtEq.lift(nameToParquetField(name).fieldType)
          .map(_(nameToParquetField(name).fieldNames, value))

      case sources.And(lhs, rhs) =>
        // At here, it is not safe to just convert one side and remove the other side
        // if we do not understand what the parent filters are.
        //
        // Here is an example used to explain the reason.
        // Let's say we have NOT(a = 2 AND b in ('1')) and we do not understand how to
        // convert b in ('1'). If we only convert a = 2, we will end up with a filter
        // NOT(a = 2), which will generate wrong results.
        //
        // Pushing one side of AND down is only safe to do at the top level or in the child
        // AND before hitting NOT or OR conditions, and in this case, the unsupported predicate
        // can be safely removed.
        val lhsFilterOption =
          createFilterHelper(lhs, canPartialPushDownConjuncts)
        val rhsFilterOption =
          createFilterHelper(rhs, canPartialPushDownConjuncts)

        (lhsFilterOption, rhsFilterOption) match {
          case (Some(lhsFilter), Some(rhsFilter)) => Some(FilterApi.and(lhsFilter, rhsFilter))
          case (Some(lhsFilter), None) if canPartialPushDownConjuncts => Some(lhsFilter)
          case (None, Some(rhsFilter)) if canPartialPushDownConjuncts => Some(rhsFilter)
          case _ => None
        }

      case sources.Or(lhs, rhs) =>
        // The Or predicate is convertible when both of its children can be pushed down.
        // That is to say, if one/both of the children can be partially pushed down, the Or
        // predicate can be partially pushed down as well.
        //
        // Here is an example used to explain the reason.
        // Let's say we have
        // (a1 AND a2) OR (b1 AND b2),
        // a1 and b1 is convertible, while a2 and b2 is not.
        // The predicate can be converted as
        // (a1 OR b1) AND (a1 OR b2) AND (a2 OR b1) AND (a2 OR b2)
        // As per the logical in And predicate, we can push down (a1 OR b1).
        for {
          lhsFilter <- createFilterHelper(lhs, canPartialPushDownConjuncts)
          rhsFilter <- createFilterHelper(rhs, canPartialPushDownConjuncts)
        } yield FilterApi.or(lhsFilter, rhsFilter)

      // Refuse to push a negated predicate that references a shredded-variant path: not() of the
      // guarded shredded predicate is rewritten by parquet-mr into a form that can drop an
      // all-fallback row group (see `referencesShreddedName`).
      case sources.Not(pred) if referencesShreddedName(pred) => None
      case sources.Not(pred) =>
        createFilterHelper(pred, canPartialPushDownConjuncts = false)
          .map(FilterApi.not)

      // Every value must be pushable, not just the head: both branches below convert *all* the
      // values (per-element `makeEq` when under the threshold, `makeInPredicate` otherwise), so a
      // head-only check lets a non-pushable tail element reach the converter. For a value-range-
      // sensitive type (e.g. nanosecond timestamps, whose encoder throws outside the int64
      // epoch-nanos range) that would crash filter creation instead of falling back to a full
      // scan. `forall` short-circuits and, for type-only `valueCanMakeFilterOn` checks, is
      // equivalent to the previous head check (all `In` values share the coerced column type).
      case sources.In(name, values) if pushDownInFilterThreshold > 0 && values.nonEmpty &&
          values.forall(canMakeFilterOn(name, _)) =>
        val fieldType = nameToParquetField(name).fieldType
        val fieldNames = nameToParquetField(name).fieldNames
        if (values.length <= pushDownInFilterThreshold) {
          values.distinct.flatMap { v =>
            makeEq.lift(fieldType).map(_(fieldNames, v))
          }.reduceLeftOption(FilterApi.or)
        } else if (canPartialPushDownConjuncts) {
          if (values.contains(null)) {
            Seq(makeEq.lift(fieldType).map(_(fieldNames, null)),
              makeInPredicate.lift(fieldType).map(_(fieldNames, values.filter(_ != null)))
            ).flatten.reduceLeftOption(FilterApi.or)
          } else {
            makeInPredicate.lift(fieldType).map(_(fieldNames, values))
          }
        } else {
          None
        }

      case sources.StringStartsWith(name, prefix)
          if pushDownStringPredicate && canMakeFilterOn(name, prefix) =>
        Option(prefix).map { v =>
          FilterApi.userDefined(binaryColumn(nameToParquetField(name).fieldNames),
            new UserDefinedPredicate[Binary] with Serializable {
              private val strToBinary = Binary.fromReusedByteArray(v.getBytes(UTF_8))
              private val size = strToBinary.length

              override def canDrop(statistics: Statistics[Binary]): Boolean = {
                val comparator = PrimitiveComparator.UNSIGNED_LEXICOGRAPHICAL_BINARY_COMPARATOR
                val max = statistics.getMax
                val min = statistics.getMin
                comparator.compare(max.slice(0, math.min(size, max.length)), strToBinary) < 0 ||
                  comparator.compare(min.slice(0, math.min(size, min.length)), strToBinary) > 0
              }

              override def inverseCanDrop(statistics: Statistics[Binary]): Boolean = {
                val comparator = PrimitiveComparator.UNSIGNED_LEXICOGRAPHICAL_BINARY_COMPARATOR
                val max = statistics.getMax
                val min = statistics.getMin
                comparator.compare(max.slice(0, math.min(size, max.length)), strToBinary) == 0 &&
                  comparator.compare(min.slice(0, math.min(size, min.length)), strToBinary) == 0
              }

              override def keep(value: Binary): Boolean = {
                value != null && UTF8String.fromBytes(value.getBytesUnsafe).startsWith(
                  UTF8String.fromBytes(strToBinary.getBytesUnsafe))
              }
            }
          )
        }

      case sources.StringEndsWith(name, suffix)
          if pushDownStringPredicate && canMakeFilterOn(name, suffix) =>
        Option(suffix).map { v =>
          FilterApi.userDefined(binaryColumn(nameToParquetField(name).fieldNames),
            new UserDefinedPredicate[Binary] with Serializable {
              private val suffixStr = UTF8String.fromString(v)
              override def canDrop(statistics: Statistics[Binary]): Boolean = false
              override def inverseCanDrop(statistics: Statistics[Binary]): Boolean = false
              override def keep(value: Binary): Boolean = {
                value != null && UTF8String.fromBytes(value.getBytesUnsafe).endsWith(suffixStr)
              }
            }
          )
        }

      case sources.StringContains(name, value)
          if pushDownStringPredicate && canMakeFilterOn(name, value) =>
        Option(value).map { v =>
          FilterApi.userDefined(binaryColumn(nameToParquetField(name).fieldNames),
            new UserDefinedPredicate[Binary] with Serializable {
              private val subStr = UTF8String.fromString(v)
              override def canDrop(statistics: Statistics[Binary]): Boolean = false
              override def inverseCanDrop(statistics: Statistics[Binary]): Boolean = false
              override def keep(value: Binary): Boolean = {
                value != null && UTF8String.fromBytes(value.getBytesUnsafe).contains(subStr)
              }
            }
          )
        }

      case _ => None
    }
  }
}
