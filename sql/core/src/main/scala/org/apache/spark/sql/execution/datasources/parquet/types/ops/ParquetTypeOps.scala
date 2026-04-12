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

package org.apache.spark.sql.execution.datasources.parquet.types.ops

import java.lang.{Boolean => JBoolean, Long => JLong}
import java.util.HashSet
import javax.annotation.Nullable

import org.apache.parquet.column.ColumnDescriptor
import org.apache.parquet.filter2.predicate.{FilterApi, FilterPredicate}
import org.apache.parquet.filter2.predicate.SparkFilterApi._
import org.apache.parquet.io.api.RecordConsumer
import org.apache.parquet.schema.{LogicalTypeAnnotation, Type}
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.apache.parquet.schema.Type.Repetition

import org.apache.spark.sql.catalyst.expressions.SpecializedGetters
import org.apache.spark.sql.catalyst.util.RebaseDateTime.RebaseSpec
import org.apache.spark.sql.execution.datasources.parquet.{HasParentContainerUpdater, ParentContainerUpdater, ParquetVectorUpdater}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DataType, StructType, TimeType}

/**
 * Optional trait for Parquet storage format integration in the Types Framework.
 *
 * Implement this trait to enable Parquet read/write support for a framework type. Each framework
 * type that supports Parquet provides a concrete implementation and registers it in the companion
 * object's apply() method.
 *
 * The trait covers all Parquet concerns:
 *   - Schema conversion: Spark DataType <-> Parquet schema type
 *   - Value write: writing values to Parquet RecordConsumer
 *   - Row-based read: creating Parquet converters for reading into InternalRow
 *   - Vectorized read: creating batch updaters for columnar reading
 *   - Filter pushdown: creating Parquet filter predicates for predicate pushdown
 *   - Type gates: declaring Parquet support/capability
 *   - Schema clipping: declaring internal struct schema for column pruning
 *
 * DISPATCH PATTERN: Framework FIRST at all integration sites. Each Parquet infrastructure
 * method wraps itself with:
 * {{{
 *   ParquetTypeOps(dt).map(_.method(...)).getOrElse(methodDefault(dt, ...))
 * }}}
 * The original code is extracted to a *Default method unchanged. When the framework flag is ON,
 * the ops handles the type. When OFF, the *Default fallback executes the original code path.
 *
 * STRUCT-BACKED TYPES: Types stored as Parquet groups should override the
 * extended newConverter overload (which provides schemaConverter/convertTz/rebase specs for
 * recursive field conversion) and declare parquetStructSchema for column pruning.
 *
 * @see TimeTypeParquetOps for a reference implementation (primitive Long-backed type)
 * @since 4.2.0
 */
private[parquet] trait ParquetTypeOps extends Serializable {

  /** The DataType this Ops instance handles. */
  def dataType: DataType

  // ==================== Schema Conversion ====================

  /**
   * Converts this Spark DataType to a Parquet schema Type (for the write path).
   *
   * For primitive types: returns a PrimitiveType with the appropriate annotation.
   * For struct-backed types: returns a GroupType with sub-fields and a logical type annotation.
   *
   * @param fieldName the column/field name in the Parquet schema
   * @param repetition REQUIRED, OPTIONAL, or REPEATED
   * @return the Parquet Type for this DataType
   */
  def convertToParquetType(fieldName: String, repetition: Repetition): Type

  // ==================== Value Write ====================

  /**
   * Creates a value writer that writes values of this type to a Parquet RecordConsumer.
   *
   * The writer is a closure that captures the RecordConsumer and is called once per row during
   * Parquet file writing. The RecordConsumer is instance-specific (one per output file).
   *
   * IMPORTANT: The RecordConsumer is passed as a lazy supplier (() => RecordConsumer) because
   * makeWriter is called during ParquetWriteSupport.init(), when recordConsumer is still null.
   * It gets set later in prepareForWrite(). The closure must evaluate the supplier at write
   * time, not at creation time. The existing infrastructure closures work because they capture
   * `this.recordConsumer` (a var field - Scala closures over vars capture the reference, not
   * the value). The supplier lambda achieves the same lazy evaluation for ops code.
   *
   * For primitive types: directly calls recordConsumer.addLong/addBinary/etc. with any
   * necessary conversion (e.g., nanos to micros for TimeType).
   *
   * For struct-backed types: uses ParquetWriteSupport companion utilities (consumeGroup,
   * writeFields) to write the struct fields. These utilities are private[parquet] static
   * methods shared between existing infrastructure and framework ops.
   *
   * @param recordConsumer lazy supplier for the Parquet output stream (null during init)
   * @param makeFieldWriter callback into ParquetWriteSupport for recursive field writer creation
   *                        (struct-backed types use this to create writers for sub-fields)
   * @return a closure that writes a value from a row at the given ordinal
   */
  def makeWriter(
      recordConsumer: () => RecordConsumer,
      makeFieldWriter: DataType => (SpecializedGetters, Int) => Unit
  ): (SpecializedGetters, Int) => Unit

  // ==================== Row-Based Read ====================

  /**
   * Creates a Parquet Converter for reading values of this type (simple version).
   *
   * Primitive types override this method. The converter typically extends
   * ParquetPrimitiveConverter and overrides addLong/addBinary/etc. to perform any necessary
   * conversion (e.g., micros to nanos for TimeType).
   *
   * WARNING: Struct-backed types must override the EXTENDED overload below instead. This simple
   * version does not provide the schemaConverter, timezone, or rebase specs needed for recursive
   * field conversion. Overriding only this method for a struct-backed type will compile but
   * produce incorrect behavior at runtime (missing timezone conversion, calendar rebasing, etc.).
   *
   * @param parquetType the Parquet schema type for this field
   * @param updater the parent container to set converted values into
   * @return a Converter that reads Parquet values into the parent container
   */
  def newConverter(
      parquetType: org.apache.parquet.schema.Type,
      updater: ParentContainerUpdater): org.apache.parquet.io.api.Converter
    with HasParentContainerUpdater

  /**
   * Creates a Parquet Converter for reading values of this type (extended version).
   *
   * Struct-backed types override this method to receive the extra context needed for recursive
   * field conversion (schemaConverter for nested type resolution, timezone for timestamp
   * conversion, rebase specs for calendar rebasing).
   *
   * Default delegates to the simple version - primitive types inherit this default and
   * ignore the extra parameters.
   *
   * @param parquetType the Parquet schema type for this field
   * @param updater the parent container to set converted values into
   * @param schemaConverter for resolving nested Parquet schemas to Spark types
   * @param convertTz timezone for timestamp conversion
   * @param datetimeRebaseSpec calendar rebasing spec for datetime types
   * @param int96RebaseSpec calendar rebasing spec for INT96 timestamps
   * @return a Converter that reads Parquet values into the parent container
   */
  def newConverter(
      parquetType: org.apache.parquet.schema.Type,
      updater: ParentContainerUpdater,
      schemaConverter: org.apache.spark.sql.execution.datasources.parquet
        .ParquetToSparkSchemaConverter,
      convertTz: Option[java.time.ZoneId],
      datetimeRebaseSpec: RebaseSpec,
      int96RebaseSpec: RebaseSpec): org.apache.parquet.io.api.Converter
    with HasParentContainerUpdater =
    newConverter(parquetType, updater)

  // ==================== Vectorized Read ====================

  /**
   * Creates a vectorized updater for batch reading this type from Parquet.
   *
   * The updater is used by VectorizedColumnReader to read batches of values directly into
   * ColumnVectors, bypassing row-by-row conversion for better performance.
   *
   * @param descriptor the Parquet column descriptor
   * @param annotation the Parquet logical type annotation
   * @return a ParquetVectorUpdater for batch reading
   */
  def getVectorUpdater(
      descriptor: ColumnDescriptor,
      annotation: LogicalTypeAnnotation): Option[ParquetVectorUpdater] = None

  /**
   * Whether lazy dictionary decoding is supported for this type.
   *
   * Lazy decoding skips dictionary expansion when values can be read directly from the
   * dictionary page. Types that need value conversion (e.g., TimeType converts micros to
   * nanos) must return false to ensure values are properly converted.
   *
   * @param typeName the Parquet primitive type name
   * @param annotation the Parquet logical type annotation
   * @return true if lazy decoding is supported (default), false if conversion is needed
   */
  def isLazyDecodingSupported(
      typeName: PrimitiveTypeName,
      annotation: LogicalTypeAnnotation): Boolean = true

  // ==================== Filter Pushdown ====================
  //
  // ParquetSchemaType is a private inner class of ParquetFilters - it cannot be referenced
  // from outside that class. The ops declares its Parquet filter identity as a tuple of raw
  // top-level Parquet library types (LogicalTypeAnnotation, PrimitiveTypeName). The
  // infrastructure inside ParquetFilters destructures its inner ParquetSchemaType to extract
  // these components and matches against registered ops.
  //
  // Filter predicates are type-specific - the ops handles the backing type (Long/Int/Binary)
  // internally. The infrastructure passes a ParquetFilterOp enum and gets back a
  // FilterPredicate, without knowing what column type or value conversion is used.

  /**
   * Parquet filter identity for this type: the logical type annotation and primitive type
   * that identify this type's columns in Parquet file schemas. Returns None if this type
   * does not support filter pushdown (e.g., struct-backed types where primitive filters
   * are not applicable).
   */
  def parquetFilterType: Option[(LogicalTypeAnnotation, PrimitiveTypeName)] = None

  /**
   * Creates a Parquet filter predicate for a comparison operation.
   *
   * The implementation handles the backing type internally - for Long-backed types, it uses
   * FilterApi with longColumn; for Int-backed types, intColumn; etc. The infrastructure
   * only needs the operation enum and the column path + value.
   *
   * IMPORTANT: Filter values must be converted to the Parquet storage representation, NOT
   * Spark's internal representation. For TimeType: LocalTime -> micros (MICRO_OF_DAY),
   * matching Parquet's TIME(MICROS). Using nanos would cause filters to be off by 1000x.
   *
   * @param op the comparison operation (Eq, NotEq, Lt, LtEq, Gt, GtEq)
   * @param columnPath the dotted column path in the Parquet schema
   * @param value the filter value (in external representation, e.g., LocalTime)
   * @return a Parquet FilterPredicate
   */
  def makeFilterPredicate(
      op: ParquetFilterOp,
      columnPath: Array[String],
      @Nullable value: Any): Option[FilterPredicate] = None

  /**
   * Creates a Parquet IN filter predicate for multiple values.
   *
   * @param columnPath the dotted column path in the Parquet schema
   * @param values the filter values (in external representation)
   * @return Some(FilterPredicate) if supported, None otherwise
   */
  def makeInFilterPredicate(
      columnPath: Array[String],
      values: Array[Any]): Option[FilterPredicate] = None

  /**
   * Whether a value can be used in a filter predicate for this type.
   *
   * @param value the filter value to check
   * @return true if the value is of the correct type for filtering
   */
  def isFilterableValue(value: Any): Boolean = false

  // ==================== Type Gates ====================

  /**
   * Whether this type is supported by the Parquet data source.
   * Used by ParquetFileFormat.supportDataType.
   */
  def supportDataType: Boolean = true

  /**
   * Whether vectorized (batch) reading is supported for this type.
   * Used by ParquetUtils.isBatchReadSupported. Default is false - types must opt in
   * by overriding to true and implementing getVectorUpdater. When false, Spark uses
   * the row-based read path (newConverter) which is always available.
   *
   * @param sqlConf the active SQL configuration
   */
  def isBatchReadSupported(sqlConf: SQLConf): Boolean = false

  // ==================== Schema Clipping (Struct-Backed Types) ====================

  /**
   * The Parquet-level struct schema for column pruning.
   *
   * Struct-backed types (stored as a Parquet GROUP) return the field schema so that
   * ParquetReadSupport.clipParquetType can prune sub-fields based on the query's
   * requested columns.
   *
   * This is independent of PhysicalDataType - Parquet storage representation may differ from
   * internal row representation. A type could be PhysicalBinaryType in rows but a GROUP in
   * Parquet (e.g., a type stored as binary in rows but as a GROUP with metadata fields on disk).
   *
   * Primitive types return None (no sub-fields to clip).
   */
  def parquetStructSchema: Option[StructType] = None
}

/**
 * Factory object for creating ParquetTypeOps instances.
 *
 * Provides forward lookup (DataType -> ops) for framework-first dispatch at Parquet
 * integration sites, and reverse lookups for read-path schema conversion (Parquet
 * annotation -> Spark DataType) and filter dispatch (Parquet filter type -> ops).
 *
 * Feature flag gating is inside apply() - callers don't check it separately.
 */
private[parquet] object ParquetTypeOps {

  /**
   * Returns a ParquetTypeOps instance for the given DataType, if supported.
   *
   * Returns None if the type has no Parquet ops or the framework is disabled.
   * This is the single registration point for all Parquet type operations.
   */
  def apply(dt: DataType): Option[ParquetTypeOps] = {
    if (!SQLConf.get.typesFrameworkEnabled) return None
    dt match {
      case tt: TimeType => Some(TimeTypeParquetOps(tt))
      // Add new types here - single registration point
      case _ => None
    }
  }

  // ==================== Reverse Lookups for Read-Path Schema Conversion ====================
  //
  // Read-path schema conversion goes from Parquet annotations to Spark DataTypes (the
  // reverse direction from write). The dispatch key is the Parquet annotation, not a Spark
  // DataType, so we can't use apply(). These methods follow the same pattern as
  // ProtoTypeOps.opsForKindCase in Phase 1c.

  /**
   * Maps a Parquet primitive type annotation to a Spark DataType.
   * Used by ParquetToSparkSchemaConverter.convertPrimitiveField.
   *
   * @param typeName the Parquet primitive type name (INT32, INT64, BINARY, etc.)
   * @param annotation the Parquet logical type annotation (may be null)
   * @return Some(DataType) if a framework type matches, None otherwise
   */
  def fromParquetPrimitive(
      typeName: PrimitiveTypeName,
      @Nullable annotation: LogicalTypeAnnotation): Option[DataType] = {
    if (!SQLConf.get.typesFrameworkEnabled) return None
    (typeName, annotation) match {
      case (PrimitiveTypeName.INT64, time: LogicalTypeAnnotation.TimeLogicalTypeAnnotation)
          if time.getUnit == LogicalTypeAnnotation.TimeUnit.MICROS &&
            !time.isAdjustedToUTC =>
        Some(TimeType(TimeType.MICROS_PRECISION))
      // Add new primitive type mappings here
      case _ => None
    }
  }

  /**
   * Maps a Parquet group type annotation to a Spark DataType.
   * Used by ParquetToSparkSchemaConverter.convertGroupField.
   *
   * @param annotation the Parquet logical type annotation on the group
   * @return Some(DataType) if a framework type matches, None otherwise
   */
  def fromParquetGroup(
      @Nullable annotation: LogicalTypeAnnotation): Option[DataType] = {
    if (!SQLConf.get.typesFrameworkEnabled) return None
    annotation match {
      // Add new group type mappings here
      case _ => None
    }
  }

  // ==================== Java-Friendly Wrappers ====================
  //
  // ParquetVectorUpdaterFactory.java and VectorizedColumnReader.java are Java files that
  // need to call into ParquetTypeOps. These wrappers return null/boxed types instead of
  // Option for clean Java interop. Null means "not handled by framework, fall through to
  // default code."

  /**
   * Java-friendly wrapper for getVectorUpdater.
   * Returns null if no framework ops or the type has no vectorized updater.
   */
  def getVectorUpdaterOrNull(
      dt: DataType,
      descriptor: ColumnDescriptor,
      annotation: LogicalTypeAnnotation): ParquetVectorUpdater = {
    apply(dt).flatMap(_.getVectorUpdater(descriptor, annotation)).orNull
  }

  /**
   * Java-friendly wrapper for isLazyDecodingSupported.
   * Returns boxed Boolean (null = not handled, TRUE/FALSE = framework result).
   */
  def isLazyDecodingSupportedFor(
      dt: DataType,
      typeName: PrimitiveTypeName,
      annotation: LogicalTypeAnnotation): JBoolean = {
    apply(dt).map(ops =>
      JBoolean.valueOf(ops.isLazyDecodingSupported(typeName, annotation))
    ).orNull
  }

  // ==================== Filter Helpers ====================
  //
  // Type-specific helpers for building Parquet filter predicates. Ops implementations
  // delegate to these rather than calling FilterApi directly, reducing boilerplate.
  // Each helper handles one backing type (Long, Int, Binary, etc.).

  /**
   * Creates a Parquet comparison filter for a Long-backed type.
   * Reusable by any type stored as INT64 in Parquet (TimeType, future types).
   *
   * @param op the comparison operation
   * @param path the column path components
   * @param value the filter value (in external representation)
   * @param valueToLong converts the external value to Long for Parquet filter comparison
   * @return a Parquet FilterPredicate
   */
  def makeLongFilter(
      op: ParquetFilterOp,
      path: Array[String],
      @Nullable value: Any,
      valueToLong: Any => JLong): FilterPredicate = {
    val column = longColumn(path)
    val converted: JLong = Option(value).map(valueToLong).orNull
    op match {
      case ParquetFilterOp.Eq => FilterApi.eq(column, converted)
      case ParquetFilterOp.NotEq => FilterApi.notEq(column, converted)
      case ParquetFilterOp.Lt => FilterApi.lt(column, converted)
      case ParquetFilterOp.LtEq => FilterApi.ltEq(column, converted)
      case ParquetFilterOp.Gt => FilterApi.gt(column, converted)
      case ParquetFilterOp.GtEq => FilterApi.gtEq(column, converted)
    }
  }

  /**
   * Creates a Parquet IN filter for a Long-backed type.
   *
   * @param path the column path components
   * @param values the filter values (in external representation)
   * @param valueToLong converts each external value to Long
   * @return a Parquet FilterPredicate for the IN operation
   */
  def makeLongInFilter(
      path: Array[String],
      values: Array[Any],
      valueToLong: Any => JLong): FilterPredicate = {
    val column = longColumn(path)
    val set = new HashSet[JLong]()
    for (v <- values) {
      set.add(Option(v).map(valueToLong).orNull)
    }
    FilterApi.in(column, set)
  }

  // Future: makeIntFilter, makeBinaryFilter, etc. for other backing types.

  // ==================== Filter Reverse Lookup ====================

  /**
   * Finds a ParquetTypeOps by its Parquet filter type (annotation + primitive type).
   *
   * Used by the framework dispatch code inside ParquetFilters to match a Parquet column's
   * schema type to a registered framework ops. The match is on raw Parquet library types
   * because ParquetSchemaType is a private inner class of ParquetFilters.
   *
   * @param annotation the Parquet logical type annotation
   * @param primitiveType the Parquet primitive type name
   * @return Some(ops) if a framework type matches, None otherwise
   */
  def findByParquetFilter(
      @Nullable annotation: LogicalTypeAnnotation,
      primitiveType: PrimitiveTypeName): Option[ParquetTypeOps] = {
    if (!SQLConf.get.typesFrameworkEnabled) return None
    // Reverse lookup: given a Parquet annotation + primitive type, find the
    // framework DataType, then get its ops. Uses fromParquetPrimitive for the
    // DataType lookup (same reverse mapping as the read-path schema conversion),
    // then apply() for the ops lookup. This avoids maintaining a separate
    // registration list and ensures consistency with the other lookup paths.
    fromParquetPrimitive(primitiveType, annotation)
      .flatMap(apply)
      .filter(_.parquetFilterType.isDefined)
  }
}

/**
 * Enumeration of Parquet comparison filter operations.
 *
 * Used by [[ParquetTypeOps.makeFilterPredicate]] so the infrastructure can request any
 * comparison filter without knowing the type's backing Parquet column type. The ops
 * implementation handles the type-specific FilterApi call internally.
 */
sealed trait ParquetFilterOp

object ParquetFilterOp {
  case object Eq extends ParquetFilterOp
  case object NotEq extends ParquetFilterOp
  case object Lt extends ParquetFilterOp
  case object LtEq extends ParquetFilterOp
  case object Gt extends ParquetFilterOp
  case object GtEq extends ParquetFilterOp
}
