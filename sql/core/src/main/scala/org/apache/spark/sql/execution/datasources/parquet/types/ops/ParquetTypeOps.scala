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

import java.time.ZoneId

import org.apache.parquet.column.ColumnDescriptor
import org.apache.parquet.io.api.{Converter, RecordConsumer}
import org.apache.parquet.schema.Type
import org.apache.parquet.schema.Type.Repetition

import org.apache.spark.sql.catalyst.expressions.SpecializedGetters
import org.apache.spark.sql.catalyst.util.RebaseDateTime.RebaseSpec
import org.apache.spark.sql.execution.datasources.parquet.{HasParentContainerUpdater, ParentContainerUpdater, ParquetToSparkSchemaConverter, ParquetVectorUpdater}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DataType, StructType, TimeType}

/**
 * Optional trait for Parquet storage format integration in the Types Framework.
 *
 * Implement this trait to enable Parquet read/write support for a framework type. Each framework
 * type that supports Parquet provides a concrete implementation and registers it in the companion
 * object's apply() method.
 *
 * The trait covers these Parquet concerns:
 *   - Schema conversion: Spark DataType -> Parquet schema type (write path)
 *   - Value write: writing values to Parquet RecordConsumer
 *   - Row-based read: creating Parquet converters for reading into InternalRow
 *   - Vectorized read: the batch capability flag (isBatchReadSupported) and the batch
 *     updater (getVectorUpdater)
 *   - Type gates: declaring Parquet support (supportDataType)
 *   - Schema clipping: declaring internal struct schema for column pruning
 *
 * NOT yet on the trait (deferred to follow-ups): filter-pushdown predicates.
 *
 * DISPATCH PATTERN: Framework FIRST at all integration sites. Each Parquet infrastructure
 * method wraps itself with:
 * {{{
 *   ParquetTypeOps(dt).map(_.method(...)).getOrElse(methodDefault(dt, ...))
 * }}}
 * The original code is extracted to a *Default method unchanged. For a framework-managed type
 * the ops handles it; for any other type ParquetTypeOps(dt) is None and the *Default fallback
 * executes the original code path.
 *
 * STRUCT-BACKED TYPES: Types stored as Parquet groups should override the
 * extended newConverter overload (which provides schemaConverter/convertTz/rebase specs for
 * recursive field conversion) and declare parquetStructSchema for column pruning.
 *
 * @see TimeTypeParquetOps for a reference implementation (primitive Long-backed type)
 * @since 4.3.0
 */
private[parquet] trait ParquetTypeOps extends Serializable {

  // ==================== Schema Conversion ====================

  /**
   * Converts this Spark DataType to a Parquet schema Type (for the write path).
   *
   * For primitive types: returns a PrimitiveType with the appropriate annotation.
   * For struct-backed types: returns a GroupType with sub-fields and a logical type annotation.
   *
   * @param fieldName the column/field name in the Parquet schema
   * @param repetition REQUIRED, OPTIONAL, or REPEATED
   * @param inShredded whether the field is nested within a shredded Variant schema
   * @return the Parquet Type for this DataType
   */
  def convertToParquetType(
      fieldName: String, repetition: Repetition, inShredded: Boolean = false): Type

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
      parquetType: Type,
      updater: ParentContainerUpdater): Converter with HasParentContainerUpdater

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
      parquetType: Type,
      updater: ParentContainerUpdater,
      schemaConverter: ParquetToSparkSchemaConverter,
      convertTz: Option[ZoneId],
      datetimeRebaseSpec: RebaseSpec,
      int96RebaseSpec: RebaseSpec): Converter with HasParentContainerUpdater =
    newConverter(parquetType, updater)

  // ==================== Type Gates ====================

  /**
   * Whether this type is supported by the Parquet data source.
   * Used by ParquetFileFormat.supportDataType.
   */
  def supportDataType: Boolean = true

  /**
   * Whether vectorized (batch) reading is supported for this type.
   * Used by ParquetUtils.isBatchReadSupported. Default is false - types must opt in
   * by overriding to true. When false, Spark uses the row-based read path (newConverter)
   * which is always available.
   *
   * A type that returns true must also supply a batch decoder via [[getVectorUpdater]]
   * (dispatched from ParquetVectorUpdaterFactory.getUpdater); otherwise the vectorized factory
   * would not recognize it. TimeType returns true and overrides getVectorUpdater accordingly.
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

  // ==================== Vectorized Read ====================

  /**
   * The vectorized (batch) [[ParquetVectorUpdater]] for this type, or None to fall back to the
   * built-in `ParquetVectorUpdaterFactory`. A type that returns Some here should also return
   * true from [[isBatchReadSupported]]. Dispatched (Spark DataType -> ops) at the top of
   * `ParquetVectorUpdaterFactory.getUpdater`, before its built-in cases.
   *
   * @param descriptor the Parquet column descriptor being read
   */
  def getVectorUpdater(descriptor: ColumnDescriptor): Option[ParquetVectorUpdater] = None
}

/**
 * Factory object for creating ParquetTypeOps instances.
 *
 * Provides forward lookup (DataType -> ops) for framework-first dispatch at Parquet
 * integration sites. apply() returns Some only for framework-managed types, so callers
 * fall back to the legacy path for everything else.
 */
private[parquet] object ParquetTypeOps {

  /**
   * Returns a ParquetTypeOps instance for the given DataType, if supported.
   *
   * Returns None if the type has no Parquet ops.
   * This is the single registration point for all Parquet type operations.
   */
  def apply(dt: DataType): Option[ParquetTypeOps] = {
    dt match {
      case tt: TimeType => Some(TimeTypeParquetOps(tt))
      // Add new types here - single registration point
      case _ => None
    }
  }

  /**
   * Java-friendly entry point for `ParquetVectorUpdaterFactory`: the framework vectorized
   * updater for `dt`, or null if `dt` is not framework-managed (so the factory falls through
   * to its built-in updaters).
   */
  private[parquet] def getVectorUpdaterOrNull(
      dt: DataType, descriptor: ColumnDescriptor): ParquetVectorUpdater =
    apply(dt).flatMap(_.getVectorUpdater(descriptor)).orNull
}
