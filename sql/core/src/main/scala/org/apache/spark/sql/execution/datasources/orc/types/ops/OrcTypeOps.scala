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

package org.apache.spark.sql.execution.datasources.orc.types.ops

import org.apache.hadoop.hive.ql.io.sarg.PredicateLeaf
import org.apache.hadoop.io.WritableComparable
import org.apache.orc.TypeDescription

import org.apache.spark.sql.catalyst.expressions.SpecializedGetters
import org.apache.spark.sql.types.{DataType, TimestampLTZNanosType, TimestampNTZNanosType, TimeType}

/**
 * Optional trait for ORC storage-format integration in the Types Framework.
 *
 * Implement this trait to enable ORC read/write support for a framework type. Each framework
 * type that supports ORC provides a concrete implementation and registers it in the companion
 * object's apply() method.
 *
 * The trait covers the ORC concerns that are homogeneous across the row-based code paths:
 *   - Schema mapping: Spark DataType -> ORC schema string (OrcUtils.getOrcSchemaString) and the
 *     ORC TypeDescription category (OrcUtils.orcTypeDescription). ORC has no native TIME /
 *     nanosecond-timestamp category, so framework types map onto an existing physical ORC
 *     category and round-trip the true Spark type via the CATALYST_TYPE_ATTRIBUTE_NAME attribute
 *     (stamped uniformly by the caller, so the ops only chooses the category).
 *   - Value write (serialize): Catalyst value -> ORC WritableComparable
 *     (OrcSerializer.newConverter)
 *   - Row-based read (deserialize): ORC WritableComparable -> Catalyst value
 *     (OrcDeserializer.newWriter)
 *   - Predicate pushdown: PredicateLeaf.Type mapping + literal casting (OrcFilters)
 *
 * DELIBERATELY NOT ON THE TRAIT:
 *   - Vectorized read. ORC's vectorized path (OrcAtomicColumnVector, a Java class) dispatches via
 *     boolean `instanceof` flags set in the constructor plus typed accessor methods
 *     (getTimestampNTZNanos/getTimestampLTZNanos), NOT via an `Ops(dt).map(_.x)` closure. It has
 *     no per-type extension seam, so it stays inline. This mirrors Parquet, whose vectorized read
 *     is likewise not routed through ParquetTypeOps (it dispatches on the Spark type inline in
 *     ParquetVectorUpdaterFactory.getUpdater).
 *
 *     CAVEAT for a new type author: the vectorized reader is a SEPARATE registration you must
 *     handle in addition to the ops class. OrcUtils.supportColumnarReads returns true for every
 *     `AtomicType` and OrcAtomicColumnVector dispatches by `instanceof`, so a new framework
 *     `AtomicType` wired only into this ops registry is still routed to the vectorized reader,
 *     where it would be read as raw physical values (silently wrong) rather than failing loudly.
 *     Add an OrcAtomicColumnVector arm (or exclude the type from supportColumnarReads) as well.
 *     The current types are wired correctly; this note is so the "one ops class + one registry
 *     arm" framing does not mislead.
 *   - supportDataType. OrcFileFormat.supportDataType / OrcTable.supportsDataType already admit
 *     every `AtomicType` via `case _: AtomicType => true`, so framework types are supported with
 *     no per-type arm; no gate method is needed (unlike Parquet, whose default differs).
 *
 * DISPATCH PATTERN: each ORC integration site keeps its existing built-in-type arms unchanged and
 * routes only framework types through the ops. The built-in types are matched first; framework
 * types are handled by an added arm reached after them (i.e. framework types are dispatched last,
 * not first), so this change never alters how a built-in type is handled. Two shapes are used:
 *   - Inside a `dataType match` (OrcSerializer.newConverter, OrcDeserializer.newWriter,
 *     OrcUtils.orcTypeDescription, OrcFilters.castLiteralValue): an added
 *     `case OrcTypeOps(ops) => ops.method(...)` arm placed among the existing arms. The `unapply`
 *     extractor binds the ops in a single registry lookup.
 *   - In expression position (OrcUtils.getOrcSchemaString, OrcFilters.getPredicateLeafType): the
 *     original fallback stays inline and framework types are folded in with
 *     `OrcTypeOps(dt).map(_.method).getOrElse(<original fallback>)`.
 * There are no extracted `*Default` methods; the original ORC code is left in place as the
 * fallback arm/expression.
 *
 * DECOUPLING NOTE: makeDeserializer takes the Catalyst setter callbacks it needs
 * ((Int, Long) => Unit and (Int, Any) => Unit) rather than OrcDeserializer's CatalystDataUpdater,
 * because that updater is a sealed trait nested in OrcDeserializer and is not visible to this
 * sub-package. The callbacks are the only two setter shapes the current framework types use.
 *
 * @see TimeTypeOrcOps for a reference implementation (primitive Long-backed type)
 * @see TimestampLTZNanosOrcOps / TimestampNTZNanosOrcOps for OrcTimestamp-backed types
 * @since 5.0.0
 */
private[orc] trait OrcTypeOps extends Serializable {

  // ==================== Schema Mapping ====================

  /**
   * The ORC schema-string fragment for this type (OrcUtils.getOrcSchemaString). Examples:
   * TimeType -> "bigint" (LongType.catalogString), TimestampLTZNanosType -> "timestamp with local
   * time zone", TimestampNTZNanosType -> "timestamp".
   */
  def orcSchemaString: String

  /**
   * The ORC TypeDescription category for this type (OrcUtils.orcTypeDescription). The caller
   * stamps CATALYST_TYPE_ATTRIBUTE_NAME = sparkType.typeName onto the returned descriptor, so the
   * ops only chooses the physical category.
   */
  def orcCategory: TypeDescription.Category

  // ==================== Value Write (serialize) ====================

  /**
   * Creates a converter that turns a Catalyst value at an ordinal into an ORC WritableComparable
   * (OrcSerializer.newConverter).
   *
   * @param reuseObj whether the serializer may reuse a single mutable Writable across rows
   *                 (OrcSerializer passes this through; the primitive Long-backed TimeType reuses,
   *                 the OrcTimestamp-backed nanos types do not).
   */
  def makeSerializer(reuseObj: Boolean): (SpecializedGetters, Int) => WritableComparable[_]

  // ==================== Row-Based Read (deserialize) ====================

  /**
   * Creates a writer that sets a decoded ORC value into the Catalyst row at an ordinal
   * (OrcDeserializer.newWriter). The WritableComparable is the raw ORC value (LongWritable for
   * LONG-category types, OrcTimestamp for the timestamp categories).
   *
   * @param setLong callback into the row's setLong (used by primitive Long-backed types)
   * @param set     callback into the row's generic set (used by object-backed types, e.g. nanos)
   */
  def makeDeserializer(
      setLong: (Int, Long) => Unit,
      set: (Int, Any) => Unit): (Int, WritableComparable[_]) => Unit

  // ==================== Predicate Pushdown ====================

  /**
   * The ORC PredicateLeaf.Type for this type, consumed by OrcFilters.getPredicateLeafType. Return
   * Some(type) to enable predicate pushdown for this type.
   *
   * A None keeps the pre-existing OrcFilters behavior for a type with no leaf mapping: it reaches
   * getPredicateLeafType's final arm, which throws unsupportedOperationForDataTypeError. In
   * practice that arm is only reached for a column already deemed pushdown-eligible upstream
   * (OrcFiltersBase.getSearchableTypeMap admits any AtomicType), so a framework AtomicType that
   * returns None and is used in a pushed filter would fail during planning. A future type that
   * must be pushdown-eligible therefore has to return Some here (and, if its literal needs
   * conversion, override castFilterLiteral); returning None is only safe for a type that can never
   * reach getPredicateLeafType.
   *
   * TimeType returns Some(LONG). The nanosecond-timestamp types return None: they had no
   * getPredicateLeafType arm before this change either, so the behavior (throw if ever reached) is
   * preserved, not newly introduced.
   */
  def predicateLeafType: Option[PredicateLeaf.Type] = None

  /**
   * Casts a filter literal to the ORC search-argument representation (OrcFilters.castLiteralValue).
   * Only meaningful for types that also return a predicateLeafType. Default is identity.
   */
  def castFilterLiteral(value: Any): Any = value
}

/**
 * Factory object for creating OrcTypeOps instances.
 *
 * Provides forward lookup (DataType -> ops) for the framework-type dispatch arms at ORC
 * integration sites. apply() returns Some only for framework-managed types, so callers fall back
 * to the original inline path for everything else.
 */
private[orc] object OrcTypeOps {

  /**
   * Returns an OrcTypeOps instance for the given DataType, if supported.
   *
   * Returns None if the type has no ORC ops. This is the single registration point for all ORC
   * type operations.
   */
  def apply(dt: DataType): Option[OrcTypeOps] = dt match {
    case tt: TimeType => Some(TimeTypeOrcOps(tt))
    case t: TimestampLTZNanosType => Some(TimestampLTZNanosOrcOps(t))
    case t: TimestampNTZNanosType => Some(TimestampNTZNanosOrcOps(t))
    // Add new types here - single registration point
    case _ => None
  }

  /**
   * Extractor so a `dataType match` arm can bind the ops in a single lookup:
   * {{{
   *   case OrcTypeOps(ops) => ops.method(...)
   * }}}
   * Delegates to apply(); returning the same Option keeps the registry the single source of truth
   * and avoids the double lookup of a `case _ if OrcTypeOps(dt).isDefined => OrcTypeOps(dt).get`
   * guard.
   */
  def unapply(dt: DataType): Option[OrcTypeOps] = apply(dt)
}
