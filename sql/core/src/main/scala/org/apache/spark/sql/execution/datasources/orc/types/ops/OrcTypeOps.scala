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
 *   - Value write (serialize): Catalyst value -> ORC WritableComparable (OrcSerializer.newConverter)
 *   - Row-based read (deserialize): ORC WritableComparable -> Catalyst value
 *     (OrcDeserializer.newWriter)
 *   - Predicate pushdown: PredicateLeaf.Type mapping + literal casting (OrcFilters)
 *
 * DELIBERATELY NOT ON THE TRAIT:
 *   - Vectorized read. ORC's vectorized path (OrcAtomicColumnVector, a Java class) dispatches via
 *     boolean `instanceof` flags set in the constructor plus typed accessor methods
 *     (getTimestampNTZNanos/getTimestampLTZNanos), NOT via an `Ops(dt).map(_.x)` closure. It has
 *     no per-type extension seam analogous to Parquet's getVectorUpdater, so it stays inline. This
 *     mirrors ParquetTypeOps, whose vectorized-read hook was also added only later.
 *   - supportDataType. OrcFileFormat.supportDataType / OrcTable.supportsDataType already admit
 *     every `AtomicType` via `case _: AtomicType => true`, so framework types are supported with
 *     no per-type arm; no gate method is needed (unlike Parquet, whose default differs).
 *
 * DISPATCH PATTERN: framework FIRST at each row-path integration site. Each ORC infrastructure
 * method wraps itself with:
 * {{{
 *   OrcTypeOps(dt).map(_.method(...)).getOrElse(methodDefault(dt, ...))
 * }}}
 * The original inline code is extracted to a *Default method unchanged. For a framework-managed
 * type the ops handles it; for any other type OrcTypeOps(dt) is None and the *Default fallback
 * executes the original path.
 *
 * DECOUPLING NOTE: makeDeserializer takes the Catalyst setter callbacks it needs
 * ((Int, Long) => Unit and (Int, Any) => Unit) rather than OrcDeserializer's CatalystDataUpdater,
 * because that updater is a sealed trait nested in OrcDeserializer and is not visible to this
 * sub-package. The callbacks are the only two setter shapes the current framework types use.
 *
 * @see TimeTypeOrcOps for a reference implementation (primitive Long-backed type)
 * @see TimestampLTZNanosOrcOps / TimestampNTZNanosOrcOps for OrcTimestamp-backed types
 * @since 4.4.0
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
   * The ORC PredicateLeaf.Type for this type (OrcFilters.getPredicateLeafType), or None to opt out
   * of pushdown (the caller then treats the column as non-convertible). TimeType returns
   * Some(LONG); the nanos-timestamp types return None (no pushdown today - the inline code never
   * added a nanos arm to getPredicateLeafType).
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
 * Provides forward lookup (DataType -> ops) for framework-first dispatch at ORC integration
 * sites. apply() returns Some only for framework-managed types, so callers fall back to the legacy
 * inline path for everything else.
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
