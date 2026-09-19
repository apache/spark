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

import java.lang.{Long => JLong}
import java.util.HashSet

import org.apache.parquet.filter2.predicate.{FilterApi, FilterPredicate}
import org.apache.parquet.filter2.predicate.Operators.{Column, SupportsLtGt}
import org.apache.parquet.filter2.predicate.SparkFilterApi.longColumn
import org.apache.parquet.schema.LogicalTypeAnnotation
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName

/**
 * Optional Parquet filter-pushdown support for a Types Framework type.
 *
 * A framework type that wants its predicates pushed down to Parquet provides a
 * [[ParquetFilterOps]] and registers it in [[ParquetTypeOps.filterOpsList]]; types that don't
 * support pushdown add nothing and are read without filtering.
 *
 * Dispatch is keyed off the Parquet file's column encoding, not the requested Spark type,
 * because filter pushdown matches the on-disk schema. The ops therefore declares the
 * Parquet primitive + logical annotation it owns ([[primitiveTypeName]] /
 * [[logicalTypeAnnotation]]); `ParquetFilters` reverse-looks-up the ops for a field via
 * [[ParquetTypeOps.filterOpsFor]] and routes that field's predicates here. This keeps the
 * per-type filter knowledge (value conversion, predicate construction) with the type
 * instead of scattered across `ParquetFilters`.
 *
 * Implementations build the parquet-mr [[FilterPredicate]] for each comparison directly, so they
 * own the choice of physical column and the external-value -> physical-value conversion. The
 * eq/notEq/in builders must tolerate a null `value` (used for IsNull / IsNotNull); the ordered
 * builders (lt/ltEq/gt/gtEq) are only invoked with non-null values. Rather than implement the
 * builders directly, a type should extend [[TypedParquetFilterOps]] (or its INT64 alias
 * [[LongParquetFilterOps]]), which writes the boilerplate and the null-handling split once.
 *
 * @see TimeTypeParquetOps.filterOps for a reference implementation (INT64-backed TimeType)
 * @since 4.3.0
 */
private[parquet] trait ParquetFilterOps {

  /** The Parquet logical type annotation of the column this ops handles (may be null). */
  def logicalTypeAnnotation: LogicalTypeAnnotation

  /** The Parquet primitive type of the column this ops handles. */
  def primitiveTypeName: PrimitiveTypeName

  /** Whether `value` (a non-null external filter value) is pushable for this type. */
  def acceptsValue(value: Any): Boolean

  def makeEq(columnPath: Array[String], value: Any): FilterPredicate
  def makeNotEq(columnPath: Array[String], value: Any): FilterPredicate
  def makeLt(columnPath: Array[String], value: Any): FilterPredicate
  def makeLtEq(columnPath: Array[String], value: Any): FilterPredicate
  def makeGt(columnPath: Array[String], value: Any): FilterPredicate
  def makeGtEq(columnPath: Array[String], value: Any): FilterPredicate
  def makeIn(columnPath: Array[String], values: Array[Any]): FilterPredicate
}

/**
 * Base [[ParquetFilterOps]] for a type stored in an ordered Parquet primitive column of physical
 * type `T` (e.g. `java.lang.Long` for INT64, `java.lang.Integer` for INT32, `Binary` for BINARY).
 * Implements all seven predicate builders once against the parquet-mr `FilterApi`, so a concrete
 * type supplies only the encoding it owns ([[logicalTypeAnnotation]] / [[primitiveTypeName]]), the
 * [[column]] accessor for its physical type, [[acceptsValue]], and the [[toPhysical]] conversion.
 * The null-handling split (eq/notEq/in tolerate a null value for IsNull / IsNotNull; the ordered
 * builders never receive null) and the `makeIn` set construction live here once, so an
 * implementer can't get them wrong. The column must support ordering (`SupportsLtGt`, which
 * extends `SupportsEqNotEq`); every framework-relevant physical type does (only `BooleanColumn`
 * is eq-only, and no framework type is boolean-backed).
 *
 * `T` is confined to this base: the public [[ParquetFilterOps]] the registry, `filterOpsFor`,
 * and the `ParquetFilters` extractor consume stays non-generic.
 */
private[parquet] abstract class TypedParquetFilterOps[T <: Comparable[T]] extends ParquetFilterOps {

  /** The physical column accessor for `T` (e.g. `SparkFilterApi.longColumn`). */
  protected def column(columnPath: Array[String]): Column[T] with SupportsLtGt

  /** Converts a non-null pushable `value` to the physical value `T` stored in the file. */
  protected def toPhysical(value: Any): T

  private def toPhysicalOrNull(value: Any): T =
    if (value == null) null.asInstanceOf[T] else toPhysical(value)

  override def makeEq(columnPath: Array[String], value: Any): FilterPredicate =
    FilterApi.eq(column(columnPath), toPhysicalOrNull(value))
  override def makeNotEq(columnPath: Array[String], value: Any): FilterPredicate =
    FilterApi.notEq(column(columnPath), toPhysicalOrNull(value))
  override def makeLt(columnPath: Array[String], value: Any): FilterPredicate =
    FilterApi.lt(column(columnPath), toPhysical(value))
  override def makeLtEq(columnPath: Array[String], value: Any): FilterPredicate =
    FilterApi.ltEq(column(columnPath), toPhysical(value))
  override def makeGt(columnPath: Array[String], value: Any): FilterPredicate =
    FilterApi.gt(column(columnPath), toPhysical(value))
  override def makeGtEq(columnPath: Array[String], value: Any): FilterPredicate =
    FilterApi.gtEq(column(columnPath), toPhysical(value))
  override def makeIn(columnPath: Array[String], values: Array[Any]): FilterPredicate = {
    val set = new HashSet[T]()
    values.foreach(v => set.add(toPhysicalOrNull(v)))
    FilterApi.in(column(columnPath), set)
  }
}

/**
 * [[TypedParquetFilterOps]] specialized to an INT64 (`longColumn`) physical column. A concrete
 * type supplies only [[logicalTypeAnnotation]], [[acceptsValue]], and the [[toLong]] conversion.
 */
private[parquet] abstract class LongParquetFilterOps extends TypedParquetFilterOps[JLong] {

  override val primitiveTypeName: PrimitiveTypeName = PrimitiveTypeName.INT64

  override protected def column(columnPath: Array[String]): Column[JLong] with SupportsLtGt =
    longColumn(columnPath)

  /** Converts a non-null pushable `value` to the INT64 physical value stored in the file. */
  protected def toLong(value: Any): JLong

  override protected def toPhysical(value: Any): JLong = toLong(value)
}
