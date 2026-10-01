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

package org.apache.spark.sql.catalyst.util

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{BaseOrdering, Expression, Murmur3HashFunction, RowOrdering}
import org.apache.spark.sql.connector.read.{HasPartitionKey, InputPartition}
import org.apache.spark.sql.types.{DataType, StructField, StructType}
import org.apache.spark.util.NonFateSharingCache

/**
 * Wraps the [[InternalRow]] with the corresponding [[DataType]] to make it comparable with
 * the values in [[InternalRow]].
 * It uses Spark's internal murmur hash to compute hash code from an row, and uses [[RowOrdering]]
 * to perform equality checks.
 *
 * @param dataTypes the types this row is compared at, always `comparableTypes` of the types the row
 *                  was built from. No constructor can produce a raw list, and the two caches below
 *                  are keyed by erased lists for the same reason.
 */
class InternalRowComparableWrapper private (
    val row: InternalRow,
    val dataTypes: Seq[DataType],
    @transient private var _structType: StructType,
    @transient private var _ordering: BaseOrdering) extends Serializable {

  /**
   * Previous constructor for binary compatibility. Prefer using
   * `getInternalRowComparableWrapperFactory` for the creation of InternalRowComparableWrapper's in
   * hot paths to avoid excessive cache lookups.
   */
  @deprecated
  def this(row: InternalRow, dataTypes: Seq[DataType]) = this(
    // Erases like the factory, so a wrapper built here compares with the ones the factory built.
    row, InternalRowComparableWrapper.comparableTypes(dataTypes), null, null)

  // `structType` and `ordering` cannot cross the wire (the ordering may be generated code), so
  // they are transient and re-derived from the shared caches on first use after deserialization.
  def structType: StructType = {
    if (_structType == null) {
      _structType = InternalRowComparableWrapper.structTypeCache.get(dataTypes)
    }
    _structType
  }

  def ordering: BaseOrdering = {
    if (_ordering == null) {
      _ordering = InternalRowComparableWrapper.orderingCache.get(dataTypes)
    }
    _ordering
  }

  override def hashCode(): Int = Murmur3HashFunction.hash(
    row,
    structType,
    42L,
    isCollationAware = true,
    // legacyCollationAwareHashing only matters when isCollationAware is false.
    legacyCollationAwareHashing = false).toInt

  override def equals(other: Any): Boolean = {
    if (!other.isInstanceOf[InternalRowComparableWrapper]) {
      return false
    }
    val otherWrapper = other.asInstanceOf[InternalRowComparableWrapper]
    if (!otherWrapper.dataTypes.equals(this.dataTypes)) {
      return false
    }
    ordering.compare(row, otherWrapper.row) == 0
  }
}

object InternalRowComparableWrapper {
  private final val MAX_CACHE_ENTRIES = 1024

  private val orderingCache = {
    val loadFunc = (dataTypes: Seq[DataType]) => {
      RowOrdering.createNaturalAscendingOrdering(dataTypes)
    }
    NonFateSharingCache(loadFunc, MAX_CACHE_ENTRIES)
  }

  private val structTypeCache = {
    val loadFunc = (dataTypes: Seq[DataType]) => {
      StructType(dataTypes.map(t => StructField("f", t)))
    }
    NonFateSharingCache(loadFunc, MAX_CACHE_ENTRIES)
  }

  def apply(
      partition: InputPartition with HasPartitionKey,
      partitionExpression: Seq[Expression]): InternalRowComparableWrapper = {
    apply(partition.partitionKey(), partitionExpression)
  }

  def apply(
      partitionRow: InternalRow,
      partitionExpression: Seq[Expression]): InternalRowComparableWrapper = {
    getInternalRowComparableWrapperFactory(partitionExpression.map(_.dataType))(partitionRow)
  }

  /**
   * The types a row is compared at, which is the given types with their naming erased: struct field
   * names and every nullability go, and nothing else does.
   *
   * Two rows of the same value belong together whatever the columns they came from were called. A
   * storage-partitioned join relies on that: an equi-join across two structs whose fields are named
   * differently is legal, `identity` carries that name into the key type, and
   * `KeyedShuffleSpec.createPartitioning` puts one side's expressions over the other side's keys.
   * So the naming is erased once, here, and every wrapper compares at these types.
   *
   * Everything else is kept exactly, since it decides where a value belongs: a collation and a
   * decimal precision still tell two rows apart.
   */
  private[catalyst] def comparableTypes(dataTypes: Seq[DataType]): Seq[DataType] = {
    val erased = dataTypes.map(t => erasePositionalNames(t.asNullable))
    // Erasing is idempotent, and a list that was already erased is returned as it is rather than
    // rebuilt. Callers pass their own types here and then hand the result back in, so keeping the
    // instance is what lets `equals` settle two wrappers of one list by reference.
    if (erased == dataTypes) dataTypes else erased
  }

  /**
   * `asNullable` above is the nullability half. This is the naming half: positional field names, so
   * two structs that agree field by field agree here. A field's metadata goes with its name, since
   * nothing that compares or hashes a row reads either. `transformRecursively` stops at the first
   * match, so a nested struct is reached by the recursive call rather than by the walk.
   */
  private def erasePositionalNames(dataType: DataType): DataType =
    dataType.transformRecursively {
      case s: StructType => StructType(s.fields.zipWithIndex.map { case (field, i) =>
        StructField(i.toString, erasePositionalNames(field.dataType))
      })
    }

  /**
   * Builds wrappers over one row schema, holding the cache lookups that schema needs so a caller
   * does not repeat them per row.
   *
   * It also answers for the schema it settled on. `dataTypes` is what the rows it builds compare
   * at, which is `comparableTypes` of what it was given, and `ordering` sorts them. A caller
   * reporting a type list beside those rows, or sorting them, takes it from here rather than
   * deriving it again, so the two cannot answer differently.
   */
  final class Factory private[InternalRowComparableWrapper] (val dataTypes: Seq[DataType])
    extends (InternalRow => InternalRowComparableWrapper) {

    val ordering: BaseOrdering = orderingCache.get(dataTypes)
    private[this] val structType = structTypeCache.get(dataTypes)

    override def apply(row: InternalRow): InternalRowComparableWrapper =
      new InternalRowComparableWrapper(row, dataTypes, structType, ordering)
  }

  /** Creates a shared factory for a given row schema to avoid excessive cache lookups. */
  def getInternalRowComparableWrapperFactory(dataTypes: Seq[DataType]): Factory =
    new Factory(comparableTypes(dataTypes))
}
