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

package org.apache.spark.sql.execution.streaming.state

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{GenericInternalRow, Literal, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.types.{CharType, DataType, StructType, UserDefinedType, VariantType}
import org.apache.spark.unsafe.types.UTF8String

/**
 * Utilities for building boundary rows used as start / end keys in state store
 * range scans.
 *
 * Callers of [[StateStore.rangeScan]] / [[StateStore.rangeScanWithMultiValues]]
 * must provide fully-typed `UnsafeRow` boundaries matching the column family's key
 * schema, even when they only care about the ordering column (e.g. timestamp).
 * The non-ordering columns must encode byte-wise no larger than any real entry at
 * the same ordering prefix, or `seek()` will silently skip matching entries.
 *
 * The helpers here fill non-ordering columns with recursive defaults. For most
 * types, `Literal.default` (recursive zero / empty / false) is already byte-wise
 * smallest in UnsafeRow -- numerics (including negatives, whose non-zero two's-
 * complement bytes still sort greater than all-zero), and variable-length types
 * (size=0 beats any non-zero size).
 *
 * Exceptions handled explicitly:
 *   - `CharType(n)`: `Literal.default` is space-padded (0x20) because every
 *     stored value has exactly `n` characters, and real values may legally
 *     contain control bytes (0x00..0x1F). We override with `n` bytes of `0x00`
 *     (U+0000 is a legal 1-byte UTF-8 code point), giving the smallest possible
 *     UnsafeRow encoding (minimum byte length `n`, minimum per-byte content).
 *
 * Types rejected at runtime because we cannot safely hand-encode a byte-wise
 * minimum:
 *   - `VariantType`: the Variant binary layout is not guaranteed minimized by
 *     `castToVariant(0, IntegerType)`, and the Variant spec is `@Unstable`, so
 *     the minimum would be brittle against future spec changes. In practice,
 *     Spark analysis (grouping / hashing checks in `ExprUtils`, `HashExpression`)
 *     already rejects VariantType in state-store key positions before reaching
 *     the range-scan helpers, so this assertion is defensive only.
 *
 * A cleaner long-term fix would extend the state store API to accept an
 * ordering-column-only range bound and avoid synthesizing boundary rows at all.
 *
 * Callers must also ensure real stored entries never have internally-null
 * non-ordering fields; this holds today because entries go through the user's
 * expression encoder.
 */
private[sql] object RangeScanBoundaryUtils {

  /**
   * Build an `InternalRow` whose fields are the recursive byte-wise-smallest
   * defaults of `schema`. See the object-level docstring for the ordering
   * guarantees and the CharType / VariantType handling.
   */
  def defaultInternalRow(schema: StructType): InternalRow = {
    assertBoundarySchemaSupported(schema)
    InternalRow.fromSeq(schema.map(f => recursiveDefaultValue(f.dataType)))
  }

  /**
   * Build an `UnsafeRow` that is the recursive default of `schema`, suitable for use
   * as a scan boundary where only the ordering prefix of the key matters. The
   * returned row is a fresh copy; callers may retain it across subsequent projection
   * calls.
   */
  def defaultUnsafeRow(schema: StructType): UnsafeRow = {
    UnsafeProjection.create(schema).apply(defaultInternalRow(schema)).copy()
  }

  /**
   * Produce the byte-wise smallest legitimate value for `dt`. Falls back to
   * `Literal.default` for types where it is already byte-wise smallest; overrides
   * `CharType` with `n` zero-bytes; recurses through `StructType` and unwraps
   * `UserDefinedType`. `ArrayType` / `MapType` defaults are empty and therefore
   * already smallest regardless of element type, so no recursion is needed.
   */
  private def recursiveDefaultValue(dt: DataType): Any = dt match {
    case c: CharType =>
      UTF8String.fromBytes(new Array[Byte](c.length))
    case struct: StructType =>
      new GenericInternalRow(
        struct.fields.map(f => recursiveDefaultValue(f.dataType)))
    case udt: UserDefinedType[_] =>
      recursiveDefaultValue(udt.sqlType)
    case _ =>
      Literal.default(dt).value
  }

  /**
   * Reject schemas containing types for which we cannot hand-encode a byte-wise
   * minimum (see object-level docstring). Existing Spark analysis already blocks
   * VariantType in grouping / hashing positions, so this assertion is defensive
   * and should never fire in practice.
   */
  private def assertBoundarySchemaSupported(schema: StructType): Unit = {
    val hasVariantType = schema.existsRecursively(_.isInstanceOf[VariantType])
    assert(!hasVariantType,
      "RangeScanBoundaryUtils cannot build a scan boundary for a schema containing " +
        "VariantType; see RangeScanBoundaryUtils docstring for details. " +
        s"schema=${schema.catalogString}")
  }
}
