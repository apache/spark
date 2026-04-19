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
import org.apache.spark.sql.catalyst.expressions.{Literal, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.types.StructType

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
 * The helpers here fill non-ordering columns with `Literal.default` (recursive
 * zero / empty / false). In UnsafeRow, that encodes as the byte-wise smallest
 * representation for all standard Dataset/SQL-encoded types -- numerics (including
 * negatives, whose non-zero two's-complement bytes still sort greater than
 * all-zero), and variable-length types (size=0 beats any non-zero size).
 *
 * Caveats where the "smallest" property does NOT hold and which therefore should
 * not appear in caller key schemas:
 *   - `CharType(n)`: default is space-padded (0x20), but real values may legally
 *     contain control bytes (0x00..0x1F).
 *   - `VariantType`: the Variant binary layout is not guaranteed minimized by
 *     `castToVariant(0, IntegerType)`.
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
   * Build an `InternalRow` whose fields are the recursive defaults of `schema`. See
   * the object-level docstring for the byte-wise-ordering guarantees and caveats.
   */
  def defaultInternalRow(schema: StructType): InternalRow = {
    InternalRow.fromSeq(schema.map(f => Literal.default(f.dataType).value))
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
}
