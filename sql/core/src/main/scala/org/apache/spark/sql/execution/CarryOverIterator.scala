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

package org.apache.spark.sql.execution

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.TypeUtils
import org.apache.spark.sql.types.{DataType, StructType}

/**
 * Streaming iterator for CDC carry-over removal.
 *
 * Runs a three-buffer state machine (`pendingDelete`, `pendingEmit`, `pendingNext`; see field
 * docs below) over a pre-sorted iterator (sorted by rowId, rowVersion, _change_type ASC),
 * comparing consecutive delete+insert pairs with the same (rowId, rowVersion):
 *   - If all data columns are identical -> carry-over, drop both rows
 *   - If data columns differ -> real change, emit both rows
 *   - Unpaired delete or insert -> emit as-is
 *
 * @param input pre-sorted iterator of InternalRow
 * @param rowIdOrdinals column indices for row identity columns (supports composite keys)
 * @param rowVersionOrdinal column index for row version comparison
 * @param changeTypeOrdinal column index for _change_type (String: "delete" or "insert")
 * @param dataOrdinals column indices for data column comparison (field-by-field equality)
 * @param schema the output schema for generic data column comparison
 */
class CarryOverIterator(
    input: Iterator[InternalRow],
    rowIdOrdinals: Array[Int],
    rowVersionOrdinal: Int,
    changeTypeOrdinal: Int,
    dataOrdinals: Array[Int],
    schema: StructType) extends Iterator[InternalRow] {

  // State-machine buffers.
  //
  //  pendingDelete: a delete row that has NOT yet decided its fate. Set when we read a delete
  //                 from input; cleared when we either emit it (no paired insert / paired
  //                 with different data) or drop it (paired with identical insert = carry-over).
  //
  //  pendingEmit:   the row to return on the next next()/hasNext() call. Serves as the
  //                 "ready" signal: advance() runs until pendingEmit is non-null or input
  //                 is drained. Cleared by next() after emission.
  //
  //  pendingNext:   a row read from input that could not be consumed in the current pass and
  //                 must be re-examined on the next advance() tick. Set when the peeked-at
  //                 row turns out to be cross-group (new rowId) or a stray delete.
  //
  //  Invariants at every iteration boundary (hasNext -> advance -> next):
  //    - At most one of pendingDelete / pendingEmit is set on entry to advance().
  //    - pendingNext may be set independently of the other two.
  //    - advance() exits only when pendingEmit != null OR input is drained AND pendingDelete
  //      is null AND pendingNext is null.
  private var pendingDelete: InternalRow = null
  private var pendingEmit: InternalRow = null
  private var pendingNext: InternalRow = null

  // Cached at construction: types (for generic InternalRow.get) and type-aware orderings for
  // data columns. Orderings are needed because `==` on Array[Byte] / ArrayData / InternalRow
  // is reference equality.
  private val rowIdTypes: Array[DataType] =
    rowIdOrdinals.map(i => schema(i).dataType)
  private val rowVersionType: DataType = schema(rowVersionOrdinal).dataType
  private val dataColumnTypes: Array[DataType] =
    dataOrdinals.map(i => schema(i).dataType)
  private val dataColumnOrderings: Array[Ordering[Any]] =
    dataColumnTypes.map(TypeUtils.getInterpretedOrdering)

  override def hasNext: Boolean = {
    if (pendingEmit != null) return true
    advance()
    pendingEmit != null
  }

  override def next(): InternalRow = {
    if (!hasNext) throw new NoSuchElementException
    val result = pendingEmit
    pendingEmit = null
    result
  }

  /**
   * Drives the carry-over removal state machine forward until `pendingEmit` holds a row to
   * return or the input is drained with nothing left to emit.
   *
   * Each tick handles one of four input states, resolved in this priority order:
   *
   *  (1) `pendingNext` non-null. A row re-queued by a previous tick (cross-group or stray
   *      delete). If it's a delete, becomes `pendingDelete` and we loop. Otherwise it's the
   *      next thing to emit and we're done.
   *
   *  (2) No `pendingDelete` and input empty. Nothing left to do -> exit.
   *
   *  (3) `pendingDelete` non-null and input empty. The buffered delete has no partner and
   *      must be emitted as-is (real change).
   *
   *  (4) No `pendingDelete` and input has more. Read the next row. If it's a delete, buffer
   *      it (needs the next row to classify). Otherwise emit directly.
   *
   *  (5) `pendingDelete` non-null and input has more. Read the next row and decide:
   *      - same (rowId, rowVersion) AND `_change_type == insert` AND all data columns equal
   *        -> CoW carry-over: drop both.
   *      - same group AND insert AND data differs: real UPDATE-as-delete-insert -> emit the
   *        delete now, re-queue the insert as `pendingNext` for emission next tick.
   *      - different group OR not an insert: the buffered delete was unpaired; emit it and
   *        re-queue the peeked row.
   *
   * Invariant: sort order is (rowId, rowVersion, `_change_type ASC`) so `delete` always
   * precedes `insert` within a group. This lets us hold at most one unmatched delete at a
   * time without multi-row lookahead.
   *
   * Termination: every iteration either (a) consumes one row from `input`, (b) resolves a
   * buffered state by clearing `pendingNext` or `pendingDelete`, or (c) sets `pendingEmit`
   * and exits. No path leaves all three buffers unchanged while `input` still has rows, so
   * no infinite loop is possible.
   */
  private def advance(): Unit = {
    while (pendingEmit == null) {
      if (pendingNext != null) {
        val row = pendingNext
        pendingNext = null
        if (getChangeType(row) == "delete") {
          pendingDelete = row
        } else {
          pendingEmit = row
          return
        }
      }
      if (pendingDelete == null && !input.hasNext) {
        return
      } else if (pendingDelete != null && !input.hasNext) {
        pendingEmit = pendingDelete
        pendingDelete = null
      } else if (pendingDelete == null && input.hasNext) {
        val row = input.next().copy()
        if (getChangeType(row) == "delete") {
          pendingDelete = row
        } else {
          pendingEmit = row
        }
      } else {
        // pendingDelete != null && input.hasNext
        val nextRow = input.next().copy()
        if (getChangeType(nextRow) == "insert" && sameGroup(pendingDelete, nextRow)) {
          if (dataColumnsEqual(pendingDelete, nextRow)) {
            // carry-over: drop both
            pendingDelete = null
          } else {
            pendingEmit = pendingDelete
            pendingDelete = null
            pendingNext = nextRow
          }
        } else {
          // no partner
          pendingEmit = pendingDelete
          pendingDelete = null
          pendingNext = nextRow
        }
      }
    }
  }

  /**
   * Checks whether two rows have the same rowId and rowVersion values.
   * rowId columns are always top-level in `plan.output`; connectors with nested identity
   * fields currently have to project them to the top level (see
   * [[org.apache.spark.sql.catalyst.analysis.ResolveChangelogTable]] for the restriction
   * point and the TODO that tracks re-adding nested support).
   */
  private def sameGroup(a: InternalRow, b: InternalRow): Boolean = {
    val n = rowIdOrdinals.length
    var k = 0
    while (k < n) {
      val i = rowIdOrdinals(k)
      val aNull = a.isNullAt(i)
      val bNull = b.isNullAt(i)
      if (aNull != bNull) return false
      if (!aNull && a.get(i, rowIdTypes(k)) != b.get(i, rowIdTypes(k))) return false
      k += 1
    }
    val aNull = a.isNullAt(rowVersionOrdinal)
    val bNull = b.isNullAt(rowVersionOrdinal)
    if (aNull != bNull) return false
    if (aNull) return true
    a.get(rowVersionOrdinal, rowVersionType) == b.get(rowVersionOrdinal, rowVersionType)
  }

  /** Checks whether two rows have identical data column values via type-aware orderings. */
  private def dataColumnsEqual(a: InternalRow, b: InternalRow): Boolean = {
    val n = dataOrdinals.length
    var k = 0
    while (k < n) {
      val i = dataOrdinals(k)
      val aNull = a.isNullAt(i)
      val bNull = b.isNullAt(i)
      if (aNull != bNull) return false
      if (!aNull) {
        val dt = dataColumnTypes(k)
        if (dataColumnOrderings(k).compare(a.get(i, dt), b.get(i, dt)) != 0) return false
      }
      k += 1
    }
    true
  }

  /**
   * Returns the _change_type string value from a row. Fails with a descriptive error if
   * the column is null; a null _change_type indicates a misbehaving connector.
   */
  private def getChangeType(row: InternalRow): String = {
    if (row.isNullAt(changeTypeOrdinal)) {
      throw new IllegalStateException(
        "Connector emitted a change row with null _change_type; " +
        "expected 'insert', 'delete', 'update_preimage', or 'update_postimage'.")
    }
    row.getUTF8String(changeTypeOrdinal).toString
  }
}
