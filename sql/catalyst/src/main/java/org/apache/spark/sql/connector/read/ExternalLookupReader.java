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

package org.apache.spark.sql.connector.read;

import java.io.Closeable;
import java.io.IOException;

import org.apache.spark.annotation.Evolving;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.metric.CustomTaskMetric;

/**
 * A task-local client that executes lookup requests.
 * <ul>
 *   <li>Calls to a reader and its cursors are sequential, but may come from different threads.
 *   Apart from the end of the task, Spark closes each cursor that it obtains, including one that
 *   it has read to its end, before its next request and before it closes the reader. It may read
 *   {@link #currentMetricsValues()} while a cursor is open.</li>
 *   <li>At the end of the task, if another thread is still consuming the join, Spark closes the
 *   reader once, without waiting for that thread, and first closes the reader's open cursor if it
 *   finds one. It reads {@link #currentMetricsValues()} before it closes the reader. That thread
 *   makes no call once it has seen the close, but the one call that has already passed its check
 *   for the close may still run during or after {@code close()}: a request may reach a closed
 *   reader, and Spark may close that thread's cursor before the reader, after it, or not at all.
 *   Spark may also still read the row that the cursor's {@code get()} returned last while or after
 *   the cursor and the reader close, as it copies that row after its last check for the close. So
 *   closing a reader must release every cursor of it that is still open and tolerate one that Spark
 *   has closed or closes later; a cursor's {@code close()} must work after its reader is closed; a
 *   request on a closed reader must fail; a call in progress must tolerate the close, for example
 *   by being cancelled or by failing; and a connector that frees the memory of its rows in
 *   {@code close()} must keep the row that {@code get()} returned last readable. Spark closes each
 *   reader and cursor at most once.</li>
 *   <li>Requests are not snapshot boundaries: results may mix source versions.</li>
 *   <li>Retried or speculative tasks may send the same request again, so a source must
 *   tolerate repeated requests. Within one request, matches are returned as
 *   {@link #lookupBatch} describes.</li>
 *   <li>Implementations should respond to thread interruption if their client supports it.</li>
 * </ul>
 *
 * @since 4.4.0
 */
@Evolving
public interface ExternalLookupReader extends Closeable {

  /**
   * Looks up {@code uniqueParameters} and returns a cursor over all matches. For example:
   * <pre>{@code
   *   uniqueParameters: [(1), (2), (3)]
   *   cursor yields:    (0, a), (0, b), (2, c)    // (inputOrdinal, value row)
   *   => row 0 has two matches, row 1 is a miss, row 2 has one match
   * }</pre>
   * Spark guarantees that {@code uniqueParameters} is non-null and holds between 1 and
   * {@link ExternalLookup#maxParameterRowsPerRequest()} rows, none of them null. When a selected
   * predicate is a range comparison whose parameter reads a stream column, each request holds
   * exactly one parameter row. Field {@code i} of each row is the value for the {@code i}-th
   * selected predicate, in Spark's internal representation: an integral or BOOLEAN value as its
   * Java primitive; DECIMAL as a {@code Decimal} at the column's precision and scale; DATE as days
   * since 1970-01-01 in the proleptic Gregorian calendar; TIMESTAMP as microseconds since
   * 1970-01-01 00:00:00 UTC; TIMESTAMP_NTZ as microseconds since 1970-01-01 00:00:00 of the local
   * date and time, with no time zone; BINARY as its bytes; and STRING as UTF-8 bytes, compared
   * byte by byte. A conversion that loses precision, such as to milliseconds, can lose matches.
   * No two rows are equal; a null {@code <=>} parameter equals a null. Only {@code <=>} parameters
   * may be null at the top level, and a null one matches the rows whose column is null.
   * <p>
   * The cursor must be non-null. Each row's results must be complete: every stored row that
   * passes the filters this source reports as pushed and for which all selected predicates hold,
   * under Spark semantics, with no cap and no row lost to paging. They may omit a row that fails a
   * pushed filter. They depend only on the row; Spark may reuse them for equal rows. A stored row
   * that matches several parameter rows is returned once with each of their ordinals, and equal
   * stored rows are each returned. A source that matches more loosely than Spark may also return
   * pairs of a row and a parameter row that do not match; Spark drops them. Spark checks the
   * selected predicates on every returned row, but cannot find a missing one. The input is
   * read-only and must not be kept after the cursor closes.
   * <p>
   * When Spark closes the returned cursor, also at the end of the task, is stated in the
   * documentation of this interface.
   */
  ExternalLookupResultCursor lookupBatch(InternalRow[] uniqueParameters) throws IOException;

  /** Returns cumulative values of the metrics declared by {@link Scan#supportedCustomMetrics()}. */
  default CustomTaskMetric[] currentMetricsValues() {
    return new CustomTaskMetric[0];
  }
}
