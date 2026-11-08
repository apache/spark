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
 *   The one exception is the end of the task, when another thread may still be consuming the
 *   join. Spark then calls {@code close()} on the open cursor and then {@link #close()} on this
 *   reader, from the thread that ends the task. Exactly these may overlap or follow that close:
 *   <ul>
 *     <li>One call from the consuming thread that was in progress, or that Spark checked just
 *     before the close. Implementations must tolerate the close during or before such a call,
 *     for example by cancelling or failing it. Spark does not wait for it.</li>
 *     <li>Spark's read of the value row that the cursor returned, and of its buffers, which may
 *     overlap the cursor's {@code close()}. Spark waits for this read to end before it closes
 *     this reader, so this reader may release the memory behind its rows in
 *     {@code close()}.</li>
 *     <li>{@link #currentMetricsValues()}, on this reader and on the task's other readers, while
 *     a call is in progress on the consuming thread. That method must be safe to call
 *     concurrently and must not wait for the call in progress.</li>
 *     <li>The close of the cursor that a request in progress returns, which comes after this
 *     reader's close, as {@link #lookupBatch} describes.</li>
 *     <li>For a reader that a {@link ExternalLookupReaderFactory#createReader()} call in progress
 *     returns, a read of its metrics and its close, with no other call on it.</li>
 *   </ul>
 *   Apart from the closes listed above, once the consuming thread has seen the close, Spark makes
 *   no further call on this reader or its cursors. Spark closes each of them exactly once.</li>
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
   * {@link ExternalLookup#maxParameterRowsPerRequest()} rows, none of them null. Field {@code i}
   * of each row is the value for the {@code i}-th selected predicate, in Spark's internal
   * representation: an integral or BOOLEAN value as its Java primitive; DECIMAL as a
   * {@code Decimal} at the column's precision and scale; DATE as days since 1970-01-01 in the
   * proleptic Gregorian calendar; TIMESTAMP as microseconds since 1970-01-01 00:00:00 UTC;
   * TIMESTAMP_NTZ as microseconds of the local date and time, with no time zone; BINARY as its
   * bytes; and STRING as UTF-8 bytes, compared byte by byte. A conversion that loses precision,
   * such as to milliseconds, can lose matches.
   * No two rows are equal; a null {@code <=>} parameter equals a null. Only {@code <=>} parameters
   * may be null at the top level.
   * <p>
   * The cursor must be non-null. Each row's results must be complete, with no cap and no row lost
   * to paging, and depend only on the row; Spark may reuse them for equal rows. A stored row that
   * matches several parameter rows is returned once with each of their ordinals, and equal stored
   * rows are each returned. A source that matches more loosely than Spark may also return pairs
   * of a row and a parameter row that do not match; Spark drops them. Spark checks the selected
   * predicates on every returned row, but cannot find a missing one. The input is read-only and
   * must not be kept after the cursor closes.
   * <p>
   * If the task ends while this method is in progress, Spark closes this reader first, and then
   * closes the cursor that the call returns. Closing that cursor must work after the reader is
   * closed. Otherwise, Spark closes every cursor it obtains, including one it has read to the
   * end, before the next call and before closing this reader.
   */
  ExternalLookupResultCursor lookupBatch(InternalRow[] uniqueParameters) throws IOException;

  /** Returns cumulative values of the metrics declared by {@link Scan#supportedCustomMetrics()}. */
  default CustomTaskMetric[] currentMetricsValues() {
    return new CustomTaskMetric[0];
  }
}
