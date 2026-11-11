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
 *   The one exception is the end of the task: if another thread is still consuming the join,
 *   Spark may call {@link #close()} on the reader, and {@code close()} on its open cursor, while
 *   one call from that thread is in progress. Spark starts no new call after the close, so
 *   implementations must tolerate {@code close()} during an in-progress call, for example by
 *   cancelling or failing that call.</li>
 *   <li>Requests are not snapshot boundaries: results may mix source versions.</li>
 *   <li>Retried or speculative tasks may repeat a request; duplicates must be tolerated.</li>
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
   * representation.
   * Rows are distinct under Spark SQL equality (NaN equals NaN, {@code -0.0} equals {@code 0.0},
   * strings compare by collation). Only {@code <=>} parameters may be null at the top level.
   * <p>
   * The cursor must be non-null. Each row's results must be complete and depend only on the row;
   * Spark may reuse them for equal rows. The input is read-only and must not be kept after the
   * cursor closes. Spark closes every cursor it obtains, including one it has read to the end,
   * before the next call and before closing this reader.
   */
  ExternalLookupResultCursor lookupBatch(InternalRow[] uniqueParameters) throws IOException;

  /** Returns cumulative values of the metrics declared by {@link Scan#supportedCustomMetrics()}. */
  default CustomTaskMetric[] currentMetricsValues() {
    return new CustomTaskMetric[0];
  }
}
