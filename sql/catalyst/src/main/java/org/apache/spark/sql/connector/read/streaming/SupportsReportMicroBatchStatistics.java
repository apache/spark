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

package org.apache.spark.sql.connector.read.streaming;

import org.apache.spark.annotation.Evolving;
import org.apache.spark.sql.connector.read.Statistics;
import org.apache.spark.sql.connector.read.SupportsReportStatistics;

/**
 * A mix-in interface for {@link MicroBatchStream}. Data sources can implement this interface to
 * report the statistics of the data that one micro-batch reads.
 * <p>
 * Spark builds the {@link org.apache.spark.sql.connector.read.Scan} of a streaming query once, when
 * the query starts, so {@link SupportsReportStatistics#estimateStatistics()} can only describe the
 * whole source. When the stream implements this interface, Spark plans each micro-batch with the
 * statistics of its own offset range instead, for example to decide whether to broadcast it in a
 * join. Real-time and continuous queries do not call this interface and use the statistics of the
 * scan.
 *
 * @since 4.4.0
 */
@Evolving
public interface SupportsReportMicroBatchStatistics extends MicroBatchStream {

  /**
   * Returns the estimated statistics of the data that
   * {@link #planInputPartitions(Offset, Offset)} reads for the same {@code start} and {@code end},
   * the offset range of one micro-batch.
   * <p>
   * Spark calls this method on the driver when it plans the micro-batch, and can call it more than
   * once for the same range, before or after {@link #planInputPartitions(Offset, Offset)}. An
   * implementation that does expensive work for it, for example to list files, should cache it.
   * <p>
   * When the statistics of the range are unknown, return {@code null} or statistics with no
   * values. Spark then uses the statistics of the scan, as if the stream did not implement this
   * interface.
   */
  Statistics estimateStatistics(Offset start, Offset end);
}
