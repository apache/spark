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
import org.apache.spark.sql.connector.metric.CustomTaskMetric;

/**
 * A partition reader returned by {@link PartitionReaderFactory#createReader(InputPartition)} or
 * {@link PartitionReaderFactory#createColumnarReader(InputPartition)}. It's responsible for
 * outputting data for a RDD partition.
 * <p>
 * Note that, Currently the type `T` can only be {@link org.apache.spark.sql.catalyst.InternalRow}
 * for normal data sources, or {@link org.apache.spark.sql.vectorized.ColumnarBatch} for columnar
 * data sources(whose {@link PartitionReaderFactory#supportColumnarReads(InputPartition)}
 * returns true).
 *
 * @since 3.0.0
 */
@Evolving
public interface PartitionReader<T> extends Closeable {

  /**
   * Proceed to next record, returns false if there is no more records.
   *
   * @throws IOException if failure happens during disk/network IO like reading files.
   */
  boolean next() throws IOException;

  /**
   * Return the current record. This method should return same value until `next` is called.
   */
  T get();

  /**
   * Returns an array of custom task metrics. By default it returns empty array. Note that it is
   * not recommended to put heavy logic in this method as it may affect reading performance.
   */
  default CustomTaskMetric[] currentMetricsValues() {
    CustomTaskMetric[] NO_METRICS = {};
    return NO_METRICS;
  }
}
