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

package org.apache.spark.sql.connector.metric;

import org.apache.spark.annotation.Evolving;
import org.apache.spark.sql.connector.read.PartitionReader;

/**
 * A custom task metric. This is a logical representation of a metric reported by data sources
 * at the executor side. During query execution, Spark will collect the task metrics per partition
 * by {@link PartitionReader} and update internal metrics based on collected metric values.
 * For streaming query, Spark will collect and combine metrics for a final result per micro batch.
 * <p>
 * The metrics will be gathered during query execution back to the driver and then combined. How
 * the task metrics are combined is defined by corresponding {@link CustomMetric} with same metric
 * name. The final result will be shown up in the data source scan operator in Spark UI.
 * <p>
 * There are a few special metric names: "bytesWritten" and "recordsWritten". If the data source
 * defines custom metrics with the same names, the metric values will also be updated to
 * corresponding task metrics.
 *
 * @since 3.2.0
 */
@Evolving
public interface CustomTaskMetric {
  /**
   * Returns the name of custom task metric.
   */
  String name();

  /**
   * Returns the long value of custom task metric.
   */
  long value();

  /**
   * Merges this metric with another metric of the same name, returning a new
   * {@link CustomTaskMetric} that represents the combined value. This is called when a task reads
   * multiple partitions concurrently (e.g., k-way merge coalescing) to produce a single
   * task-level value before reporting to the driver.
   *
   * <p>The default implementation returns a new metric whose value is the sum of the two values,
   * which is correct for count-type metrics. Data sources with non-additive metrics (e.g., max,
   * average, last-value) must override this method to provide correct merge semantics.
   *
   * @param other another metric with the same name to merge with
   * @return a new metric representing the merged value
   * @since 4.2.0
   */
  default CustomTaskMetric mergeWith(CustomTaskMetric other) {
    final String metricName = this.name();
    if (!metricName.equals(other.name())) {
      throw new IllegalArgumentException(
        "Cannot merge metrics with different names: '" + metricName + "' and '" + other.name() +
            "'");
    }
    final long mergedValue = this.value() + other.value();
    return new CustomTaskMetric() {
      @Override public String name() { return metricName; }
      @Override public long value() { return mergedValue; }
    };
  }
}
