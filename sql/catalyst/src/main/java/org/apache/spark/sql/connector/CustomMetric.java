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

package org.apache.spark.sql.connector;

import org.apache.spark.annotation.Evolving;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.Scan;

/**
 * A custom metric. This is a logical representation of a metric reported by data sources during
 * read path. Data sources can report supported metric list by {@link Scan} to Spark in query
 * planning. During query execution, Spark will collect the metrics per partition by
 * {@link PartitionReader} and combine metrics from partitions to the final result. How Spark
 * combines metrics depends on the metric type. For streaming query, Spark will collect and combine
 * metrics for a final result per micro batch.
 *
 * The metrics will be gathered during query execution back to the driver and then combined. The
 * final result will be shown up in the physical operator in Spark UI.
 *
 * @since 3.2.0
 */
@Evolving
public interface CustomMetric {
    /**
     * Returns the name of custom metric.
     */
    String name();

    /**
     * Returns the description of custom metric.
     */
    String description();

    /**
     * Supported metric type. The metric types must be supported by Spark SQL internal metrics.
     * SUM: Spark sums up metrics from partitions as the final result.
     */
    enum MetricType {
      SUM
    }

    /**
     * Returns the type of custom metric.
     */
    MetricType type();
}
