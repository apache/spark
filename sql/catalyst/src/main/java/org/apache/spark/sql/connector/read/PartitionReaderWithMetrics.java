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

import org.apache.spark.annotation.Evolving;
import org.apache.spark.sql.connector.CustomMetric;

/**
 * A mix in interface for {@link PartitionReader}. Partition reader can this interface to
 * report custom metrics to Spark.
 *
 * @since 3.2.0
 */
@Evolving
public interface PartitionReaderWithMetrics<T> extends PartitionReader<T> {

    /**
     * Returns an array of custom metrics. By default it returns empty array.
     */
    default CustomMetric[] getCustomMetrics() {
        CustomMetric[] NO_METRICS = {};
        return NO_METRICS;
    }
}
