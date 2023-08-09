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

import java.util.Map;
import java.util.Optional;

import org.apache.spark.annotation.Evolving;

/**
 * A mix-in interface for {@link SparkDataStream} streaming sources to signal that they can report
 * metrics.
 *
 * @since 3.2.0
 */
@Evolving
public interface ReportsSourceMetrics extends SparkDataStream {
    /**
     * Returns the metrics reported by the streaming source with respect to
     * the latest consumed offset.
     *
     * @param latestConsumedOffset the end offset (exclusive) of the latest triggered batch.
     */
    Map<String, String> metrics(Optional<Offset> latestConsumedOffset);
}
