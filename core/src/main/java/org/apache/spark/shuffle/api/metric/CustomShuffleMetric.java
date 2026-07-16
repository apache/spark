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

package org.apache.spark.shuffle.api.metric;

import org.apache.spark.annotation.Private;

/**
 * :: Private ::
 * A custom shuffle metric.
 *
 * @since 4.3.0
 */
@Private
public interface CustomShuffleMetric {

  /**
   * The name of this metric. Must match the name reported by the corresponding
   * {@link CustomShuffleTaskMetric} so per-task values can be matched to this declaration.
   */
  String name();

  /**
   * A human-readable description of this metric.
   */
  String description();

  /**
   * Defines how the per-task values are represented as a string in the UI. Per-task values are
   * always aggregated as a plain sum across tasks. Must be one of:
   * {@code "sum"} (raw count), {@code "size"} (bytes), {@code "timing"} (milliseconds), or
   * {@code "nsTiming"} (nanoseconds). Any other value is rejected.
   */
  String metricType();
}
