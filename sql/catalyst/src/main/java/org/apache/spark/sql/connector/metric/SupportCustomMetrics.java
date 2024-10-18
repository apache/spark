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

public interface SupportCustomMetrics {

  /**
   * Returns an array of supported custom metrics with name and description.
   * By default, it returns empty array.
   */
  default CustomMetric[] supportedCustomMetrics() {
    return new CustomMetric[]{};
  }

  /**
   * Returns an array of custom metrics which are collected with values at the driver side only.
   * Note that these metrics must be included in the supported custom metrics reported by
   * `supportedCustomMetrics`.
   *
   * @since 3.4.0
   */
  default CustomTaskMetric[] reportDriverMetrics() {
    return new CustomTaskMetric[]{};
  }
}
