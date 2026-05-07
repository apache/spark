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

/**
 * A custom metric. Data source can define supported custom metrics using this interface.
 * During query execution, Spark will collect the task metrics using {@link CustomTaskMetric}
 * and combine the metrics at the driver side. How to combine task metrics is defined by the
 * metric class with the same metric name.
 *
 * When Spark needs to aggregate task metrics, it will internally construct the instance of
 * custom metric class defined in data source by using reflection. Spark requires the class
 * implementing this interface to have a 0-arg constructor.
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
   * The initial value of this metric.
   */
  long initialValue = 0L;

  /**
   * Given an array of task metric values, returns aggregated final metric value.
   */
  String aggregateTaskMetrics(long[] taskMetrics);
}
