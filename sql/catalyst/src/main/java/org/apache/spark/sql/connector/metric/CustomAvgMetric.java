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

import java.text.DecimalFormat;

/**
 * Built-in `CustomMetric` that computes average of metric values. Note that please extend this
 * class and override `name` and `description` to create your custom metric for real usage.
 *
 * @since 3.2.0
 */
@Evolving
public abstract class CustomAvgMetric implements CustomMetric {
  @Override
  public String aggregateTaskMetrics(long[] taskMetrics) {
    if (taskMetrics.length > 0) {
      long sum = 0L;
      for (long taskMetric : taskMetrics) {
        sum += taskMetric;
      }
      double average = ((double) sum) / taskMetrics.length;
      return new DecimalFormat("#0.000").format(average);
    } else {
      return "0";
    }
  }
}
