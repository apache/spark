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

package org.apache.spark.network.yarn;

import java.util.Map;

import com.codahale.metrics.*;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.MetricsSource;

/**
 * Forward {@link org.apache.spark.network.shuffle.ExternalBlockHandler.ShuffleMetrics}
 * to hadoop metrics system.
 * NodeManager by default exposes JMX endpoint where can be collected.
 */
class YarnShuffleServiceMetrics implements MetricsSource {

  private final String metricsNamespace;
  private final MetricSet metricSet;

  YarnShuffleServiceMetrics(String metricsNamespace, MetricSet metricSet) {
    this.metricsNamespace = metricsNamespace;
    this.metricSet = metricSet;
  }

  /**
   * Get metrics from the source
   *
   * @param collector to contain the resulting metrics snapshot
   * @param all       if true, return all metrics even if unchanged.
   */
  @Override
  public void getMetrics(MetricsCollector collector, boolean all) {
    MetricsRecordBuilder metricsRecordBuilder = collector.addRecord(metricsNamespace);

    for (Map.Entry<String, Metric> entry : metricSet.getMetrics().entrySet()) {
      collectMetric(metricsRecordBuilder, entry.getKey(), entry.getValue());
    }
  }

  /**
   * The metric types used in
   * {@link org.apache.spark.network.shuffle.ExternalBlockHandler.ShuffleMetrics}.
   * Visible for testing.
   */
  public static void collectMetric(
    MetricsRecordBuilder metricsRecordBuilder, String name, Metric metric) {

    if (metric instanceof Timer t) {
      // Timer records both the operations count and delay
      // Snapshot inside the Timer provides the information for the operation delay
      Snapshot snapshot = t.getSnapshot();
      metricsRecordBuilder
        .addCounter(new ShuffleServiceMetricsInfo(name + "_count", "Count of timer " + name),
          t.getCount())
        .addGauge(
          new ShuffleServiceMetricsInfo(name + "_rate15", "15 minute rate of timer " + name),
          t.getFifteenMinuteRate())
        .addGauge(
          new ShuffleServiceMetricsInfo(name + "_rate5", "5 minute rate of timer " + name),
          t.getFiveMinuteRate())
        .addGauge(
          new ShuffleServiceMetricsInfo(name + "_rate1", "1 minute rate of timer " + name),
          t.getOneMinuteRate())
        .addGauge(new ShuffleServiceMetricsInfo(name + "_rateMean", "Mean rate of timer " + name),
          t.getMeanRate())
        .addGauge(
          getShuffleServiceMetricsInfoForGenericValue(name, "max"), snapshot.getMax())
        .addGauge(
          getShuffleServiceMetricsInfoForGenericValue(name, "min"), snapshot.getMin())
        .addGauge(
          getShuffleServiceMetricsInfoForGenericValue(name, "mean"), snapshot.getMean())
        .addGauge(
          getShuffleServiceMetricsInfoForGenericValue(name, "stdDev"), snapshot.getStdDev());
      for (int percentileThousands : new int[] { 10, 50, 250, 500, 750, 950, 980, 990, 999 }) {
        String percentileStr = switch (percentileThousands) {
          case 10 -> "1stPercentile";
          case 999 -> "999thPercentile";
          default -> String.format("%dthPercentile", percentileThousands / 10);
        };
        metricsRecordBuilder.addGauge(
          getShuffleServiceMetricsInfoForGenericValue(name, percentileStr),
          snapshot.getValue(percentileThousands / 1000.0));
      }
    } else if (metric instanceof Meter m) {
      metricsRecordBuilder
        .addCounter(new ShuffleServiceMetricsInfo(name + "_count", "Count of meter " + name),
          m.getCount())
        .addGauge(
          new ShuffleServiceMetricsInfo(name + "_rate15", "15 minute rate of meter " + name),
          m.getFifteenMinuteRate())
        .addGauge(
          new ShuffleServiceMetricsInfo(name + "_rate5", "5 minute rate of meter " + name),
          m.getFiveMinuteRate())
        .addGauge(
          new ShuffleServiceMetricsInfo(name + "_rate1", "1 minute rate of meter " + name),
          m.getOneMinuteRate())
        .addGauge(new ShuffleServiceMetricsInfo(name + "_rateMean", "Mean rate of meter " + name),
          m.getMeanRate());
    } else if (metric instanceof Gauge gauge) {
      final Object gaugeValue = gauge.getValue();
      if (gaugeValue instanceof Integer integer) {
        metricsRecordBuilder.addGauge(getShuffleServiceMetricsInfoForGauge(name), integer);
      } else if (gaugeValue instanceof Long longVal) {
        metricsRecordBuilder.addGauge(getShuffleServiceMetricsInfoForGauge(name), longVal);
      } else if (gaugeValue instanceof Float floatVal) {
        metricsRecordBuilder.addGauge(getShuffleServiceMetricsInfoForGauge(name), floatVal);
      } else if (gaugeValue instanceof Double doubleVal) {
        metricsRecordBuilder.addGauge(getShuffleServiceMetricsInfoForGauge(name), doubleVal);
      } else {
        throw new IllegalStateException(
                "Not supported class type of metric[" + name + "] for value " + gaugeValue);
      }
    } else if (metric instanceof Counter c) {
      long counterValue = c.getCount();
      metricsRecordBuilder.addGauge(getShuffleServiceMetricsInfoForCounter(name), counterValue);
    }
  }

  private static MetricsInfo getShuffleServiceMetricsInfoForGauge(String name) {
    return new ShuffleServiceMetricsInfo(name, "Value of gauge " + name);
  }

  private static ShuffleServiceMetricsInfo getShuffleServiceMetricsInfoForCounter(String name) {
    return new ShuffleServiceMetricsInfo(name, "Value of counter " + name);
  }

  private static ShuffleServiceMetricsInfo getShuffleServiceMetricsInfoForGenericValue(
      String baseName, String valueName) {
    return new ShuffleServiceMetricsInfo(
      baseName + "_" + valueName,
      valueName + " value of " + baseName);
  }

  private record ShuffleServiceMetricsInfo(String name, String description) implements MetricsInfo {
  }
}
