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

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricSet;
import com.codahale.metrics.Timer;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.MetricsSource;

import java.util.Map;

/**
 * Modeled off of YARN's NodeManagerMetrics.
 */
public class YarnShuffleServiceMetrics implements MetricsSource {

    private final MetricSet metricSet;

    public YarnShuffleServiceMetrics(MetricSet metricSet) {
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
        MetricsRecordBuilder metricsRecordBuilder = collector.addRecord("shuffleService");

        for (Map.Entry<String, Metric> entry : metricSet.getMetrics().entrySet()) {
            String name = entry.getKey();

            // The metric types used in ExternalShuffleBlockHandler.ShuffleMetrics
            if (entry.getValue() instanceof Timer) {
                Timer t = (Timer) entry.getValue();
                metricsRecordBuilder
                        .addCounter(new ShuffleServiceMetricsInfo(name + "_count", "Count of timer " + name),
                                t.getCount())
                        .addGauge(new ShuffleServiceMetricsInfo(name + "_rate15", "15 minute rate of timer " + name),
                                t.getFifteenMinuteRate())
                        .addGauge(new ShuffleServiceMetricsInfo(name + "_rate5", "5 minute rate of timer " + name),
                                t.getFiveMinuteRate())
                        .addGauge(new ShuffleServiceMetricsInfo(name + "_rate1", "1 minute rate of timer " + name),
                                t.getOneMinuteRate())
                        .addGauge(new ShuffleServiceMetricsInfo(name + "_rateMean", "Mean rate of timer " + name),
                                t.getMeanRate());
            } else if (entry.getValue() instanceof Meter) {
                Meter m = (Meter) entry.getValue();
                metricsRecordBuilder
                        .addCounter(new ShuffleServiceMetricsInfo(name + "_count", "Count of meter " + name),
                                m.getCount())
                        .addGauge(new ShuffleServiceMetricsInfo(name + "_rate15", "15 minute rate of meter " + name),
                                m.getFifteenMinuteRate())
                        .addGauge(new ShuffleServiceMetricsInfo(name + "_rate5", "5 minute rate of meter " + name),
                                m.getFiveMinuteRate())
                        .addGauge(new ShuffleServiceMetricsInfo(name + "_rate1", "1 minute rate of meter " + name),
                                m.getOneMinuteRate())
                        .addGauge(new ShuffleServiceMetricsInfo(name + "_rateMean", "Mean rate of meter " + name),
                                m.getMeanRate());
            } else if (entry.getValue() instanceof Gauge) {
                Gauge m = (Gauge) entry.getValue();
                Object gaugeValue = m.getValue();
                if (gaugeValue instanceof Integer) {
                    Integer intValue = (Integer) gaugeValue;
                    metricsRecordBuilder
                            .addGauge(new ShuffleServiceMetricsInfo(name, "Integer value of " +
                                    "gauge " + name), intValue.intValue());
                }
            }
        }
    }

    private static class ShuffleServiceMetricsInfo implements MetricsInfo {

        private final String name;
        private final String description;

        ShuffleServiceMetricsInfo(String name, String description) {
            this.name = name;
            this.description = description;
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public String description() {
            return description;
        }
    }
}
