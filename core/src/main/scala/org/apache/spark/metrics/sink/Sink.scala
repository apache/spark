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

package org.apache.spark.metrics.sink

import java.util.{Locale, Properties}
import java.util.concurrent.TimeUnit

import com.codahale.metrics.MetricRegistry

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.metrics.MetricsSystem

/**
 * :: DeveloperApi ::
 * The abstract class of metrics Sink, by achiving the methods and registered through metrics
 * .properties user could register customer metrics Sink into MetricsSystem.
 *
 * @param properties Properties related this specific Sink, properties are read from
 *                   configuration file, user could define their own configurations and get
 *                   from this parameter.
 * @param metricRegistry The MetricRegistry for you to dump the collected metrics.
 */
@DeveloperApi
abstract class Sink(properties: Properties, metricRegistry: MetricRegistry) {

  protected val pollPeriod = properties.getProperty("period", "10").toInt

  protected val pollUnit = Option(properties.getProperty("unit"))
    .map(s => TimeUnit.valueOf(s.toUpperCase(Locale.ROOT)))
    .getOrElse(TimeUnit.SECONDS)

  MetricsSystem.checkMinimalPollingPeriod(pollUnit, pollPeriod)

  /**
   * Start this metrics Sink, this will be called by MetricsSystem. If this [[Sink]] fails to
   * start, Metrics system will unregister and remove it.
   */
  def start(): Unit

  /**
   * Stop this metrics Sink, this will be called by MetricsSystem
   */
  def stop(): Unit

  /**
   * Report the current registered metrics.
   */
  def report(): Unit
}
