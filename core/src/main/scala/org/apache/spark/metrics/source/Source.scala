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

package org.apache.spark.metrics.source

import com.codahale.metrics.MetricRegistry

import org.apache.spark.annotation.DeveloperApi

/**
 * :: DeveloperApi ::
 * The interface of metrics Source, this could be mixed into user code to get different metrics
 * offered by codahale metrics libray. To enable this metrics Souce, user should configure
 * the full classpath into metrics.properties and make class be accessed by all instances.
 *
 * Metrics Source will be registered into MetricsSystem to downstream the collected metrics to
 * metrics Sink.
 */
@DeveloperApi
trait Source {

  /**
   * The name of this metrics Source, name should be unique and will be prepended with app id and
   * executor id to distinguish.
   * @return name of this Source
   */
  def sourceName: String

  /**
   * A MetricRegistry in which all the collected metrics are registered.
   * @return a MetricRegistry object which will be registered in MetricsSystem for collection
   */
  def metricRegistry: MetricRegistry
}
