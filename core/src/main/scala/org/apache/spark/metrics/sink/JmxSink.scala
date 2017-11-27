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

import java.util.Properties

import com.codahale.metrics.{JmxReporter, MetricRegistry}

import org.apache.spark.SecurityManager

/**
 * A metrics [[Sink]] which will output registered metrics with JMX format, user can use
 * jconsole and others to attach to the Spark process and get the metrics report.
 *
 * @param property [[JmxSink]] specific properties
 * @param registry A [[MetricRegistry]] can this sink to register
 * @param securityMgr A [[SecurityManager]] to check security related stuffs.
 */
private[spark] class JmxSink(
    property: Properties,
    registry: MetricRegistry,
    securityMgr: SecurityManager) extends Sink(property, registry) {

  private val reporter: JmxReporter = JmxReporter.forRegistry(registry).build()

  override def start() {
    reporter.start()
  }

  override def stop() {
    reporter.stop()
  }

  override def report() { }

}
