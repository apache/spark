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

import java.lang.management.ManagementFactory
import java.rmi.registry.LocateRegistry
import java.util.Properties

import com.codahale.metrics.{Metric, MetricFilter, MetricRegistry}
import com.codahale.metrics.jmx.JmxReporter
import javax.management.MBeanServer
import javax.management.remote.{JMXConnectorServer, JMXConnectorServerFactory, JMXServiceURL}

import org.apache.spark.SecurityManager

private[spark] class JmxSink(val property: Properties, val registry: MetricRegistry,
    securityMgr: SecurityManager) extends Sink {
  val JMX_KEY_HOST = "host"
  val JMX_KEY_PORT = "port"
  val JMX_KEY_REGEX = "regex"

  def propertyToOption(prop: String): Option[String] = Option(property.getProperty(prop))

  if (propertyToOption(JMX_KEY_HOST).isEmpty) {
    throw new Exception("Jmx sink requires 'host' property.")
  }

  if (propertyToOption(JMX_KEY_PORT).isEmpty) {
    throw new Exception("Jmx sink requires 'port' property.")
  }

  val host = propertyToOption(JMX_KEY_HOST).get
  val port = propertyToOption(JMX_KEY_PORT).get.toInt

  val filter = propertyToOption(JMX_KEY_REGEX) match {
    case Some(pattern) => new MetricFilter() {
      override def matches(name: String, metric: Metric): Boolean = {
        pattern.r.findFirstMatchIn(name).isDefined
      }
    }
    case None => MetricFilter.ALL
  }

  LocateRegistry.createRegistry(port)
  val serviceUrl: String = s"service:jmx:rmi://${host}:${port}/jndi/rmi://${host}:${port}/jmxrmi"
  val url = new JMXServiceURL(serviceUrl)
  val mBeanServer: MBeanServer = ManagementFactory.getPlatformMBeanServer
  val connector: JMXConnectorServer = JMXConnectorServerFactory
    .newJMXConnectorServer(url, null, mBeanServer)
  val reporter: JmxReporter = JmxReporter.forRegistry(registry)
    .registerWith(mBeanServer)
    .filter(filter)
    .build

  override def start(): Unit = {
    connector.start()
    reporter.start()
  }

  override def stop(): Unit = {
    reporter.stop()
    connector.stop()
  }

  override def report(): Unit = { }

}
