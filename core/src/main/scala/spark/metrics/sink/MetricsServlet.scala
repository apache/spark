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

package spark.metrics.sink

import com.codahale.metrics.MetricRegistry
import com.codahale.metrics.json.MetricsModule

import com.fasterxml.jackson.databind.ObjectMapper

import java.util.Properties
import java.util.concurrent.TimeUnit
import javax.servlet.http.HttpServletRequest

import org.eclipse.jetty.server.Handler

import spark.ui.JettyUtils

class MetricsServlet(val property: Properties, val registry: MetricRegistry) extends Sink {
  val SERVLET_KEY_URI = "uri"
  val SERVLET_KEY_SAMPLE = "sample"

  val servletURI = property.getProperty(SERVLET_KEY_URI)

  val servletShowSample = property.getProperty(SERVLET_KEY_SAMPLE).toBoolean

  val mapper = new ObjectMapper().registerModule(
    new MetricsModule(TimeUnit.SECONDS, TimeUnit.MILLISECONDS, servletShowSample))

  def getHandlers = Array[(String, Handler)](
    (servletURI, JettyUtils.createHandler(request => getMetricsSnapshot(request), "text/json"))
  )

  def getMetricsSnapshot(request: HttpServletRequest): String = {
    mapper.writeValueAsString(registry)
  }

  override def start() { }

  override def stop() { }
}
