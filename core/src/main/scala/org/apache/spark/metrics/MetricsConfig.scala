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

package org.apache.spark.metrics

import java.util.Properties
import java.io.{File, FileInputStream, InputStream, IOException}

import scala.collection.mutable
import scala.util.matching.Regex

import org.apache.spark.Logging

private[spark] class MetricsConfig(val configFile: Option[String]) extends Logging {

  val DEFAULT_PREFIX = "*"
  val INSTANCE_REGEX = "^(\\*|[a-zA-Z]+)\\.(.+)".r
  val METRICS_CONF = "metrics.properties"

  val properties = new Properties()
  var propertyCategories: mutable.HashMap[String, Properties] = null

  private def setDefaultProperties(prop: Properties) {
    prop.setProperty("*.sink.servlet.class", "org.apache.spark.metrics.sink.MetricsServlet")
    prop.setProperty("*.sink.servlet.path", "/metrics/json")
    prop.setProperty("master.sink.servlet.path", "/metrics/master/json")
    prop.setProperty("applications.sink.servlet.path", "/metrics/applications/json")
  }

  def initialize() {
    //Add default properties in case there's no properties file
    setDefaultProperties(properties)

    // If spark.metrics.conf is not set, try to get file in class path
    var is: InputStream = null
    try {
      is = configFile match {
        case Some(f) => new FileInputStream(f)
        case None => getClass.getClassLoader.getResourceAsStream(METRICS_CONF)
      }

      if (is != null) {
        properties.load(is)
      }
    } catch {
      case e: Exception => logError("Error loading configure file", e)
    } finally {
      if (is != null) is.close()
    }

    propertyCategories = subProperties(properties, INSTANCE_REGEX)
    if (propertyCategories.contains(DEFAULT_PREFIX)) {
      import scala.collection.JavaConversions._

      val defaultProperty = propertyCategories(DEFAULT_PREFIX)
      for { (inst, prop) <- propertyCategories
            if (inst != DEFAULT_PREFIX)
            (k, v) <- defaultProperty
            if (prop.getProperty(k) == null) } {
        prop.setProperty(k, v)
      }
    }
  }

  def subProperties(prop: Properties, regex: Regex): mutable.HashMap[String, Properties] = {
    val subProperties = new mutable.HashMap[String, Properties]
    import scala.collection.JavaConversions._
    prop.foreach { kv =>
      if (regex.findPrefixOf(kv._1).isDefined) {
        val regex(prefix, suffix) = kv._1
        subProperties.getOrElseUpdate(prefix, new Properties).setProperty(suffix, kv._2)
      }
    }
    subProperties
  }

  def getInstance(inst: String): Properties = {
    propertyCategories.get(inst) match {
      case Some(s) => s
      case None => propertyCategories.getOrElse(DEFAULT_PREFIX, new Properties)
    }
  }
}

