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

import java.io.{FileInputStream, InputStream}
import java.util.Properties

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.matching.Regex

import org.apache.spark.util.Utils
import org.apache.spark.{Logging, SparkConf}

private[spark] class MetricsConfig(conf: SparkConf) extends Logging {

  private val DEFAULT_PREFIX = "*"
  private val INSTANCE_REGEX = "^(\\*|[a-zA-Z]+)\\.(.+)".r
  private val DEFAULT_METRICS_CONF_FILENAME = "metrics.properties"

  private[metrics] val properties = new Properties()
  private[metrics] var propertyCategories: mutable.HashMap[String, Properties] = null

  private def setDefaultProperties(prop: Properties) {
    prop.setProperty("*.sink.servlet.class", "org.apache.spark.metrics.sink.MetricsServlet")
    prop.setProperty("*.sink.servlet.path", "/metrics/json")
    prop.setProperty("master.sink.servlet.path", "/metrics/master/json")
    prop.setProperty("applications.sink.servlet.path", "/metrics/applications/json")
  }

  def initialize() {
    // Add default properties in case there's no properties file
    setDefaultProperties(properties)

    loadPropertiesFromFile(conf.getOption("spark.metrics.conf"))

    // Also look for the properties in provided Spark configuration
    val prefix = "spark.metrics.conf."
    conf.getAll.foreach {
      case (k, v) if k.startsWith(prefix) =>
        properties.setProperty(k.substring(prefix.length()), v)
      case _ =>
    }

    propertyCategories = subProperties(properties, INSTANCE_REGEX)
    if (propertyCategories.contains(DEFAULT_PREFIX)) {
      val defaultProperty = propertyCategories(DEFAULT_PREFIX).asScala
      for((inst, prop) <- propertyCategories if (inst != DEFAULT_PREFIX);
          (k, v) <- defaultProperty if (prop.get(k) == null)) {
        prop.put(k, v)
      }
    }
  }

  def subProperties(prop: Properties, regex: Regex): mutable.HashMap[String, Properties] = {
    val subProperties = new mutable.HashMap[String, Properties]
    prop.asScala.foreach { kv =>
      if (regex.findPrefixOf(kv._1.toString).isDefined) {
        val regex(prefix, suffix) = kv._1.toString
        subProperties.getOrElseUpdate(prefix, new Properties).setProperty(suffix, kv._2.toString)
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

  /**
   * Loads configuration from a config file. If no config file is provided, try to get file
   * in class path.
   */
  private[this] def loadPropertiesFromFile(path: Option[String]): Unit = {
    var is: InputStream = null
    try {
      is = path match {
        case Some(f) => new FileInputStream(f)
        case None => Utils.getSparkClassLoader.getResourceAsStream(DEFAULT_METRICS_CONF_FILENAME)
      }

      if (is != null) {
        properties.load(is)
      }
    } catch {
      case e: Exception =>
        val file = path.getOrElse(DEFAULT_METRICS_CONF_FILENAME)
        logError(s"Error loading configuration file $file", e)
    } finally {
      if (is != null) {
        is.close()
      }
    }
  }

}
