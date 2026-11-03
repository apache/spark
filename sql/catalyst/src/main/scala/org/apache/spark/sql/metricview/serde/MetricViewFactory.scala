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

package org.apache.spark.sql.metricview.serde

import scala.util.control.NonFatal

private[sql] object MetricViewFactory {
  private case class YAMLVersionHandler(
      deserialize: String => MetricView,
      serialize: MetricView => String)

  private val versionHandlers = Seq(
    "0.1" -> YAMLVersionHandler(
      yamlContent => MetricViewYAMLDeserializerV01.parseYaml(yamlContent).toCanonical,
      metricView => MetricViewYAMLSerializerV01.toYaml(MetricViewV01.fromCanonical(metricView))))

  private def invalidVersionMessage(version: String): String = {
    s"Invalid YAML version: $version. " +
      s"Supported YAML versions: ${versionHandlers.map(_._1).mkString(", ")}"
  }

  private def versionHandler(version: String): YAMLVersionHandler = {
    versionHandlers.find(_._1 == version).map(_._2).getOrElse {
      throw MetricViewValidationException(invalidVersionMessage(version))
    }
  }

  def fromYAML(yamlContent: String): MetricView = {
    try {
      val yamlVersion =
        YamlMapperProviderV01.mapperWithAllFields.readValue(yamlContent, classOf[YAMLVersion])
      versionHandler(yamlVersion.version).deserialize(yamlContent)
    } catch {
      case e: MetricViewSerdeException =>
        throw e
      case NonFatal(e) =>
        throw MetricViewYAMLParsingException(
          s"Failed to parse YAML: ${e.getMessage}",
          Some(e)
        )
    }
  }

  def toYAML(metricView: MetricView): String = {
    try {
      versionHandler(metricView.version).serialize(metricView)
    } catch {
      case e: MetricViewSerdeException =>
        throw e
      case NonFatal(e) =>
        throw MetricViewYAMLParsingException(
          s"Failed to serialize to YAML: ${e.getMessage}",
          Some(e)
        )
    }
  }
}
