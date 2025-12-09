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

import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.{YAMLFactory, YAMLGenerator}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.yaml.snakeyaml.DumperOptions

private[sql] object Constants {
  final val MAXIMUM_PROPERTY_SIZE: Int = 1 * 1024
  final val COLUMN_TYPE_PROPERTY_KEY = "metric_view.type"
  final val COLUMN_EXPR_PROPERTY_KEY = "metric_view.expr"
}

private[sql] trait YamlMapperProviderBase {
  def mapperWithAllFields: ObjectMapper = {
    val options = new DumperOptions()
    // Set flow style to BLOCK for better readability (each key-value pair on separate lines)
    options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK)
    // Set indentation to 2 spaces
    options.setIndent(2)
    // Set indicator indentation to 2 spaces for list/dict indicators
    options.setIndicatorIndent(2)
    // Enable indentation with indicators for better readability
    options.setIndentWithIndicator(true)
    // Disable pretty flow so that it doesn't add unnecessary newlines after dashes
    options.setPrettyFlow(false)

    val yamlFactory = YAMLFactory.builder()
      // Minimize quotes around strings when possible
      .configure(YAMLGenerator.Feature.MINIMIZE_QUOTES, true)
      // Don't force numbers to be quoted as strings (preserve numeric types)
      .configure(YAMLGenerator.Feature.ALWAYS_QUOTE_NUMBERS_AS_STRINGS, false)
      // Don't write YAML document start marker (---)
      .configure(YAMLGenerator.Feature.WRITE_DOC_START_MARKER, false)
      // Disable native type IDs and use explicit type instead
      .configure(YAMLGenerator.Feature.USE_NATIVE_TYPE_ID, false)
      .dumperOptions(options)
      .build()

    val mapper = new ObjectMapper(yamlFactory)
      // Exclude null values from serialized output
      .setDefaultPropertyInclusion(JsonInclude.Include.NON_NULL)
      // Exclude empty collections/strings from serialized output
      .setDefaultPropertyInclusion(JsonInclude.Include.NON_EMPTY)

    mapper.registerModule(DefaultScalaModule)
    mapper
  }
}

/**
 * Common YAML parsing logic shared by version-specific YAML parsers.
 * This trait provides the core parsing functionality while allowing version-specific
 * implementations to specify the target type and YAML configuration.
 */
private[sql] trait BaseMetricViewYAMLDeserializer[T] {
  /**
   * The YAML utilities to use for deserialization.
   * Subclasses can override this to provide version-specific YAML behavior.
   */
  protected def yamlMapperProvider: YamlMapperProviderBase

  /**
   * Parse YAML content into the specified type.
   * @param yamlContent The YAML content to parse
   * @return The parsed MetricView of type T
   */
  def parseYaml(yamlContent: String): T = {
    try {
      yamlMapperProvider.mapperWithAllFields.readValue(yamlContent, getTargetClass)
    } catch {
      case NonFatal(e) =>
        throw MetricViewYAMLParsingException(
          s"Failed to parse YAML: ${e.getMessage}",
          Some(e)
        )
    }
  }
  /**
   * Get the target class for deserialization.
   * This must be implemented by version-specific implementations.
   * @return The Class object for the target type
   */
  protected def getTargetClass: Class[T]
}

private[sql] trait BaseMetricViewYAMLSerializer[T] {
  protected def yamlMapperProvider: YamlMapperProviderBase

  def toYaml(obj: T): String = {
    try {
      yamlMapperProvider.mapperWithAllFields.writeValueAsString(obj)
    } catch {
      case NonFatal(e) =>
        throw MetricViewYAMLParsingException(
          s"Failed to serialize to YAML: ${e.getMessage}",
          Some(e)
        )
    }
  }
}

private[sql] trait ColumnBase {
  def name: String
  def expr: String
  def toCanonical(ordinal: Int, isDimension: Boolean): Column = {
    if (isDimension) {
      Column(
        name = name,
        expression = DimensionExpression(expr),
        ordinal = ordinal
      )
    } else {
      Column(
        name = name,
        expression = MeasureExpression(expr),
        ordinal = ordinal
      )
    }
  }
}

private[sql] trait MetricViewBase {
  def version: String
  def source: String
  def filter: Option[String]
  def dimensions: Seq[ColumnBase]
  def measures: Seq[ColumnBase]

  // Different YAML versions could have differnt syntax to describe a MetricView, but
  // all of them should be able to be converted to the same canonical form.
  // For example, in later versions, we may change the syntax of source to support
  // multiple sources (e.g. joins, compared to the current single source). But the
  // canonical form should be able to represent both syntaxes.
  def toCanonical: MetricView = {
    // Convert dimensions with proper ordinals (0 to dimensions.length-1)
    val dimensionsCanonical = dimensions.zipWithIndex.map {
      case (column, index) => column.toCanonical(index, isDimension = true)
    }
    // Convert measures with proper ordinals
    // (dimensions.length to dimensions.length + measures.length - 1)
    val measuresCanonical = measures.zipWithIndex.map {
      case (column, index) =>
        column.toCanonical(dimensions.length + index, isDimension = false)
    }
    MetricView(
      version = version,
      from = Source(source),
      where = filter,
      select = dimensionsCanonical ++ measuresCanonical
    )
  }
}

private[sql] object MetricViewBase {
  /**
   * Factory method to create the appropriate version-specific MetricView from canonical form.
   * @param canonical The canonical MetricView to convert from
   * @return The appropriate version-specific MetricView
   */
  def fromCanonical(canonical: MetricView): MetricViewBase = {
    canonical.version match {
      case "0.1" =>
        MetricViewV01.fromCanonical(canonical)
      case _ =>
        throw new IllegalArgumentException(s"Unsupported version: ${canonical.version}")
    }
  }
}
