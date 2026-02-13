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

import com.fasterxml.jackson.annotation.JsonProperty

private[sql] case class ColumnV01(
    @JsonProperty(required = true) name: String,
    @JsonProperty(required = true) expr: String
) extends ColumnBase

private[sql] object ColumnV01 {
  def fromCanonical(canonical: Column): ColumnV01 = {
    val name = canonical.name
    val expr = canonical.expression match {
      case DimensionExpression(exprStr) => exprStr
      case MeasureExpression(exprStr) => exprStr
      case _ =>
        throw new IllegalArgumentException(
          s"Unsupported expression type: ${canonical.expression.getClass.getName}")
    }
    ColumnV01(name = name, expr = expr)
  }
}

private[sql] case class MetricViewV01(
    @JsonProperty(required = true) version: String,
    @JsonProperty(required = true) source: String,
    filter: Option[String] = None,
    dimensions: Seq[ColumnV01] = Seq.empty,
    measures: Seq[ColumnV01] = Seq.empty) extends MetricViewBase

private[sql] object MetricViewV01 {
  def fromCanonical(canonical: MetricView): MetricViewV01 = {
    val source = canonical.from.toString
    val filter = canonical.where
    // Separate dimensions and measures based on expression type
    val dimensions = canonical.select.collect {
      case column if column.expression.isInstanceOf[DimensionExpression] =>
        ColumnV01.fromCanonical(column)
    }
    val measures = canonical.select.collect {
      case column if column.expression.isInstanceOf[MeasureExpression] =>
        ColumnV01.fromCanonical(column)
    }
    MetricViewV01(
      version = canonical.version,
      source = source,
      filter = filter,
      dimensions = dimensions,
      measures = measures
    )
  }
}

private[sql] object YamlMapperProviderV01 extends YamlMapperProviderBase

private[sql] object MetricViewYAMLDeserializerV01
  extends BaseMetricViewYAMLDeserializer[MetricViewV01] {
  override protected def yamlMapperProvider: YamlMapperProviderBase = YamlMapperProviderV01

  protected def getTargetClass: Class[MetricViewV01] = classOf[MetricViewV01]
}

private[sql] object MetricViewYAMLSerializerV01
  extends BaseMetricViewYAMLSerializer[MetricViewV01] {
  override protected def yamlMapperProvider: YamlMapperProviderBase = YamlMapperProviderV01
}
