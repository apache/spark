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

package org.apache.spark.sql.execution.metric

import org.apache.spark.SparkContext

case class VariantMetricDescriptor (
  name: String,
  metricType: String,
  description: String
)

object VariantConstructionMetrics {
  // Top level variant metrics
  val VARIANT_BUILDER_TOP_LEVEL_NUMBER_OF_VARIANTS = "variantBuilderTopLevelNumVariants"
  val VARIANT_BUILDER_TOP_LEVEL_BYTE_SIZE_BOUND = "variantBuilderTopLevelByteSizeBound"
  val VARIANT_BUILDER_TOP_LEVEL_NUM_SCALARS = "variantBuilderTopLevelNumScalars"
  val VARIANT_BUILDER_TOP_LEVEL_NUM_PATHS = "variantBuilderTopLevelNumPaths"
  val VARIANT_BUILDER_TOP_LEVEL_MAX_DEPTH = "variantBuilderTopLevelMaxDepth"
  // Nested variant metrics
  val VARIANT_BUILDER_NESTED_NUMBER_OF_VARIANTS = "variantBuilderNestedNumVariants"
  val VARIANT_BUILDER_NESTED_BYTE_SIZE_BOUND = "variantBuilderNestedByteSizeBound"
  val VARIANT_BUILDER_NESTED_NUM_SCALARS = "variantBuilderNestedNumScalars"
  val VARIANT_BUILDER_NESTED_NUM_PATHS = "variantBuilderNestedNumPaths"
  val VARIANT_BUILDER_NESTED_MAX_DEPTH = "variantBuilderNestedMaxDepth"

  final val all: Array[VariantMetricDescriptor] = Array(
    VariantMetricDescriptor(
      VARIANT_BUILDER_TOP_LEVEL_NUMBER_OF_VARIANTS,
      SQLMetrics.SUM_METRIC,
      "variant top-level - total count"
    ),
    VariantMetricDescriptor(
      VARIANT_BUILDER_TOP_LEVEL_BYTE_SIZE_BOUND,
      SQLMetrics.SIZE_METRIC,
      "variant top-level - total byte size"
    ),
    VariantMetricDescriptor(
      VARIANT_BUILDER_TOP_LEVEL_NUM_SCALARS,
      SQLMetrics.SUM_METRIC,
      "variant top-level - total number of scalar values"
    ),
    VariantMetricDescriptor(
      VARIANT_BUILDER_TOP_LEVEL_NUM_PATHS,
      SQLMetrics.SUM_METRIC,
      "variant top-level - total number of paths"
    ),
    VariantMetricDescriptor(
      VARIANT_BUILDER_TOP_LEVEL_MAX_DEPTH,
      SQLMetrics.MAX_METRIC,
      "variant top-level - total number of paths"
    ),
    VariantMetricDescriptor(
      VARIANT_BUILDER_NESTED_NUMBER_OF_VARIANTS,
      SQLMetrics.SUM_METRIC,
      "variant nested - total count"
    ),
    VariantMetricDescriptor(
      VARIANT_BUILDER_NESTED_BYTE_SIZE_BOUND,
      SQLMetrics.SIZE_METRIC,
      "variant nested - total byte size"
    ),
    VariantMetricDescriptor(
      VARIANT_BUILDER_NESTED_NUM_SCALARS,
      SQLMetrics.SUM_METRIC,
      "variant nested - total number of scalar values"
    ),
    VariantMetricDescriptor(
      VARIANT_BUILDER_NESTED_NUM_PATHS,
      SQLMetrics.SUM_METRIC,
      "variant nested - total number of paths"
    ),
    VariantMetricDescriptor(
      VARIANT_BUILDER_NESTED_MAX_DEPTH,
      SQLMetrics.MAX_METRIC,
      "variant nested - total number of paths"
    )
  )

  def createSQLMetrics(sparkContext: SparkContext): Map[String, SQLMetric] = {
    all.map(
      d => {
        d.name -> {
          d.metricType match {
            case SQLMetrics.SUM_METRIC => SQLMetrics.createMetric(sparkContext, d.description)
            case SQLMetrics.SIZE_METRIC => SQLMetrics.createSizeMetric(sparkContext, d.description)
            case SQLMetrics.MAX_METRIC => SQLMetrics.createMaxMetric(sparkContext, d.description)
            case _ => throw new IllegalArgumentException(s"Unknown metric type: ${d.metricType}")
          }
        }
      }
    ).toMap
  }
}
