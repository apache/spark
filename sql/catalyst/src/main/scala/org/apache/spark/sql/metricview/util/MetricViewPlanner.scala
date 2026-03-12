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

package org.apache.spark.sql.metricview.util

import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan}
import org.apache.spark.sql.catalyst.types.DataTypeUtils
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.metricview.logical.MetricViewPlaceholder
import org.apache.spark.sql.metricview.serde.{AssetSource, MetricView, MetricViewFactory, MetricViewValidationException, MetricViewYAMLParsingException, SQLSource}
import org.apache.spark.sql.types.StructType

object MetricViewPlanner {

  def planWrite(
      metadata: CatalogTable,
      yaml: String,
      sqlParser: ParserInterface): MetricViewPlaceholder = {
    val (metricView, dataModelPlan) = parseYAML(yaml, sqlParser)
    MetricViewPlaceholder(
      metadata,
      metricView,
      Seq.empty,
      dataModelPlan,
      isCreate = true
    )
  }

  def planRead(
      metadata: CatalogTable,
      yaml: String,
      sqlParser: ParserInterface,
      expectedSchema: StructType): MetricViewPlaceholder = {
    val (metricView, dataModelPlan) = parseYAML(yaml, sqlParser)
    MetricViewPlaceholder(
      metadata,
      metricView,
      DataTypeUtils.toAttributes(expectedSchema),
      dataModelPlan
    )
  }

  private def parseYAML(
      yaml: String,
      sqlParser: ParserInterface): (MetricView, LogicalPlan) = {
    val metricView = try {
      MetricViewFactory.fromYAML(yaml)
    } catch {
      case e: MetricViewValidationException =>
        throw QueryCompilationErrors.invalidLiteralForWindowDurationError()
      case e: MetricViewYAMLParsingException =>
        throw QueryCompilationErrors.invalidLiteralForWindowDurationError()
    }
    val source = metricView.from match {
      case asset: AssetSource => UnresolvedRelation(sqlParser.parseMultipartIdentifier(asset.name))
      case sqlSource: SQLSource => sqlParser.parsePlan(sqlSource.sql)
      case _ => throw SparkException.internalError("Either SQLSource or AssetSource")
    }
    // Compute filter here because all necessary information is available.
    val parsedPlan = metricView.where.map { cond =>
      Filter(sqlParser.parseExpression(cond), source)
    }.getOrElse(source)
    (metricView, parsedPlan)
  }
}
