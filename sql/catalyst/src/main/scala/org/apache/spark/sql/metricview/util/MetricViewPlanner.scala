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
import org.apache.spark.sql.metricview.logical.{DimensionInputColumn, InputColumn, MeasureInputColumn, MetricViewPlaceholder}
import org.apache.spark.sql.metricview.serde.{AssetSource, DimensionExpression, JsonUtils, MeasureExpression, MetricView, MetricViewFactory, MetricViewValidationException, MetricViewYAMLParsingException, SQLSource}
import org.apache.spark.sql.types.{Metadata, StructType}

object MetricViewPlanner {

  def planWrite(
      metadata: CatalogTable,
      yaml: String,
      sqlParser: ParserInterface): (MetricViewPlaceholder, MetricView) = {
    val (metricView, dataModelPlan) = parseYAML(yaml, sqlParser)
    val inputColumns = buildInputColumns(metricView, sqlParser)
    val placeholder = MetricViewPlaceholder(
      metadata,
      inputColumns,
      Seq.empty,
      dataModelPlan,
      isCreate = true
    )
    (placeholder, metricView)
  }

  def planRead(
      metadata: CatalogTable,
      yaml: String,
      sqlParser: ParserInterface,
      expectedSchema: StructType): MetricViewPlaceholder = {
    val (metricView, dataModelPlan) = parseYAML(yaml, sqlParser)
    val inputColumns = buildInputColumns(metricView, sqlParser)
    MetricViewPlaceholder(
      metadata,
      inputColumns,
      DataTypeUtils.toAttributes(expectedSchema),
      dataModelPlan
    )
  }

  /**
   * Parses every column's `MeasureExpression` / `DimensionExpression` from the YAML descriptor
   * into a typed [[InputColumn]] (with the SQL expression already parsed) so downstream
   * resolution rules read a stable representation rather than re-parsing the YAML.
   * Column metadata is converted once here from the canonical `ColumnMetadata` to Spark's
   * `Metadata`, preserving the per-column annotations (e.g. dimension / measure type marker,
   * source expression text) the resolver attaches to output attributes.
   */
  private def buildInputColumns(
      metricView: MetricView,
      sqlParser: ParserInterface): Seq[InputColumn] = {
    metricView.select.map { col =>
      val md = Metadata.fromJson(JsonUtils.toJson(col.getColumnMetadata))
      col.expression match {
        case DimensionExpression(expr) =>
          DimensionInputColumn(col.name, sqlParser.parseExpression(expr), md)
        case MeasureExpression(expr) =>
          MeasureInputColumn(col.name, sqlParser.parseExpression(expr), md)
      }
    }
  }

  private def parseYAML(
      yaml: String,
      sqlParser: ParserInterface): (MetricView, LogicalPlan) = {
    val metricView = try {
      MetricViewFactory.fromYAML(yaml)
    } catch {
      // Both cases are user-correctable errors in the YAML body, not internal Spark bugs;
      // surface them as `INVALID_METRIC_VIEW_YAML` AnalysisExceptions so the message is
      // categorized as user input error rather than "please contact support".
      case e: MetricViewValidationException =>
        throw QueryCompilationErrors.invalidMetricViewYamlError(e.getMessage, e)
      case e: MetricViewYAMLParsingException =>
        throw QueryCompilationErrors.invalidMetricViewYamlError(e.getMessage, e)
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
