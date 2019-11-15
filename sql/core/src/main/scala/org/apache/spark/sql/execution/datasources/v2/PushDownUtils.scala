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

package org.apache.spark.sql.execution.datasources.v2

import scala.collection.mutable

import org.apache.spark.sql.catalyst.expressions.{AttributeReference, AttributeSet, Expression, PredicateHelper}
import org.apache.spark.sql.connector.read.{Scan, ScanBuilder, SupportsPushDownFilters, SupportsPushDownRequiredColumns}
import org.apache.spark.sql.execution.datasources.DataSourceStrategy
import org.apache.spark.sql.sources

object PushDownUtils extends PredicateHelper {
  /**
   * Pushes down filters to the data source reader
   *
   * @return pushed filter and post-scan filters.
   */
  def pushFilters(
      scanBuilder: ScanBuilder,
      filters: Seq[Expression]): (Seq[Expression], Seq[Expression]) = {
    scanBuilder match {
      case r: SupportsPushDownFilters =>
        // A map from translated data source leaf node filters to original catalyst filter
        // expressions. For a `And`/`Or` predicate, it is possible that the predicate is partially
        // pushed down. This map can be used to construct a catalyst filter expression from the
        // input filter, or a superset(partial push down filter) of the input filter.
        val translatedFilterToExpr = mutable.HashMap.empty[sources.Filter, Expression]
        val translatedFilters = mutable.ArrayBuffer.empty[sources.Filter]
        // Catalyst filter expression that can't be translated to data source filters.
        val untranslatableExprs = mutable.ArrayBuffer.empty[Expression]

        for (filterExpr <- filters) {
          val translated =
            DataSourceStrategy.translateFilterWithMapping(filterExpr, Some(translatedFilterToExpr))
          if (translated.isEmpty) {
            untranslatableExprs += filterExpr
          } else {
            translatedFilters += translated.get
          }
        }

        // Data source filters that need to be evaluated again after scanning. which means
        // the data source cannot guarantee the rows returned can pass these filters.
        // As a result we must return it so Spark can plan an extra filter operator.
        val postScanFilters = r.pushFilters(translatedFilters.toArray).map { filter =>
          DataSourceStrategy.rebuildExpressionFromFilter(filter, translatedFilterToExpr)
        }
        // The filters which are marked as pushed to this data source
        val pushedFilters = r.pushedFilters().map { filter =>
          DataSourceStrategy.rebuildExpressionFromFilter(filter, translatedFilterToExpr)
        }
        (pushedFilters, untranslatableExprs ++ postScanFilters)

      case _ => (Nil, filters)
    }
  }

  /**
   * Applies column pruning to the data source, w.r.t. the references of the given expressions.
   *
   * @return the created `ScanConfig`(since column pruning is the last step of operator pushdown),
   *         and new output attributes after column pruning.
   */
  // TODO: nested column pruning.
  def pruneColumns(
      scanBuilder: ScanBuilder,
      relation: DataSourceV2Relation,
      exprs: Seq[Expression]): (Scan, Seq[AttributeReference]) = {
    scanBuilder match {
      case r: SupportsPushDownRequiredColumns =>
        val requiredColumns = AttributeSet(exprs.flatMap(_.references))
        val neededOutput = relation.output.filter(requiredColumns.contains)
        if (neededOutput != relation.output) {
          r.pruneColumns(neededOutput.toStructType)
          val scan = r.build()
          val nameToAttr = relation.output.map(_.name).zip(relation.output).toMap
          scan -> scan.readSchema().toAttributes.map {
            // We have to keep the attribute id during transformation.
            a => a.withExprId(nameToAttr(a.name).exprId)
          }
        } else {
          r.build() -> relation.output
        }

      case _ => scanBuilder.build() -> relation.output
    }
  }
}
