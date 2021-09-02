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

import org.apache.spark.sql.catalyst.expressions.{AttributeReference, AttributeSet, Expression, NamedExpression, PredicateHelper, SchemaPruning}
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.util.CharVarcharUtils
import org.apache.spark.sql.connector.expressions.FieldReference
import org.apache.spark.sql.connector.expressions.aggregate.Aggregation
import org.apache.spark.sql.connector.read.{Scan, ScanBuilder, SupportsPushDownAggregates, SupportsPushDownFilters, SupportsPushDownRequiredColumns}
import org.apache.spark.sql.execution.datasources.{DataSourceStrategy, PushableColumnWithoutNestedColumn}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources
import org.apache.spark.sql.types.StructType

object PushDownUtils extends PredicateHelper {
  /**
   * Pushes down filters to the data source reader
   *
   * @return pushed filter and post-scan filters.
   */
  def pushFilters(
      scanBuilder: ScanBuilder,
      filters: Seq[Expression]): (Seq[sources.Filter], Seq[Expression]) = {
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
            DataSourceStrategy.translateFilterWithMapping(filterExpr, Some(translatedFilterToExpr),
              nestedPredicatePushdownEnabled = true)
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
        (r.pushedFilters(), (untranslatableExprs ++ postScanFilters).toSeq)

      case f: FileScanBuilder =>
        val postScanFilters = f.pushFilters(filters)
        (f.pushedFilters, postScanFilters)
      case _ => (Nil, filters)
    }
  }

  /**
   * Pushes down aggregates to the data source reader
   *
   * @return pushed aggregation.
   */
  def pushAggregates(
      scanBuilder: ScanBuilder,
      aggregates: Seq[AggregateExpression],
      groupBy: Seq[Expression]): Option[Aggregation] = {

    def columnAsString(e: Expression): Option[FieldReference] = e match {
      case PushableColumnWithoutNestedColumn(name) =>
        Some(FieldReference(name).asInstanceOf[FieldReference])
      case _ => None
    }

    scanBuilder match {
      case r: SupportsPushDownAggregates if aggregates.nonEmpty =>
        val translatedAggregates = aggregates.flatMap(DataSourceStrategy.translateAggregate)
        val translatedGroupBys = groupBy.flatMap(columnAsString)

        if (translatedAggregates.length != aggregates.length ||
          translatedGroupBys.length != groupBy.length) {
          return None
        }

        val agg = new Aggregation(translatedAggregates.toArray, translatedGroupBys.toArray)
        Some(agg).filter(r.pushAggregation)
      case _ => None
    }
  }

  /**
   * Applies column pruning to the data source, w.r.t. the references of the given expressions.
   *
   * @return the `Scan` instance (since column pruning is the last step of operator pushdown),
   *         and new output attributes after column pruning.
   */
  def pruneColumns(
      scanBuilder: ScanBuilder,
      relation: DataSourceV2Relation,
      projects: Seq[NamedExpression],
      filters: Seq[Expression]): (Scan, Seq[AttributeReference]) = {
    val exprs = projects ++ filters
    val requiredColumns = AttributeSet(exprs.flatMap(_.references))
    val neededOutput = relation.output.filter(requiredColumns.contains)

    scanBuilder match {
      case r: SupportsPushDownRequiredColumns if SQLConf.get.nestedSchemaPruningEnabled =>
        val rootFields = SchemaPruning.identifyRootFields(projects, filters)
        val prunedSchema = if (rootFields.nonEmpty) {
          SchemaPruning.pruneDataSchema(relation.schema, rootFields)
        } else {
          new StructType()
        }
        val neededFieldNames = neededOutput.map(_.name).toSet
        r.pruneColumns(StructType(prunedSchema.filter(f => neededFieldNames.contains(f.name))))
        val scan = r.build()
        scan -> toOutputAttrs(scan.readSchema(), relation)

      case r: SupportsPushDownRequiredColumns =>
        r.pruneColumns(neededOutput.toStructType)
        val scan = r.build()
        // always project, in case the relation's output has been updated and doesn't match
        // the underlying table schema
        scan -> toOutputAttrs(scan.readSchema(), relation)

      case _ => scanBuilder.build() -> relation.output
    }
  }

  private def toOutputAttrs(
      schema: StructType,
      relation: DataSourceV2Relation): Seq[AttributeReference] = {
    val nameToAttr = relation.output.map(_.name).zip(relation.output).toMap
    val cleaned = CharVarcharUtils.replaceCharVarcharWithStringInSchema(schema)
    cleaned.toAttributes.map {
      // we have to keep the attribute id during transformation
      a => a.withExprId(nameToAttr(a.name).exprId)
    }
  }
}
