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
import org.apache.spark.sql.catalyst.util.CharVarcharUtils
import org.apache.spark.sql.connector.expressions.SortOrder
import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.apache.spark.sql.connector.read.{Scan, ScanBuilder, SupportsPushDownFilters, SupportsPushDownLimit, SupportsPushDownRequiredColumns, SupportsPushDownTableSample, SupportsPushDownTopN, SupportsPushDownV2Filters}
import org.apache.spark.sql.execution.datasources.DataSourceStrategy
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources
import org.apache.spark.sql.types.StructType

object PushDownUtils extends PredicateHelper {
  /**
   * Pushes down filters to the data source reader
   *
   * @return pushed filter and post-scan filters.
   */
  def pushFilters(scanBuilder: ScanBuilder, filters: Seq[Expression])
      : (Either[Seq[sources.Filter], Seq[Predicate]], Seq[Expression]) = {
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
        (Left(r.pushedFilters()), (untranslatableExprs ++ postScanFilters).toSeq)

      case r: SupportsPushDownV2Filters =>
        // A map from translated data source leaf node filters to original catalyst filter
        // expressions. For a `And`/`Or` predicate, it is possible that the predicate is partially
        // pushed down. This map can be used to construct a catalyst filter expression from the
        // input filter, or a superset(partial push down filter) of the input filter.
        val translatedFilterToExpr = mutable.HashMap.empty[Predicate, Expression]
        val translatedFilters = mutable.ArrayBuffer.empty[Predicate]
        // Catalyst filter expression that can't be translated to data source filters.
        val untranslatableExprs = mutable.ArrayBuffer.empty[Expression]

        for (filterExpr <- filters) {
          val translated =
            DataSourceV2Strategy.translateFilterV2WithMapping(
              filterExpr, Some(translatedFilterToExpr), nestedPredicatePushdownEnabled = true)
          if (translated.isEmpty) {
            untranslatableExprs += filterExpr
          } else {
            translatedFilters += translated.get
          }
        }

        // Data source filters that need to be evaluated again after scanning. which means
        // the data source cannot guarantee the rows returned can pass these filters.
        // As a result we must return it so Spark can plan an extra filter operator.
        val postScanFilters = r.pushPredicates(translatedFilters.toArray).map { predicate =>
          DataSourceV2Strategy.rebuildExpressionFromFilter(predicate, translatedFilterToExpr)
        }
        (Right(r.pushedPredicates), (untranslatableExprs ++ postScanFilters).toSeq)

      case f: FileScanBuilder =>
        val postScanFilters = f.pushFilters(filters)
        (Left(f.pushedFilters), postScanFilters)
      case _ => (Left(Nil), filters)
    }
  }

  /**
   * Pushes down TableSample to the data source Scan
   */
  def pushTableSample(scanBuilder: ScanBuilder, sample: TableSampleInfo): Boolean = {
    scanBuilder match {
      case s: SupportsPushDownTableSample =>
        s.pushTableSample(
          sample.lowerBound, sample.upperBound, sample.withReplacement, sample.seed)
      case _ => false
    }
  }

  /**
   * Pushes down LIMIT to the data source Scan.
   *
   * @return the tuple of Boolean. The first Boolean value represents whether to push down, and
   *         the second Boolean value represents whether to push down partially, which means
   *         Spark will keep the Limit and do it again.
   */
  def pushLimit(scanBuilder: ScanBuilder, limit: Int): (Boolean, Boolean) = {
    scanBuilder match {
      case s: SupportsPushDownLimit if s.pushLimit(limit) =>
        (true, s.isPartiallyPushed)
      case _ => (false, false)
    }
  }

  /**
   * Pushes down top N to the data source Scan.
   *
   * @return the tuple of Boolean. The first Boolean value represents whether to push down, and
   *         the second Boolean value represents whether to push down partially, which means
   *         Spark will keep the Sort and Limit and do it again.
   */
  def pushTopN(
      scanBuilder: ScanBuilder,
      order: Array[SortOrder],
      limit: Int): (Boolean, Boolean) = {
    scanBuilder match {
      case s: SupportsPushDownTopN if s.pushTopN(order, limit) =>
        (true, s.isPartiallyPushed)
      case _ => (false, false)
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
          SchemaPruning.pruneSchema(relation.schema, rootFields)
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

  def toOutputAttrs(
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
