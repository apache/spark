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

import org.apache.spark.internal.{LogKeys, MDC}
import org.apache.spark.sql.catalyst.expressions.{And, AttributeReference, AttributeSet, Expression, ExpressionSet, PredicateHelper, SubqueryExpression}
import org.apache.spark.sql.catalyst.expressions.Literal.TrueLiteral
import org.apache.spark.sql.catalyst.planning.{GroupBasedRowLevelOperation, PhysicalOperation}
import org.apache.spark.sql.catalyst.plans.logical.{Join, LogicalPlan, ReplaceData}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.connector.expressions.filter.{Predicate => V2Filter}
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.connector.write.RowLevelOperation.Command.MERGE
import org.apache.spark.sql.execution.datasources.DataSourceStrategy
import org.apache.spark.sql.sources.Filter

/**
 * A rule that builds scans for group-based row-level operations.
 *
 * Note this rule must be run before [[V2ScanRelationPushDown]] as scans for group-based
 * row-level operations must be planned in a special way.
 */
object GroupBasedRowLevelOperationScanPlanning extends Rule[LogicalPlan] with PredicateHelper {

  import DataSourceV2Implicits._

  override def apply(plan: LogicalPlan): LogicalPlan = plan transformDown {
    // push down the filter from the command condition instead of the filter in the rewrite plan,
    // which is negated for data sources that only support replacing groups of data (e.g. files)
    case GroupBasedRowLevelOperation(rd: ReplaceData, cond, _, relation: DataSourceV2Relation) =>
      assert(cond.deterministic, "row-level operation conditions must be deterministic")

      val table = relation.table.asRowLevelOperationTable
      val scanBuilder = table.newScanBuilder(relation.options)

      val (pushedFilters, evaluatedFilters, postScanFilters) =
        pushFilters(cond, relation.output, scanBuilder)

      val pushedFiltersStr = if (pushedFilters.isLeft) {
        pushedFilters.swap
          .getOrElse(throw new NoSuchElementException("The left node doesn't have pushedFilters"))
          .mkString(", ")
      } else {
        pushedFilters
          .getOrElse(throw new NoSuchElementException("The right node doesn't have pushedFilters"))
          .mkString(", ")
      }

      val (scan, output) = PushDownUtils.pruneColumns(scanBuilder, relation, relation.output, Nil)

      // scalastyle:off line.size.limit
      logInfo(
        log"""
            |Pushing operators to ${MDC(LogKeys.RELATION_NAME, relation.name)}
            |Pushed filters: ${MDC(LogKeys.PUSHED_FILTERS, pushedFiltersStr)}
            |Filters evaluated on data source side: ${MDC(LogKeys.EVALUATED_FILTERS, evaluatedFilters.mkString(", "))}
            |Filters evaluated on Spark side: ${MDC(LogKeys.POST_SCAN_FILTERS, postScanFilters.mkString(", "))}}
            |Output: ${MDC(LogKeys.RELATION_OUTPUT, output.mkString(", "))}
           """.stripMargin)
      // scalastyle:on line.size.limit

      rd transformDown {
        // simplify the join condition in MERGE operations by discarding already evaluated filters
        case j @ Join(
            PhysicalOperation(_, _, r: DataSourceV2Relation), _, _, Some(cond), _)
            if rd.operation.command == MERGE && evaluatedFilters.nonEmpty && r.table.eq(table) =>
          j.copy(condition = Some(optimizeMergeJoinCondition(cond, evaluatedFilters)))

        // replace DataSourceV2Relation with DataSourceV2ScanRelation for the row operation table
        // there may be multiple read relations for UPDATEs that are rewritten as UNION
        case r: DataSourceV2Relation if r.table eq table =>
          DataSourceV2ScanRelation(r, scan, PushDownUtils.toOutputAttrs(scan.readSchema(), r))
      }
  }

  // pushes down the operation condition and returns the following information:
  // - pushed down filters
  // - filter expressions that are fully evaluated on the data source side
  //   (such filters can be discarded and don't have to be evaluated again on the Spark side)
  // - post-scan filter expressions that must be evaluated on the Spark side
  //   (such filters can overlap with pushed down filters, e.g. Parquet row group filtering)
  private def pushFilters(
      cond: Expression,
      tableAttrs: Seq[AttributeReference],
      scanBuilder: ScanBuilder)
  : (Either[Seq[Filter], Seq[V2Filter]], Seq[Expression], Seq[Expression]) = {

    val (filtersWithSubquery, filtersWithoutSubquery) = findTableFilters(cond, tableAttrs)

    val (pushedFilters, postScanFiltersWithoutSubquery) =
      PushDownUtils.pushFilters(scanBuilder, filtersWithoutSubquery)

    val postScanFilterSetWithoutSubquery = ExpressionSet(postScanFiltersWithoutSubquery)
    val evaluatedFilters = filtersWithoutSubquery.filterNot { filter =>
      postScanFilterSetWithoutSubquery.contains(filter)
    }

    val postScanFilters = postScanFiltersWithoutSubquery ++ filtersWithSubquery

    (pushedFilters, evaluatedFilters, postScanFilters)
  }

  private def findTableFilters(
      cond: Expression,
      tableAttrs: Seq[AttributeReference]): (Seq[Expression], Seq[Expression]) = {
    val tableAttrSet = AttributeSet(tableAttrs)
    val filters = splitConjunctivePredicates(cond).filter(_.references.subsetOf(tableAttrSet))
    val normalizedFilters = DataSourceStrategy.normalizeExprs(filters, tableAttrs)
    normalizedFilters.partition(SubqueryExpression.hasSubquery)
  }

  private def optimizeMergeJoinCondition(
      cond: Expression,
      evaluatedFilters: Seq[Expression]): Expression = {
    val evaluatedFilterSet = ExpressionSet(evaluatedFilters)
    val predicates = splitConjunctivePredicates(cond)
    val remainingPredicates = predicates.filterNot(evaluatedFilterSet.contains)
    remainingPredicates.reduceLeftOption(And).getOrElse(TrueLiteral)
  }
}
