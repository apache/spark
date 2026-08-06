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

package org.apache.spark.sql.execution.dynamicpruning

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeMap, AttributeReference, DynamicPruningExpression, Expression, InSubquery, ListQuery, PredicateHelper, V2ExpressionUtils}
import org.apache.spark.sql.catalyst.expressions.Literal.TrueLiteral
import org.apache.spark.sql.catalyst.optimizer.RewritePredicateSubquery
import org.apache.spark.sql.catalyst.planning.{DeltaBasedRowLevelOperation, GroupBasedRowLevelOperation}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Filter, LogicalPlan, RowLevelWrite}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.connector.read.SupportsRuntimeV2Filtering
import org.apache.spark.sql.connector.write.RowLevelOperation.Command.{DELETE, MERGE, UPDATE}
import org.apache.spark.sql.execution.datasources.v2.{DataSourceV2Implicits, DataSourceV2Relation, DataSourceV2ScanRelation, ExtractV2Scan}
import org.apache.spark.util.ArrayImplicits._

/**
 * A rule that assigns a subquery to filter groups in row-level operations at runtime.
 *
 * Data skipping during job planning for row-level operations is limited to expressions that can be
 * converted to data source filters. Since not all expressions can be pushed down that way, Spark
 * allows data sources to filter groups at runtime. If the primary scan in a row-level operation
 * supports runtime filtering, this rule will inject a subquery to find all rows that match the
 * condition so that data sources know exactly which groups have changes.
 *
 * Note that this rule is also beneficial for operations that deal with deltas of rows. Even if
 * the data source is capable of handling specific changes, it is useful to first discard entire
 * groups that are not modified. The cost of the runtime query is small as it only projects columns
 * required to evaluate the row level operation condition. The main scan, on the other hand, must
 * project all columns, meaning the cost of reading unaffected groups can dominate the runtime.
 */
class RowLevelOperationRuntimeGroupFiltering(optimizeSubqueries: Rule[LogicalPlan])
  extends Rule[LogicalPlan] with PredicateHelper {

  import DataSourceV2Implicits._

  override def apply(plan: LogicalPlan): LogicalPlan = plan transformDown {
    case GroupBasedRowLevelOperation(replaceData, _, Some(cond),
        ExtractV2Scan(scan: SupportsRuntimeV2Filtering)) if canInjectGroupFilters(cond, scan) =>
      injectGroupFilters(replaceData, cond, scan)

    case DeltaBasedRowLevelOperation(writeDelta, _, Some(cond),
        ExtractV2Scan(scan: SupportsRuntimeV2Filtering)) if canInjectGroupFilters(cond, scan) =>
      injectGroupFilters(writeDelta, cond, scan)
  }

  private def canInjectGroupFilters(
      cond: Expression,
      scan: SupportsRuntimeV2Filtering): Boolean = {
    conf.runtimeRowLevelOperationGroupFilterEnabled &&
      cond != TrueLiteral &&
      scan.filterAttributes.nonEmpty
  }

  private def injectGroupFilters(
      write: RowLevelWrite,
      cond: Expression,
      scan: SupportsRuntimeV2Filtering): LogicalPlan = {
    // use reference equality on scan to find required scan relations
    val newQuery = write.query transformUp {
      case r: DataSourceV2ScanRelation if r.scan eq scan =>
        // use the original table instance that was loaded for this row-level operation
        // in order to leverage a regular batch scan in the group filter query
        val originalTable = r.relation.table.asRowLevelOperationTable.table
        val relation = r.relation.copy(table = originalTable)
        val matchingRowsPlan = buildMatchingRowsPlan(write, relation, cond)
        val filterAttrs = scan.filterAttributes.toImmutableArraySeq
        val buildKeys = V2ExpressionUtils.resolveRefs[Attribute](filterAttrs, matchingRowsPlan)
        val pruningKeys = V2ExpressionUtils.resolveRefs[Attribute](filterAttrs, r)
        Filter(buildDynamicPruningCond(matchingRowsPlan, buildKeys, pruningKeys), r)
    }
    // optimize subqueries to rewrite them as joins and trigger job planning
    write.withNewQuery(optimizeSubqueries(newQuery))
  }

  private def buildMatchingRowsPlan(
      write: RowLevelWrite,
      relation: DataSourceV2Relation,
      cond: Expression): LogicalPlan = {

    val matchingRowsPlan = write.operation.command match {
      case DELETE =>
        Filter(cond, relation)

      case UPDATE =>
        // UPDATEs with subqueries can be rewritten using UNION with two identical scan relations
        // the analyzer assigns fresh expr IDs for one of them so that attributes don't collide
        // this rule assigns runtime filters to both scan relations (will be shared at runtime)
        // and must transform the runtime filter condition to use correct expr IDs for each relation
        // note this only applies to group-based row-level operations (i.e. ReplaceData)
        // see RewriteUpdateTable for more details
        val attrMap = buildTableToScanAttrMap(write.table.output, relation.output)
        val transformedCond = cond transform {
          case attr: AttributeReference if attrMap.contains(attr) => attrMap(attr)
        }
        Filter(transformedCond, relation)

      case MERGE =>
        // rewrite the group filter subquery as joins
        val filter = Filter(cond, relation)
        RewritePredicateSubquery(filter)
    }

    // clone the relation and assign new expr IDs to avoid conflicts
    matchingRowsPlan transformUpWithNewOutput {
      case r: DataSourceV2Relation if r eq relation =>
        val newRelation = r.newInstance()
        newRelation -> r.output.zip(newRelation.output)
    }
  }

  private def buildDynamicPruningCond(
      matchingRowsPlan: LogicalPlan,
      buildKeys: Seq[Attribute],
      pruningKeys: Seq[Attribute]): Expression = {
    assert(buildKeys.nonEmpty && pruningKeys.nonEmpty)

    val buildQuery = Aggregate(buildKeys, buildKeys, matchingRowsPlan)
    DynamicPruningExpression(
      InSubquery(pruningKeys, ListQuery(buildQuery, numCols = buildQuery.output.length)))
  }

  private def buildTableToScanAttrMap(
      tableAttrs: Seq[Attribute],
      scanAttrs: Seq[Attribute]): AttributeMap[Attribute] = {

    val attrMapping = tableAttrs.map { tableAttr =>
      scanAttrs
        .find(scanAttr => conf.resolver(scanAttr.name, tableAttr.name))
        .map(scanAttr => tableAttr -> scanAttr)
        .getOrElse {
          throw new AnalysisException(
            errorClass = "_LEGACY_ERROR_TEMP_3075",
            messageParameters = Map(
              "tableAttr" -> tableAttr.toString,
              "scanAttrs" -> scanAttrs.mkString(",")))
        }
    }
    AttributeMap(attrMapping)
  }
}
