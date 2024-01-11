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
import org.apache.spark.sql.catalyst.planning.GroupBasedRowLevelOperation
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Filter, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.connector.read.SupportsRuntimeV2Filtering
import org.apache.spark.sql.connector.write.RowLevelOperation.Command
import org.apache.spark.sql.connector.write.RowLevelOperation.Command.{DELETE, MERGE, UPDATE}
import org.apache.spark.sql.execution.datasources.v2.{DataSourceV2Implicits, DataSourceV2Relation, DataSourceV2ScanRelation}
import org.apache.spark.util.ArrayImplicits._

/**
 * A rule that assigns a subquery to filter groups in row-level operations at runtime.
 *
 * Data skipping during job planning for row-level operations is limited to expressions that can be
 * converted to data source filters. Since not all expressions can be pushed down that way and
 * rewriting groups is expensive, Spark allows data sources to filter group at runtime.
 * If the primary scan in a group-based row-level operation supports runtime filtering, this rule
 * will inject a subquery to find all rows that match the condition so that data sources know
 * exactly which groups must be rewritten.
 *
 * Note this rule only applies to group-based row-level operations.
 */
class RowLevelOperationRuntimeGroupFiltering(optimizeSubqueries: Rule[LogicalPlan])
  extends Rule[LogicalPlan] with PredicateHelper {

  import DataSourceV2Implicits._

  override def apply(plan: LogicalPlan): LogicalPlan = plan transformDown {
    // apply special dynamic filtering only for group-based row-level operations
    case GroupBasedRowLevelOperation(replaceData, _, Some(cond),
        DataSourceV2ScanRelation(_, scan: SupportsRuntimeV2Filtering, _, _, _))
        if conf.runtimeRowLevelOperationGroupFilterEnabled && cond != TrueLiteral
          && scan.filterAttributes().nonEmpty =>

      // use reference equality on scan to find required scan relations
      val newQuery = replaceData.query transformUp {
        case r: DataSourceV2ScanRelation if r.scan eq scan =>
          // use the original table instance that was loaded for this row-level operation
          // in order to leverage a regular batch scan in the group filter query
          val originalTable = r.relation.table.asRowLevelOperationTable.table
          val relation = r.relation.copy(table = originalTable)
          val tableAttrs = replaceData.table.output
          val command = replaceData.operation.command
          val matchingRowsPlan = buildMatchingRowsPlan(relation, cond, tableAttrs, command)

          val filterAttrs = scan.filterAttributes.toImmutableArraySeq
          val buildKeys = V2ExpressionUtils.resolveRefs[Attribute](filterAttrs, matchingRowsPlan)
          val pruningKeys = V2ExpressionUtils.resolveRefs[Attribute](filterAttrs, r)
          val dynamicPruningCond = buildDynamicPruningCond(matchingRowsPlan, buildKeys, pruningKeys)

          Filter(dynamicPruningCond, r)
      }

      // optimize subqueries to rewrite them as joins and trigger job planning
      replaceData.copy(query = optimizeSubqueries(newQuery))
  }

  private def buildMatchingRowsPlan(
      relation: DataSourceV2Relation,
      cond: Expression,
      tableAttrs: Seq[Attribute],
      command: Command): LogicalPlan = {

    val matchingRowsPlan = command match {
      case DELETE =>
        Filter(cond, relation)

      case UPDATE =>
        // UPDATEs with subqueries are rewritten using UNION with two identical scan relations
        // the analyzer assigns fresh expr IDs for one of them so that attributes don't collide
        // this rule assigns runtime filters to both scan relations (will be shared at runtime)
        // and must transform the runtime filter condition to use correct expr IDs for each relation
        // see RewriteUpdateTable for more details
        val attrMap = buildTableToScanAttrMap(tableAttrs, relation.output)
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
