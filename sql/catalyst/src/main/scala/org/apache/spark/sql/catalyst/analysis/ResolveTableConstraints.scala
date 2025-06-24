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
package org.apache.spark.sql.catalyst.analysis

import scala.collection.mutable

import org.apache.spark.sql.catalyst.expressions.{And, CheckInvariant, Expression, V2ExpressionUtils}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan, V2WriteCommand}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.COMMAND
import org.apache.spark.sql.connector.catalog.CatalogManager
import org.apache.spark.sql.connector.catalog.constraints.Check
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation

class ResolveTableConstraints(val catalogManager: CatalogManager) extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsWithPruning(
    _.containsPattern(COMMAND), ruleId) {
    case v2Write: V2WriteCommand
      if v2Write.table.resolved && v2Write.query.resolved &&
        !containsCheckInvariant(v2Write.query) && v2Write.outputResolved =>
      v2Write.table match {
        case r: DataSourceV2Relation
          if r.table.constraints != null && r.table.constraints.nonEmpty =>
          // Check constraint is the only enforced constraint for DSV2 tables.
          val checkInvariants = r.table.constraints.collect {
            case c: Check =>
              val unresolvedExpr = buildCatalystExpression(c)
              val columnExtractors = mutable.Map[String, Expression]()
              buildColumnExtractors(unresolvedExpr, columnExtractors)
              CheckInvariant(unresolvedExpr, columnExtractors.toSeq, c.name, c.predicateSql)
          }
          // Combine the check invariants into a single expression using conjunctive AND.
          checkInvariants.reduceOption(And).fold(v2Write)(
            condition => v2Write.withNewQuery(Filter(condition, v2Write.query)))
        case _ =>
          v2Write
      }
  }

  private def containsCheckInvariant(plan: LogicalPlan): Boolean = {
    plan match {
      case Filter(condition, _) =>
        condition.exists(_.isInstanceOf[CheckInvariant])

      case _ => false
    }
  }

  private def buildCatalystExpression(c: Check): Expression = {
    Option(c.predicate())
      .flatMap(V2ExpressionUtils.toCatalyst)
      .getOrElse(catalogManager.v1SessionCatalog.parser.parseExpression(c.predicateSql()))
  }

  private def buildColumnExtractors(
      expr: Expression,
      columnExtractors: mutable.Map[String, Expression]): Unit = {
    expr match {
      case u: UnresolvedExtractValue =>
        // When extracting a value from a Map or Array type, we display only the specific extracted
        // value rather than the entire Map or Array structure for clarity and readability.
        columnExtractors(u.sql) = u
      case u: UnresolvedAttribute =>
        columnExtractors(u.name) = u
      case other =>
        other.children.foreach(buildColumnExtractors(_, columnExtractors))
    }
  }
}
