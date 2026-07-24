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

import org.apache.spark.sql.catalyst.expressions.{And, CheckInvariant, EqualNullSafe, Expression, V2ExpressionUtils}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan, V2WriteCommand, WriteDelta}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.COMMAND
import org.apache.spark.sql.catalyst.util.GeneratedColumn
import org.apache.spark.sql.connector.catalog.{CatalogManager, GenerationExpression}
import org.apache.spark.sql.connector.catalog.constraints.Check
import org.apache.spark.sql.connector.write.RowLevelOperation
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation

class ResolveTableConstraints(val catalogManager: CatalogManager) extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsWithPruning(
    _.containsPattern(COMMAND), ruleId) {
    // Deleting a delta of rows from an existing table doesn't produce any new rows, thus enforcing
    // check constraints is unnecessary.
    case w: WriteDelta if w.operation.command() == RowLevelOperation.Command.DELETE =>
      w
    case v2Write: V2WriteCommand
      if v2Write.table.resolved && v2Write.query.resolved &&
        !containsCheckInvariant(v2Write.query) && v2Write.outputResolved =>
      v2Write.table match {
        case r: DataSourceV2Relation =>
          val tableCheckInvariants =
            if (r.table.constraints != null && r.table.constraints.nonEmpty) {
              // Check constraint is the only enforced constraint for DSV2 tables.
              r.table.constraints.collect {
                case c: Check =>
                  val unresolvedExpr = buildCatalystExpression(c)
                  val columnExtractors = mutable.Map[String, Expression]()
                  buildColumnExtractors(unresolvedExpr, columnExtractors)
                  CheckInvariant(unresolvedExpr, columnExtractors.toSeq, c.name, c.predicateSql)
              }.toSeq
            } else {
              Seq.empty
            }
          val genColInvariants = buildGeneratedColumnConstraints(r, v2Write)
          val allInvariants = tableCheckInvariants ++ genColInvariants
          // Combine the check invariants into a single expression using conjunctive AND.
          allInvariants.reduceOption(And.apply).fold(v2Write)(
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

  /**
   * For each user-provided generated column, add a CheckInvariant that validates the column
   * value matches the generation expression. Auto-filled generated columns are excluded:
   * ResolveOutputRelation strips their GENERATION_EXPRESSION metadata from the table output,
   * so they won't appear here.
   */
  private def buildGeneratedColumnConstraints(
      r: DataSourceV2Relation,
      v2Write: V2WriteCommand): Seq[Expression] = {
    if (!GeneratedColumn.supportsGeneratedColumnsOnWrite(r.catalog, r.table.columns())) {
      return Seq.empty
    }

    // Use V2 columns from the table to access both V2 expressions and SQL strings.
    // Only add constraints for generated columns whose GENERATION_EXPRESSION metadata
    // is still present in the table output -- ResolveOutputRelation strips the metadata
    // from auto-filled columns so they are excluded here.
    val v2Columns = r.table.columns()
    val resolver = catalogManager.v1SessionCatalog.conf.resolver
    val userProvidedGenCols = r.output
      .filter(attr => GeneratedColumn.isGeneratedColumn(attr.metadata))
      .map(_.name)
      .toSet

    v2Columns.flatMap { col =>
      Option(col.columnGenerationExpression())
        .filter(_ => userProvidedGenCols.exists(n => resolver(n, col.name)))
        .map { genExpr =>
          val catalystExpr = buildGenerationCatalystExpression(genExpr)
          val colRef = UnresolvedAttribute.quoted(col.name)
          val columnExtractors = Seq(col.name -> colRef)
          val genExprSql = Option(genExpr.getSql()).getOrElse(catalystExpr.sql)
          CheckInvariant(
            EqualNullSafe(colRef, catalystExpr),
            columnExtractors,
            "Generated Column",
            s"${col.name} <=> $genExprSql")
        }
    }.toSeq
  }

  /**
   * Convert a V2 GenerationExpression to a Catalyst expression.
   * Try the V2 expression first, fall back to parsing the SQL string.
   */
  private def buildGenerationCatalystExpression(genExpr: GenerationExpression): Expression = {
    Option(genExpr.getExpression)
      .flatMap(V2ExpressionUtils.toCatalyst)
      .getOrElse(catalogManager.v1SessionCatalog.parser.parseExpression(genExpr.getSql))
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
