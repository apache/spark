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

import org.apache.spark.sql.catalyst.expressions.{And, Cast, CheckInvariant, EqualNullSafe, Expression, V2ExpressionUtils}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan, RowLevelWrite, V2WriteCommand, WriteDelta}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.{COMMAND, LAMBDA_VARIABLE}
import org.apache.spark.sql.catalyst.util.{CharVarcharUtils, GeneratedColumn, StoredExpressionText}
import org.apache.spark.sql.catalyst.util.TypeUtils.toSQLId
import org.apache.spark.sql.connector.catalog.{CatalogManager, GenerationExpression}
import org.apache.spark.sql.connector.catalog.constraints.Check
import org.apache.spark.sql.connector.write.RowLevelOperation
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation

/**
 * @param analyze Analyzes a plan under the active SQLConf. The check of a generated column goes
 *                through it, so that the settings its stored text is read under cover its analysis
 *                without applying to the query of the write.
 */
class ResolveTableConstraints(
    val catalogManager: CatalogManager,
    analyze: LogicalPlan => LogicalPlan) extends Rule[LogicalPlan] {

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
          allInvariants.reduceOption(And).fold(v2Write)(
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
      .getOrElse(StoredExpressionFunctions.qualifyBuiltinFunctions(
        catalogManager.v1SessionCatalog.parser.parseExpression(c.predicateSql())))
  }

  /**
   * For each generated column whose value was provided by the write, add a CheckInvariant that
   * validates the column value matches the generation expression. Generated columns Spark
   * auto-filled are excluded: ResolveOutputRelation marks them in the table output and their
   * values are correct by construction.
   */
  private def buildGeneratedColumnConstraints(
      r: DataSourceV2Relation,
      v2Write: V2WriteCommand): Seq[Expression] = {
    if (!GeneratedColumn.supportsGeneratedColumnsOnWrite(r.table, r.table.columns())) {
      return Seq.empty
    }

    // A DELETE writes no new generated value, so the rows it carries have nothing to check.
    v2Write match {
      case w: RowLevelWrite if w.operation.command() == RowLevelOperation.Command.DELETE =>
        return Seq.empty
      case _ =>
    }

    val resolver = catalogManager.v1SessionCatalog.conf.resolver
    val autoFilledGenCols = r.output
      .filter(GeneratedColumn.isAutoFilledGeneratedColumn)
      .map(_.name)
      .toSet

    // Use V2 columns from the table to access both V2 expressions and SQL strings.
    r.table.columns().flatMap { col =>
      Option(col.columnGenerationExpression())
        .filter(_ => !autoFilledGenCols.exists(n => resolver(n, col.name)))
        .map { genExpr =>
          val catalystExpr = buildGenerationCatalystExpression(r, col.name, genExpr)
          val colRef = UnresolvedAttribute.quoted(col.name)
          val columnExtractors = Seq(col.name -> colRef)
          val genExprSql = Option(genExpr.getSql()).getOrElse(catalystExpr.sql)
          // Compare with the value an auto-fill stores, which store assignment casts to the column
          // type. Otherwise, the comparison can cast the provided value to the expression type.
          val colType = CharVarcharUtils.replaceCharVarcharWithString(col.dataType())
          def check(expr: Expression): Expression = CheckInvariant(
            EqualNullSafe(
              colRef, Cast(expr, colType, Option(conf.sessionLocalTimeZone), ansiEnabled = true)),
            columnExtractors,
            "Generated Column",
            s"${col.name} <=> $genExprSql")
          // Analyze the check under the settings an auto-filled value is analyzed under, so that a
          // row Spark auto-filled passes it when a later write carries it.
          StoredExpressionText.analyze(
            check(catalystExpr),
            check(Option(genExpr.getSql).map(StoredExpressionText.parseWithSessionAnsi)
              .getOrElse(catalystExpr)),
            v2Write.query.output,
            analyze)
        }
    }.toSeq
  }

  /**
   * Convert a V2 GenerationExpression to a Catalyst expression. Read the SQL string first, as an
   * auto-filled value does, and fall back to the V2 expression.
   */
  private def buildGenerationCatalystExpression(
      r: DataSourceV2Relation,
      colName: String,
      genExpr: GenerationExpression): Expression = {
    Option(genExpr.getSql)
      .map(StoredExpressionText.parse)
      .orElse(Option(genExpr.getExpression).flatMap(V2ExpressionUtils.toCatalyst))
      .getOrElse {
        val operation = s"a value for the generated column ${toSQLId(colName)}, because " +
          s"Spark cannot convert its expression ${genExpr.getExpression}"
        // A relation of a TableProvider has no catalog and no identifier.
        throw ((r.catalog, r.identifier) match {
          case (Some(catalog), Some(ident)) =>
            QueryCompilationErrors.unsupportedTableOperationError(catalog, ident, operation)
          case _ => QueryCompilationErrors.unsupportedTableOperationError(r.table, operation)
        })
      }
  }

  private def buildColumnExtractors(
      expr: Expression,
      columnExtractors: mutable.Map[String, Expression]): Unit = {
    expr match {
      // An extraction that refers to a lambda variable, such as `x[0]` in
      // `forall(a, x -> x[0] > 0)`, cannot be an extractor: the lambda variable does not resolve
      // outside its lambda function.
      case u: UnresolvedExtractValue if !u.containsPattern(LAMBDA_VARIABLE) =>
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
