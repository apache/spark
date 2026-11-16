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
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan, RowLevelWrite, V2WriteCommand}
import org.apache.spark.sql.catalyst.trees.TreePattern.LAMBDA_VARIABLE
import org.apache.spark.sql.catalyst.util.{CharVarcharUtils, GeneratedColumn, StoredExpressionText}
import org.apache.spark.sql.connector.catalog.CatalogManager
import org.apache.spark.sql.connector.catalog.constraints.Check
import org.apache.spark.sql.connector.write.RowLevelOperation
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation

/** Builds check and generated-column invariants for a write to a resolved V2 table. */
private[sql] class TableConstraintResolution(
    catalogManager: => CatalogManager,
    analyze: LogicalPlan => LogicalPlan,
    sessionLocalTimeZone: => String) {

  /** Adds invariants after the query output has been aligned with the target table. */
  def resolveTableConstraints(
      v2Write: V2WriteCommand,
      r: DataSourceV2Relation): V2WriteCommand = {
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
      GeneratedColumn.columnGenerationExpression(col)
        .filter(_ => !autoFilledGenCols.exists(n => resolver(n, col.name)))
        .map { genExpr =>
          val reader = new GeneratedColumn.ExpressionReader(r, col.name, genExpr)
          val catalystExpr = reader.toCatalyst()
          val colRef = UnresolvedAttribute.quoted(col.name)
          val columnExtractors = Seq(col.name -> colRef)
          val genExprSql = reader.sql
          // Compare with the value an auto-fill stores, which store assignment casts to the column
          // type. Otherwise, the comparison can cast the provided value to the expression type.
          val colType = CharVarcharUtils.replaceCharVarcharWithString(col.dataType())
          def check(expr: Expression): Expression = CheckInvariant(
            EqualNullSafe(
              colRef, Cast(expr, colType, Option(sessionLocalTimeZone), ansiEnabled = true)),
            columnExtractors,
            "Generated Column",
            s"${col.name} <=> $genExprSql")
          // Analyze the check under the settings an auto-filled value is analyzed under, so that a
          // row Spark auto-filled passes it when a later write carries it.
          reader.analyze(
            check(catalystExpr),
            check(reader.toCatalyst(StoredExpressionText.parseWithSessionAnsi)),
            v2Write.query.output,
            analyze)
        }
    }.toSeq
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
