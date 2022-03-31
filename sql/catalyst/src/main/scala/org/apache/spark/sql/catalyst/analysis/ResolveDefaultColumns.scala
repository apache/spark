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

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{SessionCatalog, UnresolvedCatalogRelation}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.util.ResolveDefaultColumns._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

/**
 * This is a rule to process DEFAULT columns in statements such as CREATE/REPLACE TABLE.
 *
 * Background: CREATE TABLE and ALTER TABLE invocations support setting column default values for
 * later operations. Following INSERT, and INSERT MERGE commands may then reference the value
 * using the DEFAULT keyword as needed.
 *
 * Example:
 * CREATE TABLE T(a INT DEFAULT 4, b INT NOT NULL DEFAULT 5);
 * INSERT INTO T VALUES (1, 2);
 * INSERT INTO T VALUES (1, DEFAULT);
 * INSERT INTO T VALUES (DEFAULT, 6);
 * SELECT * FROM T;
 * (1, 2)
 * (1, 5)
 * (4, 6)
 *
 * @param analyzer analyzer to use for processing DEFAULT values stored as text.
 * @param catalog  the catalog to use for looking up the schema of INSERT INTO table objects.
 */
case class ResolveDefaultColumns(
  analyzer: Analyzer,
  catalog: SessionCatalog) extends Rule[LogicalPlan] {

  // This field stores the enclosing INSERT INTO command, once we find one.
  var enclosingInsert: Option[InsertIntoStatement] = None

  override def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsWithPruning(
    (_ => SQLConf.get.enableDefaultColumns), ruleId) {
    case i@InsertIntoStatement(_, _, _, _, _, _)
      if i.query.collectFirst { case u: UnresolvedInlineTable => u }.isDefined =>
      enclosingInsert = Some(i)
      i

    case table: UnresolvedInlineTable
      if enclosingInsert.isDefined &&
        table.rows.nonEmpty && table.rows.forall(_.size == table.rows(0).size) =>
      val expanded: UnresolvedInlineTable = addMissingDefaultColumnValues(table).getOrElse(table)
      replaceExplicitDefaultColumnValues(analyzer, expanded).getOrElse(table)

    case i@InsertIntoStatement(_, _, _, project: Project, _, _) =>
      enclosingInsert = Some(i)
      val expanded: Project = addMissingDefaultColumnValues(project).getOrElse(project)
      val replaced: Option[LogicalPlan] = replaceExplicitDefaultColumnValues(analyzer, expanded)
      if (replaced.isDefined) i.copy(query = replaced.get) else i
  }

  // Helper method to check if an expression is an explicit DEFAULT column reference.
  private def isExplicitDefaultColumn(expr: Expression): Boolean = expr match {
    case u: UnresolvedAttribute if u.name.equalsIgnoreCase(CURRENT_DEFAULT_COLUMN_NAME) => true
    case _ => false
  }

  /**
   * Updates an inline table to generate missing default column values.
   */
  private def addMissingDefaultColumnValues(
    table: UnresolvedInlineTable): Option[UnresolvedInlineTable] = {
    assert(enclosingInsert.isDefined)
    val numQueryOutputs: Int = table.rows(0).size
    val schema = getInsertTableSchemaWithoutPartitionColumns.getOrElse(return None)
    val newDefaultExpressions: Seq[Expression] = getDefaultExpressions(numQueryOutputs, schema)
    val newNames: Seq[String] = schema.fields.drop(numQueryOutputs).map { _.name }
    if (newDefaultExpressions.nonEmpty) {
      Some(table.copy(
        names = table.names ++ newNames,
        rows = table.rows.map { row => row ++ newDefaultExpressions }))
    } else {
      None
    }
  }

  /**
   * Adds a new expressions to a projection to generate missing default column values.
   */
  private def addMissingDefaultColumnValues(project: Project): Option[Project] = {
    val numQueryOutputs: Int = project.projectList.size
    val schema = getInsertTableSchemaWithoutPartitionColumns.getOrElse(return None)
    val newDefaultExpressions: Seq[Expression] = getDefaultExpressions(numQueryOutputs, schema)
    val newAliases: Seq[NamedExpression] =
      newDefaultExpressions.zip(schema.fields).map {
        case (expr, field) => Alias(expr, field.name)()
      }
    if (newDefaultExpressions.nonEmpty) {
      Some(project.copy(projectList = project.projectList ++ newAliases))
    } else {
      None
    }
  }

  /**
   * This is a helper for the addMissingDefaultColumnValues methods above.
   */
  private def getDefaultExpressions(numQueryOutputs: Int, schema: StructType): Seq[Expression] = {
    val remainingFields: Seq[StructField] = schema.fields.drop(numQueryOutputs)
    val numDefaultExpressionsToAdd: Int = {
      if (SQLConf.get.useNullsForMissingDefaultColumnValues) {
        remainingFields.size
      } else {
        remainingFields.takeWhile(_.metadata.contains(CURRENT_DEFAULT_COLUMN_METADATA_KEY)).size
      }
    }
    Seq.fill(numDefaultExpressionsToAdd)(UnresolvedAttribute(CURRENT_DEFAULT_COLUMN_NAME))
  }

  /**
   * Replaces unresolved DEFAULT column references with corresponding values in a logical plan.
   */
  private def replaceExplicitDefaultColumnValues(
    analyzer: Analyzer,
    input: LogicalPlan): Option[LogicalPlan] = {
    assert(enclosingInsert.isDefined)
    val schema = getInsertTableSchemaWithoutPartitionColumns.getOrElse(return None)
    val columnNames: Seq[String] = schema.fields.map { _.name }
    val defaultExpressions: Seq[Expression] = schema.fields.map {
      case f if f.metadata.contains(CURRENT_DEFAULT_COLUMN_METADATA_KEY) =>
        analyze(analyzer, f, "INSERT")
      case _ => Literal(null)
    }
    // Check the type of `input` and replace its expressions accordingly.
    // If necessary, return a more descriptive error message if the user tries to nest the DEFAULT
    // column reference inside some other expression, such as DEFAULT + 1 (this is not allowed).
    //
    // Note that we don't need to check if "SQLConf.get.useNullsForMissingDefaultColumnValues"
    // after this point because this method only takes responsibility to replace *existing*
    // DEFAULT references. In contrast, the "getDefaultExpressions" method will check that config
    // and add new NULLs if needed.
    input match {
      case table: UnresolvedInlineTable =>
        replaceExplicitDefaultColumnValues(defaultExpressions, table)
      case project: Project =>
        replaceExplicitDefaultColumnValues(defaultExpressions, columnNames, project)
    }
  }

  /**
   * Replaces unresolved DEFAULT column references with corresponding values in an inline table.
   */
  private def replaceExplicitDefaultColumnValues(
    defaultExpressions: Seq[Expression],
    table: UnresolvedInlineTable): Option[LogicalPlan] = {
    var replaced = false
    val newRows: Seq[Seq[Expression]] = {
      table.rows.map { row: Seq[Expression] =>
        for {
          i <- 0 until row.size
          expr = row(i)
          defaultExpr = if (i < defaultExpressions.size) defaultExpressions(i) else Literal(null)
        } yield expr match {
          case u: UnresolvedAttribute if isExplicitDefaultColumn(u) =>
            replaced = true
            defaultExpr
          case expr@_ if expr.find { isExplicitDefaultColumn }.isDefined =>
            throw new AnalysisException(DEFAULTS_IN_EXPRESSIONS_ERROR)
          case _ => expr
        }
      }
    }
    if (replaced) {
      Some(table.copy(rows = newRows))
    } else {
      None
    }
  }

  /**
   * Replaces unresolved DEFAULT column references with corresponding values in an projection.
   */
  private def replaceExplicitDefaultColumnValues(
    defaultExpressions: Seq[Expression],
    colNames: Seq[String],
    project: Project): Option[LogicalPlan] = {
    var replaced = false
    val updated: Seq[NamedExpression] = {
      for {
        i <- 0 until project.projectList.size
        projectExpr = project.projectList(i)
        defaultExpr = if (i < defaultExpressions.size) defaultExpressions(i) else Literal(null)
        colName = if (i < colNames.size) colNames(i) else ""
      } yield projectExpr match {
        case Alias(u: UnresolvedAttribute, _) if isExplicitDefaultColumn(u) =>
          replaced = true
          Alias(defaultExpr, colName)()
        case u: UnresolvedAttribute if isExplicitDefaultColumn(u) =>
          replaced = true
          Alias(defaultExpr, colName)()
        case expr@_ if expr.find { isExplicitDefaultColumn }.isDefined =>
          throw new AnalysisException(DEFAULTS_IN_EXPRESSIONS_ERROR)
        case _ => projectExpr
      }
    }
    if (replaced) {
      Some(project.copy(projectList = updated))
    } else {
      None
    }
  }

  /**
   * Looks up the schema for the table object of an INSERT INTO statement from the catalog.
   */
  private def getInsertTableSchemaWithoutPartitionColumns: Option[StructType] = {
    assert(enclosingInsert.isDefined)
    try {
      val tableName = enclosingInsert.get.table match {
        case r: UnresolvedRelation => TableIdentifier(r.name)
        case r: UnresolvedCatalogRelation => r.tableMeta.identifier
        case _ => return None
      }
      val lookup = catalog.lookupRelation(tableName)
      lookup match {
        case SubqueryAlias(_, r: UnresolvedCatalogRelation) =>
          Some(StructType(r.tableMeta.schema.fields.dropRight(
            enclosingInsert.get.partitionSpec.size)))
        case _ => None
      }
    } catch {
      case _: NoSuchTableException => None
    }
  }
}
