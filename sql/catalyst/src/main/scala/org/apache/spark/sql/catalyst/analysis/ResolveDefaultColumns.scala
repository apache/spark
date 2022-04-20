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
import org.apache.spark.sql.catalyst.catalog.UnresolvedCatalogRelation
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.util.ResolveDefaultColumns._
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
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
 */
case class ResolveDefaultColumns(analyzer: Analyzer) extends Rule[LogicalPlan] {

  // This field stores the enclosing INSERT INTO command, once we find one.
  var enclosingInsert: Option[InsertIntoStatement] = None
  // This field stores the schema of the target table of the above command.
  var insertTableSchemaWithoutPartitionColumns: Option[StructType] = None
  // This field records if we've replaced an expression, useful for skipping unneeded copies.
  var updated: Boolean = false

  override def apply(plan: LogicalPlan): LogicalPlan = {
    // Initialize by clearing our reference to the enclosing INSERT INTO command.
    enclosingInsert = None
    insertTableSchemaWithoutPartitionColumns = None
    updated = false
    // Traverse the logical query plan in preorder (top-down).
    plan.resolveOperatorsWithPruning(
      (_ => SQLConf.get.enableDefaultColumns), ruleId) {
      case i@InsertIntoStatement(_, _, _, _, _, _)
        // Match against a VALUES list under any combination of projections and/or aggregates.
        if i.query.exists(t =>
            t.isInstanceOf[UnresolvedInlineTable]) &&
          !i.query.exists(t =>
            !t.isInstanceOf[UnresolvedInlineTable] &&
            !t.isInstanceOf[Project] &&
            !t.isInstanceOf[Aggregate] &&
            !t.isInstanceOf[SubqueryAlias]) =>
        enclosingInsert = Some(i)
        insertTableSchemaWithoutPartitionColumns = getInsertTableSchemaWithoutPartitionColumns
        val regenerated: InsertIntoStatement = regenerateUserSpecifiedCols(i)
        regenerated

      case table: UnresolvedInlineTable
        if enclosingInsert.isDefined &&
          table.rows.nonEmpty && table.rows.forall(_.size == table.rows(0).size) =>
        updated = false
        val expanded: UnresolvedInlineTable = addMissingDefaultValuesForInsertFromInlineTable(table)
        val replaced: LogicalPlan = replaceExplicitDefaultValuesForLogicalPlan(analyzer, expanded)
        if (updated) {
          replaced
        } else {
          table
        }

      case i@InsertIntoStatement(_, _, _, project: Project, _, _) =>
        enclosingInsert = Some(i)
        insertTableSchemaWithoutPartitionColumns = getInsertTableSchemaWithoutPartitionColumns
        updated = false
        val expanded: Project = addMissingDefaultValuesForInsertFromProject(project)
        val replaced: LogicalPlan = replaceExplicitDefaultValuesForLogicalPlan(analyzer, expanded)
        val regenerated: InsertIntoStatement = regenerateUserSpecifiedCols(i.copy(query = replaced))
        if (updated) {
          regenerated
        } else {
          i
        }
    }
  }

  // Helper method to regenerate user-specified columns of an InsertIntoStatement based on the names
  // in the insertTableSchemaWithoutPartitionColumns field of this class.
  private def regenerateUserSpecifiedCols(i: InsertIntoStatement): InsertIntoStatement = {
    if (i.userSpecifiedCols.nonEmpty && insertTableSchemaWithoutPartitionColumns.isDefined) {
      i.copy(
        userSpecifiedCols = insertTableSchemaWithoutPartitionColumns.get.fields.map(_.name))
    } else {
      i
    }
  }

  // Helper method to check if an expression is an explicit DEFAULT column reference.
  private def isExplicitDefaultColumn(expr: Expression): Boolean = expr match {
    case u: UnresolvedAttribute if u.name.equalsIgnoreCase(CURRENT_DEFAULT_COLUMN_NAME) => true
    case _ => false
  }

  /**
   * Updates an inline table to generate missing default column values.
   */
  private def addMissingDefaultValuesForInsertFromInlineTable(
      table: UnresolvedInlineTable): UnresolvedInlineTable = {
    assert(enclosingInsert.isDefined)
    val numQueryOutputs: Int = table.rows(0).size
    val schema = insertTableSchemaWithoutPartitionColumns.getOrElse(return table)
    val newDefaultExpressions: Seq[Expression] =
      getDefaultExpressionsForInsert(numQueryOutputs, schema)
    val newNames: Seq[String] = schema.fields.drop(numQueryOutputs).map { _.name }
    if (newDefaultExpressions.nonEmpty) updated = true
    table.copy(
      names = table.names ++ newNames,
      rows = table.rows.map { row => row ++ newDefaultExpressions })
  }

  /**
   * Adds a new expressions to a projection to generate missing default column values.
   */
  private def addMissingDefaultValuesForInsertFromProject(project: Project): Project = {
    val numQueryOutputs: Int = project.projectList.size
    val schema = insertTableSchemaWithoutPartitionColumns.getOrElse(return project)
    val newDefaultExpressions: Seq[Expression] =
      getDefaultExpressionsForInsert(numQueryOutputs, schema)
    val newAliases: Seq[NamedExpression] =
      newDefaultExpressions.zip(schema.fields).map {
        case (expr, field) => Alias(expr, field.name)()
      }
    if (newDefaultExpressions.nonEmpty) updated = true
    project.copy(projectList = project.projectList ++ newAliases)
  }

  /**
   * This is a helper for the addMissingDefaultValues* methods above.
   */
  private def getDefaultExpressionsForInsert(
      numQueryOutputs: Int,
      schema: StructType): Seq[Expression] = {
    val remainingFields: Seq[StructField] = schema.fields.drop(numQueryOutputs)
    val numDefaultExpressionsToAdd = getStructFieldsForDefaultExpressions(remainingFields).size
    Seq.fill(numDefaultExpressionsToAdd)(UnresolvedAttribute(CURRENT_DEFAULT_COLUMN_NAME))
  }

  /**
   * This is a helper for the getDefaultExpressionsForInsert method above.
   */
  private def getStructFieldsForDefaultExpressions(fields: Seq[StructField]): Seq[StructField] = {
    if (SQLConf.get.useNullsForMissingDefaultColumnValues) {
      fields
    } else {
      fields.takeWhile(_.metadata.contains(CURRENT_DEFAULT_COLUMN_METADATA_KEY))
    }
  }

  /**
   * Replaces unresolved DEFAULT column references with corresponding values in a logical plan.
   */
  private def replaceExplicitDefaultValuesForLogicalPlan(
      analyzer: Analyzer,
      input: LogicalPlan): LogicalPlan = {
    assert(enclosingInsert.isDefined)
    val schema = insertTableSchemaWithoutPartitionColumns.getOrElse(return input)
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
        replaceExplicitDefaultValuesForInlineTable(defaultExpressions, table)
      case project: Project =>
        replaceExplicitDefaultValuesForProject(defaultExpressions, columnNames, project)
    }
  }

  /**
   * Replaces unresolved DEFAULT column references with corresponding values in an inline table.
   */
  private def replaceExplicitDefaultValuesForInlineTable(
      defaultExpressions: Seq[Expression],
      table: UnresolvedInlineTable): LogicalPlan = {
    val newRows: Seq[Seq[Expression]] = {
      table.rows.map { row: Seq[Expression] =>
        for {
          i <- 0 until row.size
          expr = row(i)
          defaultExpr = if (i < defaultExpressions.size) defaultExpressions(i) else Literal(null)
        } yield expr match {
          case u: UnresolvedAttribute if isExplicitDefaultColumn(u) =>
            updated = true
            defaultExpr
          case expr@_ if expr.find { isExplicitDefaultColumn }.isDefined =>
            throw new AnalysisException(DEFAULTS_IN_EXPRESSIONS_ERROR)
          case _ => expr
        }
      }
    }
    table.copy(rows = newRows)
  }

  /**
   * Replaces unresolved DEFAULT column references with corresponding values in a projection.
   */
  private def replaceExplicitDefaultValuesForProject(
      defaultExpressions: Seq[Expression],
      colNames: Seq[String],
      project: Project): LogicalPlan = {
    val newProjectList: Seq[NamedExpression] = {
      for {
        i <- 0 until project.projectList.size
        projectExpr = project.projectList(i)
        defaultExpr = if (i < defaultExpressions.size) defaultExpressions(i) else Literal(null)
        colName = if (i < colNames.size) colNames(i) else ""
      } yield projectExpr match {
        case Alias(u: UnresolvedAttribute, _) if isExplicitDefaultColumn(u) =>
          updated = true
          Alias(defaultExpr, colName)()
        case u: UnresolvedAttribute if isExplicitDefaultColumn(u) =>
          updated = true
          Alias(defaultExpr, colName)()
        case expr@_ if expr.find { isExplicitDefaultColumn }.isDefined =>
          throw new AnalysisException(DEFAULTS_IN_EXPRESSIONS_ERROR)
        case _ => projectExpr
      }
    }
    project.copy(projectList = newProjectList)
  }

  /**
   * Looks up the schema for the table object of an INSERT INTO statement from the catalog.
   */
  private def getInsertTableSchemaWithoutPartitionColumns: Option[StructType] = {
    assert(enclosingInsert.isDefined)
    // Lookup the relation from the catalog by name. This either succeeds or returns some "not
    // found" error. In the latter cases, return out of this rule without changing anything and let
    // the analyzer return a proper error message elsewhere.
    val lookup: LogicalPlan = try {
      analyzer.ResolveRelations(enclosingInsert.get.table)
    } catch {
      case _: AnalysisException => return None
    }
    val schema: StructType = lookup match {
      case SubqueryAlias(_, r: UnresolvedCatalogRelation) =>
        StructType(r.tableMeta.schema.fields.dropRight(
          enclosingInsert.get.partitionSpec.size))
      case SubqueryAlias(_, r: DataSourceV2Relation) =>
        StructType(r.schema.fields.dropRight(
          enclosingInsert.get.partitionSpec.size))
      case _ => return None
    }
    // Rearrange the columns in the result schema to match the order of the explicit column list,
    // if any.
    val userSpecifiedCols: Seq[String] = enclosingInsert.get.userSpecifiedCols
    if (userSpecifiedCols.isEmpty) {
      return Some(schema)
    }
    def normalize(str: String) = {
      if (SQLConf.get.getConf(SQLConf.CASE_SENSITIVE)) str else str.toLowerCase()
    }
    val colNamesToFields: Map[String, StructField] =
      schema.fields.map {
        field: StructField => normalize(field.name) -> field
      }.toMap
    val userSpecifiedFields: Seq[StructField] =
      userSpecifiedCols.map {
        name: String => colNamesToFields.getOrElse(normalize(name), return None)
      }
    val userSpecifiedColNames: Set[String] = userSpecifiedCols.map(normalize).toSet
    val nonUserSpecifiedFields: Seq[StructField] =
      schema.fields.filter {
        field => !userSpecifiedColNames.contains(normalize(field.name))
      }
    Some(StructType(userSpecifiedFields ++
      getStructFieldsForDefaultExpressions(nonUserSpecifiedFields)))
  }
}
