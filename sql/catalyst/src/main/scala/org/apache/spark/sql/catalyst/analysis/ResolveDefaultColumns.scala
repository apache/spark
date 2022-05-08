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

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{SessionCatalog, UnresolvedCatalogRelation}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.util.ResolveDefaultColumns._
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

/**
 * This is a rule to process DEFAULT columns in statements such as CREATE/REPLACE TABLE.
 *
 * Background: CREATE TABLE and ALTER TABLE invocations support setting column default values for
 * later operations. Following INSERT, UPDATE, and MERGE commands may then reference the value
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
  override def apply(plan: LogicalPlan): LogicalPlan = {
    plan.resolveOperatorsWithPruning(
      (_ => SQLConf.get.enableDefaultColumns), ruleId) {
      case i: InsertIntoStatement if insertsFromInlineTable(i) =>
        resolveDefaultColumnsForInsertFromInlineTable(i)
      case i@InsertIntoStatement(_, _, _, project: Project, _, _)
        if !project.projectList.exists(_.isInstanceOf[Star]) =>
        resolveDefaultColumnsForInsertFromProject(i)
      case u: UpdateTable =>
        resolveDefaultColumnsForUpdate(u)
    }
  }

  /**
   * Checks if a logical plan is an INSERT INTO command where the inserted data comes from a VALUES
   * list, with possible projection(s), aggregate(s), and/or alias(es) in between.
   */
  private def insertsFromInlineTable(i: InsertIntoStatement): Boolean = {
    var query = i.query
    while (query.children.size == 1) {
      query match {
        case _: Project | _: Aggregate | _: SubqueryAlias =>
          query = query.children(0)
        case _ =>
          return false
      }
    }
    query match {
      case u: UnresolvedInlineTable
        if u.rows.nonEmpty && u.rows.forall(_.size == u.rows(0).size) =>
        true
      case _ =>
        false
    }
  }

  /**
   * Resolves DEFAULT column references for an INSERT INTO command satisfying the
   * [[insertsFromInlineTable]] method.
   */
  private def resolveDefaultColumnsForInsertFromInlineTable(i: InsertIntoStatement): LogicalPlan = {
    val children = mutable.Buffer.empty[LogicalPlan]
    var node = i.query
    while (node.children.size == 1) {
      children.append(node)
      node = node.children(0)
    }
    val table = node.asInstanceOf[UnresolvedInlineTable]
    val insertTableSchemaWithoutPartitionColumns: Option[StructType] =
      getInsertTableSchemaWithoutPartitionColumns(i)
    insertTableSchemaWithoutPartitionColumns.map { schema: StructType =>
      val regenerated: InsertIntoStatement =
        regenerateUserSpecifiedCols(i, schema)
      val expanded: UnresolvedInlineTable =
        addMissingDefaultValuesForInsertFromInlineTable(table, schema)
      val replaced: Option[LogicalPlan] =
        replaceExplicitDefaultValuesForInputOfInsertInto(analyzer, schema, expanded)
      replaced.map { r: LogicalPlan =>
        node = r
        for (child <- children.reverse) {
          node = child.withNewChildren(Seq(node))
        }
        regenerated.copy(query = node)
      }.getOrElse(i)
    }.getOrElse(i)
  }

  /**
   * Resolves DEFAULT column references for an INSERT INTO command whose query is a general
   * projection.
   */
  private def resolveDefaultColumnsForInsertFromProject(i: InsertIntoStatement): LogicalPlan = {
    val insertTableSchemaWithoutPartitionColumns: Option[StructType] =
      getInsertTableSchemaWithoutPartitionColumns(i)
    insertTableSchemaWithoutPartitionColumns.map { schema =>
      val regenerated: InsertIntoStatement = regenerateUserSpecifiedCols(i, schema)
      val project: Project = i.query.asInstanceOf[Project]
      val expanded: Project =
        addMissingDefaultValuesForInsertFromProject(project, schema)
      val replaced: Option[LogicalPlan] =
        replaceExplicitDefaultValuesForInputOfInsertInto(analyzer, schema, expanded)
      replaced.map { r =>
        regenerated.copy(query = r)
      }.getOrElse(i)
    }.getOrElse(i)
  }

  /**
   * Resolves DEFAULT column references for an UPDATE command.
   */
  private def resolveDefaultColumnsForUpdate(u: UpdateTable): LogicalPlan = {
    // Return a more descriptive error message if the user tries to use a DEFAULT column reference
    // inside an UPDATE command's WHERE clause; this is not allowed.
    u.condition.foreach { c: Expression =>
      if (c.find(isExplicitDefaultColumn).isDefined) {
        throw QueryCompilationErrors.defaultReferencesNotAllowedInUpdateWhereClause()
      }
    }
    val schemaForTargetTable: Option[StructType] = getSchemaForTargetTable(u.table)
    schemaForTargetTable.map { schema =>
      val defaultExpressions: Seq[Expression] = schema.fields.map {
        case f if f.metadata.contains(CURRENT_DEFAULT_COLUMN_METADATA_KEY) =>
          analyze(analyzer, f, "UPDATE")
        case _ => Literal(null)
      }
      // Create a map from each column name in the target table to its DEFAULT expression.
      val columnNamesToExpressions: Map[String, Expression] =
        mapStructFieldNamesToExpressions(schema, defaultExpressions)
      // For each assignment in the UPDATE command's SET clause with a DEFAULT column reference on
      // the right-hand side, look up the corresponding expression from the above map.
      val newAssignments: Option[Seq[Assignment]] =
      replaceExplicitDefaultValuesForUpdateAssignments(u.assignments, columnNamesToExpressions)
      newAssignments.map { n =>
        u.copy(assignments = n)
      }.getOrElse(u)
    }.getOrElse(u)
  }

  /**
   * Regenerates user-specified columns of an InsertIntoStatement based on the names in the
   * insertTableSchemaWithoutPartitionColumns field of this class.
   */
  private def regenerateUserSpecifiedCols(
      i: InsertIntoStatement,
      insertTableSchemaWithoutPartitionColumns: StructType): InsertIntoStatement = {
    if (i.userSpecifiedCols.nonEmpty) {
      i.copy(
        userSpecifiedCols = insertTableSchemaWithoutPartitionColumns.fields.map(_.name))
    } else {
      i
    }
  }

  /**
   * Returns true if an expression is an explicit DEFAULT column reference.
   */
  private def isExplicitDefaultColumn(expr: Expression): Boolean = expr match {
    case u: UnresolvedAttribute if u.name.equalsIgnoreCase(CURRENT_DEFAULT_COLUMN_NAME) => true
    case _ => false
  }

  /**
   * Updates an inline table to generate missing default column values.
   */
  private def addMissingDefaultValuesForInsertFromInlineTable(
      table: UnresolvedInlineTable,
      insertTableSchemaWithoutPartitionColumns: StructType): UnresolvedInlineTable = {
    val numQueryOutputs: Int = table.rows(0).size
    val schema = insertTableSchemaWithoutPartitionColumns
    val newDefaultExpressions: Seq[Expression] =
      getDefaultExpressionsForInsert(numQueryOutputs, schema)
    val newNames: Seq[String] = schema.fields.drop(numQueryOutputs).map { _.name }
    table.copy(
      names = table.names ++ newNames,
      rows = table.rows.map { row => row ++ newDefaultExpressions })
  }

  /**
   * Adds a new expressions to a projection to generate missing default column values.
   */
  private def addMissingDefaultValuesForInsertFromProject(
      project: Project,
      insertTableSchemaWithoutPartitionColumns: StructType): Project = {
    val numQueryOutputs: Int = project.projectList.size
    val schema = insertTableSchemaWithoutPartitionColumns
    val newDefaultExpressions: Seq[Expression] =
      getDefaultExpressionsForInsert(numQueryOutputs, schema)
    val newAliases: Seq[NamedExpression] =
      newDefaultExpressions.zip(schema.fields).map {
        case (expr, field) => Alias(expr, field.name)()
      }
    project.copy(projectList = project.projectList ++ newAliases)
  }

  /**
   * This is a helper for the addMissingDefaultValuesForInsertFromInlineTable methods above.
   */
  private def getDefaultExpressionsForInsert(
      numQueryOutputs: Int,
      schema: StructType): Seq[Expression] = {
    val remainingFields: Seq[StructField] = schema.fields.drop(numQueryOutputs)
    val numDefaultExpressionsToAdd = getStructFieldsForDefaultExpressions(remainingFields).size
    Seq.fill(numDefaultExpressionsToAdd)(UnresolvedAttribute(CURRENT_DEFAULT_COLUMN_NAME))
  }

  /**
   * This is a helper for the getDefaultExpressionsForInsert methods above.
   */
  private def getStructFieldsForDefaultExpressions(fields: Seq[StructField]): Seq[StructField] = {
    if (SQLConf.get.useNullsForMissingDefaultColumnValues) {
      fields
    } else {
      fields.takeWhile(_.metadata.contains(CURRENT_DEFAULT_COLUMN_METADATA_KEY))
    }
  }

  /**
   * Replaces unresolved DEFAULT column references with corresponding values in an INSERT INTO
   * command from a logical plan.
   */
  private def replaceExplicitDefaultValuesForInputOfInsertInto(
      analyzer: Analyzer,
      insertTableSchemaWithoutPartitionColumns: StructType,
      input: LogicalPlan): Option[LogicalPlan] = {
    val schema = insertTableSchemaWithoutPartitionColumns
    val defaultExpressions: Seq[Expression] = schema.fields.map {
      case f if f.metadata.contains(CURRENT_DEFAULT_COLUMN_METADATA_KEY) =>
        analyze(analyzer, f, "INSERT")
      case _ => Literal(null)
    }
    // Check the type of `input` and replace its expressions accordingly.
    // If necessary, return a more descriptive error message if the user tries to nest the DEFAULT
    // column reference inside some other expression, such as DEFAULT + 1 (this is not allowed).
    //
    // Note that we don't need to check if "SQLConf.get.useNullsForMissingDefaultColumnValues" after
    // this point because this method only takes responsibility to replace *existing* DEFAULT
    // references. In contrast, the "getDefaultExpressionsForInsert" method will check that config
    // and add new NULLs if needed.
    input match {
      case table: UnresolvedInlineTable =>
        replaceExplicitDefaultValuesForInlineTable(defaultExpressions, table)
      case project: Project =>
        replaceExplicitDefaultValuesForProject(defaultExpressions, project)
    }
  }

  /**
   * Replaces unresolved DEFAULT column references with corresponding values in an inline table.
   */
  private def replaceExplicitDefaultValuesForInlineTable(
      defaultExpressions: Seq[Expression],
      table: UnresolvedInlineTable): Option[LogicalPlan] = {
    var replaced = false
    val updated: Seq[Seq[Expression]] = {
      table.rows.map { row: Seq[Expression] =>
        for {
          i <- 0 until row.size
          expr = row(i)
          defaultExpr = if (i < defaultExpressions.size) defaultExpressions(i) else Literal(null)
        } yield replaceExplicitDefaultReferenceInExpression(
          expr, defaultExpr, CommandType.Insert, addAlias = false).map { e =>
          replaced = true
          e
        }.getOrElse(expr)
      }
    }
    if (replaced) {
      Some(table.copy(rows = updated))
    } else {
      None
    }
  }

  /**
   * Replaces unresolved DEFAULT column references with corresponding values in a projection.
   */
  private def replaceExplicitDefaultValuesForProject(
      defaultExpressions: Seq[Expression],
      project: Project): Option[LogicalPlan] = {
    var replaced = false
    val updated: Seq[NamedExpression] = {
      for {
        i <- 0 until project.projectList.size
        projectExpr = project.projectList(i)
        defaultExpr = if (i < defaultExpressions.size) defaultExpressions(i) else Literal(null)
      } yield replaceExplicitDefaultReferenceInExpression(
        projectExpr, defaultExpr, CommandType.Insert, addAlias = true).map { e =>
        replaced = true
        e.asInstanceOf[NamedExpression]
      }.getOrElse(projectExpr)
    }
    if (replaced) {
      Some(project.copy(projectList = updated))
    } else {
      None
    }
  }

  /**
   * Represents a type of command we are currently processing.
   */
  private object CommandType extends Enumeration {
    val Insert, Update = Value
  }

  /**
   * Checks if a given input expression is an unresolved "DEFAULT" attribute reference.
   *
   * @param input the input expression to examine.
   * @param defaultExpr the default to return if [[input]] is an unresolved "DEFAULT" reference.
   * @param isInsert the type of command we are currently processing.
   * @param addAlias if true, wraps the result with an alias of the original default column name.
   * @return [[defaultExpr]] if [[input]] is an unresolved "DEFAULT" attribute reference.
   */
  private def replaceExplicitDefaultReferenceInExpression(
      input: Expression,
      defaultExpr: Expression,
      command: CommandType.Value,
      addAlias: Boolean): Option[Expression] = {
    input match {
      case a@Alias(u: UnresolvedAttribute, _)
        if isExplicitDefaultColumn(u) =>
        Some(Alias(defaultExpr, a.name)())
      case u: UnresolvedAttribute
        if isExplicitDefaultColumn(u) =>
        if (addAlias) {
          Some(Alias(defaultExpr, u.name)())
        } else {
          Some(defaultExpr)
        }
      case expr@_
        if expr.find(isExplicitDefaultColumn).isDefined =>
        command match {
          case CommandType.Insert =>
            throw QueryCompilationErrors
              .defaultReferencesNotAllowedInComplexExpressionsInInsertValuesList()
          case CommandType.Update =>
            throw QueryCompilationErrors
              .defaultReferencesNotAllowedInComplexExpressionsInUpdateSetClause()
        }
      case _ =>
        None
    }
  }

  /**
   * Looks up the schema for the table object of an INSERT INTO statement from the catalog.
   */
  private def getInsertTableSchemaWithoutPartitionColumns(
      enclosingInsert: InsertIntoStatement): Option[StructType] = {
    val target: StructType = getSchemaForTargetTable(enclosingInsert.table).getOrElse(return None)
    val schema: StructType = StructType(target.fields.dropRight(enclosingInsert.partitionSpec.size))
    // Rearrange the columns in the result schema to match the order of the explicit column list,
    // if any.
    val userSpecifiedCols: Seq[String] = enclosingInsert.userSpecifiedCols
    if (userSpecifiedCols.isEmpty) {
      return Some(schema)
    }
    val colNamesToFields: Map[String, StructField] = mapStructFieldNamesToFields(schema)
    val userSpecifiedFields: Seq[StructField] =
      userSpecifiedCols.map {
        name: String => colNamesToFields.getOrElse(normalizeFieldName(name), return None)
      }
    val userSpecifiedColNames: Set[String] = userSpecifiedCols.toSet
    val nonUserSpecifiedFields: Seq[StructField] =
      schema.fields.filter {
        field => !userSpecifiedColNames.contains(field.name)
      }
    Some(StructType(userSpecifiedFields ++
      getStructFieldsForDefaultExpressions(nonUserSpecifiedFields)))
  }

  /**
   * Normalizes a schema field name suitable for use in map lookups.
   */
  private def normalizeFieldName(str: String): String = {
    if (SQLConf.get.caseSensitiveAnalysis) {
      str
    } else {
      str.toLowerCase()
    }
  }

  /**
   * Returns a map of the names of fields in a schema to the fields themselves.
   */
  private def mapStructFieldNamesToFields(schema: StructType): Map[String, StructField] = {
    schema.fields.map {
      field: StructField => normalizeFieldName(field.name) -> field
    }.toMap
  }

  /**
   * Returns a map of the names of fields in a schema to corresponding expressions.
   */
  private def mapStructFieldNamesToExpressions(
      schema: StructType,
      expressions: Seq[Expression]): Map[String, Expression] = {
    val namesToFields: Map[String, StructField] = mapStructFieldNamesToFields(schema)
    val namesAndExpressions: Seq[(String, Expression)] = namesToFields.keys.toSeq.zip(expressions)
    namesAndExpressions.toMap
  }

  /**
   * Returns the schema for the target table of a DML command, looking into the catalog if needed.
   */
  private def getSchemaForTargetTable(table: LogicalPlan): Option[StructType] = {
    // Check if the target table is already resolved. If so, return the computed schema.
    table match {
      case r: NamedRelation if r.schema.fields.nonEmpty => return Some(r.schema)
      case SubqueryAlias(_, r: NamedRelation) if r.schema.fields.nonEmpty => return Some (r.schema)
      case _ =>
    }
    // Lookup the relation from the catalog by name. This either succeeds or returns some "not
    // found" error. In the latter cases, return out of this rule without changing anything and let
    // the analyzer return a proper error message elsewhere.
    val tableName: TableIdentifier = table match {
      case r: UnresolvedRelation => TableIdentifier(r.name)
      case r: UnresolvedCatalogRelation => r.tableMeta.identifier
      case _ => return None
    }
    val lookup: LogicalPlan = try {
      catalog.lookupRelation(tableName)
    } catch {
      case _: AnalysisException => return None
    }
    lookup match {
      case SubqueryAlias(_, r: UnresolvedCatalogRelation) =>
        Some(r.tableMeta.schema)
      case SubqueryAlias(_, r: View) if r.isTempView =>
        Some(r.schema)
      case _ => None
    }
  }

  /**
   * Replaces unresolved DEFAULT column references with corresponding values in a series of
   * assignments in an UPDATE command.
   */
  private def replaceExplicitDefaultValuesForUpdateAssignments(
      assignments: Seq[Assignment],
      columnNamesToExpressions: Map[String, Expression]): Option[Seq[Assignment]] = {
    var replaced = false
    val newAssignments: Seq[Assignment] =
      for (assignment <- assignments) yield {
        val destColName = assignment.key match {
          case a: AttributeReference => a.name
          case u: UnresolvedAttribute => u.nameParts.last
          case _ => ""
        }
        val adjusted: String = normalizeFieldName(destColName)
        val lookup: Option[Expression] = columnNamesToExpressions.get(adjusted)
        val newValue: Expression = lookup.map { defaultExpr =>
          val updated: Option[Expression] =
            replaceExplicitDefaultReferenceInExpression(
              assignment.value,
              defaultExpr,
              CommandType.Update,
              addAlias = false)
          updated.map { e =>
            replaced = true
            e
          }.getOrElse(assignment.value)
        }.getOrElse(assignment.value)
        assignment.copy(value = newValue)
      }
    if (replaced) {
      Some(newAssignments)
    } else {
      None
    }
  }
}
