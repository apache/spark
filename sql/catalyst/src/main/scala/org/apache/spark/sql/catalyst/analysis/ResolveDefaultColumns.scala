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

import org.apache.spark.sql.catalyst.catalog.UnresolvedCatalogRelation
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.util.ResolveDefaultColumns._
import org.apache.spark.sql.connector.catalog.CatalogV2Util
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
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
 * @param resolveRelation function to resolve relations from the catalog. This should generally map
 *                        to the 'resolveRelationOrTempView' method of the ResolveRelations rule.
 */
case class ResolveDefaultColumns(
    resolveRelation: UnresolvedRelation => LogicalPlan) extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = {
    plan.resolveOperatorsWithPruning(
      (_ => SQLConf.get.enableDefaultColumns), ruleId) {
      case i: InsertIntoStatement if insertsFromInlineTable(i) =>
        resolveDefaultColumnsForInsertFromInlineTable(i)
      case i: InsertIntoStatement if insertsFromProject(i).isDefined =>
        resolveDefaultColumnsForInsertFromProject(i)
      case u: UpdateTable =>
        resolveDefaultColumnsForUpdate(u)
      case m: MergeIntoTable =>
        resolveDefaultColumnsForMerge(m)
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
      case r: LocalRelation
        if r.data.nonEmpty && r.data.forall(_.numFields == r.data(0).numFields) =>
        true
      case _ =>
        false
    }
  }

  /**
   * Checks if a logical plan is an INSERT INTO command where the inserted data comes from a SELECT
   * list, with possible other unary operators like sorting and/or alias(es) in between.
   */
  private def insertsFromProject(i: InsertIntoStatement): Option[Project] = {
    var node = i.query
    def matches(node: LogicalPlan): Boolean = node match {
      case _: GlobalLimit | _: LocalLimit | _: Offset | _: SubqueryAlias | _: Sort => true
      case _ => false
    }
    while (matches(node)) {
      node = node.children.head
    }
    node match {
      case p: Project => Some(p)
      case _ => None
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
    val insertTableSchemaWithoutPartitionColumns: Option[StructType] =
      getInsertTableSchemaWithoutPartitionColumns(i)
    insertTableSchemaWithoutPartitionColumns.map { schema: StructType =>
      val regenerated: InsertIntoStatement =
        regenerateUserSpecifiedCols(i, schema)
      val (expanded: LogicalPlan, addedDefaults: Boolean) =
        addMissingDefaultValuesForInsertFromInlineTable(node, schema, i.userSpecifiedCols.size)
      val replaced: Option[LogicalPlan] =
        replaceExplicitDefaultValuesForInputOfInsertInto(schema, expanded, addedDefaults)
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
      val project: Project = insertsFromProject(i).get
      if (project.projectList.exists(_.isInstanceOf[Star])) {
        i
      } else {
        val (expanded: Project, addedDefaults: Boolean) =
          addMissingDefaultValuesForInsertFromProject(project, schema, i.userSpecifiedCols.size)
        val replaced: Option[LogicalPlan] =
          replaceExplicitDefaultValuesForInputOfInsertInto(schema, expanded, addedDefaults)
        replaced.map { r =>
          // Replace the INSERT INTO source relation, copying unary operators until we reach the
          // original projection which we replace with the new projection with new values.
          def replace(plan: LogicalPlan): LogicalPlan = plan match {
            case _: Project => r
            case u: UnaryNode => u.withNewChildren(Seq(replace(u.child)))
          }
          regenerated.copy(query = replace(regenerated.query))
        }.getOrElse(i)
      }
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
        case f if f.metadata.contains(CURRENT_DEFAULT_COLUMN_METADATA_KEY) => analyze(f, "UPDATE")
        case _ => Literal(null)
      }
      // Create a map from each column name in the target table to its DEFAULT expression.
      val columnNamesToExpressions: Map[String, Expression] =
        mapStructFieldNamesToExpressions(schema, defaultExpressions)
      // For each assignment in the UPDATE command's SET clause with a DEFAULT column reference on
      // the right-hand side, look up the corresponding expression from the above map.
      val newAssignments: Option[Seq[Assignment]] =
      replaceExplicitDefaultValuesForUpdateAssignments(
        u.assignments, CommandType.Update, columnNamesToExpressions)
      newAssignments.map { n =>
        u.copy(assignments = n)
      }.getOrElse(u)
    }.getOrElse(u)
  }

  /**
   * Resolves DEFAULT column references for a MERGE INTO command.
   */
  private def resolveDefaultColumnsForMerge(m: MergeIntoTable): LogicalPlan = {
    val schema: StructType = getSchemaForTargetTable(m.targetTable).getOrElse(return m)
    // Return a more descriptive error message if the user tries to use a DEFAULT column reference
    // inside an UPDATE command's WHERE clause; this is not allowed.
    m.mergeCondition.foreach { c: Expression =>
      if (c.find(isExplicitDefaultColumn).isDefined) {
        throw QueryCompilationErrors.defaultReferencesNotAllowedInMergeCondition()
      }
    }
    val defaultExpressions: Seq[Expression] = schema.fields.map {
      case f if f.metadata.contains(CURRENT_DEFAULT_COLUMN_METADATA_KEY) => analyze(f, "MERGE")
      case _ => Literal(null)
    }
    val columnNamesToExpressions: Map[String, Expression] =
      mapStructFieldNamesToExpressions(schema, defaultExpressions)
    var replaced = false
    val newMatchedActions: Seq[MergeAction] = m.matchedActions.map { action: MergeAction =>
      replaceExplicitDefaultValuesInMergeAction(action, columnNamesToExpressions).map { r =>
        replaced = true
        r
      }.getOrElse(action)
    }
    val newNotMatchedActions: Seq[MergeAction] = m.notMatchedActions.map { action: MergeAction =>
      replaceExplicitDefaultValuesInMergeAction(action, columnNamesToExpressions).map { r =>
        replaced = true
        r
      }.getOrElse(action)
    }
    val newNotMatchedBySourceActions: Seq[MergeAction] =
      m.notMatchedBySourceActions.map { action: MergeAction =>
      replaceExplicitDefaultValuesInMergeAction(action, columnNamesToExpressions).map { r =>
        replaced = true
        r
      }.getOrElse(action)
    }
    if (replaced) {
      m.copy(matchedActions = newMatchedActions,
        notMatchedActions = newNotMatchedActions,
        notMatchedBySourceActions = newNotMatchedBySourceActions)
    } else {
      m
    }
  }

  /**
   * Replaces unresolved DEFAULT column references with corresponding values in one action of a
   * MERGE INTO command.
   */
  private def replaceExplicitDefaultValuesInMergeAction(
      action: MergeAction,
      columnNamesToExpressions: Map[String, Expression]): Option[MergeAction] = {
    action match {
      case u: UpdateAction =>
        val replaced: Option[Seq[Assignment]] =
          replaceExplicitDefaultValuesForUpdateAssignments(
            u.assignments, CommandType.Merge, columnNamesToExpressions)
        replaced.map { r =>
          Some(u.copy(assignments = r))
        }.getOrElse(None)
      case i: InsertAction =>
        val replaced: Option[Seq[Assignment]] =
          replaceExplicitDefaultValuesForUpdateAssignments(
            i.assignments, CommandType.Merge, columnNamesToExpressions)
        replaced.map { r =>
          Some(i.copy(assignments = r))
        }.getOrElse(None)
      case _ => Some(action)
    }
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
   * Returns the resulting plan plus a boolean indicating whether such values were added.
   */
  def addMissingDefaultValuesForInsertFromInlineTable(
      node: LogicalPlan,
      insertTableSchemaWithoutPartitionColumns: StructType,
      numUserSpecifiedColumns: Int): (LogicalPlan, Boolean) = {
    val schema = insertTableSchemaWithoutPartitionColumns
    val newDefaultExpressions: Seq[UnresolvedAttribute] =
      getNewDefaultExpressionsForInsert(schema, numUserSpecifiedColumns, node.output.size)
    val newNames: Seq[String] = schema.fields.map(_.name)
    val resultPlan: LogicalPlan = node match {
      case _ if newDefaultExpressions.isEmpty =>
        node
      case table: UnresolvedInlineTable =>
        table.copy(
          names = newNames,
          rows = table.rows.map { row => row ++ newDefaultExpressions })
      case local: LocalRelation =>
        val newDefaultExpressionsRow = new GenericInternalRow(
          // Note that this code path only runs when there is a user-specified column list of fewer
          // column than the target table; otherwise, the above 'newDefaultExpressions' is empty and
          // we match the first case in this list instead.
          schema.fields.drop(local.output.size).map {
            case f if f.metadata.contains(CURRENT_DEFAULT_COLUMN_METADATA_KEY) =>
              analyze(f, "INSERT") match {
                case lit: Literal => lit.value
                case _ => null
              }
            case _ => null
          })
        LocalRelation(
          output = schema.toAttributes,
          data = local.data.map { row =>
            new JoinedRow(row, newDefaultExpressionsRow)
          })
      case _ =>
        node
    }
    (resultPlan, newDefaultExpressions.nonEmpty)
  }

  /**
   * Adds a new expressions to a projection to generate missing default column values.
   * Returns the logical plan plus a boolean indicating if such defaults were added.
   */
  private def addMissingDefaultValuesForInsertFromProject(
      project: Project,
      insertTableSchemaWithoutPartitionColumns: StructType,
      numUserSpecifiedColumns: Int): (Project, Boolean) = {
    val schema = insertTableSchemaWithoutPartitionColumns
    val newDefaultExpressions: Seq[Expression] =
      getNewDefaultExpressionsForInsert(schema, numUserSpecifiedColumns, project.projectList.size)
    val newAliases: Seq[NamedExpression] =
      newDefaultExpressions.zip(schema.fields).map {
        case (expr, field) => Alias(expr, field.name)()
      }
    (project.copy(projectList = project.projectList ++ newAliases),
      newDefaultExpressions.nonEmpty)
  }

  /**
   * This is a helper for the addMissingDefaultValuesForInsertFromInlineTable methods above.
   */
  private def getNewDefaultExpressionsForInsert(
      insertTableSchemaWithoutPartitionColumns: StructType,
      numUserSpecifiedColumns: Int,
      numProvidedValues: Int): Seq[UnresolvedAttribute] = {
    val remainingFields: Seq[StructField] = if (numUserSpecifiedColumns > 0) {
      insertTableSchemaWithoutPartitionColumns.fields.drop(numUserSpecifiedColumns)
    } else {
      Seq.empty
    }
    val numDefaultExpressionsToAdd = getStructFieldsForDefaultExpressions(remainingFields).size
      // Limit the number of new DEFAULT expressions to the difference of the number of columns in
      // the target table and the number of provided values in the source relation. This clamps the
      // total final number of provided values to the number of columns in the target table.
      .min(insertTableSchemaWithoutPartitionColumns.size - numProvidedValues)
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
      insertTableSchemaWithoutPartitionColumns: StructType,
      input: LogicalPlan,
      addedDefaults: Boolean): Option[LogicalPlan] = {
    val schema = insertTableSchemaWithoutPartitionColumns
    val defaultExpressions: Seq[Expression] = schema.fields.map {
      case f if f.metadata.contains(CURRENT_DEFAULT_COLUMN_METADATA_KEY) => analyze(f, "INSERT")
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
      case local: LocalRelation =>
        if (addedDefaults) {
          Some(local)
        } else {
          None
        }
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
          i <- row.indices
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
        i <- project.projectList.indices
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
    val Insert, Update, Merge = Value
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
          case CommandType.Merge =>
            throw QueryCompilationErrors
              .defaultReferencesNotAllowedInComplexExpressionsInMergeInsertsOrUpdates()
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
    schema.fields.zip(expressions).map {
      case (field: StructField, expression: Expression) =>
        normalizeFieldName(field.name) -> expression
    }.toMap
  }

  /**
   * Returns the schema for the target table of a DML command, looking into the catalog if needed.
   */
  private def getSchemaForTargetTable(table: LogicalPlan): Option[StructType] = {
    val resolved = table match {
      case r: UnresolvedRelation if !r.skipSchemaResolution && !r.isStreaming =>
        resolveRelation(r)
      case other =>
        other
    }
    resolved.collectFirst {
      case r: UnresolvedCatalogRelation =>
        r.tableMeta.schema
      case d: DataSourceV2Relation if !d.skipSchemaResolution && !d.isStreaming =>
        CatalogV2Util.v2ColumnsToStructType(d.table.columns())
      case v: View if v.isTempViewStoringAnalyzedPlan =>
        v.schema
    }
  }

  /**
   * Replaces unresolved DEFAULT column references with corresponding values in a series of
   * assignments in an UPDATE assignment, either comprising an UPDATE command or as part of a MERGE.
   */
  private def replaceExplicitDefaultValuesForUpdateAssignments(
      assignments: Seq[Assignment],
      command: CommandType.Value,
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
              command,
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
