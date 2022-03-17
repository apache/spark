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
import org.apache.spark.sql.catalyst.analysis.ResolveDefaultColumns.{analyze, CURRENT_DEFAULT_COLUMN_METADATA_KEY, CURRENT_DEFAULT_COLUMN_NAME}
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.optimizer.ConstantFolding
import org.apache.spark.sql.catalyst.parser.{CatalystSqlParser, ParseException}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.AlwaysProcess
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

/**
 * This object contains fields to help process DEFAULT columns with the below rule of the same name.
 */
object ResolveDefaultColumns {
  // This column metadata indicates the default value associated with a particular table column that
  // is in effect at any given time. Its value begins at the time of the initial CREATE/REPLACE
  // TABLE statement with DEFAULT column definition(s), if any. It then changes whenever an ALTER
  // TABLE statement SETs the DEFAULT. The intent is for this "current default" to be used by
  // UPDATE, INSERT and MERGE, which evaluate each default expression for each row.
  val CURRENT_DEFAULT_COLUMN_METADATA_KEY = "CURRENT_DEFAULT"
  // This column metadata represents the default value for all existing rows in a table after a
  // column has been added. This value is determined at time of CREATE TABLE, REPLACE TABLE, or
  // ALTER TABLE ADD COLUMN, and never changes thereafter. The intent is for this "exist default"
  // to be used by any scan when the columns in the source row are incomplete.
  val EXISTS_DEFAULT_COLUMN_METADATA_KEY = "EXISTS_DEFAULT"
  // Name of attributes representing explicit references to the value stored in the above
  // CURRENT_DEFAULT_COLUMN_METADATA.
  val CURRENT_DEFAULT_COLUMN_NAME = "DEFAULT"

  /**
   * Finds "current default" expressions in CREATE/REPLACE TABLE columns and constant-folds them.
   *
   * The results are stored in the "exists default" metadata of the same columns. For example, in
   * the event of this statement:
   *
   * CREATE TABLE T(a INT, b INT DEFAULT 5 + 5)
   *
   * This method constant-folds the "current default" value, stored in the CURRENT_DEFAULT metadata
   * of the "b" column, to "10", storing the result in the "exists default" value within the
   * EXISTS_DEFAULT metadata of that same column. Meanwhile the "current default" metadata of this
   * "b" column retains its original value of "5 + 5".
   *
   * @param tableSchema represents the names and types of the columns of the statement to process.
   * @param statementType name of the statement being processed, such as INSERT; useful for errors.
   * @return a copy of `tableSchema` with field metadata updated with the constant-folded values.
   */
  def constantFoldCurrentDefaultsToExistDefaults(
      tableSchema: StructType, statementType: String): StructType = {
    if (!SQLConf.get.enableDefaultColumns) {
      return tableSchema
    }
    val newFields: Seq[StructField] = tableSchema.fields.map { field =>
      if (field.metadata.contains(EXISTS_DEFAULT_COLUMN_METADATA_KEY)) {
        val analyzed: Expression = analyze(field, statementType, foldConstants = true)
        val newMetadata: Metadata = new MetadataBuilder().withMetadata(field.metadata)
          .putString(EXISTS_DEFAULT_COLUMN_METADATA_KEY, analyzed.sql).build()
        field.copy(metadata = newMetadata)
      } else {
        field
      }
    }
    StructType(newFields)
  }

  /**
   * Parses and analyzes the DEFAULT column text in `field`, returning an error upon failure.
   *
   * @param field represents the DEFAULT column value whose "default" metadata to parse and analyze.
   * @param statementType which type of statement we are running, such as INSERT; useful for errors.
   * @param foldConstants if true, perform constant-folding on the analyzed value before returning.
   * @return Result of the analysis and constant-folding operation.
   */
  private def analyze(
      field: StructField, statementType: String, foldConstants: Boolean = false): Expression = {
    // Parse the expression.
    val colText: String = if (field.metadata.contains(CURRENT_DEFAULT_COLUMN_METADATA_KEY)) {
      field.metadata.getString(CURRENT_DEFAULT_COLUMN_METADATA_KEY)
    } else {
      "NULL"
    }
    val parsed: Expression = try {
      lazy val parser = new CatalystSqlParser()
      parser.parseExpression(colText)
    } catch {
      case ex: ParseException =>
        throw new AnalysisException(
          s"Failed to execute $statementType command because the destination table column " +
            s"${field.name} has a DEFAULT value of $colText which fails to parse as a valid " +
            s"expression: ${ex.getMessage}")
    }
    // Analyze the parse result.
    val plan = try {
      lazy val analyzer =
        new Analyzer(new SessionCatalog(new InMemoryCatalog, FunctionRegistry.builtin))
      val analyzed = analyzer.execute(Project(Seq(Alias(parsed, field.name)()), OneRowRelation()))
      analyzer.checkAnalysis(analyzed)
      if (foldConstants) ConstantFolding(analyzed) else analyzed
    } catch {
      case ex @ (_: ParseException | _: AnalysisException) =>
        val colText: String = if (field.metadata.contains(CURRENT_DEFAULT_COLUMN_METADATA_KEY)) {
          field.metadata.getString(CURRENT_DEFAULT_COLUMN_METADATA_KEY)
        } else {
          "NULL"
        }
        throw new AnalysisException(
          s"Failed to execute $statementType command because the destination table column " +
            s"${field.name} has a DEFAULT value of $colText which fails to resolve as a valid " +
            s"expression: ${ex.getMessage}")
    }
    val analyzed = plan match {
      case Project(Seq(a: Alias), OneRowRelation()) => a.child
    }
    // Perform implicit coercion from the provided expression type to the required column type.
    if (field.dataType == analyzed.dataType) {
      analyzed
    } else if (Cast.canUpCast(analyzed.dataType, field.dataType)) {
      Cast(analyzed, field.dataType)
    } else {
      throw new AnalysisException(
        s"Failed to execute $statementType command because the destination table column " +
          s"${field.name} has a DEFAULT value with type ${field.dataType}, but the " +
          s"statement provided a value of incompatible type ${analyzed.dataType}")
    }
  }
}

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
 * @param catalog the catalog to use for looking up the schema of INSERT INTO table objects.
 */
case class ResolveDefaultColumns(catalog: SessionCatalog) extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsWithPruning(
    AlwaysProcess.fn, ruleId) {
    // INSERT INTO t VALUES (...)
    case insert@InsertIntoStatement(_, _, _, table: UnresolvedInlineTable, _, _) if valid(table) =>
      val expanded = addMissingDefaultColumnValues(table, insert)
      val replaced = replaceExplicitDefaultColumnValues(expanded, insert)
      insert.copy(query = replaced)

    // INSERT INTO t VALUES (...) AS alias(...)
    case insert@InsertIntoStatement(
        _, _, _, alias@SubqueryAlias(_, table: UnresolvedInlineTable), _, _) if valid(table) =>
      val expanded = addMissingDefaultColumnValues(table, insert)
      val replaced = replaceExplicitDefaultColumnValues(expanded, insert)
      insert.copy(query = alias.copy(child = replaced))

    // INSERT INTO t SELECT * FROM VALUES (...)
    case insert@InsertIntoStatement(_, _, _, project@Project(_, table: UnresolvedInlineTable), _, _)
      if selectStar(project) && valid(table) =>
      val expanded = addMissingDefaultColumnValues(table, insert)
      val replaced = replaceExplicitDefaultColumnValues(expanded, insert)
      insert.copy(project.copy(child = replaced))

    // INSERT INTO t SELECT * FROM VALUES (...) AS alias(...)
    case insert@InsertIntoStatement(
        _, _, _, project@Project(_, alias@SubqueryAlias(_, table: UnresolvedInlineTable)), _, _)
      if selectStar(project) && valid(table) =>
      val expanded = addMissingDefaultColumnValues(table, insert)
      val replaced = replaceExplicitDefaultColumnValues(expanded, insert)
      insert.copy(query = project.copy(child = alias.copy(child = replaced)))

    // INSERT INTO t SELECT ... FROM ... (unresolved)
    case insert@InsertIntoStatement(_, _, _, project: Project, _, _) if !project.resolved =>
      val replaced = replaceExplicitDefaultColumnValues(project, insert)
      insert.copy(query = replaced)

    // INSERT INTO t SELECT ... FROM ... (resolved)
    case insert@InsertIntoStatement(_, _, _, project: Project, _, _) if project.resolved =>
      val expanded = addMissingDefaultColumnValues(project, insert)
      val replaced = replaceExplicitDefaultColumnValues(expanded, insert)
      insert.copy(query = replaced)
  }

  // Helper method to check that an inline table has an equal number of values in each row.
  private def valid(table: UnresolvedInlineTable): Boolean =
    table.rows.nonEmpty && table.rows.forall(_.size == table.rows(0).size)

  // Helper method to check that a projection is a simple SELECT *.
  private def selectStar(project: Project): Boolean =
    project.projectList.size == 1 && project.projectList(0).isInstanceOf[UnresolvedStar]

  // Each of the following methods adds a projection over the input plan to generate missing default
  // column values.
  private def addMissingDefaultColumnValues(
      table: UnresolvedInlineTable, insert: InsertIntoStatement): UnresolvedInlineTable = {
    val numQueryOutputs: Int = table.rows(0).size
    val schema: StructType = getInsertTableSchema(insert).getOrElse(return table)
    val schemaWithoutPartitionCols = StructType(schema.fields.dropRight(insert.partitionSpec.size))
    val numNonPartitionCols: Int = schemaWithoutPartitionCols.fields.drop(numQueryOutputs).size
    val numColsWithExplicitDefaults: Int =
      schema.fields.reverse.dropWhile(!_.metadata.contains(CURRENT_DEFAULT_COLUMN_NAME)).length
    val numDefaultExprsToAdd: Int = {
      if (SQLConf.get.useNullsForMissingDefaultColumnValues) {
        numNonPartitionCols
      } else {
        numNonPartitionCols.min(numColsWithExplicitDefaults)
      }
    }
    val newDefaultExprs: Seq[Expression] =
      Seq.fill(numDefaultExprsToAdd)(UnresolvedAttribute(CURRENT_DEFAULT_COLUMN_NAME))
    val newNames: Seq[String] =
      schemaWithoutPartitionCols.fields.drop(numQueryOutputs).map { _.name }
    table.copy(names = table.names ++ newNames,
      rows = table.rows.map { row => row ++ newDefaultExprs })
  }

  private def addMissingDefaultColumnValues(
      project: Project, insert: InsertIntoStatement): Project = {
    val numQueryOutputs: Int = project.projectList.size
    val schema: StructType = getInsertTableSchema(insert).getOrElse(return project)
    val schemaWithoutPartitionCols = StructType(schema.fields.dropRight(insert.partitionSpec.size))
    val numNonPartitionCols: Int = schemaWithoutPartitionCols.fields.drop(numQueryOutputs).size
    val numColsWithExplicitDefaults: Int =
      schema.fields.reverse.dropWhile(!_.metadata.contains(CURRENT_DEFAULT_COLUMN_NAME)).length
    val numDefaultExprsToAdd: Int = {
      if (SQLConf.get.useNullsForMissingDefaultColumnValues) {
        numNonPartitionCols
      } else {
        numNonPartitionCols.min(numColsWithExplicitDefaults)
      }
    }
    val newDefaultExprs: Seq[Expression] =
      Seq.fill(numDefaultExprsToAdd)(UnresolvedAttribute(CURRENT_DEFAULT_COLUMN_NAME))
    val newAliases: Seq[NamedExpression] =
      newDefaultExprs.zip(schemaWithoutPartitionCols.fields).map {
        case (expr, field) => Alias(expr, field.name)()
      }
    project.copy(projectList = project.projectList ++ newAliases)
  }

  // Each of the following methods replaces unresolved "DEFAULT" column references with matching
  // default column values.
  private def replaceExplicitDefaultColumnValues(
      table: UnresolvedInlineTable, insert: InsertIntoStatement): UnresolvedInlineTable = {
    val schema: StructType = getInsertTableSchema(insert).getOrElse(return table)
    val schemaWithoutPartitionCols =
      StructType(schema.fields.dropRight(insert.partitionSpec.size))
    val defaultExprs: Seq[Expression] = schemaWithoutPartitionCols.fields.map {
      case f if f.metadata.contains(CURRENT_DEFAULT_COLUMN_METADATA_KEY) => analyze(f, "INSERT")
      case _ => Literal(null)
    }
    val newRows: Seq[Seq[Expression]] =
      table.rows.map { row: Seq[Expression] =>
        for {
          i <- 0 until row.size
          expr = row(i)
          defaultExpr = if (i < defaultExprs.size) defaultExprs(i) else Literal(null)
        } yield expr match {
          case u: UnresolvedAttribute
            if u.name.equalsIgnoreCase(CURRENT_DEFAULT_COLUMN_NAME) => defaultExpr
          case _ => expr
        }
      }
    table.copy(rows = newRows)
  }

  private def replaceExplicitDefaultColumnValues(
      project: Project, insert: InsertIntoStatement): Project = {
    val schema: StructType = getInsertTableSchema(insert).getOrElse(return project)
    val schemaWithoutPartitionCols =
      StructType(schema.fields.dropRight(insert.partitionSpec.size))
    val colNames: Seq[String] = schemaWithoutPartitionCols.fields.map { _.name }
    val defaultExprs: Seq[Expression] = schemaWithoutPartitionCols.fields.map {
      case f if f.metadata.contains(CURRENT_DEFAULT_COLUMN_METADATA_KEY) => analyze(f, "INSERT")
      case _ => Literal(null)
    }
    val updated: Seq[NamedExpression] = {
      for {
        i <- 0 until project.projectList.size
        projectExpr = project.projectList(i)
        defaultExpr = if (i < defaultExprs.size) defaultExprs(i) else Literal(null)
        colName = if (i < colNames.size) colNames(i) else ""
      } yield projectExpr match {
        case Alias(u: UnresolvedAttribute, _)
          if u.name.equalsIgnoreCase(CURRENT_DEFAULT_COLUMN_NAME) =>
          Alias(defaultExpr, colName)()
        case u: UnresolvedAttribute
          if u.name.equalsIgnoreCase(CURRENT_DEFAULT_COLUMN_NAME) =>
          Alias(defaultExpr, colName)()
        case _ => projectExpr
      }
    }
    project.copy(projectList = updated)
  }

  /**
   * Looks up the schema for the table object of an INSERT INTO statement from the catalog.
   *
   * @return the schema of the indicated table, or None if the lookup found nothing.
   */
  private def getInsertTableSchema(insert: InsertIntoStatement): Option[StructType] = {
    val lookup = try {
      val tableName = insert.table match {
        case r: UnresolvedRelation => TableIdentifier(r.name)
        case r: UnresolvedCatalogRelation => r.tableMeta.identifier
        case _ => return None
      }
      catalog.lookupRelation(tableName)
    } catch {
      case _: NoSuchTableException => return None
    }
    lookup match {
      case SubqueryAlias(_, r: UnresolvedCatalogRelation) => Some(r.tableMeta.schema)
      case _ => None
    }
  }
}
