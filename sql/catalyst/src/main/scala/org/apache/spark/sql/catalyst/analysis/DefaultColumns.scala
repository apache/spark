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
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.expressions.{Expression, _}
import org.apache.spark.sql.catalyst.optimizer.ConstantFolding
import org.apache.spark.sql.catalyst.parser.{CatalystSqlParser, ParseException}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

/**
 * This class contains logic for processing DEFAULT columns in statements such as CREATE TABLE.
 */
object DefaultColumns {
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
   * Finds "exists default" expressions in CREATE/REPLACE TABLE commands and constant-folds them.
   *
   * For example, in the event of this statement:
   *
   * CREATE TABLE T(a INT, b INT DEFAULT 5 + 5)
   *
   * This method constant-folds the "exists default" value, stored in the EXISTS_DEFAULT metadata of
   * the column, to "10". Meanwhile the "current default" value, stored in the CURRENT_DEFAULT
   * metadata of the column, retains its original value of "5 + 5".
   *
   * @param tableSchema represents the names and types of the columns of the statement to process.
   * @param statementType name of the statement being processed, such as INSERT; useful for errors.
   * @return a copy of `tableSchema` with field metadata updated with the constant-folded values.
   */
  def constantFoldExistsDefaultExpressions(
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
   * Adds a projection over the plan in `insert.query` generating missing default column values.
   *
   * @param insert the INSERT INTO statement to add missing DEFAULT column references to.
   * @param catalog the catalog to use for looking up the schema of the INSERT INTO table object.
   * @return the updated statement with missing DEFAULT column values appended to the list.
   */
  def addProjectionForMissingDefaultColumnValues(
      insert: InsertIntoStatement, catalog: SessionCatalog): InsertIntoStatement = {
    if (!SQLConf.get.enableDefaultColumns) {
      return insert
    }
    // Compute the number of attributes returned by the INSERT INTO statement.
    val numQueryOutputs: Int = insert.query match {
      case table: UnresolvedInlineTable
        if table.rows.nonEmpty && table.rows.forall(_.size == table.rows(0).size) =>
        table.rows(0).size
      case project: Project => project.projectList.size
      case _ => return insert
    }
    // Determine the number of new DEFAULT unresolved attribute references to add.
    val schema: StructType = getInsertTableSchema(insert, catalog).getOrElse(return insert)
    val schemaWithoutPartitionCols = StructType(schema.fields.dropRight(insert.partitionSpec.size))
    val numDefaultExprs: Int = schemaWithoutPartitionCols.fields.drop(numQueryOutputs).size
    val newDefaultExprs: Seq[Expression] =
      Seq.fill(numDefaultExprs)(UnresolvedAttribute(CURRENT_DEFAULT_COLUMN_NAME))
    // Finally, return a projection of the original `insert.query` output attributes plus new
    // aliases over the DEFAULT column values.
    // If the insertQuery is an existing Project, flatten them together.
    val newQuery = insert.query match {
      case Project(projectList, child) =>
        val newAliases: Seq[NamedExpression] =
          newDefaultExprs.zip(schemaWithoutPartitionCols.fields).map {
            case (expr, field) => Alias(expr, field.name)()
          }
        Project(projectList ++ newAliases, child)
      case table: UnresolvedInlineTable =>
        val newNames: Seq[String] =
          schemaWithoutPartitionCols.fields.drop(numQueryOutputs).map { _.name }
        table.copy(names = table.names ++ newNames,
          rows = table.rows.map { row => row ++ newDefaultExprs })
      case _ => insert.query
    }
    insert.copy(query = newQuery)
  }

  /**
   * Replaces unresolved "DEFAULT" column references with matching default column values.
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
   * @param insert the INSERT INTO statement to replace explicit DEFAULT column references within.
   * @param catalog the catalog to use for looking up the schema of the INSERT INTO table object.
   * @return the updated statement with DEFAULT column references replaced with their values.
   */
  def replaceExplicitDefaultColumnValues(
      insert: InsertIntoStatement, catalog: SessionCatalog): InsertIntoStatement = {
    if (!SQLConf.get.enableDefaultColumns) {
      return insert
    }
    // Extract the list of DEFAULT column values from the INSERT INTO statement.
    val schema: StructType = getInsertTableSchema(insert, catalog).getOrElse(return insert)
    val schemaWithoutPartitionCols = StructType(schema.fields.dropRight(insert.partitionSpec.size))
    val colNames: Seq[String] = schemaWithoutPartitionCols.fields.map { _.name }
    val defaultExprs: Seq[Expression] = schemaWithoutPartitionCols.fields.map {
      case f if f.metadata.contains(CURRENT_DEFAULT_COLUMN_METADATA_KEY) => analyze(f, "INSERT")
      case _ => Literal(null)
    }
    // Handle two types of logical query plans in the target of the INSERT INTO statement:
    // Inline table: VALUES (0, 1, DEFAULT, ...)
    // Projection: SELECT 0, 1, DEFAULT, ...
    // Note that the DEFAULT reference may not participate in complex expressions such as
    // "DEFAULT + 2"; this generally results in a "not found" error later in the analyzer.
    val newQuery: LogicalPlan = insert.query match {
      case table: UnresolvedInlineTable
        if table.rows.nonEmpty && table.rows.forall(_.size == defaultExprs.size) =>
        val newRows: Seq[Seq[Expression]] =
          table.rows.map { row: Seq[Expression] =>
            // Map each row of the VALUES list to its corresponding DEFAULT expression in the
            // INSERT INTO object table, if the two lists are equal in length.
            row.zip(defaultExprs).map {
              case (expr: Expression, defaultExpr: Expression) =>
                expr match {
                  case u: UnresolvedAttribute
                    if u.name.equalsIgnoreCase(CURRENT_DEFAULT_COLUMN_NAME) => defaultExpr
                  case _ => expr
                }
            }
          }
        table.copy(rows = newRows)
      case project: Project if project.projectList.size == defaultExprs.size =>
        val updated: Seq[NamedExpression] =
          // Map each expression of the project list to its corresponding DEFAULT expression in the
          // INSERT INTO object table, if the two lists are equal in length.
          (project.projectList, defaultExprs, colNames).zipped.map {
            case (expr: Expression, defaultExpr: Expression, colName: String) =>
              expr match {
                case u: UnresolvedAttribute
                  if u.name.equalsIgnoreCase(CURRENT_DEFAULT_COLUMN_NAME) =>
                  Alias(defaultExpr, colName)()
                case Alias(u: UnresolvedAttribute, _)
                  if u.name.equalsIgnoreCase(CURRENT_DEFAULT_COLUMN_NAME) =>
                  Alias(defaultExpr, colName)()
                case _ => expr
              }
          }
        project.copy(projectList = updated)
      case _ => insert.query
    }
    insert.copy(query = newQuery)
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

  /**
   * Looks up the schema for the table object of an INSERT INTO statement from the catalog.
   *
   * @param insert the INSERT INTO statement to lookup the schema for.
   * @param catalog the catalog to use for looking up the schema of the INSERT INTO table object.
   * @return the schema of the indicated table, or None if the lookup found nothing.
   */
  private def getInsertTableSchema(
      insert: InsertIntoStatement, catalog: SessionCatalog): Option[StructType] = {
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
