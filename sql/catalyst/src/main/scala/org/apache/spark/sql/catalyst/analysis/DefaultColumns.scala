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
import org.apache.spark.sql.types._

/**
 * This class contains logic for processing DEFAULT columns in statements such as CREATE TABLE.
 */
object DefaultColumns {
  val default = "default"
  val analysisPrefix =
    " has a DEFAULT value which fails to resolve to a valid constant expression: "
  val columnDefaultNotFound = "Column 'default' does not exist"
  lazy val parser = new CatalystSqlParser()
  lazy val analyzer =
    new Analyzer(new SessionCatalog(new InMemoryCatalog, FunctionRegistry.builtin))

  /**
   * Finds DEFAULT expressions in CREATE/REPLACE TABLE commands and constant-folds then.
   *
   * Example:
   * CREATE TABLE T(a INT, b INT DEFAULT 5 + 5) becomes
   * CREATE TABLE T(a INT, b INT DEFAULT 10)
   *
   * @param tableSchema represents the names and types of the columns of the statement to process.
   * @param statementType name of the statement being processed, such as INSERT; useful for errors.
   * @return a copy of `tableSchema` with field metadata updated with the constant-folded values.
   */
  def ConstantFoldDefaultExpressions(tableSchema: StructType, statementType: String): StructType = {
    // Get the list of column indexes in the CREATE TABLE command with DEFAULT values.
    val (fields: Array[StructField], indexes: Array[Int]) =
      tableSchema.fields.zipWithIndex.filter { case (f, i) => f.metadata.contains(default) }.unzip
    // Extract the list of DEFAULT column values from the CreateTable command.
    val colNames: Seq[String] = fields.map { _.name }
    val defaults: Seq[String] = fields.map { _.metadata.getString(default) }
    // Extract the list of DEFAULT column values from the CreateTable command.
    val exprs: Seq[Expression] = colNames.zip(defaults).map {
      case (name, text) => Parse(name, text, statementType)
    }
    // Analyze and constant-fold each parse result.
    val analyzed: Seq[Expression] = (exprs, defaults, colNames).zipped.map {
      case (expr, default, name) => Analyze(expr, default, name, statementType)
    }
    // Create a map from the column index of each DEFAULT column to its type.
    val indexMap: Map[Int, StructField] = (indexes, fields, analyzed).zipped.map {
      case (index, field, expr) =>
        val newMetadata: Metadata = new MetadataBuilder().withMetadata(field.metadata)
          .putString(default, expr.sql).build()
        (index, field.copy(metadata = newMetadata))
    }.toMap
    // Finally, replace the original struct fields with the new ones.
    val newFields: Seq[StructField] =
      tableSchema.fields.zipWithIndex.map { case (f, i) => indexMap.getOrElse(i, f) }
    StructType(newFields)
  }

  /**
   * Adds a projection over the plan in `insert.query` generating missing default column values.
   *
   * @param insert the INSERT INTO statement to add missing DEFAULT column references to.
   * @param catalog the catalog to use for looking up the schema of the INSERT INTO table object.
   * @return the updated statement with missing DEFAULT column values appended to the list.
   */
  def AddProjectionForMissingDefaultColumnValues(
      insert: InsertIntoStatement, catalog: SessionCatalog): InsertIntoStatement = {
    // Compute the number of attributes returned by the INSERT INTO statement.
    val numQueryOutputs: Int = insert.query match {
      case table: UnresolvedInlineTable
        if table.rows.nonEmpty && table.rows.forall(_.size == table.rows(0).size) =>
        table.rows(0).size
      case project: Project => project.projectList.size
      case _ => return insert
    }
    // The table value provides the DEFAULT column values as text; analyze them into expressions.
    val schema: StructType = getInsertTableSchema(insert, catalog).getOrElse(return insert)
    val schemaWithoutPartitionCols = StructType(schema.fields.dropRight(insert.partitionSpec.size))
    val coerced: Seq[Expression] = for {
      field <- schemaWithoutPartitionCols.fields.drop(numQueryOutputs)
      name: String = field.name
      text: String =
        if (field.metadata.contains(default)) field.metadata.getString(default) else "NULL"
      // Parse the DEFAULT column expression. If the parsing fails, throw an error to the user.
      expr: Expression = Parse(name, text, "INSERT")
      // Analyze and constant-fold each result.
      analyzed: Expression = Analyze(expr, text, name, "INSERT")
      // Perform implicit coercion from the provided expression type to the required column type.
      errorPrefix = "Failed to execute INSERT command because the destination table column "
      coerced: Expression =
        if (field.dataType == analyzed.dataType) {
          analyzed
        } else if (Cast.canUpCast(analyzed.dataType, field.dataType)) {
          Cast(analyzed, field.dataType)
        } else {
          throw new AnalysisException(errorPrefix +
            s"$name has a DEFAULT value with type ${field.dataType}, but the " +
            s"query provided a value of incompatible type ${analyzed.dataType}")
        }
    } yield coerced
    // Finally, return a projection of the original `insert.query` output attributes plus new
    // aliases over the DEFAULT column values.
    // If the insertQuery is an existing Project, flatten them together.
    val newQuery = insert.query match {
      case Project(projectList, child) =>
        val newAliases: Seq[NamedExpression] = coerced.zip(schemaWithoutPartitionCols.fields).map {
          case (expr, field) => Alias(expr, field.name)() }
        Project(projectList ++ newAliases, child)
      case table: UnresolvedInlineTable =>
        val newNames: Seq[String] =
          schemaWithoutPartitionCols.fields.drop(numQueryOutputs).map { _.name }
        table.copy(names = table.names ++ newNames, rows = table.rows.map { row => row ++ coerced })
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
  def ReplaceExplicitDefaultColumnValues(
      insert: InsertIntoStatement, catalog: SessionCatalog): InsertIntoStatement = {
    // Extract the list of DEFAULT column values from the INSERT INTO statement.
    val schema: StructType = getInsertTableSchema(insert, catalog).getOrElse(return insert)
    val colNames: Seq[String] = schema.fields.map { _.name }
    val defaultExprs: Seq[Expression] = schema.fields.map {
      case f if f.metadata.contains(default) =>
        parser.parseExpression(f.metadata.getString(default))
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
                  case u: UnresolvedAttribute if u.name.equalsIgnoreCase(default) => defaultExpr
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
                case u: UnresolvedAttribute if u.name.equalsIgnoreCase(default) =>
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
   * Parses DEFAULT column text to an expression, returning a reasonable error upon failure.
   *
   * @param colName the name of the DEFAULT column whose text we endeavor to parse.
   * @param colText the string contents of the DEFAULT column value.
   * @param statementType which type of statement we are running, such as INSERT; useful for errors.
   * @return the expression resulting from the parsing step.
   */
  private def Parse(colName: String, colText: String, statementType: String): Expression = {
    try {
      parser.parseExpression(colText)
    } catch {
      case ex: ParseException =>
        throw new AnalysisException(
          s"Failed to execute $statementType command because the destination table column " +
            colName + analysisPrefix + s"$colText yields ${ex.getMessage}")
    }
  }

  /**
   * Analyzes and constant-folds `colExpr`, returning a reasonable error message upon failure.
   *
   * @param colExpr result of a parsing operation suitable for consumption by analysis.
   * @param colText string contents of the DEFAULT column value; useful for errors.
   * @param colName string name of the DEFAULT column; useful for errors.
   * @param statementType which type of statement we are running, such as INSERT; useful for errors.
   * @return Result of the analysis and constant-folding operation.
   */
  private def Analyze(colExpr: Expression, colText: String, colName: String,
                      statementType: String): Expression = {
    try {
      // Invoke the analyzer over the 'colExpr'.
      val plan = analyzer.execute(Project(Seq(Alias(colExpr, colName)()), OneRowRelation()))
      analyzer.checkAnalysis(plan)
      // Perform constant folding over the result.
      val folded = ConstantFolding(plan)
      val result = folded match {
        case Project(Seq(a: Alias), OneRowRelation()) => a.child
      }
      // Make sure the constant folding was successful.
      result match {
        case _: Literal => result
        case _ => throw new AnalysisException("non-constant value")
      }
    } catch {
      case ex: AnalysisException =>
        throw new AnalysisException(
          s"Failed to execute $statementType command because the destination table column " +
            colName + analysisPrefix + s"$colText yields ${ex.getMessage}")
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
