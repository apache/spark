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
   * @param catalog session catalog for use when looking up table names for the statement.
   * @return the updated statement with DEFAULT column references replaced with their values.
   */
  def ReplaceExplicitDefaultColumnValues(insert: InsertIntoStatement,
                                         catalog: SessionCatalog): InsertIntoStatement = {
    // Extract the list of DEFAULT column values from the INSERT INTO statement.
    val lookup = try {
      val tableName = insert.table match {
        case r: UnresolvedRelation => TableIdentifier(r.name)
        case r: UnresolvedCatalogRelation => r.tableMeta.identifier
        case _ => return insert
      }
      catalog.lookupRelation(tableName)
    } catch {
      case _: NoSuchTableException => return insert
    }
    val schema: StructType = lookup match {
      case SubqueryAlias(_, r: UnresolvedCatalogRelation) => r.tableMeta.schema
      case _ => return insert
    }
    val colNames: Seq[String] = schema.fields.map { _.name }
    val defaultExprs: Seq[Option[Expression]] = schema.fields.map {
      case f if f.metadata.contains(default) =>
        Some(parser.parseExpression(f.metadata.getString(default)))
      case _ => None
    }
    if (defaultExprs.isEmpty) {
      // Fast path: if there are no explicit default columns, there is nothing to do.
      return insert
    }
    // Handle two types of logical query plans in the target of the INSERT INTO statement:
    // Inline table: VALUES (0, 1, DEFAULT, ...)
    // Projection: SELECT 0, 1, DEFAULT, ...
    // Note that the DEFAULT reference may not participate in complex expressions such as
    // "DEFAULT + 2"; this generally results in a "not found" error later in the analyzer.
    val newQuery: LogicalPlan = insert.query match {
      case table: UnresolvedInlineTable =>
        val newRows: Seq[Seq[Expression]] =
          table.rows.map { row: Seq[Expression] =>
            row.zip(defaultExprs).map {
              case (expr: Expression, defaultExpr: Option[Expression]) =>
                expr match {
                  case u: UnresolvedAttribute
                    if u.name.equalsIgnoreCase(default) && defaultExpr.isDefined => defaultExpr.get
                  case _ => expr
                }
            }
        }
        table.copy(rows = newRows)
      case project: Project =>
        val updated: Seq[NamedExpression] =
          (project.projectList, defaultExprs, colNames).zipped.map {
          case (expr: Expression, defaultExpr: Option[Expression], colName: String) =>
            expr match {
              case u: UnresolvedAttribute
                if u.name.equalsIgnoreCase(default) && defaultExpr.isDefined =>
                Alias(defaultExpr.get, colName)()
              case _ => expr
            }
        }
        project.copy(projectList = updated)
      case _ => insert.query
    }
    insert.copy(query = newQuery)
  }

  /**
   * Adds a projection over the plan in `insert.query` generating missing default column values.
   *
   * @param insert the INSERT INTO statement to add missing DEFAULT column references to.
   * @return the updated statement with missing DEFAULT column values appended to the list.
   */
  def AddProjectionForMissingDefaultColumnValues(
      insert: InsertIntoStatement): InsertIntoStatement = {
    val colIndexes = insert.query.output.size until insert.table.output.size
    if (colIndexes.isEmpty) {
      // Fast path: if there are no missing default columns, there is nothing to do.
      return insert
    }
    val columns = colIndexes.map(insert.table.schema.fields(_))
    val colNames: Seq[String] = columns.map(_.name)
    val colTypes: Seq[DataType] = columns.map(_.dataType)
    val colTexts: Seq[String] = columns.map(_.metadata.getString(default))
    // Parse the DEFAULT column expression. If the parsing fails, throw an error to the user.
    val colExprs: Seq[Expression] = colNames.zip(colTexts).map {
      case (name, text) => Parse(name, text, "INSERT")
    }
    // Analyze and constant-fold each result.
    val analyzed = (colExprs, colTexts, colNames).zipped.map {
      case (expr, text, name) => Analyze(expr, text, name, "INSERT")
    }
    // Perform implicit coercion from the provided expression type to the required column type.
    val colExprsCoerced: Seq[Expression] = (analyzed, colTypes, colNames).zipped.map {
      case (expr, datatype, _)
        if datatype != expr.dataType && Cast.canUpCast(expr.dataType, datatype) =>
        Cast(expr, datatype)
      case (expr, dataType, colName)
        if dataType != expr.dataType =>
        throw new AnalysisException(
          s"Failed to execute INSERT command because the destination table column " +
          s"$colName has a DEFAULT value with type $dataType, but the query " +
            s"provided a value of incompatible type ${expr.dataType}")
      case (expr, _, _) => expr
    }
    // Finally, return a projection of the original `insert.query` output attributes plus new
    // aliases over the DEFAULT column values.
    // If the insertQuery is an existing Project, flatten them together.
    val newAliases: Seq[NamedExpression] = {
      colExprsCoerced.zip(colNames).map { case (expr, name) => Alias(expr, name)() }
    }
    val newQuery = insert.query match {
      case Project(projectList, child) => Project(projectList ++ newAliases, child)
      case _ => Project(insert.query.output ++ newAliases, insert.query)
    }
    insert.copy(query = newQuery)
  }

}
