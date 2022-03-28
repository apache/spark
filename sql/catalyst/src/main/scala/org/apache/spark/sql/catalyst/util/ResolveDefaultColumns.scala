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

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.Analyzer
import org.apache.spark.sql.catalyst.catalog.{SessionCatalog, UnresolvedCatalogRelation}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.optimizer.ConstantFolding
import org.apache.spark.sql.catalyst.parser.{CatalystSqlParser, ParseException}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.util.ResolveDefaultColumns._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

package org.apache.spark.sql.catalyst.analysis {
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
   * @param insert   the enclosing INSERT statement for which this rule is processing the query, if
   *                 any.
   */
  case class ResolveDefaultColumns(
      analyzer: Analyzer,
      catalog: SessionCatalog,
      insert: Option[InsertIntoStatement] = None) extends Rule[LogicalPlan] {
    override def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsWithPruning(
      (_ => SQLConf.get.enableDefaultColumns), ruleId) {
      case i@InsertIntoStatement(_, _, _, _, _, _)
        if i.query.collectFirst { case u: UnresolvedInlineTable => u }.isDefined =>
        // Create a helper instance of this same rule with the `insert` argument populated as `i`.
        // Then recursively apply it on the `query` of `i` to transform the result. This recursive
        // application lets the below case matching against `UnresolvedInlineTable` to trigger
        // anywhere that operator may appear in the descendants of `i.query`.
        val helper = ResolveDefaultColumns(analyzer, catalog, Some(i))
        val newQuery = helper.apply(i.query)
        i.copy(query = newQuery)

      case table: UnresolvedInlineTable
        if insert.isDefined &&
          table.rows.nonEmpty && table.rows.forall(_.size == table.rows(0).size) =>
        val expanded: UnresolvedInlineTable = addMissingDefaultColumnValues(table).getOrElse(table)
        replaceExplicitDefaultColumnValues(analyzer, expanded).getOrElse(table)

      case i@InsertIntoStatement(_, _, _, project: Project, _, _) =>
        val helper = ResolveDefaultColumns(analyzer, catalog, Some(i))
        val expanded: Project = helper.addMissingDefaultColumnValues(project).getOrElse(project)
        val replaced: Option[LogicalPlan] =
          helper.replaceExplicitDefaultColumnValues(analyzer, expanded)
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
      assert(insert.isDefined)
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
      assert(insert.isDefined)
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
      assert(insert.isDefined)
      try {
        val tableName = insert.get.table match {
          case r: UnresolvedRelation => TableIdentifier(r.name)
          case r: UnresolvedCatalogRelation => r.tableMeta.identifier
          case _ => return None
        }
        val lookup = catalog.lookupRelation(tableName)
        lookup match {
          case SubqueryAlias(_, r: UnresolvedCatalogRelation) =>
            Some(StructType(r.tableMeta.schema.fields.dropRight(insert.get.partitionSpec.size)))
          case _ => None
        }
      } catch {
        case _: NoSuchTableException => None
      }
    }
  }
}

package org.apache.spark.sql.catalyst.util {
  /**
   * This object contains fields to help process DEFAULT columns.
   */
  object ResolveDefaultColumns {
    // This column metadata indicates the default value associated with a particular table column
    // that is in effect at any given time. Its value begins at the time of the initial
    // CREATE/REPLACE TABLE statement with DEFAULT column definition(s), if any. It then changes
    // whenever an ALTER TABLE statement SETs the DEFAULT. The intent is for this "current default"
    // to be used by UPDATE, INSERT and MERGE, which evaluate each default expression for each row.
    val CURRENT_DEFAULT_COLUMN_METADATA_KEY = "CURRENT_DEFAULT"
    // This column metadata represents the default value for all existing rows in a table after a
    // column has been added. This value is determined at time of CREATE TABLE, REPLACE TABLE, or
    // ALTER TABLE ADD COLUMN, and never changes thereafter. The intent is for this "exist default"
    // to be used by any scan when the columns in the source row are missing data. For example,
    // consider the following sequence:
    // CREATE TABLE t (c1 INT)
    // INSERT INTO t VALUES (42)
    // ALTER TABLE t ADD COLUMNS (c2 INT DEFAULT 43)
    // SELECT c1, c2 FROM t
    // In this case, the final query is expected to return 42, 43. The ALTER TABLE ADD COLUMNS
    // command executed after there was already data in the table, so in order to enforce this
    // invariant, we need either (1) an expensive backfill of value 43 at column c2 into all
    // previous rows, or (2) indicate to each data source that selected columns missing data are to
    // generate the corresponding DEFAULT value instead. We choose option (2) for efficiency, and
    // represent this value as the text representation of a folded constant in the "EXISTS_DEFAULT"
    // column metadata.
    val EXISTS_DEFAULT_COLUMN_METADATA_KEY = "EXISTS_DEFAULT"
    // Name of attributes representing explicit references to the value stored in the above
    // CURRENT_DEFAULT_COLUMN_METADATA.
    val CURRENT_DEFAULT_COLUMN_NAME = "DEFAULT"
    // Return a more descriptive error message if the user tries to nest the DEFAULT column
    // reference inside some other expression, such as DEFAULT + 1 (this is not allowed).
    val DEFAULTS_IN_EXPRESSIONS_ERROR = "Failed to execute INSERT INTO command because the " +
      "VALUES list contains a DEFAULT column reference as part of another expression; this is " +
      "not allowed"

    /**
     * Finds "current default" expressions in CREATE/REPLACE TABLE columns and constant-folds them.
     *
     * The results are stored in the "exists default" metadata of the same columns. For example, in
     * the event of this statement:
     *
     * CREATE TABLE T(a INT, b INT DEFAULT 5 + 5)
     *
     * This method constant-folds the "current default" value, stored in the CURRENT_DEFAULT
     * metadata of the "b" column, to "10", storing the result in the "exists default" value within
     * the EXISTS_DEFAULT metadata of that same column. Meanwhile the "current default" metadata of
     * this "b" column retains its original value of "5 + 5".
     *
     * The reason for constant-folding the EXISTS_DEFAULT is to make the end-user visible behavior
     * the same, after executing an ALTER TABLE ADD COLUMNS command with DEFAULT value, as if the
     * system had performed an exhaustive backfill of the provided value to all previously existing
     * rows in the table instead. We choose to avoid doing such a backfill because it would be a
     * time-consuming and costly operation. Instead, we elect to store the EXISTS_DEFAULT in the
     * column metadata for future reference when querying data out of the data source. In turn, each
     * data source then takes responsibility to provide the constant-folded value in the
     * EXISTS_DEFAULT metadata for such columns where the value is not present in storage.
     *
     * @param analyzer      used for analyzing the result of parsing the column expression stored as
     *                      text.
     * @param tableSchema   represents the names and types of the columns of the statement to
     *                      process.
     * @param statementType name of the statement being processed, such as INSERT; useful for
     *                      errors.
     * @return a copy of `tableSchema` with field metadata updated with the constant-folded values.
     */
    def constantFoldCurrentDefaultsToExistDefaults(
      analyzer: Analyzer,
      tableSchema: StructType,
      statementType: String): StructType = {
      if (!SQLConf.get.enableDefaultColumns) {
        return tableSchema
      }
      val newFields: Seq[StructField] = tableSchema.fields.map { field =>
        if (field.metadata.contains(CURRENT_DEFAULT_COLUMN_METADATA_KEY)) {
          val analyzed: Expression = analyze(analyzer, field, statementType)
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
     * @param field         represents the DEFAULT column value whose "default" metadata to parse
     *                      and analyze.
     * @param statementType which type of statement we are running, such as INSERT; useful for
     *                      errors.
     * @return Result of the analysis and constant-folding operation.
     */
    def analyze(
      analyzer: Analyzer,
      field: StructField,
      statementType: String): Expression = {
      // Parse the expression.
      val colText: String = field.metadata.getString(CURRENT_DEFAULT_COLUMN_METADATA_KEY)
      lazy val parser = new CatalystSqlParser()
      val parsed: Expression = try {
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
        val analyzed = analyzer.execute(Project(Seq(Alias(parsed, field.name)()), OneRowRelation()))
        analyzer.checkAnalysis(analyzed)
        ConstantFolding(analyzed)
      } catch {
        case ex: AnalysisException =>
          throw new AnalysisException(
            s"Failed to execute $statementType command because the destination table column " +
              s"${field.name} has a DEFAULT value of $colText which fails to resolve as a valid " +
              s"expression: ${ex.getMessage}")
      }
      val analyzed: Expression = plan.collectFirst {
        case Project(Seq(a: Alias), OneRowRelation()) => a.child
      }.get
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
}
