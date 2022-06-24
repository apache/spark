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

package org.apache.spark.sql.catalyst.util

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.catalog.{CatalogDatabase, InMemoryCatalog, SessionCatalog}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.{Literal => ExprLiteral}
import org.apache.spark.sql.catalyst.optimizer.ConstantFolding
import org.apache.spark.sql.catalyst.parser.{CatalystSqlParser, ParseException}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.connector.catalog.{CatalogManager, FunctionCatalog, Identifier}
import org.apache.spark.sql.connector.catalog.functions.UnboundFunction
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.connector.V1Function
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/**
 * This object contains fields to help process DEFAULT columns.
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
  // ALTER TABLE ADD COLUMN, and never changes thereafter. The intent is for this "exist default" to
  // be used by any scan when the columns in the source row are missing data. For example, consider
  // the following sequence:
  // CREATE TABLE t (c1 INT)
  // INSERT INTO t VALUES (42)
  // ALTER TABLE t ADD COLUMNS (c2 INT DEFAULT 43)
  // SELECT c1, c2 FROM t
  // In this case, the final query is expected to return 42, 43. The ALTER TABLE ADD COLUMNS command
  // executed after there was already data in the table, so in order to enforce this invariant, we
  // need either (1) an expensive backfill of value 43 at column c2 into all previous rows, or (2)
  // indicate to each data source that selected columns missing data are to generate the
  // corresponding DEFAULT value instead. We choose option (2) for efficiency, and represent this
  // value as the text representation of a folded constant in the "EXISTS_DEFAULT" column metadata.
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
   * The reason for constant-folding the EXISTS_DEFAULT is to make the end-user visible behavior the
   * same, after executing an ALTER TABLE ADD COLUMNS command with DEFAULT value, as if the system
   * had performed an exhaustive backfill of the provided value to all previously existing rows in
   * the table instead. We choose to avoid doing such a backfill because it would be a
   * time-consuming and costly operation. Instead, we elect to store the EXISTS_DEFAULT in the
   * column metadata for future reference when querying data out of the data source. In turn, each
   * data source then takes responsibility to provide the constant-folded value in the
   * EXISTS_DEFAULT metadata for such columns where the value is not present in storage.
   *
   * @param tableSchema   represents the names and types of the columns of the statement to process.
   * @param tableProvider provider of the target table to store default values for, if any.
   * @param statementType name of the statement being processed, such as INSERT; useful for errors.
   * @return a copy of `tableSchema` with field metadata updated with the constant-folded values.
   */
  def constantFoldCurrentDefaultsToExistDefaults(
      tableSchema: StructType,
      tableProvider: Option[String],
      statementType: String): StructType = {
    if (SQLConf.get.enableDefaultColumns) {
      val allowedTableProviders: Array[String] =
        SQLConf.get.getConf(SQLConf.DEFAULT_COLUMN_ALLOWED_PROVIDERS)
          .toLowerCase().split(",").map(_.trim)
      val givenTableProvider: String = tableProvider.getOrElse("").toLowerCase()
      val newFields: Seq[StructField] = tableSchema.fields.map { field =>
        if (field.metadata.contains(CURRENT_DEFAULT_COLUMN_METADATA_KEY)) {
          // Make sure that the target table has a provider that supports default column values.
          if (!allowedTableProviders.contains(givenTableProvider)) {
            throw QueryCompilationErrors.defaultReferencesNotAllowedInDataSource(givenTableProvider)
          }
          val analyzed: Expression = analyze(field, statementType)
          val newMetadata: Metadata = new MetadataBuilder().withMetadata(field.metadata)
            .putString(EXISTS_DEFAULT_COLUMN_METADATA_KEY, analyzed.sql).build()
          field.copy(metadata = newMetadata)
        } else {
          field
        }
      }
      StructType(newFields)
    } else {
      tableSchema
    }
  }

  /**
   * Parses and analyzes the DEFAULT column text in `field`, returning an error upon failure.
   *
   * @param field         represents the DEFAULT column value whose "default" metadata to parse
   *                      and analyze.
   * @param statementType which type of statement we are running, such as INSERT; useful for errors.
   * @return Result of the analysis and constant-folding operation.
   */
  def analyze(field: StructField, statementType: String): Expression = {
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
      val analyzer: Analyzer = DefaultColumnAnalyzer
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
  /**
   * Normalizes a schema field name suitable for use in looking up into maps keyed by schema field
   * names.
   * @param str the field name to normalize
   * @return the normalized result
   */
  def normalizeFieldName(str: String): String = {
    if (SQLConf.get.caseSensitiveAnalysis) {
      str
    } else {
      str.toLowerCase()
    }
  }

  /**
   * Parses the text representing constant-folded default column literal values. These are known as
   * "existence" default values because each one is the constant-folded result of the original
   * default value first assigned to the column at table/column creation time. When scanning a field
   * from any data source, if the corresponding value is not present in storage, the output row
   * returns this "existence" default value instead of NULL.
   * @return a sequence of either (1) NULL, if the column had no default value, or (2) an object of
   *         Any type suitable for assigning into a row using the InternalRow.update method.
   */
  def getExistenceDefaultValues(schema: StructType): Array[Any] = {
    schema.fields.map { field: StructField =>
      val defaultValue: Option[String] = field.getExistenceDefaultValue()
      defaultValue.map { text: String =>
        val expr = try {
          val expr = CatalystSqlParser.parseExpression(text)
          expr match {
            case _: ExprLiteral | _: Cast => expr
          }
        } catch {
          case _: ParseException | _: MatchError =>
            throw QueryCompilationErrors.failedToParseExistenceDefaultAsLiteral(field.name, text)
        }
        // The expression should be a literal value by this point, possibly wrapped in a cast
        // function. This is enforced by the execution of commands that assign default values.
        expr.eval()
      }.orNull
    }
  }

  /**
   * Returns an array of boolean values equal in size to the result of [[getExistenceDefaultValues]]
   * above, for convenience.
   */
  def getExistenceDefaultsBitmask(schema: StructType): Array[Boolean] = {
    Array.fill[Boolean](schema.existenceDefaultValues.size)(true)
  }

  /**
   * Resets the elements of the array initially returned from [[getExistenceDefaultsBitmask]] above.
   * Afterwards, set element(s) to false before calling [[applyExistenceDefaultValuesToRow]] below.
   */
  def resetExistenceDefaultsBitmask(schema: StructType): Unit = {
    for (i <- 0 until schema.existenceDefaultValues.size) {
      schema.existenceDefaultsBitmask(i) = (schema.existenceDefaultValues(i) != null)
    }
  }

  /**
   * Updates a subset of columns in the row with default values from the metadata in the schema.
   */
  def applyExistenceDefaultValuesToRow(schema: StructType, row: InternalRow): Unit = {
    if (schema.hasExistenceDefaultValues) {
      for (i <- 0 until schema.existenceDefaultValues.size) {
        if (schema.existenceDefaultsBitmask(i)) {
          row.update(i, schema.existenceDefaultValues(i))
        }
      }
    }
  }

  /**
   * This is an Analyzer for processing default column values using built-in functions only.
   */
  object DefaultColumnAnalyzer extends Analyzer(
    new CatalogManager(BuiltInFunctionCatalog, BuiltInFunctionCatalog.v1Catalog)) {
  }

  /**
   * This is a FunctionCatalog for performing analysis using built-in functions only. It is a helper
   * for the DefaultColumnAnalyzer above.
   */
  object BuiltInFunctionCatalog extends FunctionCatalog {
    val v1Catalog = new SessionCatalog(
      new InMemoryCatalog, FunctionRegistry.builtin, TableFunctionRegistry.builtin) {
      override def createDatabase(
          dbDefinition: CatalogDatabase, ignoreIfExists: Boolean): Unit = {}
    }
    import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._
    override def initialize(name: String, options: CaseInsensitiveStringMap): Unit = {}
    override def name(): String = CatalogManager.SESSION_CATALOG_NAME
    override def listFunctions(namespace: Array[String]): Array[Identifier] = {
      throw new UnsupportedOperationException()
    }
    override def loadFunction(ident: Identifier): UnboundFunction = {
      V1Function(v1Catalog.lookupPersistentFunction(ident.asFunctionIdentifier))
    }
    override def functionExists(ident: Identifier): Boolean = {
      v1Catalog.isPersistentFunction(ident.asFunctionIdentifier)
    }
  }
}
