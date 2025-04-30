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

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.{SparkException, SparkThrowable, SparkUnsupportedOperationException}
import org.apache.spark.internal.{Logging, MDC}
import org.apache.spark.internal.LogKeys._
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.{InternalRow, SQLConfHelper}
import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.catalog.{CatalogDatabase, InMemoryCatalog, SessionCatalog}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.{Literal => ExprLiteral}
import org.apache.spark.sql.catalyst.optimizer.{ConstantFolding, Optimizer}
import org.apache.spark.sql.catalyst.parser.{CatalystSqlParser, ParseException}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.trees.TreePattern.PLAN_EXPRESSION
import org.apache.spark.sql.connector.catalog.{CatalogManager, DefaultValue, FunctionCatalog, Identifier, TableCatalog, TableCatalogCapability}
import org.apache.spark.sql.connector.catalog.functions.UnboundFunction
import org.apache.spark.sql.errors.{QueryCompilationErrors, QueryErrorsBase}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.connector.V1Function
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.util.ArrayImplicits._
import org.apache.spark.util.Utils

/**
 * This object contains fields to help process DEFAULT columns.
 */
object ResolveDefaultColumns extends QueryErrorsBase
  with ResolveDefaultColumnsUtils
  with SQLConfHelper
  with Logging {
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
   * @param statementType name of the statement being processed, such as INSERT; useful for errors.
   * @return a copy of `tableSchema` with field metadata updated with the constant-folded values.
   */
  def constantFoldCurrentDefaultsToExistDefaults(
      tableSchema: StructType,
      statementType: String): StructType = {
    if (SQLConf.get.enableDefaultColumns) {
      val newFields: Seq[StructField] = tableSchema.fields.map { field =>
        if (field.metadata.contains(CURRENT_DEFAULT_COLUMN_METADATA_KEY)) {
          val analyzed: Expression = analyze(field, statementType)
          val newMetadata: Metadata = new MetadataBuilder().withMetadata(field.metadata)
            .putString(EXISTS_DEFAULT_COLUMN_METADATA_KEY, analyzed.sql).build()
          field.copy(metadata = newMetadata)
        } else {
          field
        }
      }.toImmutableArraySeq
      StructType(newFields)
    } else {
      tableSchema
    }
  }

  // Fails if the given catalog does not support column default value.
  def validateCatalogForDefaultValue(
      columns: Seq[ColumnDefinition],
      catalog: TableCatalog,
      ident: Identifier): Unit = {
    if (SQLConf.get.enableDefaultColumns && columns.exists(_.defaultValue.isDefined) &&
      !catalog.capabilities().contains(TableCatalogCapability.SUPPORT_COLUMN_DEFAULT_VALUE)) {
      throw QueryCompilationErrors.unsupportedTableOperationError(
        catalog, ident, "column default value")
    }
  }

  // Fails if the given table provider of the session catalog does not support column default value.
  def validateTableProviderForDefaultValue(
      schema: StructType,
      tableProvider: Option[String],
      statementType: String,
      addNewColumnToExistingTable: Boolean): Unit = {
    if (SQLConf.get.enableDefaultColumns &&
      schema.exists(_.metadata.contains(CURRENT_DEFAULT_COLUMN_METADATA_KEY))) {
      val keywords: Array[String] = SQLConf.get.getConf(SQLConf.DEFAULT_COLUMN_ALLOWED_PROVIDERS)
        .toLowerCase().split(",").map(_.trim)
      val allowedTableProviders: Array[String] = if (Utils.isTesting) {
        "in-memory" +: keywords.map(_.stripSuffix("*"))
      } else {
        keywords.map(_.stripSuffix("*"))
      }
      val addColumnExistingTableBannedProviders: Array[String] =
        keywords.filter(_.endsWith("*")).map(_.stripSuffix("*"))
      val givenTableProvider: String = tableProvider.getOrElse("").toLowerCase()
      // Make sure that the target table has a provider that supports default column values.
      if (!allowedTableProviders.contains(givenTableProvider)) {
        throw QueryCompilationErrors.defaultReferencesNotAllowedInDataSource(
          statementType, givenTableProvider)
      }
      if (addNewColumnToExistingTable &&
        givenTableProvider.nonEmpty &&
        addColumnExistingTableBannedProviders.contains(givenTableProvider)) {
        throw QueryCompilationErrors.addNewDefaultColumnToExistingTableNotAllowed(
          statementType, givenTableProvider)
      }
    }
  }

  /**
   * Returns true if the unresolved column is an explicit DEFAULT column reference.
   */
  def isExplicitDefaultColumn(col: UnresolvedAttribute): Boolean = {
    col.name.equalsIgnoreCase(CURRENT_DEFAULT_COLUMN_NAME)
  }

  /**
   * Returns true if the given expression contains an explicit DEFAULT column reference.
   */
  def containsExplicitDefaultColumn(expr: Expression): Boolean = {
    expr.exists {
      case u: UnresolvedAttribute => isExplicitDefaultColumn(u)
      case _ => false
    }
  }

  /**
   * Resolves the column "DEFAULT" in UPDATE/MERGE assignment value expression if the following
   * conditions are met:
   * 1. The assignment value expression is a single `UnresolvedAttribute` with name "DEFAULT". This
   *    means `key = DEFAULT` is allowed but `key = DEFAULT + 1` is not.
   * 2. The assignment key expression is a top-level column. This means `col = DEFAULT` is allowed
   *    but `col.field = DEFAULT` is not.
   *
   * The column "DEFAULT" will be resolved to the default value expression defined for the column of
   * the assignment key.
   */
  def resolveColumnDefaultInAssignmentValue(
      key: Expression,
      value: Expression,
      invalidColumnDefaultException: => Throwable): Expression = {
    key match {
      case attr: AttributeReference =>
        value match {
          case u: UnresolvedAttribute if isExplicitDefaultColumn(u) =>
            getDefaultValueExprOrNullLit(attr)
          case other if containsExplicitDefaultColumn(other) =>
            throw invalidColumnDefaultException
          case other => other
        }
      case _ => value
    }
  }

  private def getDefaultValueExprOpt(field: StructField): Option[Expression] = {
    if (field.metadata.contains(CURRENT_DEFAULT_COLUMN_METADATA_KEY)) {
      Some(analyze(field, "INSERT"))
    } else {
      None
    }
  }

  /**
   * Generates the expression of the default value for the given field. If there is no
   * user-specified default value for this field and the field is nullable, returns null
   * literal, otherwise an exception is thrown.
   */
  def getDefaultValueExprOrNullLit(field: StructField): Expression = {
    val defaultValue = getDefaultValueExprOrNullLit(field, useNullAsDefault = true)
    if (defaultValue.isEmpty) {
      throw new AnalysisException(
        errorClass = "NO_DEFAULT_COLUMN_VALUE_AVAILABLE",
        messageParameters = Map("colName" -> toSQLId(Seq(field.name))))
    }
    defaultValue.get
  }

  /**
   * Generates the expression of the default value for the given attribute. If there is no
   * user-specified default value for this attribute and the attribute is nullable, returns null
   * literal, otherwise an exception is thrown.
   */
  def getDefaultValueExprOrNullLit(attr: Attribute): Expression = {
    val field = StructField(attr.name, attr.dataType, attr.nullable, attr.metadata)
    getDefaultValueExprOrNullLit(field)
  }

  /**
   * Generates the expression of the default value for the given field. If there is no
   * user-specified default value for this field, returns null literal if `useNullAsDefault` is
   * true and the field is nullable.
   */
  def getDefaultValueExprOrNullLit(
      field: StructField, useNullAsDefault: Boolean): Option[NamedExpression] = {
    getDefaultValueExprOpt(field).orElse {
      if (useNullAsDefault && field.nullable) {
        Some(Literal(null, field.dataType))
      } else {
        None
      }
    }.map(expr => Alias(expr, field.name)())
  }

  /**
   * Generates the expression of the default value for the given attribute. If there is no
   * user-specified default value for this attribute, returns null literal if `useNullAsDefault` is
   * true and the attribute is nullable.
   */
  def getDefaultValueExprOrNullLit(
      attr: Attribute, useNullAsDefault: Boolean): Option[NamedExpression] = {
    val field = StructField(attr.name, attr.dataType, attr.nullable, attr.metadata)
    getDefaultValueExprOrNullLit(field, useNullAsDefault)
  }

  /**
   * Parses and analyzes the DEFAULT column text in `field`, returning an error upon failure.
   *
   * @param field         represents the DEFAULT column value whose "default" metadata to parse
   *                      and analyze.
   * @param statementType which type of statement we are running, such as INSERT; useful for errors.
   * @param metadataKey   which key to look up from the column metadata; generally either
   *                      CURRENT_DEFAULT_COLUMN_METADATA_KEY or EXISTS_DEFAULT_COLUMN_METADATA_KEY.
   * @return Result of the analysis and constant-folding operation.
   */
  def analyze(
      field: StructField,
      statementType: String,
      metadataKey: String = CURRENT_DEFAULT_COLUMN_METADATA_KEY): Expression = {
    analyze(field.name, field.dataType, field.metadata.getString(metadataKey), statementType)
  }

  /**
   * Parses and analyzes the DEFAULT column SQL string, returning an error upon failure.
   *
   * @return Result of the analysis and constant-folding operation.
   */
  def analyze(
      colName: String,
      dataType: DataType,
      defaultSQL: String,
      statementType: String): Expression = {
    // Parse the expression.
    lazy val parser = new CatalystSqlParser()
    val parsed: Expression = try {
      parser.parseExpression(defaultSQL)
    } catch {
      case ex: ParseException =>
        throw QueryCompilationErrors.defaultValuesUnresolvedExprError(
          statementType, colName, defaultSQL, ex)
    }
    analyze(colName, dataType, parsed, defaultSQL, statementType)
  }

  /**
   * Analyzes the connector default value.
   *
   * If the default value is defined as a connector expression, Spark first attempts to convert it
   * to a Catalyst expression. If conversion fails but a SQL string is provided, the SQL is parsed
   * instead. If only a SQL string is present, it is parsed directly.
   *
   * @return the result of the analysis and constant-folding operation
   */
  def analyze(
      colName: String,
      dataType: DataType,
      defaultValue: DefaultValue,
      statementType: String): Expression = {
    if (defaultValue.getExpression != null) {
      V2ExpressionUtils.toCatalyst(defaultValue.getExpression) match {
        case Some(defaultExpr) =>
          val defaultSQL = Option(defaultValue.getSql).getOrElse(defaultExpr.sql)
          analyze(colName, dataType, defaultExpr, defaultSQL, statementType)

        case None if defaultValue.getSql != null =>
          analyze(colName, dataType, defaultValue.getSql, statementType)

        case _ =>
          throw SparkException.internalError(s"Can't convert $defaultValue to Catalyst")
      }
    } else {
      analyze(colName, dataType, defaultValue.getSql, statementType)
    }
  }

  private def analyze(
      colName: String,
      dataType: DataType,
      defaultExpr: Expression,
      defaultSQL: String,
      statementType: String): Expression = {
    // Check invariants before moving on to analysis.
    if (defaultExpr.containsPattern(PLAN_EXPRESSION)) {
      throw QueryCompilationErrors.defaultValuesMayNotContainSubQueryExpressions(
        statementType, colName, defaultSQL)
    }

    // Analyze the parse result.
    val plan = try {
      val analyzer: Analyzer = DefaultColumnAnalyzer
      val analyzed = analyzer.execute(Project(Seq(Alias(defaultExpr, colName)()), OneRowRelation()))
      analyzer.checkAnalysis(analyzed)
      // Eagerly execute finish-analysis and constant-folding rules before checking whether the
      // expression is foldable and resolved.
      ConstantFolding(DefaultColumnOptimizer.FinishAnalysis(analyzed))
    } catch {
      case ex: AnalysisException =>
        throw QueryCompilationErrors.defaultValuesUnresolvedExprError(
          statementType, colName, defaultSQL, ex)
    }
    val analyzed: Expression = plan.collectFirst {
      case Project(Seq(a: Alias), OneRowRelation()) => a.child
    }.get

    if (!analyzed.foldable) {
      throw QueryCompilationErrors.defaultValueNotConstantError(statementType, colName, defaultSQL)
    }

    // Another extra check, expressions should already be resolved if AnalysisException is not
    // thrown in the code block above
    if (!analyzed.resolved) {
      throw QueryCompilationErrors.defaultValuesUnresolvedExprError(
        statementType,
        colName,
        defaultSQL,
        cause = null)
    }

    // Perform implicit coercion from the provided expression type to the required column type.
    coerceDefaultValue(analyzed, dataType, statementType, colName, defaultSQL)
  }

  /**
   * Analyze EXISTS_DEFAULT value.  EXISTS_DEFAULT value was created from CURRENT_DEFAQULT
   * via [[analyze]] and thus this can skip most of those steps.
   */
  private def analyzeExistenceDefaultValue(field: StructField): Expression = {
    val defaultSQL = field.metadata.getString(EXISTS_DEFAULT_COLUMN_METADATA_KEY)

    // Parse the expression.
    val expr = Literal.fromSQL(defaultSQL) match {
      // EXISTS_DEFAULT will have a cast from analyze() due to coerceDefaultValue
      // hence we need to add timezone to the cast if necessary
      case c: Cast if c.child.resolved && c.needsTimeZone =>
        c.withTimeZone(SQLConf.get.sessionLocalTimeZone)
      case e: Expression => e
    }

    // Check invariants
    if (expr.containsPattern(PLAN_EXPRESSION)) {
      throw QueryCompilationErrors.defaultValuesMayNotContainSubQueryExpressions(
        "", field.name, defaultSQL)
    }

    val resolvedExpr = expr match {
      case _: ExprLiteral => expr
      case c: Cast if c.resolved => expr
      case _ =>
        fallbackResolveExistenceDefaultValue(field)
    }

    coerceDefaultValue(resolvedExpr, field.dataType, "", field.name, defaultSQL)
  }

  // In most cases, column existsDefault should already be persisted as resolved
  // and constant-folded literal sql, but because they are fetched from external catalog,
  // it is possible that this assumption does not hold, so we fallback to full analysis
  // if we encounter an unresolved existsDefault
  private def fallbackResolveExistenceDefaultValue(
      field: StructField): Expression = {
    field.getExistenceDefaultValue().map { defaultSQL: String =>

      logWarning(log"Encountered unresolved exists default value: " +
        log"'${MDC(COLUMN_DEFAULT_VALUE, defaultSQL)}' " +
        log"for column ${MDC(COLUMN_NAME, field.name)} " +
        log"with ${MDC(COLUMN_DATA_TYPE_SOURCE, field.dataType)}, " +
        log"falling back to full analysis.")

      val expr = analyze(field, "", EXISTS_DEFAULT_COLUMN_METADATA_KEY)
      val literal = expr match {
        case _: ExprLiteral | _: Cast => expr
        case _ => throw SparkException.internalError(s"parse existence default as literal err," +
          s" field name: ${field.name}, value: $defaultSQL")
      }
      literal
    }.orNull
  }

  /**
   * If the provided default value is a literal of a wider type than the target column,
   * but the literal value fits within the narrower type, just coerce it for convenience.
   * Exclude boolean/array/struct/map types from consideration for this type coercion to
   * avoid surprising behavior like interpreting "false" as integer zero.
   */
  private def defaultValueFromWiderTypeLiteral(
      expr: Expression,
      targetType: DataType,
      colName: String): Option[Expression] = {
    expr match {
      case l: Literal if !Seq(targetType, l.dataType).exists(_ match {
        case _: BooleanType | _: ArrayType | _: StructType | _: MapType => true
        case _ => false
      }) =>
        val casted = Cast(l, targetType, Some(conf.sessionLocalTimeZone), evalMode = EvalMode.TRY)
        try {
          Option(casted.eval(EmptyRow)).map(Literal(_, targetType))
        } catch {
          case e @ ( _: SparkThrowable | _: RuntimeException) =>
            logWarning(log"Failed to cast default value '${MDC(COLUMN_DEFAULT_VALUE, l)}' " +
              log"for column ${MDC(COLUMN_NAME, colName)} " +
              log"from ${MDC(COLUMN_DATA_TYPE_SOURCE, l.dataType)} " +
              log"to ${MDC(COLUMN_DATA_TYPE_TARGET, targetType)} " +
              log"due to ${MDC(ERROR, e.getMessage)}", e)
            None
        }
      case _ => None
    }
  }

  /**
   * Returns the result of type coercion from [[analyzed]] to [[dataType]], or throws an error if
   * the expression is not coercible.
   */
  def coerceDefaultValue(
      analyzed: Expression,
      dataType: DataType,
      statementType: String,
      colName: String,
      defaultSQL: String): Expression = {
    val supplanted = CharVarcharUtils.replaceCharVarcharWithString(dataType)
    // Perform implicit coercion from the provided expression type to the required column type.
    val ret = analyzed match {
      case equivalent if equivalent.dataType == supplanted =>
        equivalent
      case canUpCast if Cast.canUpCast(canUpCast.dataType, supplanted) =>
        Cast(analyzed, supplanted, Some(conf.sessionLocalTimeZone))
      case other =>
        defaultValueFromWiderTypeLiteral(other, supplanted, colName).getOrElse(
          throw QueryCompilationErrors.defaultValuesDataTypeError(
            statementType, colName, defaultSQL, dataType, other.dataType))
    }
    if (!conf.charVarcharAsString && CharVarcharUtils.hasCharVarchar(dataType)) {
      CharVarcharUtils.stringLengthCheck(ret, dataType).eval(EmptyRow)
    }
    ret
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
    schema.fields.map(getExistenceDefaultValue)
  }

  def getExistenceDefaultValue(field: StructField): Any = {
    if (field.hasExistenceDefaultValue) {
      val expr = analyzeExistenceDefaultValue(field)
      // The expression should be a literal value by this point, possibly wrapped in a cast
      // function. This is enforced by the execution of commands that assign default values.
      expr.eval()
    } else {
      null
    }
  }

  /**
   * Returns an array of boolean values equal in size to the result of [[getExistenceDefaultValues]]
   * above, for convenience.
   */
  def getExistenceDefaultsBitmask(schema: StructType): Array[Boolean] = {
    Array.fill[Boolean](existenceDefaultValues(schema).length)(true)
  }

  /**
   * Resets the elements of the array initially returned from [[getExistenceDefaultsBitmask]] above.
   * Afterwards, set element(s) to false before calling [[applyExistenceDefaultValuesToRow]] below.
   */
  def resetExistenceDefaultsBitmask(schema: StructType, bitmask: Array[Boolean]): Unit = {
    val defaultValues = existenceDefaultValues(schema)
    for (i <- 0 until defaultValues.length) {
      bitmask(i) = (defaultValues(i) != null)
    }
  }

  /**
   * Updates a subset of columns in the row with default values from the metadata in the schema.
   */
  def applyExistenceDefaultValuesToRow(schema: StructType, row: InternalRow,
      bitmask: Array[Boolean]): Unit = {
    val existingValues = existenceDefaultValues(schema)
    if (hasExistenceDefaultValues(schema)) {
      for (i <- 0 until existingValues.length) {
        if (bitmask(i)) {
          row.update(i, existingValues(i))
        }
      }
    }
  }

  /** If any fields in a schema have default values, appends them to the result. */
  def getDescribeMetadata(schema: StructType): Seq[(String, String, String)] = {
    val rows = new ArrayBuffer[(String, String, String)]()
    if (schema.fields.exists(_.metadata.contains(CURRENT_DEFAULT_COLUMN_METADATA_KEY))) {
      rows.append(("", "", ""))
      rows.append(("# Column Default Values", "", ""))
      schema.foreach { column =>
        column.getCurrentDefaultValue().map { value =>
          rows.append((column.name, column.dataType.simpleString, value))
        }
      }
    }
    rows.toSeq
  }

  /**
   * These define existence default values for the struct fields for efficiency purposes.
   * The caller should avoid using such methods in a loop for efficiency.
   */
  def existenceDefaultValues(schema: StructType): Array[Any] =
    getExistenceDefaultValues(schema)
  def existenceDefaultsBitmask(schema: StructType): Array[Boolean] =
    getExistenceDefaultsBitmask(schema)
  def hasExistenceDefaultValues(schema: StructType): Boolean =
    existenceDefaultValues(schema).exists(_ != null)

  // Called to check default value expressions in the analyzed plan.
  def validateDefaultValueExpr(
      default: DefaultValueExpression,
      statement: String,
      colName: String,
      targetType: DataType): Unit = {
    if (default.containsPattern(PLAN_EXPRESSION)) {
      throw QueryCompilationErrors.defaultValuesMayNotContainSubQueryExpressions(
        statement, colName, default.originalSQL)
    } else if (default.resolved) {
      val dataType = CharVarcharUtils.replaceCharVarcharWithString(targetType)
      if (!Cast.canUpCast(default.child.dataType, dataType) &&
        defaultValueFromWiderTypeLiteral(default.child, dataType, colName).isEmpty) {
        throw QueryCompilationErrors.defaultValuesDataTypeError(
          statement, colName, default.originalSQL, targetType, default.child.dataType)
      }
      // Our analysis check passes here. We do not further inspect whether the
      // expression is `foldable` here, as the plan is not optimized yet.
    }

    if (default.references.nonEmpty || default.exists(_.isInstanceOf[VariableReference])) {
      // Ideally we should let the rest of `CheckAnalysis` report errors about why the default
      // expression is unresolved. But we should report a better error here if the default
      // expression references columns, which means it's not a constant for sure.
      // Note that, session variable should be considered as non-constant as well.
      throw QueryCompilationErrors.defaultValueNotConstantError(
        statement, colName, default.originalSQL)
    }
  }

  /**
   * This is an Analyzer for processing default column values using built-in functions only.
   */
  object DefaultColumnAnalyzer extends Analyzer(
    new CatalogManager(BuiltInFunctionCatalog, BuiltInFunctionCatalog.v1Catalog)) {
  }

  /**
   * This is an Optimizer for convert default column expressions to foldable literals.
   */
  object DefaultColumnOptimizer extends Optimizer(DefaultColumnAnalyzer.catalogManager)

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
      throw SparkUnsupportedOperationException()
    }
    override def loadFunction(ident: Identifier): UnboundFunction = {
      V1Function(v1Catalog.lookupPersistentFunction(ident.asFunctionIdentifier))
    }
    override def functionExists(ident: Identifier): Boolean = {
      v1Catalog.isPersistentFunction(ident.asFunctionIdentifier)
    }
  }
}
