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

import java.util.{Map => JMap}

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.catalog.{CatalogDatabase, InMemoryCatalog, SessionCatalog}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.{Literal => ExprLiteral}
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.trees.TreePattern.PLAN_EXPRESSION
import org.apache.spark.sql.connector.catalog.{CatalogManager, CatalogPlugin, CatalogV2Util, FunctionCatalog, Identifier, TableCatalog}
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
   * Parses and analyzes the DEFAULT column text in `field`. The default value has already been
   * validated in CREATE/REPLACE/ALTER TABLE commands. We don't need to validate it again when
   * reading it out.
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
    // Parse the expression.
    val colText: String = field.metadata.getString(metadataKey)
    val parser = new CatalystSqlParser()
    val parsed: Expression = parser.parseExpression(colText)
    // Analyze the parse result.
    val analyzer: Analyzer = DefaultColumnAnalyzer
    val plan = analyzer.execute(Project(Seq(Alias(parsed, field.name)()), OneRowRelation()))
    val analyzed: Expression = plan.collectFirst {
      case Project(Seq(a: Alias), OneRowRelation()) => a.child
    }.get
    // Perform implicit coercion from the provided expression type to the required column type.
    if (field.dataType == analyzed.dataType) {
      analyzed
    } else {
      Cast(analyzed, field.dataType)
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
          val expr = analyze(field, "", EXISTS_DEFAULT_COLUMN_METADATA_KEY)
          expr match {
            case _: ExprLiteral | _: Cast => expr
          }
        } catch {
          case _: AnalysisException | _: MatchError =>
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

  def checkDefaultValuesInPlan(plan: LogicalPlan, isForV1: Boolean = false): Unit = {
    plan match {
      // Do not check anything if the children are not resolved yet.
      case _ if !plan.childrenResolved =>
      case AlterColumn(t: ResolvedTable, col: ResolvedFieldName, _, _, _, _,
          Some(default: DefaultValueExpression)) =>
        checkTableProvider(t.catalog, t.name, getTableProviderFromProp(t.table.properties()))
        checkDefaultValue(default, t.name, col.name, col.field.dataType, isForV1)

      case cmd: V2CreateTablePlan if cmd.columns.exists(_.defaultValue.isDefined) =>
        val ident = cmd.resolvedName
        checkTableProvider(ident.catalog, ident.name, cmd.tableSpec.provider)
        cmd.columns.filter(_.defaultValue.isDefined).foreach { col =>
          val Column(name, dataType, _, _, Some(default), _) = col
          // CREATE/REPLACE TABLE only has top-level columns
          val colName = Seq(name)
          checkDefaultValue(default, ident.name, colName, dataType, isForV1)
        }

      case cmd: AlterTableCommand =>
        val table = cmd.resolvedTable
        cmd.transformExpressionsDown {
          case q @ QualifiedColType(path, Column(name, dataType, _, _, Some(default), _), _)
            if path.resolved =>
            checkTableProvider(
              table.catalog, table.name, getTableProviderFromProp(table.table.properties()))
            checkDefaultValue(
              default,
              table.name,
              path.name :+ name,
              dataType,
              isForV1)
            q
        }

      case _ =>
    }
  }

  private def getTableProviderFromProp(props: JMap[String, String]): Option[String] = {
    Option(props.get(TableCatalog.PROP_PROVIDER))
  }

  private def checkTableProvider(
      catalog: CatalogPlugin,
      tableName: String,
      provider: Option[String]): Unit = {
    // We only need to check table provider for the session catalog. Other custom v2 catalogs
    // can check table providers in their implementations of createTable, alterTable, etc.
    if (CatalogV2Util.isSessionCatalog(catalog)) {
      val conf = SQLConf.get
      val allowedProviders: Array[String] = conf.getConf(SQLConf.DEFAULT_COLUMN_ALLOWED_PROVIDERS)
        .toLowerCase().split(",").map(_.trim)
      val providerName = provider.getOrElse(conf.defaultDataSourceName).toLowerCase()
      if (!allowedProviders.contains(providerName)) {
        throw QueryCompilationErrors.defaultReferencesNotAllowedInDataSource(tableName)
      }
    }
  }

  private def checkDefaultValue(
      default: DefaultValueExpression,
      tblName: String,
      colName: Seq[String],
      targetType: DataType,
      isForV1: Boolean): Unit = {
    if (default.resolved) {
      if (!default.child.foldable) {
        throw QueryCompilationErrors.notConstantDefaultValueError(
          tblName, colName, default.originalSQL)
      }
      if (!Cast.canUpCast(default.child.dataType, targetType)) {
        throw QueryCompilationErrors.incompatibleTypeDefaultValueError(
          tblName, colName, targetType, default.child, default.originalSQL)
      }
    } else {
      // Ideally we should let the rest of `CheckAnalysis` to report errors about why the default
      // expression is unresolved. But we should report a better error here if the default
      // expression references columns or contains subquery expressions, which means it's not a
      // constant for sure.
      if (default.references.nonEmpty || default.containsPattern(PLAN_EXPRESSION)) {
        throw QueryCompilationErrors.notConstantDefaultValueError(
          tblName, colName, default.originalSQL)
      }
      // When converting to v1 commands, the plan is not fully resolved and we can't do a complete
      // analysis check. There is no "rest of CheckAnalysis" to report better errors and we must
      // fail here. This is temporary and we can remove it when using v2 commands by default.
      if (isForV1) {
        throw QueryCompilationErrors.notConstantDefaultValueError(
          tblName, colName, default.originalSQL)
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
