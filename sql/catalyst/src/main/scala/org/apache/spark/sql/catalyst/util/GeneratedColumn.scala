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

import org.apache.spark.SparkException
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.analysis.Analyzer
import org.apache.spark.sql.catalyst.expressions.{Alias, Cast, Expression}
import org.apache.spark.sql.catalyst.parser.{CatalystSqlParser, ParseException}
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, Project}
import org.apache.spark.sql.catalyst.trees.TreePattern.PLAN_EXPRESSION
import org.apache.spark.sql.catalyst.util.ResolveDefaultColumns.BuiltInFunctionCatalog
import org.apache.spark.sql.connector.catalog.{CatalogManager, Identifier, TableCatalog, TableCatalogCapability}
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DataType, StructField, StructType}
import org.apache.spark.sql.util.SchemaUtils

/**
 * This object contains utility methods and values for Generated Columns
 */
object GeneratedColumn {

  /**
   * The metadata key for saving a generation expression in a generated column's metadata. This is
   * only used internally and connectors should access generation expressions from the V2 columns.
   */
  val GENERATION_EXPRESSION_METADATA_KEY = "GENERATION_EXPRESSION"

  /** Parser for parsing generation expression SQL strings */
  private lazy val parser = new CatalystSqlParser()

  /**
   * Whether the given `field` is a generated column
   */
  def isGeneratedColumn(field: StructField): Boolean = {
    field.metadata.contains(GENERATION_EXPRESSION_METADATA_KEY)
  }

  /**
   * Returns the generation expression stored in the column metadata if it exists
   */
  def getGenerationExpression(field: StructField): Option[String] = {
    if (isGeneratedColumn(field)) {
      Some(field.metadata.getString(GENERATION_EXPRESSION_METADATA_KEY))
    } else {
      None
    }
  }

  /**
   * Whether the `schema` has one or more generated columns
   */
  def hasGeneratedColumns(schema: StructType): Boolean = {
    schema.exists(isGeneratedColumn)
  }

  /**
   * Parse and analyze `expressionStr` and perform verification. This means:
   * - The expression cannot reference itself
   * - The expression cannot reference other generated columns
   * - No user-defined expressions
   * - The expression must be deterministic
   * - The expression data type can be safely up-cast to the destination column data type
   * - No subquery expressions
   *
   * Throws an [[AnalysisException]] if the expression cannot be converted or is an invalid
   * generation expression according to the above rules.
   */
  private def analyzeAndVerifyExpression(
      expressionStr: String,
      fieldName: String,
      dataType: DataType,
      schema: StructType,
      statementType: String): Unit = {
    def unsupportedExpressionError(reason: String): AnalysisException = {
      new AnalysisException(
        errorClass = "UNSUPPORTED_EXPRESSION_GENERATED_COLUMN",
        messageParameters = Map(
          "fieldName" -> fieldName,
          "expressionStr" -> expressionStr,
          "reason" -> reason))
    }

    // Parse the expression string
    val parsed: Expression = try {
      parser.parseExpression(expressionStr)
    } catch {
      case ex: ParseException =>
        // Shouldn't be possible since we check that the expression is a valid catalyst expression
        // during parsing
        throw SparkException.internalError(
          s"Failed to execute $statementType command because the column $fieldName has " +
            s"generation expression $expressionStr which fails to parse as a valid expression:" +
            s"\n${ex.getMessage}")
    }
    // Don't allow subquery expressions
    if (parsed.containsPattern(PLAN_EXPRESSION)) {
      throw unsupportedExpressionError("subquery expressions are not allowed for generated columns")
    }
    // Analyze the parsed result
    val allowedBaseColumns = schema
      .filterNot(_.name == fieldName) // Can't reference itself
      .filterNot(isGeneratedColumn) // Can't reference other generated columns
    val relation = LocalRelation(
      CharVarcharUtils.replaceCharVarcharWithStringInSchema(StructType(allowedBaseColumns)))
    val plan = try {
      val analyzer: Analyzer = GeneratedColumnAnalyzer
      val analyzed = analyzer.execute(Project(Seq(Alias(parsed, fieldName)()), relation))
      analyzer.checkAnalysis(analyzed)
      analyzed
    } catch {
      case ex: AnalysisException =>
        // Improve error message if possible
        if (ex.getErrorClass == "UNRESOLVED_COLUMN.WITH_SUGGESTION") {
          ex.messageParameters.get("objectName").foreach { unresolvedCol =>
            val resolver = SQLConf.get.resolver
            // Whether `col` = `unresolvedCol` taking into account case-sensitivity
            def isUnresolvedCol(col: String) =
              resolver(unresolvedCol, QueryCompilationErrors.toSQLId(col))
            // Check whether the unresolved column is this column
            if (isUnresolvedCol(fieldName)) {
              throw unsupportedExpressionError("generation expression cannot reference itself")
            }
            // Check whether the unresolved column is another generated column in the schema
            if (schema.exists(col => isGeneratedColumn(col) && isUnresolvedCol(col.name))) {
              throw unsupportedExpressionError(
                "generation expression cannot reference another generated column")
            }
          }
        }
        if (ex.getErrorClass == "UNRESOLVED_ROUTINE") {
          // Cannot resolve function using built-in catalog
          ex.messageParameters.get("routineName").foreach { fnName =>
            throw unsupportedExpressionError(s"failed to resolve $fnName to a built-in function")
          }
        }
        throw ex
    }
    val analyzed = plan.collectFirst {
      case Project(Seq(a: Alias), _: LocalRelation) => a.child
    }.get
    if (!analyzed.deterministic) {
      throw unsupportedExpressionError("generation expression is not deterministic")
    }
    if (!Cast.canUpCast(analyzed.dataType, dataType)) {
      throw unsupportedExpressionError(
        s"generation expression data type ${analyzed.dataType.simpleString} " +
        s"is incompatible with column data type ${dataType.simpleString}")
    }
    if (analyzed.exists(e => SchemaUtils.hasNonUTF8BinaryCollation(e.dataType))) {
      throw unsupportedExpressionError(
        "generation expression cannot contain non utf8 binary collated string type")
    }
  }

  /**
   * For any generated columns in `schema`, parse, analyze and verify the generation expression.
   */
  private def verifyGeneratedColumns(schema: StructType, statementType: String): Unit = {
    schema.foreach { field =>
      getGenerationExpression(field).foreach { expressionStr =>
        analyzeAndVerifyExpression(expressionStr, field.name, field.dataType, schema, statementType)
      }
    }
  }

  /**
   * If `schema` contains any generated columns:
   * 1) Check whether the table catalog supports generated columns. Otherwise throw an error.
   * 2) Parse, analyze and verify the generation expressions for any generated columns.
   */
  def validateGeneratedColumns(
      schema: StructType,
      catalog: TableCatalog,
      ident: Identifier,
      statementType: String): Unit = {
    if (hasGeneratedColumns(schema)) {
      if (!catalog.capabilities().contains(
        TableCatalogCapability.SUPPORTS_CREATE_TABLE_WITH_GENERATED_COLUMNS)) {
        throw QueryCompilationErrors.unsupportedTableOperationError(
          catalog, ident, "generated columns")
      }
      GeneratedColumn.verifyGeneratedColumns(schema, statementType)
    }
  }
}

/**
 * Analyzer for processing generated column expressions using built-in functions only.
 */
object GeneratedColumnAnalyzer extends Analyzer(
  new CatalogManager(BuiltInFunctionCatalog, BuiltInFunctionCatalog.v1Catalog)) {
}
