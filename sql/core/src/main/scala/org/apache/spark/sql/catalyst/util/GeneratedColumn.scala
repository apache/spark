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

import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.sql.catalyst.analysis.Analyzer
import org.apache.spark.sql.catalyst.expressions.{Alias, Expression}
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, Project}
import org.apache.spark.sql.catalyst.util.ResolveDefaultColumns.BuiltInFunctionCatalog
import org.apache.spark.sql.connector.catalog.CatalogManager
import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.types.{MetadataBuilder, StructField, StructType}

/**
 * This object contains utility methods for Generated Columns
 */
object GeneratedColumn {

  /**
   * Whether the given `field` is a generated column
   */
  private def isGeneratedColumn(field: StructField): Boolean = {
    field.metadata.contains(Table.GENERATION_EXPRESSION_METADATA_KEY)
  }

  /**
   * Returns the generation expression stored in the column metadata if it exists
   */
  private def getGenerationExpression(field: StructField): Option[String] = {
    if (isGeneratedColumn(field)) {
      Some(field.metadata.getString(Table.GENERATION_EXPRESSION_METADATA_KEY))
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
   * Verify that `expressionStr` can be converted to V2
   * [[org.apache.spark.sql.connector.expressions.Expression]] and return the V2 expression
   * as a SQL string.
   *
   * Throws an [[AnalysisException]] if the expression cannot be converted.
   */
  private def convertToV2ExpressionSQL(
      session: SparkSession,
      expressionStr: String,
      fieldName: String,
      schema: StructType,
      statementType: String): String = {
    // Parse the expression string
    val parsed: Expression = try {
      session.sessionState.sqlParser.parseExpression(expressionStr)
    } catch {
      case ex: ParseException =>
        // Shouldn't be possible since we check that the expression is a valid catalyst expression
        // during parsing
        throw new AnalysisException(
          s"Failed to execute $statementType command because the column $fieldName has " +
            s"generation expression $expressionStr which fails to parse as a valid expression:" +
            s"\n${ex.getMessage}")
    }
    // Analyze the parse result
    // Generated column can't reference itself
    val relation = new LocalRelation(StructType(schema.filterNot(_.name == fieldName)).toAttributes)
    val plan = try {
      val analyzer: Analyzer = GeneratedColumnAnalyzer
      val analyzed = analyzer.execute(Project(Seq(Alias(parsed, fieldName)()), relation))
      analyzer.checkAnalysis(analyzed)
      analyzed
    } catch {
      case ex: AnalysisException =>
        val columnList = schema.filterNot(_.name == fieldName).map(_.name).mkString("[", ",", "]")
        throw new AnalysisException(
          s"Failed to execute $statementType command because the column $fieldName has " +
            s"generation expression $expressionStr which fails to resolve as a valid expression " +
            s"given columns $columnList:" +
            s"\n${ex.getMessage}")
    }
    val analyzed = plan.collectFirst {
      case Project(Seq(a: Alias), _: LocalRelation) => a.child
    }.get
    // Try to convert to V2 Expression and then to SQL string
    new V2ExpressionBuilder(analyzed).build().getOrElse {
      throw new AnalysisException(
        errorClass = "UNSUPPORTED_EXPRESSION_GENERATED_COLUMN",
        messageParameters = Map(
          "fieldName" -> fieldName,
          "expressionStr" -> expressionStr
        )
      )
    }.toString // toString uses V2ExpressionSQLBuilder
  }

  /**
   * For any generated columns in `schema`, verify that the generation expression is a valid
   * V2 [[org.apache.spark.sql.connector.expressions.Expression]] and convert the expression string
   * to V2 Expression SQL.
   */
  def verifyAndConvertToV2ExpressionSQL(session: SparkSession,
    schema: StructType, statementType: String): StructType = {
    val newFields = schema.map { field =>
      getGenerationExpression(field).map { expressionStr =>
        val updatedExpressionStr =
          convertToV2ExpressionSQL(session, expressionStr, field.name, schema, statementType)
        field.copy(
          metadata = new MetadataBuilder().withMetadata(field.metadata)
            .putString(Table.GENERATION_EXPRESSION_METADATA_KEY, updatedExpressionStr)
            .build()
        )
      }.getOrElse(field)
    }
    StructType(newFields)
  }
}

/**
 * Analyzer for processing generated column expressions using built-in functions only.
 */
object GeneratedColumnAnalyzer extends Analyzer(
  new CatalogManager(BuiltInFunctionCatalog, BuiltInFunctionCatalog.v1Catalog)) {
}
