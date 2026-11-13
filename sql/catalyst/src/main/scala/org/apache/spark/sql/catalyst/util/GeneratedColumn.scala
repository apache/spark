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
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, Expression, V2ExpressionUtils}
import org.apache.spark.sql.catalyst.plans.logical.{ColumnDefinition, LogicalPlan}
import org.apache.spark.sql.catalyst.util.TypeUtils.toSQLId
import org.apache.spark.sql.connector.catalog.{Column, GenerationExpression, Identifier, Table,
  TableCapability, TableCatalog, TableCatalogCapability}
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{Metadata, MetadataBuilder, StructField, StructType}

/**
 * This object contains utility methods and values for Generated Columns
 */
object GeneratedColumn {

  /**
   * The metadata key for saving a generation expression in a generated column's metadata. This is
   * only used internally and connectors should access generation expressions from the V2 columns.
   */
  val GENERATION_EXPRESSION_METADATA_KEY = "GENERATION_EXPRESSION"

  /**
   * The metadata key marking a generated column in a write target's output as one whose value
   * Spark computed from the generation expression, so that it is not validated against the
   * expression that produced it. Only set while resolving a write.
   */
  val AUTO_FILLED_GENERATED_COLUMN_METADATA_KEY = "__auto_filled_generated_column"

  /**
   * Whether the given `field` is a generated column
   */
  def isGeneratedColumn(field: StructField): Boolean = {
    isGeneratedColumn(field.metadata)
  }

  /**
   * Whether the given metadata indicates a generated column
   */
  def isGeneratedColumn(metadata: Metadata): Boolean = {
    metadata.contains(GENERATION_EXPRESSION_METADATA_KEY)
  }

  /**
   * Returns the generation expression stored in the column metadata if it exists
   */
  def getGenerationExpression(field: StructField): Option[String] = {
    getGenerationExpression(field.metadata)
  }

  /**
   * Returns the generation expression stored in the metadata if it exists
   */
  def getGenerationExpression(metadata: Metadata): Option[String] = {
    if (isGeneratedColumn(metadata)) {
      Some(metadata.getString(GENERATION_EXPRESSION_METADATA_KEY))
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
   * Check if the table catalog supports generated columns.
   * This is called from DataSourceV2Strategy for CREATE/REPLACE TABLE commands.
   */
  def validateCatalogForGeneratedColumn(
      columns: Seq[ColumnDefinition],
      catalog: TableCatalog,
      ident: Identifier): Unit = {
    if (columns.exists(_.generationExpression.isDefined)) {
      if (!catalog.capabilities().contains(
        TableCatalogCapability.SUPPORTS_CREATE_TABLE_WITH_GENERATED_COLUMNS)) {
        throw QueryCompilationErrors.unsupportedTableOperationError(
          catalog, ident, "generated columns")
      }
    }
  }

  /**
   * Whether the table wants Spark to auto-fill generated column values and enforce generated
   * column constraints during writes (the
   * [[TableCapability.GENERATE_COLUMN_VALUES_ON_WRITE]] capability). Without it, the connector is
   * responsible for handling generated column values.
   */
  def supportsGeneratedColumnsOnWrite(table: Table): Boolean = {
    table.capabilities().contains(TableCapability.GENERATE_COLUMN_VALUES_ON_WRITE)
  }

  /**
   * Returns the generation expression of the V2 column `col`, if it is a generated column. A
   * connector can implement only the legacy SQL getter [[Column.generationExpression]], which
   * auto-fill reads as well, so fall back to it. Also retain that SQL when the new getter provides
   * only a modeled expression.
   */
  def columnGenerationExpression(col: Column): Option[GenerationExpression] = {
    Option(col.columnGenerationExpression())
      .map { expression =>
        if (expression.getSql != null) {
          expression
        } else {
          Option(col.generationExpression())
            .map(sql => new GenerationExpression(sql, expression.getExpression))
            .getOrElse(expression)
        }
      }
      .orElse(Option(col.generationExpression()).map(new GenerationExpression(_)))
  }

  /**
   * Whether the table supports generated columns on write (see
   * [[supportsGeneratedColumnsOnWrite]]) and the given columns include at least one generated
   * column.
   */
  def supportsGeneratedColumnsOnWrite(
      table: Table,
      columns: Array[Column]): Boolean = {
    supportsGeneratedColumnsOnWrite(table) &&
      columns.exists(columnGenerationExpression(_).isDefined)
  }

  /**
   * Reads a generation expression for both auto-fill and validation. Convert the modeled expression
   * once, falling back to SQL text when it is absent or cannot be converted, as defaults and CHECK
   * constraints do. Conversion is deferred so presence checks do not read either form.
   */
  private[sql] class ExpressionReader(
      relation: DataSourceV2Relation,
      colName: String,
      genExpr: GenerationExpression) {
    private lazy val modeledExpression = Option(genExpr.getExpression).flatMap { expression =>
      SQLConf.withExistingConf(StoredExpressionText.fixedConf(SQLConf.get)) {
        V2ExpressionUtils.toCatalyst(expression)
      }
    }

    def toCatalyst(readSql: String => Expression = StoredExpressionText.parse): Expression = {
      modeledExpression.orElse(Option(genExpr.getSql).map(readSql))
        .getOrElse {
          val operation = s"a value for the generated column ${toSQLId(colName)}, because " +
            s"Spark cannot convert its expression ${genExpr.getExpression}"
          // A relation of a TableProvider has no catalog and no identifier.
          throw ((relation.catalog, relation.identifier) match {
            case (Some(catalog), Some(ident)) =>
              QueryCompilationErrors.unsupportedTableOperationError(catalog, ident, operation)
            case _ =>
              QueryCompilationErrors.unsupportedTableOperationError(relation.table, operation)
          })
        }
    }

    def sql: String = modeledExpression.map(_.sql).getOrElse(genExpr.getSql)

    /** Only SQL text may fall back to analysis under the writing session's settings. */
    def analyze[E <: Expression](
        expr: E,
        inSession: => E,
        input: Seq[Attribute],
        analyzer: LogicalPlan => LogicalPlan): E = {
      if (modeledExpression.isEmpty) {
        StoredExpressionText.analyze(expr, inSession, input, analyzer)
      } else {
        try {
          StoredExpressionText.analyzeStrict(expr, input, analyzer)
        } catch {
          case error: AnalysisException =>
            val tableName = (relation.catalog, relation.identifier) match {
              case (Some(catalog), Some(ident)) =>
                toSQLId(Seq(catalog.name) ++ ident.namespace.toSeq :+ ident.name)
              case _ => toSQLId(relation.table.name())
            }
            throw new AnalysisException(
              errorClass = "INVALID_GENERATED_COLUMN_EXPRESSION",
              messageParameters = Map(
                "tableName" -> tableName,
                "columnName" -> toSQLId(colName),
                "expression" -> sql,
                "reason" -> error.getMessage),
              cause = Some(error))
        }
      }
    }
  }

  /**
   * When a write supplies its own value for a generated column, that value must agree with the
   * generation expression, so ResolveTableConstraints validates it with a CheckInvariant, the same
   * way it enforces a table's CHECK constraints. In contrast, values computed by Spark from the
   * generation expression pass that check by construction, so this mark is what tells the rule to
   * skip them.
   *
   * Returns a write target's `output` with the generated columns named in `autoFilled` marked as
   * computed by Spark, so that it can skip CheckInvariant validation.
   *
   * Columns are left unmarked by default, so a value that did not come from the generation
   * expression is always validated.
   */
  def markAutoFilledGeneratedColumns(
      output: Seq[AttributeReference],
      autoFilled: Set[String]): Seq[AttributeReference] = {
    output.map { attr =>
      if (autoFilled.contains(attr.name)) {
        withMetadataEntry(attr, AUTO_FILLED_GENERATED_COLUMN_METADATA_KEY, "true")
      } else {
        attr
      }
    }
  }

  /**
   * Whether `attr` is a generated column whose value Spark computed from the generation
   * expression (see [[markAutoFilledGeneratedColumns]]).
   */
  def isAutoFilledGeneratedColumn(attr: Attribute): Boolean = {
    attr.metadata.contains(AUTO_FILLED_GENERATED_COLUMN_METADATA_KEY)
  }

  private def withMetadataEntry(
      attr: AttributeReference,
      key: String,
      value: String): AttributeReference = {
    val metadata = new MetadataBuilder()
      .withMetadata(attr.metadata)
      .putString(key, value)
      .build()
    attr.withMetadata(metadata).asInstanceOf[AttributeReference]
  }
}
