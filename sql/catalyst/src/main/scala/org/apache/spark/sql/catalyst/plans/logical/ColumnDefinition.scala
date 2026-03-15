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

package org.apache.spark.sql.catalyst.plans.logical

import org.apache.spark.SparkException
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.expressions.{AnalysisAwareExpression, AttributeReference, Cast, Expression, Literal, UnaryExpression, Unevaluable}
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.trees.TreePattern.{ANALYSIS_AWARE_EXPRESSION, PLAN_EXPRESSION, TreePattern}
import org.apache.spark.sql.catalyst.util.{CharVarcharUtils, GeneratedColumn, IdentityColumn, V2ExpressionBuilder}
import org.apache.spark.sql.catalyst.util.ResolveDefaultColumns.validateDefaultValueExpr
import org.apache.spark.sql.catalyst.util.ResolveDefaultColumnsUtils.{CURRENT_DEFAULT_COLUMN_METADATA_KEY, EXISTS_DEFAULT_COLUMN_METADATA_KEY}
import org.apache.spark.sql.connector.catalog.{Column => V2Column, ColumnDefaultValue, DefaultValue, GenerationExpression, IdentityColumnSpec}
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits.MultipartIdentifierHelper
import org.apache.spark.sql.connector.expressions.{Expression => V2Expression, LiteralValue}
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.connector.ColumnImpl
import org.apache.spark.sql.types.{DataType, Metadata, MetadataBuilder, StructField}
import org.apache.spark.sql.util.SchemaUtils

/**
 * User-specified column definition for CREATE/REPLACE TABLE commands. This is an expression so that
 * analyzer can resolve the default value expression automatically.
 *
 * For CREATE/REPLACE TABLE commands, columns are created from scratch, so we store the
 * user-specified default value as both the current default and exists default, in methods
 * `toV1Column` and `toV2Column`.
 */
case class ColumnDefinition(
    name: String,
    dataType: DataType,
    nullable: Boolean = true,
    comment: Option[String] = None,
    defaultValue: Option[DefaultValueExpression] = None,
    generationExpression: Option[GeneratedColumnExpression] = None,
    identityColumnSpec: Option[IdentityColumnSpec] = None,
    metadata: Metadata = Metadata.empty) extends Expression with Unevaluable {
  assert(
    generationExpression.isEmpty || identityColumnSpec.isEmpty,
    "A ColumnDefinition cannot contain both a generation expression and an identity column spec.")

  override def children: Seq[Expression] = defaultValue.toSeq ++ generationExpression.toSeq

  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[Expression]): Expression = {
    val hasDefault = defaultValue.isDefined
    val hasGenExpr = generationExpression.isDefined
    val newDefault = if (hasDefault) {
      Some(newChildren.head.asInstanceOf[DefaultValueExpression])
    } else {
      None
    }
    val newGenExpr = if (hasGenExpr) {
      val idx = if (hasDefault) 1 else 0
      Some(newChildren(idx).asInstanceOf[GeneratedColumnExpression])
    } else {
      None
    }
    copy(defaultValue = newDefault, generationExpression = newGenExpr)
  }

  def toV2Column(statement: String): V2Column = {
    ColumnImpl(
      name,
      dataType,
      nullable,
      comment.orNull,
      defaultValue.map(_.toV2(statement, name)).orNull,
      generationExpression.map(_.toV2).orNull,
      identityColumnSpec.orNull,
      if (metadata == Metadata.empty) null else metadata.json)
  }

  def toV1Column: StructField = {
    val metadataBuilder = new MetadataBuilder().withMetadata(metadata)
    comment.foreach { c =>
      metadataBuilder.putString("comment", c)
    }
    defaultValue.foreach { default =>
      metadataBuilder.putExpression(
        CURRENT_DEFAULT_COLUMN_METADATA_KEY, default.originalSQL, Some(default.child))
      val existsSQL = default.child match {
        case l: Literal => l.sql
        case _ => default.originalSQL
      }
      metadataBuilder.putString(EXISTS_DEFAULT_COLUMN_METADATA_KEY, existsSQL)
    }
    generationExpression.foreach { genExpr =>
      metadataBuilder.putString(GeneratedColumn.GENERATION_EXPRESSION_METADATA_KEY,
        genExpr.originalSQL)
    }
    encodeIdentityColumnSpec(metadataBuilder)
    StructField(name, dataType, nullable, metadataBuilder.build())
  }

  private def encodeIdentityColumnSpec(metadataBuilder: MetadataBuilder): Unit = {
    identityColumnSpec.foreach { spec: IdentityColumnSpec =>
      metadataBuilder.putLong(IdentityColumn.IDENTITY_INFO_START, spec.getStart)
      metadataBuilder.putLong(IdentityColumn.IDENTITY_INFO_STEP, spec.getStep)
      metadataBuilder.putBoolean(
        IdentityColumn.IDENTITY_INFO_ALLOW_EXPLICIT_INSERT,
        spec.isAllowExplicitInsert)
    }
  }

  /**
   * Returns true if the default value's type has been coerced to match this column's dataType.
   * After type coercion, the default value expression's dataType should match the column's
   * dataType (with CHAR/VARCHAR replaced by STRING).
   */
  def isDefaultValueTypeCoerced: Boolean = defaultValue.forall { d =>
    ColumnDefinition.isDefaultValueTypeMatched(d.child.dataType, dataType)
  }
}

object ColumnDefinition {

  /**
   * Returns true if the default value's type matches the target column type.
   * CHAR/VARCHAR types are replaced with STRING before comparison since type coercion
   * converts them to STRING.
   */
  def isDefaultValueTypeMatched(defaultValueType: DataType, targetType: DataType): Boolean = {
    val expectedType = CharVarcharUtils.replaceCharVarcharWithString(targetType)
    defaultValueType == expectedType
  }

  def fromV1Column(col: StructField, parser: ParserInterface): ColumnDefinition = {
    val metadataBuilder = new MetadataBuilder().withMetadata(col.metadata)
    metadataBuilder.remove("comment")
    metadataBuilder.remove(CURRENT_DEFAULT_COLUMN_METADATA_KEY)
    metadataBuilder.remove(EXISTS_DEFAULT_COLUMN_METADATA_KEY)
    metadataBuilder.remove(GeneratedColumn.GENERATION_EXPRESSION_METADATA_KEY)
    metadataBuilder.remove(IdentityColumn.IDENTITY_INFO_START)
    metadataBuilder.remove(IdentityColumn.IDENTITY_INFO_STEP)
    metadataBuilder.remove(IdentityColumn.IDENTITY_INFO_ALLOW_EXPLICIT_INSERT)

    val hasDefaultValue = col.getCurrentDefaultValue().isDefined &&
      col.getExistenceDefaultValue().isDefined
    val defaultValue = if (hasDefaultValue) {
      // `ColumnDefinition` is for CREATE/REPLACE TABLE commands, and it only needs one
      // default value. Here we assume user wants the current default of the v1 column to be
      // the default value of this column definition.
      val defaultValueSQL = col.getCurrentDefaultValue().get
      Some(DefaultValueExpression(parser.parseExpression(defaultValueSQL), defaultValueSQL))
    } else {
      None
    }
    val generationExpr = GeneratedColumn.getGenerationExpression(col).map { sql =>
      GeneratedColumnExpression(parser.parseExpression(sql), sql)
    }
    val identityColumnSpec = if (col.metadata.contains(IdentityColumn.IDENTITY_INFO_START)) {
      Some(new IdentityColumnSpec(
        col.metadata.getLong(IdentityColumn.IDENTITY_INFO_START),
        col.metadata.getLong(IdentityColumn.IDENTITY_INFO_STEP),
        col.metadata.getBoolean(IdentityColumn.IDENTITY_INFO_ALLOW_EXPLICIT_INSERT)
      ))
    } else {
      None
    }
    ColumnDefinition(
      col.name,
      col.dataType,
      col.nullable,
      col.getComment(),
      defaultValue,
      generationExpr,
      identityColumnSpec,
      metadataBuilder.build()
    )
  }

  // Called by `CheckAnalysis` to check column definitions in DDL commands.
  def checkColumnDefinitions(plan: LogicalPlan): Unit = {
    plan match {
      // Do not check anything if the children are not resolved yet.
      case _ if !plan.childrenResolved =>

      // Wrap errors for default values in a more user-friendly message.
      case cmd: V2CreateTablePlan if cmd.columns.exists(_.defaultValue.isDefined) =>
        val statement = cmd match {
          case _: CreateTable => "CREATE TABLE"
          case _: ReplaceTable => "REPLACE TABLE"
          case other =>
            val cmd = other.getClass.getSimpleName
            throw SparkException.internalError(
              s"Command $cmd should not have column default value expression.")
        }
        cmd.columns.foreach { col =>
          col.defaultValue.foreach { default =>
            checkDefaultColumnConflicts(col)
            validateDefaultValueExpr(default, statement, col.name, Some(col.dataType))
          }
        }

      case cmd: AddColumns if cmd.columnsToAdd.exists(_.default.isDefined) =>
        cmd.columnsToAdd.foreach { c =>
          c.default.foreach { d =>
            validateDefaultValueExpr(d, "ALTER TABLE ADD COLUMNS", c.colName, Some(c.dataType))
          }
        }

      case cmd: AlterColumns if cmd.specs.exists(_.newDefaultExpression.isDefined) =>
        cmd.specs.foreach { c =>
          c.newDefaultExpression.foreach { d =>
            validateDefaultValueExpr(d, "ALTER TABLE ALTER COLUMN", c.column.name.quoted,
              None)
          }
        }

      case _ =>
    }
  }

  private def checkDefaultColumnConflicts(col: ColumnDefinition): Unit = {
    if (col.generationExpression.isDefined) {
      throw new AnalysisException(
        errorClass = "GENERATED_COLUMN_WITH_DEFAULT_VALUE",
        messageParameters = Map(
          "colName" -> col.name,
          "defaultValue" -> col.defaultValue.get.originalSQL,
          "genExpr" -> col.generationExpression.get.originalSQL
        )
      )
    }
    if (col.identityColumnSpec.isDefined) {
      throw new AnalysisException(
        errorClass = "IDENTITY_COLUMN_WITH_DEFAULT_VALUE",
        messageParameters = Map(
          "colName" -> col.name,
          "defaultValue" -> col.defaultValue.get.originalSQL,
          "identityColumnSpec" -> col.identityColumnSpec.get.toString
        )
      )
    }
  }
}

/**
 * A fake expression to hold the column/variable default value expression and its original SQL text.
 */
case class DefaultValueExpression(
    child: Expression,
    originalSQL: String,
    analyzedChild: Option[Expression] = None)
  extends UnaryExpression
  with Unevaluable
  with AnalysisAwareExpression[DefaultValueExpression] {

  final override val nodePatterns: Seq[TreePattern] = Seq(ANALYSIS_AWARE_EXPRESSION)

  override def dataType: DataType = child.dataType
  override def stringArgs: Iterator[Any] = Iterator(child, originalSQL)
  override def markAsAnalyzed(): DefaultValueExpression =
    copy(analyzedChild = Some(child))
  override protected def withNewChildInternal(newChild: Expression): Expression =
    copy(child = newChild)

  // Convert the default expression to ColumnDefaultValue, which is required by DS v2 APIs.
  def toV2(statement: String, colName: String): ColumnDefaultValue = child match {
    case Literal(value, dataType) =>
      val currentDefault = analyzedChild.flatMap(new V2ExpressionBuilder(_).build())
      val existsDefault = LiteralValue(value, dataType)
      new ColumnDefaultValue(originalSQL, currentDefault.orNull, existsDefault)
    case _ =>
      throw QueryCompilationErrors.defaultValueNotConstantError(statement, colName, originalSQL)
  }

  // Convert the default expression to DefaultValue, which is required by DS v2 APIs.
  def toV2CurrentDefault(statement: String, colName: String): DefaultValue = child match {
    case Literal(_, _) =>
      val currentDefault = analyzedChild.flatMap(new V2ExpressionBuilder(_).build())
      new DefaultValue(originalSQL, currentDefault.orNull)
    case _ =>
      throw QueryCompilationErrors.defaultValueNotConstantError(statement, colName, originalSQL)
  }
}

/**
 * A wrapper expression to hold the generation expression and its original SQL text.
 * The child expression is resolved by the normal analyzer rules through the expression tree.
 */
case class GeneratedColumnExpression(
    child: Expression,
    originalSQL: String)
  extends UnaryExpression with Unevaluable {

  override def dataType: DataType = child.dataType

  override def stringArgs: Iterator[Any] = Iterator(child, originalSQL)

  override protected def withNewChildInternal(newChild: Expression): Expression =
    copy(child = newChild)

  /**
   * Validate the generation expression and throw an AnalysisException if invalid.
   * Validations include:
   * - The expression cannot reference itself
   * - The expression cannot reference other generated columns
   * - The expression must be deterministic
   * - The expression data type can be safely up-cast to the destination column data type
   * - No subquery expressions
   * - No non-UTF8 binary collation
   */
  def validate(
      fieldName: String,
      targetDataType: DataType,
      allColumns: Seq[ColumnDefinition]): Unit = {
    def unsupportedExpressionError(reason: String): AnalysisException = {
      new AnalysisException(
        errorClass = "UNSUPPORTED_EXPRESSION_GENERATED_COLUMN",
        messageParameters = Map(
          "fieldName" -> fieldName,
          "expressionStr" -> originalSQL,
          "reason" -> reason))
    }

    // Don't allow subquery expressions
    if (child.containsPattern(PLAN_EXPRESSION)) {
      throw unsupportedExpressionError("subquery expressions are not allowed for generated columns")
    }

    // Use the resolver to respect case sensitivity settings
    val resolver = SQLConf.get.resolver

    // Check for self-reference - the expression cannot reference itself
    val referencedColumns = child.collect {
      case a: AttributeReference => a.name
    }
    if (referencedColumns.exists(resolver(_, fieldName))) {
      throw unsupportedExpressionError("generation expression cannot reference itself")
    }

    // Check for references to other generated columns
    val generatedColumnNames = allColumns
      .filter(col => col.generationExpression.isDefined && !resolver(col.name, fieldName))
      .map(_.name)
    if (referencedColumns.exists(ref => generatedColumnNames.exists(resolver(ref, _)))) {
      throw unsupportedExpressionError(
        "generation expression cannot reference another generated column")
    }

    if (!child.deterministic) {
      throw unsupportedExpressionError("generation expression is not deterministic")
    }

    if (!Cast.canUpCast(child.dataType, targetDataType)) {
      throw unsupportedExpressionError(
        s"generation expression data type ${child.dataType.simpleString} " +
          s"is incompatible with column data type ${targetDataType.simpleString}")
    }

    if (child.exists(e => SchemaUtils.hasNonUTF8BinaryCollation(e.dataType))) {
      throw unsupportedExpressionError(
        "generation expression cannot contain non utf8 binary collated string type")
    }
  }

  // Convert the generation expression to V2 GenerationExpression
  def toV2: GenerationExpression = {
    val v2Expr: V2Expression = new V2ExpressionBuilder(child).build().orNull
    new GenerationExpression(originalSQL, v2Expr)
  }
}
