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

package org.apache.spark.sql.catalyst.parser

import java.util.Locale
import java.util.concurrent.TimeUnit

import scala.collection.mutable.{ArrayBuffer, ListBuffer, Set}
import scala.jdk.CollectionConverters._
import scala.util.{Left, Right}

import org.antlr.v4.runtime.{ParserRuleContext, RuleContext, Token}
import org.antlr.v4.runtime.misc.Interval
import org.antlr.v4.runtime.tree.{ParseTree, RuleNode, TerminalNode}

import org.apache.spark.{SparkArithmeticException, SparkException, SparkIllegalArgumentException, SparkThrowable}
import org.apache.spark.internal.{Logging, MDC}
import org.apache.spark.internal.LogKeys.PARTITION_SPECIFICATION
import org.apache.spark.sql.catalyst.{EvaluateUnresolvedInlineTable, FunctionIdentifier, SQLConfHelper, TableIdentifier}
import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FUNC_ALIAS
import org.apache.spark.sql.catalyst.catalog.{BucketSpec, CatalogStorageFormat, ClusterBySpec}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.{AnyValue, First, Last}
import org.apache.spark.sql.catalyst.parser.SqlBaseParser._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.trees.CurrentOrigin
import org.apache.spark.sql.catalyst.trees.TreePattern.PARAMETER
import org.apache.spark.sql.catalyst.types.DataTypeUtils
import org.apache.spark.sql.catalyst.util.{CharVarcharUtils, DateTimeUtils, IntervalUtils}
import org.apache.spark.sql.catalyst.util.DateTimeUtils.{convertSpecialDate, convertSpecialTimestamp, convertSpecialTimestampNTZ, getZoneId, stringToDate, stringToTimestamp, stringToTimestampWithoutTimeZone}
import org.apache.spark.sql.connector.catalog.{CatalogV2Util, SupportsNamespaces, TableCatalog, TableWritePrivilege}
import org.apache.spark.sql.connector.catalog.TableChange.ColumnPosition
import org.apache.spark.sql.connector.expressions.{ApplyTransform, BucketTransform, DaysTransform, Expression => V2Expression, FieldReference, HoursTransform, IdentityTransform, LiteralValue, MonthsTransform, Transform, YearsTransform}
import org.apache.spark.sql.errors.{DataTypeErrorsBase, QueryCompilationErrors, QueryParsingErrors, SqlScriptingErrors}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.SQLConf.LEGACY_BANG_EQUALS_NOT
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.unsafe.types.{CalendarInterval, UTF8String}
import org.apache.spark.util.ArrayImplicits._
import org.apache.spark.util.random.RandomSampler

/**
 * The AstBuilder converts an ANTLR4 ParseTree into a catalyst Expression, LogicalPlan or
 * TableIdentifier.
 */
class AstBuilder extends DataTypeAstBuilder
  with SQLConfHelper with Logging with DataTypeErrorsBase {
  import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._
  import ParserUtils._

  protected def withIdentClause(
      ctx: IdentifierReferenceContext,
      builder: Seq[String] => LogicalPlan): LogicalPlan = {
    val exprCtx = ctx.expression
    if (exprCtx != null) {
      PlanWithUnresolvedIdentifier(withOrigin(exprCtx) { expression(exprCtx) }, Nil,
        (ident, _) => builder(ident))
    } else {
      builder.apply(visitMultipartIdentifier(ctx.multipartIdentifier))
    }
  }

  protected def withIdentClause(
      ctx: IdentifierReferenceContext,
      otherPlans: Seq[LogicalPlan],
      builder: (Seq[String], Seq[LogicalPlan]) => LogicalPlan): LogicalPlan = {
    val exprCtx = ctx.expression
    if (exprCtx != null) {
      PlanWithUnresolvedIdentifier(withOrigin(exprCtx) { expression(exprCtx) }, otherPlans, builder)
    } else {
      builder.apply(visitMultipartIdentifier(ctx.multipartIdentifier), otherPlans)
    }
  }

  protected def withFuncIdentClause(
      ctx: FunctionNameContext,
      otherPlans: Seq[LogicalPlan],
      builder: (Seq[String], Seq[LogicalPlan]) => LogicalPlan): LogicalPlan = {
    val exprCtx = ctx.expression
    if (exprCtx != null) {
      PlanWithUnresolvedIdentifier(withOrigin(exprCtx) { expression(exprCtx) }, otherPlans, builder)
    } else {
      builder.apply(getFunctionMultiparts(ctx), otherPlans)
    }
  }

  protected def withFuncIdentClause(
      ctx: FunctionNameContext,
      otherExprs: Seq[Expression],
      builder: (Seq[String], Seq[Expression]) => Expression): Expression = {
    val exprCtx = ctx.expression
    if (exprCtx != null) {
      ExpressionWithUnresolvedIdentifier(
        withOrigin(exprCtx) { expression(exprCtx) },
        otherExprs,
        builder)
    } else {
      builder.apply(getFunctionMultiparts(ctx), otherExprs)
    }
  }

  /**
   * Override the default behavior for all visit methods. This will only return a non-null result
   * when the context has only one child. This is done because there is no generic method to
   * combine the results of the context children. In all other cases null is returned.
   */
  override def visitChildren(node: RuleNode): AnyRef = {
    if (node.getChildCount == 1) {
      node.getChild(0).accept(this)
    } else {
      null
    }
  }

  override def visitCompoundOrSingleStatement(
      ctx: CompoundOrSingleStatementContext): CompoundBody = withOrigin(ctx) {
    Option(ctx.singleCompoundStatement()).map { s =>
      visit(s).asInstanceOf[CompoundBody]
    }.getOrElse {
      val logicalPlan = visitSingleStatement(ctx.singleStatement())
      CompoundBody(Seq(SingleStatement(parsedPlan = logicalPlan)),
        Some(java.util.UUID.randomUUID.toString.toLowerCase(Locale.ROOT)))
    }
  }

  override def visitSingleCompoundStatement(ctx: SingleCompoundStatementContext): CompoundBody = {
    visit(ctx.beginEndCompoundBlock()).asInstanceOf[CompoundBody]
  }

  private def visitCompoundBodyImpl(
      ctx: CompoundBodyContext,
      label: Option[String],
      allowVarDeclare: Boolean): CompoundBody = {
    val buff = ListBuffer[CompoundPlanStatement]()
    ctx.compoundStatements.forEach(compoundStatement => {
      buff += visit(compoundStatement).asInstanceOf[CompoundPlanStatement]
    })

    val compoundStatements = buff.toList

    val candidates = if (allowVarDeclare) {
      compoundStatements.dropWhile {
        case SingleStatement(_: CreateVariable) => true
        case _ => false
      }
    } else {
      compoundStatements
    }

    val declareVarStatement = candidates.collectFirst {
      case SingleStatement(c: CreateVariable) => c
    }

    declareVarStatement match {
      case Some(c: CreateVariable) =>
        if (allowVarDeclare) {
          throw SqlScriptingErrors.variableDeclarationOnlyAtBeginning(
            c.origin,
            toSQLId(c.name.asInstanceOf[UnresolvedIdentifier].nameParts),
            c.origin.line.get.toString)
        } else {
          throw SqlScriptingErrors.variableDeclarationNotAllowedInScope(
            c.origin,
            toSQLId(c.name.asInstanceOf[UnresolvedIdentifier].nameParts),
            c.origin.line.get.toString)
        }
      case _ =>
    }

    CompoundBody(buff.toSeq, label)
  }


  private def generateLabelText(
      beginLabelCtx: Option[BeginLabelContext],
      endLabelCtx: Option[EndLabelContext]): String = {

    (beginLabelCtx, endLabelCtx) match {
      case (Some(bl: BeginLabelContext), Some(el: EndLabelContext))
        if bl.multipartIdentifier().getText.nonEmpty &&
          bl.multipartIdentifier().getText.toLowerCase(Locale.ROOT) !=
            el.multipartIdentifier().getText.toLowerCase(Locale.ROOT) =>
        withOrigin(bl) {
          throw SqlScriptingErrors.labelsMismatch(
            CurrentOrigin.get, bl.multipartIdentifier().getText, el.multipartIdentifier().getText)
        }
      case (None, Some(el: EndLabelContext)) =>
        withOrigin(el) {
          throw SqlScriptingErrors.endLabelWithoutBeginLabel(
            CurrentOrigin.get, el.multipartIdentifier().getText)
        }
      case _ =>
    }

    beginLabelCtx.map(_.multipartIdentifier().getText)
      .getOrElse(java.util.UUID.randomUUID.toString).toLowerCase(Locale.ROOT)
  }

  override def visitBeginEndCompoundBlock(ctx: BeginEndCompoundBlockContext): CompoundBody = {
    val labelText = generateLabelText(Option(ctx.beginLabel()), Option(ctx.endLabel()))
    visitCompoundBodyImpl(ctx.compoundBody(), Some(labelText), allowVarDeclare = true)
  }

  override def visitCompoundBody(ctx: CompoundBodyContext): CompoundBody = {
    visitCompoundBodyImpl(ctx, None, allowVarDeclare = false)
  }

  override def visitCompoundStatement(ctx: CompoundStatementContext): CompoundPlanStatement =
    withOrigin(ctx) {
      Option(ctx.statement().asInstanceOf[ParserRuleContext])
        .orElse(Option(ctx.setStatementWithOptionalVarKeyword().asInstanceOf[ParserRuleContext]))
        .map { s =>
          SingleStatement(parsedPlan = visit(s).asInstanceOf[LogicalPlan])
        }.getOrElse {
          visitChildren(ctx).asInstanceOf[CompoundPlanStatement]
        }
    }

  override def visitIfElseStatement(ctx: IfElseStatementContext): IfElseStatement = {
    IfElseStatement(
      conditions = ctx.booleanExpression().asScala.toList.map(boolExpr => withOrigin(boolExpr) {
        SingleStatement(
          Project(
            Seq(Alias(expression(boolExpr), "condition")()),
            OneRowRelation()))
      }),
      conditionalBodies = ctx.conditionalBodies.asScala.toList.map(body => visitCompoundBody(body)),
      elseBody = Option(ctx.elseBody).map(body => visitCompoundBody(body))
    )
  }

  override def visitWhileStatement(ctx: WhileStatementContext): WhileStatement = {
    val labelText = generateLabelText(Option(ctx.beginLabel()), Option(ctx.endLabel()))
    val boolExpr = ctx.booleanExpression()

    val condition = withOrigin(boolExpr) {
      SingleStatement(
        Project(
          Seq(Alias(expression(boolExpr), "condition")()),
          OneRowRelation()))}
    val body = visitCompoundBody(ctx.compoundBody())

    WhileStatement(condition, body, Some(labelText))
  }
  
  override def visitSearchedCaseStatement(ctx: SearchedCaseStatementContext)
  : SearchedCaseStatement = {
    SearchedCaseStatement(
      conditions = ctx.conditions.asScala.toList.map(boolExpr => withOrigin(boolExpr) {
        SingleStatement(
          Project(
            Seq(Alias(expression(boolExpr), "condition")()),
            OneRowRelation()))
      }),
      conditionalBodies = ctx.conditionalBodies.asScala.toList.map(body => visitCompoundBody(body)),
      elseBody = Option(ctx.elseBody).map(body => visitCompoundBody(body)))
  }

  override def visitSimpleCaseStatement(ctx: SimpleCaseStatementContext): SearchedCaseStatement = {
    // uses EqualTo to compare the case variable(the main case expression)
    // to the WHEN clause expressions
    SearchedCaseStatement(
      conditions = ctx.conditionExpressions.asScala.toList.map(expr => withOrigin(expr) {
          SingleStatement(
            Project(
              Seq(Alias(EqualTo(expression(ctx.caseVariable), expression(expr)), "condition")()),
              OneRowRelation()))
      }),
      conditionalBodies = ctx.conditionalBodies.asScala.toList.map(body => visitCompoundBody(body)),
      elseBody = Option(ctx.elseBody).map(body => visitCompoundBody(body)))
  }

  override def visitRepeatStatement(ctx: RepeatStatementContext): RepeatStatement = {
    val labelText = generateLabelText(Option(ctx.beginLabel()), Option(ctx.endLabel()))
    val boolExpr = ctx.booleanExpression()

    val condition = withOrigin(boolExpr) {
      SingleStatement(
        Project(
          Seq(Alias(expression(boolExpr), "condition")()),
          OneRowRelation()))}
    val body = visitCompoundBody(ctx.compoundBody())

    RepeatStatement(condition, body, Some(labelText))
  }

  private def leaveOrIterateContextHasLabel(
      ctx: RuleContext, label: String, isIterate: Boolean): Boolean = {
    ctx match {
      case c: BeginEndCompoundBlockContext
        if Option(c.beginLabel()).isDefined &&
          c.beginLabel().multipartIdentifier().getText.toLowerCase(Locale.ROOT).equals(label) =>
        if (isIterate) {
          throw SqlScriptingErrors.invalidIterateLabelUsageForCompound(CurrentOrigin.get, label)
        }
        true
      case c: WhileStatementContext
        if Option(c.beginLabel()).isDefined &&
          c.beginLabel().multipartIdentifier().getText.toLowerCase(Locale.ROOT).equals(label)
        => true
      case c: RepeatStatementContext
        if Option(c.beginLabel()).isDefined &&
          c.beginLabel().multipartIdentifier().getText.toLowerCase(Locale.ROOT).equals(label)
        => true
      case _ => false
    }
  }

  override def visitLeaveStatement(ctx: LeaveStatementContext): LeaveStatement =
    withOrigin(ctx) {
      val labelText = ctx.multipartIdentifier().getText.toLowerCase(Locale.ROOT)
      var parentCtx = ctx.parent

      while (Option(parentCtx).isDefined) {
        if (leaveOrIterateContextHasLabel(parentCtx, labelText, isIterate = false)) {
          return LeaveStatement(labelText)
        }
        parentCtx = parentCtx.parent
      }

      throw SqlScriptingErrors.labelDoesNotExist(
        CurrentOrigin.get, labelText, "LEAVE")
    }

  override def visitIterateStatement(ctx: IterateStatementContext): IterateStatement =
    withOrigin(ctx) {
      val labelText = ctx.multipartIdentifier().getText.toLowerCase(Locale.ROOT)
      var parentCtx = ctx.parent

      while (Option(parentCtx).isDefined) {
        if (leaveOrIterateContextHasLabel(parentCtx, labelText, isIterate = true)) {
          return IterateStatement(labelText)
        }
        parentCtx = parentCtx.parent
      }

      throw SqlScriptingErrors.labelDoesNotExist(
        CurrentOrigin.get, labelText, "ITERATE")
    }

  override def visitSingleStatement(ctx: SingleStatementContext): LogicalPlan = withOrigin(ctx) {
    Option(ctx.statement().asInstanceOf[ParserRuleContext])
      .orElse(Option(ctx.setResetStatement().asInstanceOf[ParserRuleContext]))
      .map { s => visit(s).asInstanceOf[LogicalPlan] }
      .get
  }

  override def visitSingleExpression(ctx: SingleExpressionContext): Expression = withOrigin(ctx) {
    visitNamedExpression(ctx.namedExpression)
  }

  override def visitSingleTableIdentifier(
      ctx: SingleTableIdentifierContext): TableIdentifier = withOrigin(ctx) {
    visitTableIdentifier(ctx.tableIdentifier)
  }

  override def visitSingleFunctionIdentifier(
      ctx: SingleFunctionIdentifierContext): FunctionIdentifier = withOrigin(ctx) {
    visitFunctionIdentifier(ctx.functionIdentifier)
  }

  override def visitSingleMultipartIdentifier(
      ctx: SingleMultipartIdentifierContext): Seq[String] = withOrigin(ctx) {
    visitMultipartIdentifier(ctx.multipartIdentifier)
  }

  override def visitSingleDataType(ctx: SingleDataTypeContext): DataType = withOrigin(ctx) {
    typedVisit[DataType](ctx.dataType)
  }

  override def visitSingleTableSchema(ctx: SingleTableSchemaContext): StructType = {
    val schema = StructType(visitColTypeList(ctx.colTypeList))
    withOrigin(ctx)(schema)
  }

  /* ********************************************************************************************
   * Plan parsing
   * ******************************************************************************************** */
  protected def plan(tree: ParserRuleContext): LogicalPlan = typedVisit(tree)

  /**
   * Create a top-level plan with Common Table Expressions.
   */
  override def visitQuery(ctx: QueryContext): LogicalPlan = withOrigin(ctx) {
    val query = plan(ctx.queryTerm).optionalMap(ctx.queryOrganization)(withQueryResultClauses)

    // Apply CTEs
    query.optionalMap(ctx.ctes)(withCTE)
  }

  override def visitDmlStatement(ctx: DmlStatementContext): AnyRef = withOrigin(ctx) {
    val dmlStmt = plan(ctx.dmlStatementNoWith)
    // Apply CTEs
    dmlStmt.optionalMap(ctx.ctes)(withCTE)
  }

  private def withCTE(ctx: CtesContext, plan: LogicalPlan): LogicalPlan = {
    val ctes = ctx.namedQuery.asScala.map { nCtx =>
      val namedQuery = visitNamedQuery(nCtx)
      (namedQuery.alias, namedQuery)
    }
    // Check for duplicate names.
    val duplicates = ctes.groupBy(_._1).filter(_._2.size > 1).keys
    if (duplicates.nonEmpty) {
      throw QueryParsingErrors.duplicateCteDefinitionNamesError(
        duplicates.mkString("'", "', '", "'"), ctx)
    }
    UnresolvedWith(plan, ctes.toSeq)
  }

  /**
   * Create a logical query plan for a hive-style FROM statement body.
   */
  private def withFromStatementBody(
      ctx: FromStatementBodyContext, plan: LogicalPlan): LogicalPlan = withOrigin(ctx) {
    // two cases for transforms and selects
    if (ctx.transformClause != null) {
      withTransformQuerySpecification(
        ctx,
        ctx.transformClause,
        ctx.lateralView,
        ctx.whereClause,
        ctx.aggregationClause,
        ctx.havingClause,
        ctx.windowClause,
        plan
      )
    } else {
      withSelectQuerySpecification(
        ctx,
        ctx.selectClause,
        ctx.lateralView,
        ctx.whereClause,
        ctx.aggregationClause,
        ctx.havingClause,
        ctx.windowClause,
        plan
      )
    }
  }

  override def visitFromStatement(ctx: FromStatementContext): LogicalPlan = withOrigin(ctx) {
    val from = visitFromClause(ctx.fromClause)
    val selects = ctx.fromStatementBody.asScala.map { body =>
      withFromStatementBody(body, from).
        // Add organization statements.
        optionalMap(body.queryOrganization)(withQueryResultClauses)
    }
    // If there are multiple SELECT just UNION them together into one query.
    if (selects.length == 1) {
      selects.head
    } else {
      Union(selects.toSeq)
    }
  }

  /**
   * Create a named logical plan.
   *
   * This is only used for Common Table Expressions.
   */
  override def visitNamedQuery(ctx: NamedQueryContext): SubqueryAlias = withOrigin(ctx) {
    val subQuery: LogicalPlan = plan(ctx.query).optionalMap(ctx.columnAliases)(
      (columnAliases, plan) =>
        UnresolvedSubqueryColumnAliases(visitIdentifierList(columnAliases), plan)
    )
    SubqueryAlias(ctx.name.getText, subQuery)
  }

  /**
   * Create a logical plan which allows for multiple inserts using one 'from' statement. These
   * queries have the following SQL form:
   * {{{
   *   [WITH cte...]?
   *   FROM src
   *   [INSERT INTO tbl1 SELECT *]+
   * }}}
   * For example:
   * {{{
   *   FROM db.tbl1 A
   *   INSERT INTO dbo.tbl1 SELECT * WHERE A.value = 10 LIMIT 5
   *   INSERT INTO dbo.tbl2 SELECT * WHERE A.value = 12
   * }}}
   * This (Hive) feature cannot be combined with set-operators.
   */
  override def visitMultiInsertQuery(ctx: MultiInsertQueryContext): LogicalPlan = withOrigin(ctx) {
    val from = visitFromClause(ctx.fromClause)

    // Build the insert clauses.
    val inserts = ctx.multiInsertQueryBody.asScala.map { body =>
      withInsertInto(body.insertInto,
        withFromStatementBody(body.fromStatementBody, from).
          optionalMap(body.fromStatementBody.queryOrganization)(withQueryResultClauses))
    }

    // If there are multiple INSERTS just UNION them together into one query.
    if (inserts.length == 1) {
      inserts.head
    } else {
      Union(inserts.toSeq)
    }
  }

  /**
   * Create a logical plan for a regular (single-insert) query.
   */
  override def visitSingleInsertQuery(
      ctx: SingleInsertQueryContext): LogicalPlan = withOrigin(ctx) {
    withInsertInto(ctx.insertInto(), visitQuery(ctx.query))
  }

  /**
   * Parameters used for writing query to a table:
   *   (table ident, options, tableColumnList, partitionKeys, ifPartitionNotExists, byName).
   */
  type InsertTableParams =
    (IdentifierReferenceContext, Option[OptionsClauseContext], Seq[String],
      Map[String, Option[String]], Boolean, Boolean)

  /**
   * Parameters used for writing query to a directory: (isLocal, CatalogStorageFormat, provider).
   */
  type InsertDirParams = (Boolean, CatalogStorageFormat, Option[String])

  /**
   * Add an
   * {{{
   *   INSERT OVERWRITE TABLE tableIdentifier [partitionSpec [IF NOT EXISTS]]? [identifierList]
   *   INSERT INTO [TABLE] tableIdentifier [partitionSpec] ([BY NAME] | [identifierList])
   *   INSERT INTO [TABLE] tableIdentifier REPLACE whereClause
   *   INSERT OVERWRITE [LOCAL] DIRECTORY STRING [rowFormat] [createFileFormat]
   *   INSERT OVERWRITE [LOCAL] DIRECTORY [STRING] tableProvider [OPTIONS tablePropertyList]
   * }}}
   * operation to logical plan
   */
  private def withInsertInto(
      ctx: InsertIntoContext,
      query: LogicalPlan): LogicalPlan = withOrigin(ctx) {
    ctx match {
      // We cannot push withIdentClause() into the write command because:
      //   1. `PlanWithUnresolvedIdentifier` is not a NamedRelation
      //   2. Write commands do not hold the table logical plan as a child, and we need to add
      //      additional resolution code to resolve identifiers inside the write commands.
      case table: InsertIntoTableContext =>
        val (relationCtx, options, cols, partition, ifPartitionNotExists, byName)
        = visitInsertIntoTable(table)
        withIdentClause(relationCtx, Seq(query), (ident, otherPlans) => {
          InsertIntoStatement(
            createUnresolvedRelation(relationCtx, ident, options, Seq(TableWritePrivilege.INSERT)),
            partition,
            cols,
            otherPlans.head,
            overwrite = false,
            ifPartitionNotExists,
            byName)
        })
      case table: InsertOverwriteTableContext =>
        val (relationCtx, options, cols, partition, ifPartitionNotExists, byName)
        = visitInsertOverwriteTable(table)
        withIdentClause(relationCtx, Seq(query), (ident, otherPlans) => {
          InsertIntoStatement(
            createUnresolvedRelation(relationCtx, ident, options,
              Seq(TableWritePrivilege.INSERT, TableWritePrivilege.DELETE)),
            partition,
            cols,
            otherPlans.head,
            overwrite = true,
            ifPartitionNotExists,
            byName)
        })
      case ctx: InsertIntoReplaceWhereContext =>
        val options = Option(ctx.optionsClause())
        withIdentClause(ctx.identifierReference, Seq(query), (ident, otherPlans) => {
          OverwriteByExpression.byPosition(
            createUnresolvedRelation(ctx.identifierReference, ident, options,
              Seq(TableWritePrivilege.INSERT, TableWritePrivilege.DELETE)),
            otherPlans.head,
            expression(ctx.whereClause().booleanExpression()))
        })
      case dir: InsertOverwriteDirContext =>
        val (isLocal, storage, provider) = visitInsertOverwriteDir(dir)
        InsertIntoDir(isLocal, storage, provider, query, overwrite = true)
      case hiveDir: InsertOverwriteHiveDirContext =>
        val (isLocal, storage, provider) = visitInsertOverwriteHiveDir(hiveDir)
        InsertIntoDir(isLocal, storage, provider, query, overwrite = true)
      case _ =>
        throw QueryParsingErrors.invalidInsertIntoError(ctx)
    }
  }

  /**
   * Add an INSERT INTO TABLE operation to the logical plan.
   */
  override def visitInsertIntoTable(
      ctx: InsertIntoTableContext): InsertTableParams = withOrigin(ctx) {
    val cols = Option(ctx.identifierList()).map(visitIdentifierList).getOrElse(Nil)
    val partitionKeys = Option(ctx.partitionSpec).map(visitPartitionSpec).getOrElse(Map.empty)

    blockBang(ctx.errorCapturingNot())

    if (ctx.EXISTS != null) {
      invalidStatement("INSERT INTO ... IF NOT EXISTS", ctx)
    }

    (ctx.identifierReference, Option(ctx.optionsClause()), cols, partitionKeys, false,
      ctx.NAME() != null)
  }

  /**
   * Add an INSERT OVERWRITE TABLE operation to the logical plan.
   */
  override def visitInsertOverwriteTable(
      ctx: InsertOverwriteTableContext): InsertTableParams = withOrigin(ctx) {
    assert(ctx.OVERWRITE() != null)
    val cols = Option(ctx.identifierList()).map(visitIdentifierList).getOrElse(Nil)
    val partitionKeys = Option(ctx.partitionSpec).map(visitPartitionSpec).getOrElse(Map.empty)

    blockBang(ctx.errorCapturingNot())

    val dynamicPartitionKeys: Map[String, Option[String]] = partitionKeys.filter(_._2.isEmpty)
    if (ctx.EXISTS != null && dynamicPartitionKeys.nonEmpty) {
      operationNotAllowed("IF NOT EXISTS with dynamic partitions: " +
        dynamicPartitionKeys.keys.mkString(", "), ctx)
    }

    (ctx.identifierReference, Option(ctx.optionsClause()), cols, partitionKeys,
      ctx.EXISTS() != null, ctx.NAME() != null)
  }

  /**
   * Write to a directory, returning a [[InsertIntoDir]] logical plan.
   */
  override def visitInsertOverwriteDir(
      ctx: InsertOverwriteDirContext): InsertDirParams = withOrigin(ctx) {
    throw QueryParsingErrors.insertOverwriteDirectoryUnsupportedError()
  }

  /**
   * Write to a directory, returning a [[InsertIntoDir]] logical plan.
   */
  override def visitInsertOverwriteHiveDir(
      ctx: InsertOverwriteHiveDirContext): InsertDirParams = withOrigin(ctx) {
    throw QueryParsingErrors.insertOverwriteDirectoryUnsupportedError()
  }

  private def getTableAliasWithoutColumnAlias(
      ctx: TableAliasContext, op: String): Option[String] = {
    if (ctx == null) {
      None
    } else {
      val ident = ctx.strictIdentifier()
      if (ctx.identifierList() != null) {
        throw QueryParsingErrors.columnAliasInOperationNotAllowedError(op, ctx)
      }
      if (ident != null) Some(ident.getText) else None
    }
  }

  override def visitDeleteFromTable(
      ctx: DeleteFromTableContext): LogicalPlan = withOrigin(ctx) {
    val table = createUnresolvedRelation(
      ctx.identifierReference, writePrivileges = Seq(TableWritePrivilege.DELETE))
    val tableAlias = getTableAliasWithoutColumnAlias(ctx.tableAlias(), "DELETE")
    val aliasedTable = tableAlias.map(SubqueryAlias(_, table)).getOrElse(table)
    val predicate = if (ctx.whereClause() != null) {
      expression(ctx.whereClause().booleanExpression())
    } else {
      Literal.TrueLiteral
    }
    DeleteFromTable(aliasedTable, predicate)
  }

  override def visitUpdateTable(ctx: UpdateTableContext): LogicalPlan = withOrigin(ctx) {
    val table = createUnresolvedRelation(
      ctx.identifierReference, writePrivileges = Seq(TableWritePrivilege.UPDATE))
    val tableAlias = getTableAliasWithoutColumnAlias(ctx.tableAlias(), "UPDATE")
    val aliasedTable = tableAlias.map(SubqueryAlias(_, table)).getOrElse(table)
    val assignments = withAssignments(ctx.setClause().assignmentList())
    val predicate = if (ctx.whereClause() != null) {
      Some(expression(ctx.whereClause().booleanExpression()))
    } else {
      None
    }

    UpdateTable(aliasedTable, assignments, predicate)
  }

  protected def withAssignments(assignCtx: SqlBaseParser.AssignmentListContext): Seq[Assignment] =
    withOrigin(assignCtx) {
      assignCtx.assignment().asScala.map { assign =>
        Assignment(UnresolvedAttribute(visitMultipartIdentifier(assign.key)),
          expression(assign.value))
      }.toSeq
    }

  override def visitMergeIntoTable(ctx: MergeIntoTableContext): LogicalPlan = withOrigin(ctx) {
    val withSchemaEvolution = ctx.EVOLUTION() != null

    val sourceTableOrQuery = if (ctx.source != null) {
      createUnresolvedRelation(ctx.source)
    } else if (ctx.sourceQuery != null) {
      visitQuery(ctx.sourceQuery)
    } else {
      throw QueryParsingErrors.emptySourceForMergeError(ctx)
    }
    val sourceTableAlias = getTableAliasWithoutColumnAlias(ctx.sourceAlias, "MERGE")
    val aliasedSource =
      sourceTableAlias.map(SubqueryAlias(_, sourceTableOrQuery)).getOrElse(sourceTableOrQuery)

    val mergeCondition = expression(ctx.mergeCondition)

    val matchedActions = ctx.matchedClause().asScala.map {
      clause => {
        if (clause.matchedAction().DELETE() != null) {
          DeleteAction(Option(clause.matchedCond).map(expression))
        } else if (clause.matchedAction().UPDATE() != null) {
          val condition = Option(clause.matchedCond).map(expression)
          if (clause.matchedAction().ASTERISK() != null) {
            UpdateStarAction(condition)
          } else {
            UpdateAction(condition, withAssignments(clause.matchedAction().assignmentList()))
          }
        } else {
          throw SparkException.internalError(
            s"Unrecognized matched action: ${clause.matchedAction().getText}")
        }
      }
    }.toSeq
    val notMatchedActions = ctx.notMatchedClause().asScala.map {
      clause => {
        if (clause.notMatchedAction().INSERT() != null) {
          val condition = Option(clause.notMatchedCond).map(expression)
          if (clause.notMatchedAction().ASTERISK() != null) {
            InsertStarAction(condition)
          } else {
            val columns = clause.notMatchedAction().columns.multipartIdentifier()
                .asScala.map(attr => UnresolvedAttribute(visitMultipartIdentifier(attr)))
            val values = clause.notMatchedAction().expression().asScala.map(expression)
            if (columns.size != values.size) {
              throw QueryParsingErrors.insertedValueNumberNotMatchFieldNumberError(clause)
            }
            InsertAction(condition, columns.zip(values).map(kv => Assignment(kv._1, kv._2)).toSeq)
          }
        } else {
          throw SparkException.internalError(
            s"Unrecognized matched action: ${clause.notMatchedAction().getText}")
        }
      }
    }.toSeq
    val notMatchedBySourceActions = ctx.notMatchedBySourceClause().asScala.map {
      clause => {
        val notMatchedBySourceAction = clause.notMatchedBySourceAction()
        if (notMatchedBySourceAction.DELETE() != null) {
          DeleteAction(Option(clause.notMatchedBySourceCond).map(expression))
        } else if (notMatchedBySourceAction.UPDATE() != null) {
          val condition = Option(clause.notMatchedBySourceCond).map(expression)
          UpdateAction(condition,
            withAssignments(clause.notMatchedBySourceAction().assignmentList()))
        } else {
          throw SparkException.internalError(
            s"Unrecognized matched action: ${clause.notMatchedBySourceAction().getText}")
        }
      }
    }.toSeq
    if (matchedActions.isEmpty && notMatchedActions.isEmpty && notMatchedBySourceActions.isEmpty) {
      throw QueryParsingErrors.mergeStatementWithoutWhenClauseError(ctx)
    }
    // children being empty means that the condition is not set
    val matchedActionSize = matchedActions.length
    if (matchedActionSize >= 2 && !matchedActions.init.forall(_.condition.nonEmpty)) {
      throw QueryParsingErrors.nonLastMatchedClauseOmitConditionError(ctx)
    }
    val notMatchedActionSize = notMatchedActions.length
    if (notMatchedActionSize >= 2 && !notMatchedActions.init.forall(_.condition.nonEmpty)) {
      throw QueryParsingErrors.nonLastNotMatchedClauseOmitConditionError(ctx)
    }
    val notMatchedBySourceActionSize = notMatchedBySourceActions.length
    if (notMatchedBySourceActionSize >= 2 &&
     !notMatchedBySourceActions.init.forall(_.condition.nonEmpty)) {
      throw QueryParsingErrors.nonLastNotMatchedBySourceClauseOmitConditionError(ctx)
    }

    val targetTable = createUnresolvedRelation(
      ctx.target,
      writePrivileges = MergeIntoTable.getWritePrivileges(
        matchedActions, notMatchedActions, notMatchedBySourceActions))
    val targetTableAlias = getTableAliasWithoutColumnAlias(ctx.targetAlias, "MERGE")
    val aliasedTarget = targetTableAlias.map(SubqueryAlias(_, targetTable)).getOrElse(targetTable)
    MergeIntoTable(
      aliasedTarget,
      aliasedSource,
      mergeCondition,
      matchedActions,
      notMatchedActions,
      notMatchedBySourceActions,
      withSchemaEvolution)
  }

  /**
   * Returns the parameters for [[ExecuteImmediateQuery]] logical plan.
   * Expected format:
   * {{{
   *   EXECUTE IMMEDIATE {query_string|string_literal}
   *   [INTO target1, target2] [USING param1, param2, ...]
   * }}}
   */
  override def visitExecuteImmediate(ctx: ExecuteImmediateContext): LogicalPlan = withOrigin(ctx) {
    // Because of how parsing rules are written, we know that either
    // queryParam or targetVariable is non null - hence use Either to represent this.
    val queryString = Option(ctx.queryParam.stringLit()).map(sl => Left(string(visitStringLit(sl))))
    val queryVariable = Option(ctx.queryParam.multipartIdentifier)
      .map(mpi => Right(UnresolvedAttribute(visitMultipartIdentifier(mpi))))

    val targetVars = Option(ctx.targetVariable).toSeq
      .flatMap(v => visitMultipartIdentifierList(v))
    val exprs = Option(ctx.executeImmediateUsing).map {
      visitExecuteImmediateUsing(_)
    }.getOrElse{ Seq.empty }


    ExecuteImmediateQuery(exprs, queryString.getOrElse(queryVariable.get), targetVars)
  }

  override def visitExecuteImmediateUsing(
      ctx: ExecuteImmediateUsingContext): Seq[Expression] = withOrigin(ctx) {
    val expressions = Option(ctx).toSeq
      .flatMap(ctx => visitNamedExpressionSeq(ctx.params))
    val resultExpr = expressions.map(e => e._1)

    validateExecImmediateArguments(resultExpr, ctx)
    resultExpr
  }

  /**
   * Performs validation on the arguments to EXECUTE IMMEDIATE.
   */
  private def validateExecImmediateArguments(
    expressions: Seq[Expression],
    ctx : ExecuteImmediateUsingContext) : Unit = {
    val duplicateAliases = expressions
      .filter(_.isInstanceOf[Alias])
      .groupBy {
        case Alias(arg, name) => name
      }.filter(group => group._2.size > 1)

    if (duplicateAliases.nonEmpty) {
      throw QueryParsingErrors.duplicateArgumentNamesError(duplicateAliases.keys.toSeq, ctx)
    }
  }

  override def visitMultipartIdentifierList(
      ctx: MultipartIdentifierListContext): Seq[UnresolvedAttribute] = withOrigin(ctx) {
    ctx.multipartIdentifier.asScala.map(typedVisit[Seq[String]]).map(new UnresolvedAttribute(_))
      .toSeq
  }

/**
   * Create a partition specification map.
   */
  override def visitPartitionSpec(
      ctx: PartitionSpecContext): Map[String, Option[String]] = withOrigin(ctx) {
    val legacyNullAsString =
      conf.getConf(SQLConf.LEGACY_PARSE_NULL_PARTITION_SPEC_AS_STRING_LITERAL)
    val keepPartitionSpecAsString =
      conf.getConf(SQLConf.LEGACY_KEEP_PARTITION_SPEC_AS_STRING_LITERAL)

    val parts = ctx.partitionVal.asScala.map { pVal =>
      // Check if the query attempted to refer to a DEFAULT column value within the PARTITION clause
      // and return a specific error to help guide the user, since this is not allowed.
      if (pVal.DEFAULT != null) {
        throw QueryParsingErrors.defaultColumnReferencesNotAllowedInPartitionSpec(ctx)
      }
      val name = pVal.identifier.getText
      val value = Option(pVal.constant).map(v => {
        visitStringConstant(v, legacyNullAsString, keepPartitionSpecAsString)
      })
      name -> value
    }
    // Before calling `toMap`, we check duplicated keys to avoid silently ignore partition values
    // in partition spec like PARTITION(a='1', b='2', a='3'). The real semantical check for
    // partition columns will be done in analyzer.
    if (conf.caseSensitiveAnalysis) {
      checkDuplicateKeys(parts.toSeq, ctx)
    } else {
      checkDuplicateKeys(parts.map(kv => kv._1.toLowerCase(Locale.ROOT) -> kv._2).toSeq, ctx)
    }
    parts.toMap
  }

  /**
   * Create a partition specification map without optional values.
   */
  protected def visitNonOptionalPartitionSpec(
      ctx: PartitionSpecContext): Map[String, String] = withOrigin(ctx) {
    visitPartitionSpec(ctx).map {
      case (key, None) => throw QueryParsingErrors.emptyPartitionKeyError(key, ctx)
      case (key, Some(value)) => key -> value
    }
  }

  /**
   * Convert a constant of any type into a string. This is typically used in DDL commands, and its
   * main purpose is to prevent slight differences due to back to back conversions i.e.:
   * String -> Literal -> String.
   */
  protected def visitStringConstant(
      ctx: ConstantContext,
      legacyNullAsString: Boolean = false,
      keepPartitionSpecAsString: Boolean = false): String = withOrigin(ctx) {
    expression(ctx) match {
      case Literal(null, _) if !legacyNullAsString => null
      case l @ Literal(null, _) => l.toString
      case l: Literal =>
        if (keepPartitionSpecAsString && !ctx.isInstanceOf[StringLiteralContext]) {
          ctx.getText
        } else {
          // TODO For v2 commands, we will cast the string back to its actual value,
          //  which is a waste and can be improved in the future.
          Cast(l, conf.defaultStringType, Some(conf.sessionLocalTimeZone)).eval().toString
        }
      case other =>
        throw new SparkIllegalArgumentException(
          errorClass = "_LEGACY_ERROR_TEMP_3222",
          messageParameters = Map("expr" -> other.sql)
        )
    }
  }

  /**
   * Add ORDER BY/SORT BY/CLUSTER BY/DISTRIBUTE BY/LIMIT/WINDOWS clauses to the logical plan. These
   * clauses determine the shape (ordering/partitioning/rows) of the query result.
   */
  private def withQueryResultClauses(
      ctx: QueryOrganizationContext,
      query: LogicalPlan): LogicalPlan = withOrigin(ctx) {
    import ctx._

    // Handle ORDER BY, SORT BY, DISTRIBUTE BY, and CLUSTER BY clause.
    val withOrder = if (
      !order.isEmpty && sort.isEmpty && distributeBy.isEmpty && clusterBy.isEmpty) {
      // ORDER BY ...
      Sort(order.asScala.map(visitSortItem).toSeq, global = true, query)
    } else if (order.isEmpty && !sort.isEmpty && distributeBy.isEmpty && clusterBy.isEmpty) {
      // SORT BY ...
      Sort(sort.asScala.map(visitSortItem).toSeq, global = false, query)
    } else if (order.isEmpty && sort.isEmpty && !distributeBy.isEmpty && clusterBy.isEmpty) {
      // DISTRIBUTE BY ...
      withRepartitionByExpression(ctx, expressionList(distributeBy), query)
    } else if (order.isEmpty && !sort.isEmpty && !distributeBy.isEmpty && clusterBy.isEmpty) {
      // SORT BY ... DISTRIBUTE BY ...
      Sort(
        sort.asScala.map(visitSortItem).toSeq,
        global = false,
        withRepartitionByExpression(ctx, expressionList(distributeBy), query))
    } else if (order.isEmpty && sort.isEmpty && distributeBy.isEmpty && !clusterBy.isEmpty) {
      // CLUSTER BY ...
      val expressions = expressionList(clusterBy)
      Sort(
        expressions.map(SortOrder(_, Ascending)),
        global = false,
        withRepartitionByExpression(ctx, expressions, query))
    } else if (order.isEmpty && sort.isEmpty && distributeBy.isEmpty && clusterBy.isEmpty) {
      // [EMPTY]
      query
    } else {
      throw QueryParsingErrors.combinationQueryResultClausesUnsupportedError(ctx)
    }

    // WINDOWS
    val withWindow = withOrder.optionalMap(windowClause)(withWindowClause)

    // OFFSET
    // - OFFSET 0 is the same as omitting the OFFSET clause
    val withOffset = withWindow.optional(offset) {
      Offset(typedVisit(offset), withWindow)
    }

    // LIMIT
    // - LIMIT ALL is the same as omitting the LIMIT clause
    withOffset.optional(limit) {
      Limit(typedVisit(limit), withOffset)
    }
  }

  /**
   * Create a clause for DISTRIBUTE BY.
   */
  protected def withRepartitionByExpression(
      ctx: QueryOrganizationContext,
      expressions: Seq[Expression],
      query: LogicalPlan): LogicalPlan = {
    throw QueryParsingErrors.distributeByUnsupportedError(ctx)
  }

  override def visitTransformQuerySpecification(
      ctx: TransformQuerySpecificationContext): LogicalPlan = withOrigin(ctx) {
    val from = OneRowRelation().optional(ctx.fromClause) {
      visitFromClause(ctx.fromClause)
    }
    withTransformQuerySpecification(
      ctx,
      ctx.transformClause,
      ctx.lateralView,
      ctx.whereClause,
      ctx.aggregationClause,
      ctx.havingClause,
      ctx.windowClause,
      from
    )
  }

  override def visitRegularQuerySpecification(
      ctx: RegularQuerySpecificationContext): LogicalPlan = withOrigin(ctx) {
    val from = OneRowRelation().optional(ctx.fromClause) {
      visitFromClause(ctx.fromClause)
    }
    withSelectQuerySpecification(
      ctx,
      ctx.selectClause,
      ctx.lateralView,
      ctx.whereClause,
      ctx.aggregationClause,
      ctx.havingClause,
      ctx.windowClause,
      from
    )
  }

  private def getAliasFunc(ctx: ParseTree): Option[Expression => String] = {
    if (conf.getConf(SQLConf.STABLE_DERIVED_COLUMN_ALIAS_ENABLED)) {
      Some(_ => toExprAlias(ctx))
    } else {
      None
    }
  }

  override def visitNamedExpressionSeq(
      ctx: NamedExpressionSeqContext): Seq[(Expression, Option[Expression => String])] = {
    Option(ctx).toSeq
      .flatMap(_.namedExpression.asScala)
      .map(ctx => (typedVisit[Expression](ctx), getAliasFunc(ctx)))
  }

  override def visitExpressionSeq(
      ctx: ExpressionSeqContext): Seq[(Expression, Option[Expression => String])] = {
    Option(ctx).toSeq
      .flatMap(_.expression.asScala)
      .map(ctx => (typedVisit[Expression](ctx), getAliasFunc(ctx)))
  }

  /**
   * Create a logical plan using a having clause.
   */
  private def withHavingClause(
      ctx: HavingClauseContext, plan: LogicalPlan): LogicalPlan = {
    // Note that we add a cast to non-predicate expressions. If the expression itself is
    // already boolean, the optimizer will get rid of the unnecessary cast.
    val predicate = expression(ctx.booleanExpression) match {
      case p: Predicate => p
      case e => Cast(e, BooleanType)
    }
    UnresolvedHaving(predicate, plan)
  }

  /**
   * Create a logical plan using a where clause.
   */
  private def withWhereClause(ctx: WhereClauseContext, plan: LogicalPlan): LogicalPlan = {
    Filter(expression(ctx.booleanExpression), plan)
  }

  /**
   * Add a hive-style transform (SELECT TRANSFORM/MAP/REDUCE) query specification to a logical plan.
   */
  private def withTransformQuerySpecification(
      ctx: ParserRuleContext,
      transformClause: TransformClauseContext,
      lateralView: java.util.List[LateralViewContext],
      whereClause: WhereClauseContext,
      aggregationClause: AggregationClauseContext,
      havingClause: HavingClauseContext,
      windowClause: WindowClauseContext,
      relation: LogicalPlan): LogicalPlan = withOrigin(ctx) {
    if (transformClause.setQuantifier != null) {
      throw QueryParsingErrors.transformNotSupportQuantifierError(transformClause.setQuantifier)
    }
    // Create the attributes.
    val (attributes, schemaLess) = if (transformClause.colTypeList != null) {
      // Typed return columns.
      val schema = createSchema(transformClause.colTypeList)
      val replacedSchema = CharVarcharUtils.replaceCharVarcharWithStringInSchema(schema)
      (DataTypeUtils.toAttributes(replacedSchema), false)
    } else if (transformClause.identifierSeq != null) {
      // Untyped return columns.
      val attrs = visitIdentifierSeq(transformClause.identifierSeq).map { name =>
        AttributeReference(name, StringType, nullable = true)()
      }
      (attrs, false)
    } else {
      (Seq(AttributeReference("key", StringType)(),
        AttributeReference("value", StringType)()), true)
    }

    val plan = visitCommonSelectQueryClausePlan(
      relation,
      visitExpressionSeq(transformClause.expressionSeq),
      lateralView,
      whereClause,
      aggregationClause,
      havingClause,
      windowClause,
      isDistinct = false)

    ScriptTransformation(
      string(visitStringLit(transformClause.script)),
      attributes,
      plan,
      withScriptIOSchema(
        ctx,
        transformClause.inRowFormat,
        visitStringLit(transformClause.recordWriter),
        transformClause.outRowFormat,
        visitStringLit(transformClause.recordReader),
        schemaLess
      )
    )
  }

  /**
   * Add a regular (SELECT) query specification to a logical plan. The query specification
   * is the core of the logical plan, this is where sourcing (FROM clause), projection (SELECT),
   * aggregation (GROUP BY ... HAVING ...) and filtering (WHERE) takes place.
   *
   * Note that query hints are ignored (both by the parser and the builder).
   */
  private def withSelectQuerySpecification(
      ctx: ParserRuleContext,
      selectClause: SelectClauseContext,
      lateralView: java.util.List[LateralViewContext],
      whereClause: WhereClauseContext,
      aggregationClause: AggregationClauseContext,
      havingClause: HavingClauseContext,
      windowClause: WindowClauseContext,
      relation: LogicalPlan): LogicalPlan = withOrigin(ctx) {
    val isDistinct = selectClause.setQuantifier() != null &&
      selectClause.setQuantifier().DISTINCT() != null

    val plan = visitCommonSelectQueryClausePlan(
      relation,
      visitNamedExpressionSeq(selectClause.namedExpressionSeq),
      lateralView,
      whereClause,
      aggregationClause,
      havingClause,
      windowClause,
      isDistinct)

    // Hint
    selectClause.hints.asScala.foldRight(plan)(withHints)
  }

  def visitCommonSelectQueryClausePlan(
      relation: LogicalPlan,
      expressions: Seq[(Expression, Option[Expression => String])],
      lateralView: java.util.List[LateralViewContext],
      whereClause: WhereClauseContext,
      aggregationClause: AggregationClauseContext,
      havingClause: HavingClauseContext,
      windowClause: WindowClauseContext,
      isDistinct: Boolean): LogicalPlan = {
    // Add lateral views.
    val withLateralView = lateralView.asScala.foldLeft(relation)(withGenerate)

    // Add where.
    val withFilter = withLateralView.optionalMap(whereClause)(withWhereClause)

    // Add aggregation or a project.
    val namedExpressions = expressions.map {
      case (e: NamedExpression, _) => e
      case (e: Expression, aliasFunc) => UnresolvedAlias(e, aliasFunc)
    }

    def createProject() = if (namedExpressions.nonEmpty) {
      Project(namedExpressions, withFilter)
    } else {
      withFilter
    }

    val withProject = if (aggregationClause == null && havingClause != null) {
      if (conf.getConf(SQLConf.LEGACY_HAVING_WITHOUT_GROUP_BY_AS_WHERE)) {
        // If the legacy conf is set, treat HAVING without GROUP BY as WHERE.
        val predicate = expression(havingClause.booleanExpression) match {
          case p: Predicate => p
          case e => Cast(e, BooleanType)
        }
        Filter(predicate, createProject())
      } else {
        // According to SQL standard, HAVING without GROUP BY means global aggregate.
        withHavingClause(havingClause, Aggregate(Nil, namedExpressions, withFilter))
      }
    } else if (aggregationClause != null) {
      val aggregate = withAggregationClause(aggregationClause, namedExpressions, withFilter)
      aggregate.optionalMap(havingClause)(withHavingClause)
    } else {
      // When hitting this branch, `having` must be null.
      createProject()
    }

    // Distinct
    val withDistinct = if (isDistinct) {
      Distinct(withProject)
    } else {
      withProject
    }

    // Window
    val withWindow = withDistinct.optionalMap(windowClause)(withWindowClause)

    withWindow
  }

  // Script Transform's input/output format.
  type ScriptIOFormat =
    (Seq[(String, String)], Option[String], Seq[(String, String)], Option[String])

  protected def getRowFormatDelimited(ctx: RowFormatDelimitedContext): ScriptIOFormat = {

    def entry(key: String, value: StringLitContext): Seq[(String, String)] = {
      Option(value).toSeq.map(x => key -> string(visitStringLit(x)))
    }

    // TODO we should use the visitRowFormatDelimited function here. However HiveScriptIOSchema
    // expects a seq of pairs in which the old parsers' token names are used as keys.
    // Transforming the result of visitRowFormatDelimited would be quite a bit messier than
    // retrieving the key value pairs ourselves.
    val entries = entry("TOK_TABLEROWFORMATFIELD", ctx.fieldsTerminatedBy) ++
      entry("TOK_TABLEROWFORMATCOLLITEMS", ctx.collectionItemsTerminatedBy) ++
      entry("TOK_TABLEROWFORMATMAPKEYS", ctx.keysTerminatedBy) ++
      entry("TOK_TABLEROWFORMATNULL", ctx.nullDefinedAs) ++
      Option(ctx.linesSeparatedBy).toSeq.map { stringLitCtx =>
        val value = string(visitStringLit(stringLitCtx))
        validate(
          value == "\n",
          s"LINES TERMINATED BY only supports newline '\\n' right now: $value",
          ctx)
        "TOK_TABLEROWFORMATLINES" -> value
      }

    (entries, None, Seq.empty, None)
  }

  /**
   * Create a [[ScriptInputOutputSchema]].
   */
  protected def withScriptIOSchema(
      ctx: ParserRuleContext,
      inRowFormat: RowFormatContext,
      recordWriter: Token,
      outRowFormat: RowFormatContext,
      recordReader: Token,
      schemaLess: Boolean): ScriptInputOutputSchema = {

    def format(fmt: RowFormatContext): ScriptIOFormat = fmt match {
      case c: RowFormatDelimitedContext =>
        getRowFormatDelimited(c)

      case c: RowFormatSerdeContext =>
        throw QueryParsingErrors.transformWithSerdeUnsupportedError(ctx)

      // SPARK-32106: When there is no definition about format, we return empty result
      // to use a built-in default Serde in SparkScriptTransformationExec.
      case null =>
        (Nil, None, Seq.empty, None)
    }

    val (inFormat, inSerdeClass, inSerdeProps, reader) = format(inRowFormat)

    val (outFormat, outSerdeClass, outSerdeProps, writer) = format(outRowFormat)

    ScriptInputOutputSchema(
      inFormat, outFormat,
      inSerdeClass, outSerdeClass,
      inSerdeProps, outSerdeProps,
      reader, writer,
      schemaLess)
  }

  /**
   * Create a logical plan for a given 'FROM' clause. Note that we support multiple (comma
   * separated) relations here, these get converted into a single plan by condition-less inner join.
   */
  override def visitFromClause(ctx: FromClauseContext): LogicalPlan = withOrigin(ctx) {
    val from = ctx.relation.asScala.foldLeft(null: LogicalPlan) { (left, relation) =>
      val relationPrimary = relation.relationPrimary()
      val right = if (conf.ansiRelationPrecedence) {
        visitRelation(relation)
      } else {
        plan(relationPrimary)
      }
      val join = right.optionalMap(left) { (left, right) =>
        if (relation.LATERAL != null) {
          relationPrimary match {
            case _: AliasedQueryContext =>
            case _: TableValuedFunctionContext =>
            case other =>
              throw QueryParsingErrors.invalidLateralJoinRelationError(other)
          }
          LateralJoin(left, LateralSubquery(right), Inner, None)
        } else {
          Join(left, right, Inner, None, JoinHint.NONE)
        }
      }
      if (conf.ansiRelationPrecedence) join else withRelationExtensions(relation, join)
    }
    if (ctx.pivotClause() != null) {
      if (ctx.unpivotClause() != null) {
        throw QueryParsingErrors.unpivotWithPivotInFromClauseNotAllowedError(ctx)
      }
      if (!ctx.lateralView.isEmpty) {
        throw QueryParsingErrors.lateralWithPivotInFromClauseNotAllowedError(ctx)
      }
      withPivot(ctx.pivotClause, from)
    } else if (ctx.unpivotClause() != null) {
      if (!ctx.lateralView.isEmpty) {
        throw QueryParsingErrors.lateralWithUnpivotInFromClauseNotAllowedError(ctx)
      }
      withUnpivot(ctx.unpivotClause, from)
    } else {
      ctx.lateralView.asScala.foldLeft(from)(withGenerate)
    }
  }

  /**
   * Connect two queries by a Set operator.
   *
   * Supported Set operators are:
   * - UNION [ DISTINCT | ALL ]
   * - EXCEPT [ DISTINCT | ALL ]
   * - MINUS [ DISTINCT | ALL ]
   * - INTERSECT [DISTINCT | ALL]
   */
  override def visitSetOperation(ctx: SetOperationContext): LogicalPlan = withOrigin(ctx) {
    val left = plan(ctx.left)
    val right = plan(ctx.right)
    val all = Option(ctx.setQuantifier()).exists(_.ALL != null)
    ctx.operator.getType match {
      case SqlBaseParser.UNION if all =>
        Union(left, right)
      case SqlBaseParser.UNION =>
        Distinct(Union(left, right))
      case SqlBaseParser.INTERSECT if all =>
        Intersect(left, right, isAll = true)
      case SqlBaseParser.INTERSECT =>
        Intersect(left, right, isAll = false)
      case SqlBaseParser.EXCEPT if all =>
        Except(left, right, isAll = true)
      case SqlBaseParser.EXCEPT =>
        Except(left, right, isAll = false)
      case SqlBaseParser.SETMINUS if all =>
        Except(left, right, isAll = true)
      case SqlBaseParser.SETMINUS =>
        Except(left, right, isAll = false)
    }
  }

  /**
   * Add a [[WithWindowDefinition]] operator to a logical plan.
   */
  private def withWindowClause(
      ctx: WindowClauseContext,
      query: LogicalPlan): LogicalPlan = withOrigin(ctx) {
    // Collect all window specifications defined in the WINDOW clause.
    val baseWindowTuples = ctx.namedWindow.asScala.map {
      wCtx =>
        (wCtx.name.getText, typedVisit[WindowSpec](wCtx.windowSpec))
    }
    baseWindowTuples.groupBy(_._1).foreach { kv =>
      if (kv._2.size > 1) {
        throw QueryParsingErrors.repetitiveWindowDefinitionError(kv._1, ctx)
      }
    }
    val baseWindowMap = baseWindowTuples.toMap

    // Handle cases like
    // window w1 as (partition by p_mfgr order by p_name
    //               range between 2 preceding and 2 following),
    //        w2 as w1
    val windowMapView = baseWindowMap.transform {
      case (_, WindowSpecReference(name)) =>
        baseWindowMap.get(name) match {
          case Some(spec: WindowSpecDefinition) =>
            spec
          case Some(ref) =>
            throw QueryParsingErrors.invalidWindowReferenceError(name, ctx)
          case None =>
            throw QueryParsingErrors.cannotResolveWindowReferenceError(name, ctx)
        }
      case (_, spec: WindowSpecDefinition) => spec
    }

    // Note that mapValues creates a view instead of materialized map. We force materialization by
    // mapping over identity.
    WithWindowDefinition(windowMapView.map(identity), query)
  }

  /**
   * Add an [[Aggregate]] to a logical plan.
   */
  private def withAggregationClause(
      ctx: AggregationClauseContext,
      selectExpressions: Seq[NamedExpression],
      query: LogicalPlan): LogicalPlan = withOrigin(ctx) {
    if (ctx.groupingExpressionsWithGroupingAnalytics.isEmpty) {
      val groupByExpressions = expressionList(ctx.groupingExpressions)
      if (ctx.GROUPING != null) {
        // GROUP BY ... GROUPING SETS (...)
        // `groupByExpressions` can be non-empty for Hive compatibility. It may add extra grouping
        // expressions that do not exist in GROUPING SETS (...), and the value is always null.
        // For example, `SELECT a, b, c FROM ... GROUP BY a, b, c GROUPING SETS (a, b)`, the output
        // of column `c` is always null.
        val groupingSets =
          ctx.groupingSet.asScala.map(_.expression.asScala.map(e => expression(e)).toSeq)
        Aggregate(Seq(GroupingSets(groupingSets.toSeq, groupByExpressions)),
          selectExpressions, query)
      } else {
        // GROUP BY .... (WITH CUBE | WITH ROLLUP)?
        val mappedGroupByExpressions = if (ctx.CUBE != null) {
          Seq(Cube(groupByExpressions.map(Seq(_))))
        } else if (ctx.ROLLUP != null) {
          Seq(Rollup(groupByExpressions.map(Seq(_))))
        } else {
          groupByExpressions
        }
        Aggregate(mappedGroupByExpressions, selectExpressions, query)
      }
    } else {
      val groupByExpressions =
        ctx.groupingExpressionsWithGroupingAnalytics.asScala
          .map(groupByExpr => {
            val groupingAnalytics = groupByExpr.groupingAnalytics
            if (groupingAnalytics != null) {
              visitGroupingAnalytics(groupingAnalytics)
            } else {
              expression(groupByExpr.expression)
            }
          })
      Aggregate(groupByExpressions.toSeq, selectExpressions, query)
    }
  }

  override def visitGroupingAnalytics(
      groupingAnalytics: GroupingAnalyticsContext): BaseGroupingSets = {
    val groupingSets = groupingAnalytics.groupingSet.asScala
      .map(_.expression.asScala.map(e => expression(e)).toSeq)
    if (groupingAnalytics.CUBE != null) {
      // CUBE(A, B, (A, B), ()) is not supported.
      if (groupingSets.exists(_.isEmpty)) {
        throw QueryParsingErrors.invalidGroupingSetError("CUBE", groupingAnalytics)
      }
      Cube(groupingSets.toSeq)
    } else if (groupingAnalytics.ROLLUP != null) {
      // ROLLUP(A, B, (A, B), ()) is not supported.
      if (groupingSets.exists(_.isEmpty)) {
        throw QueryParsingErrors.invalidGroupingSetError("ROLLUP", groupingAnalytics)
      }
      Rollup(groupingSets.toSeq)
    } else {
      assert(groupingAnalytics.GROUPING != null && groupingAnalytics.SETS != null)
      val groupingSets = groupingAnalytics.groupingElement.asScala.flatMap { expr =>
        val groupingAnalytics = expr.groupingAnalytics()
        if (groupingAnalytics != null) {
          visitGroupingAnalytics(groupingAnalytics).selectedGroupByExprs
        } else {
          Seq(expr.groupingSet().expression().asScala.map(e => expression(e)).toSeq)
        }
      }
      GroupingSets(groupingSets.toSeq)
    }
  }

  /**
   * Add [[UnresolvedHint]]s to a logical plan.
   */
  private def withHints(
      ctx: HintContext,
      query: LogicalPlan): LogicalPlan = withOrigin(ctx) {
    var plan = query
    ctx.hintStatements.asScala.reverse.foreach { stmt =>
      plan = UnresolvedHint(stmt.hintName.getText,
        stmt.parameters.asScala.map(expression).toSeq, plan)
    }
    plan
  }

  /**
   * Add a [[Pivot]] to a logical plan.
   */
  private def withPivot(
      ctx: PivotClauseContext,
      query: LogicalPlan): LogicalPlan = withOrigin(ctx) {
    val aggregates = Option(ctx.aggregates).toSeq
      .flatMap(_.namedExpression.asScala)
      .map(typedVisit[Expression])
    val pivotColumn = if (ctx.pivotColumn.identifiers.size == 1) {
      UnresolvedAttribute.quoted(ctx.pivotColumn.errorCapturingIdentifier.getText)
    } else {
      CreateStruct(
        ctx.pivotColumn.identifiers.asScala.map(
          identifier => UnresolvedAttribute.quoted(identifier.getText)).toSeq)
    }
    val pivotValues = ctx.pivotValues.asScala.map(visitPivotValue)
    Pivot(None, pivotColumn, pivotValues.toSeq, aggregates, query)
  }

  /**
   * Create a Pivot column value with or without an alias.
   */
  override def visitPivotValue(ctx: PivotValueContext): Expression = withOrigin(ctx) {
    val e = expression(ctx.expression)
    if (ctx.errorCapturingIdentifier != null) {
      Alias(e, ctx.errorCapturingIdentifier.getText)()
    } else {
      e
    }
  }

  /**
   * Add an [[Unpivot]] to a logical plan.
   */
  private def withUnpivot(
      ctx: UnpivotClauseContext,
      query: LogicalPlan): LogicalPlan = withOrigin(ctx) {
    // this is needed to create unpivot and to filter unpivot for nulls further down
    val valueColumnNames =
      Option(ctx.unpivotOperator().unpivotSingleValueColumnClause())
        .map(_.unpivotValueColumn().identifier().getText)
        .map(Seq(_))
      .getOrElse(
        Option(ctx.unpivotOperator().unpivotMultiValueColumnClause())
          .map(_.unpivotValueColumns.asScala.map(_.identifier().getText).toSeq)
          .get
      )

    val unpivot = if (ctx.unpivotOperator().unpivotSingleValueColumnClause() != null) {
      val unpivotClause = ctx.unpivotOperator().unpivotSingleValueColumnClause()
      val variableColumnName = unpivotClause.unpivotNameColumn().identifier().getText
      val (unpivotColumns, unpivotAliases) =
        unpivotClause.unpivotColumns.asScala.map(visitUnpivotColumnAndAlias).toSeq.unzip

      Unpivot(
        None,
        Some(unpivotColumns.map(Seq(_))),
        // None when all elements are None
        Some(unpivotAliases).filter(_.exists(_.isDefined)),
        variableColumnName,
        valueColumnNames,
        query
      )
    } else {
      val unpivotClause = ctx.unpivotOperator().unpivotMultiValueColumnClause()
      val variableColumnName = unpivotClause.unpivotNameColumn().identifier().getText
      val (unpivotColumns, unpivotAliases) =
        unpivotClause.unpivotColumnSets.asScala.map(visitUnpivotColumnSet).toSeq.unzip

      Unpivot(
        None,
        Some(unpivotColumns),
        // None when all elements are None
        Some(unpivotAliases).filter(_.exists(_.isDefined)),
        variableColumnName,
        valueColumnNames,
        query
      )
    }

    // exclude null values by default
    val filtered = if (ctx.nullOperator == null || ctx.nullOperator.EXCLUDE() != null) {
      Filter(IsNotNull(Coalesce(valueColumnNames.map(UnresolvedAttribute(_)))), unpivot)
    } else {
      unpivot
    }

    // alias unpivot result
    if (ctx.errorCapturingIdentifier() != null) {
      val alias = ctx.errorCapturingIdentifier().getText
      SubqueryAlias(alias, filtered)
    } else {
      filtered
    }
  }

  /**
   * Create an Unpivot column.
   */
  override def visitUnpivotColumn(ctx: UnpivotColumnContext): NamedExpression = withOrigin(ctx) {
    UnresolvedAttribute(visitMultipartIdentifier(ctx.multipartIdentifier))
  }

  /**
   * Create an Unpivot column.
   */
  override def visitUnpivotColumnAndAlias(ctx: UnpivotColumnAndAliasContext):
  (NamedExpression, Option[String]) = withOrigin(ctx) {
    val attr = visitUnpivotColumn(ctx.unpivotColumn())
    val alias = Option(ctx.unpivotAlias()).map(_.errorCapturingIdentifier().getText)
    (attr, alias)
  }

  /**
   * Create an Unpivot struct column with or without an alias.
   * Each struct field is renamed to the respective value column name.
   */
  override def visitUnpivotColumnSet(ctx: UnpivotColumnSetContext):
  (Seq[NamedExpression], Option[String]) =
    withOrigin(ctx) {
      val exprs = ctx.unpivotColumns.asScala.map(visitUnpivotColumn).toSeq
      val alias = Option(ctx.unpivotAlias()).map(_.errorCapturingIdentifier().getText)
      (exprs, alias)
    }

  /**
   * Add a [[Generate]] (Lateral View) to a logical plan.
   */
  private def withGenerate(
      query: LogicalPlan,
      ctx: LateralViewContext): LogicalPlan = withOrigin(ctx) {
    val expressions = expressionList(ctx.expression)
    Generate(
      UnresolvedGenerator(visitFunctionName(ctx.qualifiedName), expressions),
      unrequiredChildIndex = Nil,
      outer = ctx.OUTER != null,
      // scalastyle:off caselocale
      Some(ctx.tblName.getText.toLowerCase),
      // scalastyle:on caselocale
      ctx.colName.asScala.map(_.getText).map(UnresolvedAttribute.quoted).toSeq,
      query)
  }

  /**
   * Create a single relation referenced in a FROM clause. This method is used when a part of the
   * join condition is nested, for example:
   * {{{
   *   select * from t1 join (t2 cross join t3) on col1 = col2
   * }}}
   */
  override def visitRelation(ctx: RelationContext): LogicalPlan = withOrigin(ctx) {
    withRelationExtensions(ctx, plan(ctx.relationPrimary))
  }

  private def withRelationExtensions(ctx: RelationContext, query: LogicalPlan): LogicalPlan = {
    ctx.relationExtension().asScala.foldLeft(query) { (left, extension) =>
      if (extension.joinRelation() != null) {
        withJoinRelation(extension.joinRelation(), left)
      } else if (extension.pivotClause() != null) {
        withPivot(extension.pivotClause(), left)
      } else {
        assert(extension.unpivotClause() != null)
        withUnpivot(extension.unpivotClause(), left)
      }
    }
  }

  /**
   * Join one more [[LogicalPlan]] to the current logical plan.
   */
  private def withJoinRelation(ctx: JoinRelationContext, base: LogicalPlan): LogicalPlan = {
    withOrigin(ctx) {
      val baseJoinType = ctx.joinType match {
        case null => Inner
        case jt if jt.CROSS != null => Cross
        case jt if jt.FULL != null => FullOuter
        case jt if jt.SEMI != null => LeftSemi
        case jt if jt.ANTI != null => LeftAnti
        case jt if jt.LEFT != null => LeftOuter
        case jt if jt.RIGHT != null => RightOuter
        case _ => Inner
      }

      if (ctx.LATERAL != null) {
        ctx.right match {
          case _: AliasedQueryContext =>
          case _: TableValuedFunctionContext =>
          case other =>
            throw QueryParsingErrors.invalidLateralJoinRelationError(other)
        }
      }

      // Resolve the join type and join condition
      val (joinType, condition) = Option(ctx.joinCriteria) match {
        case Some(c) if c.USING != null =>
          if (ctx.LATERAL != null) {
            throw QueryParsingErrors.lateralJoinWithUsingJoinUnsupportedError(ctx)
          }
          (UsingJoin(baseJoinType, visitIdentifierList(c.identifierList)), None)
        case Some(c) if c.booleanExpression != null =>
          (baseJoinType, Option(expression(c.booleanExpression)))
        case Some(c) =>
          throw SparkException.internalError(s"Unimplemented joinCriteria: $c")
        case None if ctx.NATURAL != null =>
          if (ctx.LATERAL != null) {
            throw QueryParsingErrors.incompatibleJoinTypesError(
              joinType1 = ctx.LATERAL.toString, joinType2 = ctx.NATURAL.toString, ctx = ctx
            )
          }
          if (baseJoinType == Cross) {
            throw QueryParsingErrors.incompatibleJoinTypesError(
              joinType1 = ctx.NATURAL.toString, joinType2 = baseJoinType.toString, ctx = ctx
            )
          }
          (NaturalJoin(baseJoinType), None)
        case None =>
          (baseJoinType, None)
      }
      if (ctx.LATERAL != null) {
        if (!Seq(Inner, Cross, LeftOuter).contains(joinType)) {
          throw QueryParsingErrors.unsupportedLateralJoinTypeError(ctx, joinType.sql)
        }
        LateralJoin(base, LateralSubquery(plan(ctx.right)), joinType, condition)
      } else {
        Join(base, plan(ctx.right), joinType, condition, JoinHint.NONE)
      }
    }
  }

  /**
   * Add a [[Sample]] to a logical plan.
   *
   * This currently supports the following sampling methods:
   * - TABLESAMPLE(x ROWS): Sample the table down to the given number of rows.
   * - TABLESAMPLE(x PERCENT) [REPEATABLE (y)]: Sample the table down to the given percentage with
   * seed 'y'. Note that percentages are defined as a number between 0 and 100.
   * - TABLESAMPLE(BUCKET x OUT OF y) [REPEATABLE (z)]: Sample the table down to a 'x' divided by
   * 'y' fraction with seed 'z'.
   */
  private def withSample(ctx: SampleContext, query: LogicalPlan): LogicalPlan = withOrigin(ctx) {
    // Create a sampled plan if we need one.
    def sample(fraction: Double, seed: Long): Sample = {
      // The range of fraction accepted by Sample is [0, 1]. Because Hive's block sampling
      // function takes X PERCENT as the input and the range of X is [0, 100], we need to
      // adjust the fraction.
      val eps = RandomSampler.roundingEpsilon
      validate(fraction >= 0.0 - eps && fraction <= 1.0 + eps,
        s"Sampling fraction ($fraction) must be on interval [0, 1]",
        ctx)
      Sample(0.0, fraction, withReplacement = false, seed, query)
    }

    if (ctx.sampleMethod() == null) {
      throw QueryParsingErrors.emptyInputForTableSampleError(ctx)
    }

    val seed = if (ctx.seed != null) {
      ctx.seed.getText.toLong
    } else {
      (math.random() * 1000).toLong
    }

    ctx.sampleMethod() match {
      case ctx: SampleByRowsContext =>
        Limit(expression(ctx.expression), query)

      case ctx: SampleByPercentileContext =>
        val fraction = ctx.percentage.getText.toDouble
        val sign = if (ctx.negativeSign == null) 1 else -1
        sample(sign * fraction / 100.0d, seed)

      case ctx: SampleByBytesContext =>
        val bytesStr = ctx.bytes.getText
        if (bytesStr.matches("[0-9]+[bBkKmMgG]")) {
          throw QueryParsingErrors.tableSampleByBytesUnsupportedError("byteLengthLiteral", ctx)
        } else {
          throw QueryParsingErrors.invalidByteLengthLiteralError(bytesStr, ctx)
        }

      case ctx: SampleByBucketContext if ctx.ON() != null =>
        if (ctx.identifier != null) {
          throw QueryParsingErrors.tableSampleByBytesUnsupportedError(
            "BUCKET x OUT OF y ON colname", ctx)
        } else {
          throw QueryParsingErrors.tableSampleByBytesUnsupportedError(
            "BUCKET x OUT OF y ON function", ctx)
        }

      case ctx: SampleByBucketContext =>
        sample(ctx.numerator.getText.toDouble / ctx.denominator.getText.toDouble, seed)
    }
  }

  /**
   * Create a logical plan for a sub-query.
   */
  override def visitSubquery(ctx: SubqueryContext): LogicalPlan = withOrigin(ctx) {
    plan(ctx.query)
  }

  /**
   * Create an un-aliased table reference. This is typically used for top-level table references,
   * for example:
   * {{{
   *   INSERT INTO db.tbl2
   *   TABLE db.tbl1
   * }}}
   */
  override def visitTable(ctx: TableContext): LogicalPlan = withOrigin(ctx) {
    createUnresolvedRelation(ctx.identifierReference)
  }

  /**
   * Create an aliased table reference. This is typically used in FROM clauses.
   */
  override def visitTableName(ctx: TableNameContext): LogicalPlan = withOrigin(ctx) {
    val relation = createUnresolvedRelation(ctx.identifierReference, Option(ctx.optionsClause))
    val table = mayApplyAliasPlan(
      ctx.tableAlias, relation.optionalMap(ctx.temporalClause)(withTimeTravel))
    table.optionalMap(ctx.sample)(withSample)
  }

  override def visitVersion(ctx: VersionContext): Option[String] = {
    if (ctx != null) {
      if (ctx.INTEGER_VALUE != null) {
        Some(ctx.INTEGER_VALUE().getText)
      } else {
        Option(string(visitStringLit(ctx.stringLit())))
      }
    } else {
      None
    }
  }

  private def extractNamedArgument(expr: FunctionArgumentContext, funcName: String) : Expression = {
    Option(expr.namedArgumentExpression).map { n =>
      if (conf.getConf(SQLConf.ALLOW_NAMED_FUNCTION_ARGUMENTS)) {
        NamedArgumentExpression(n.key.getText, expression(n.value))
      } else {
        throw QueryCompilationErrors.namedArgumentsNotEnabledError(funcName, n.key.getText)
      }
    }.getOrElse {
      expression(expr)
    }
  }

  private def withTimeTravel(
      ctx: TemporalClauseContext, plan: LogicalPlan): LogicalPlan = withOrigin(ctx) {
    val v = ctx.version
    val version = visitVersion(ctx.version)
    val timestamp = Option(ctx.timestamp).map(expression)
    if (timestamp.exists(_.references.nonEmpty)) {
      throw QueryParsingErrors.invalidTimeTravelSpec(
        "timestamp expression cannot refer to any columns", ctx.timestamp)
    }
    RelationTimeTravel(plan, timestamp, version)
  }

  /**
   * Create a relation argument for a table-valued function argument.
   */
  override def visitFunctionTableSubqueryArgument(
      ctx: FunctionTableSubqueryArgumentContext): Expression = withOrigin(ctx) {
    val p = Option(ctx.identifierReference).map { r =>
      // Make sure that the identifier after the TABLE keyword is surrounded by parentheses, as
      // required by the SQL standard. If not, return an informative error message.
      if (ctx.LEFT_PAREN() == null) {
        throw QueryParsingErrors.invalidTableFunctionIdentifierArgumentMissingParentheses(
          ctx, argumentName = ctx.identifierReference().getText)
      }
      createUnresolvedRelation(r)
    }.getOrElse {
      plan(ctx.query)
    }
    var withSinglePartition = false
    var partitionByExpressions = Seq.empty[Expression]
    var orderByExpressions = Seq.empty[SortOrder]
    Option(ctx.tableArgumentPartitioning).foreach { p =>
      if (p.SINGLE != null) {
        withSinglePartition = true
      }
      partitionByExpressions = p.partition.asScala.map(expression).toSeq
      orderByExpressions = p.sortItem.asScala.map(visitSortItem).toSeq
      def invalidPartitionOrOrderingExpression(clause: String): String = {
        "The table function call includes a table argument with an invalid " +
          s"partitioning/ordering specification: the $clause clause included multiple " +
          "expressions without parentheses surrounding them; please add parentheses around " +
          "these expressions and then retry the query again"
      }
      validate(
        Option(p.invalidMultiPartitionExpression).isEmpty,
        message = invalidPartitionOrOrderingExpression("PARTITION BY"),
        ctx = p.invalidMultiPartitionExpression)
      validate(
        Option(p.invalidMultiSortItem).isEmpty,
        message = invalidPartitionOrOrderingExpression("ORDER BY"),
        ctx = p.invalidMultiSortItem)
    }
    validate(
      !(withSinglePartition && partitionByExpressions.nonEmpty),
      message = "WITH SINGLE PARTITION cannot be specified if PARTITION BY is also present",
      ctx = ctx.tableArgumentPartitioning)
    validate(
      !(orderByExpressions.nonEmpty && partitionByExpressions.isEmpty && !withSinglePartition),
      message = "ORDER BY cannot be specified unless either " +
        "PARTITION BY or WITH SINGLE PARTITION is also present",
      ctx = ctx.tableArgumentPartitioning)
    FunctionTableSubqueryArgumentExpression(
      plan = p,
      partitionByExpressions = partitionByExpressions,
      withSinglePartition = withSinglePartition,
      orderByExpressions = orderByExpressions)
  }

  private def extractFunctionTableNamedArgument(
      expr: FunctionTableReferenceArgumentContext, funcName: String) : Expression = {
    Option(expr.functionTableNamedArgumentExpression).map { n =>
      if (conf.getConf(SQLConf.ALLOW_NAMED_FUNCTION_ARGUMENTS)) {
        NamedArgumentExpression(
          n.key.getText, visitFunctionTableSubqueryArgument(n.functionTableSubqueryArgument))
      } else {
        throw QueryCompilationErrors.namedArgumentsNotEnabledError(funcName, n.key.getText)
      }
    }.getOrElse {
      visitFunctionTableSubqueryArgument(expr.functionTableSubqueryArgument)
    }
  }

  /**
   * Create a table-valued function call with arguments, e.g. range(1000)
   */
  override def visitTableValuedFunction(ctx: TableValuedFunctionContext)
      : LogicalPlan = withOrigin(ctx) {
    val func = ctx.functionTable
    val aliases = if (func.tableAlias.identifierList != null) {
      visitIdentifierList(func.tableAlias.identifierList)
    } else {
      Seq.empty
    }

    withFuncIdentClause(
      func.functionName,
      Nil,
      (ident, _) => {
        if (ident.length > 1) {
          throw QueryParsingErrors.invalidTableValuedFunctionNameError(ident, ctx)
        }
        val funcName = func.functionName.getText
        val args = func.functionTableArgument.asScala.map { e =>
          Option(e.functionArgument).map(extractNamedArgument(_, funcName))
            .getOrElse {
              extractFunctionTableNamedArgument(e.functionTableReferenceArgument, funcName)
            }
        }.toSeq

        val tvf = UnresolvedTableValuedFunction(ident, args)

        val tvfAliases = if (aliases.nonEmpty) UnresolvedTVFAliases(ident, tvf, aliases) else tvf

        tvfAliases.optionalMap(func.tableAlias.strictIdentifier)(aliasPlan)
      })
  }

  /**
   * Create an inline table (a virtual table in Hive parlance).
   */
  override def visitInlineTable(ctx: InlineTableContext): LogicalPlan = withOrigin(ctx) {
    // Get the backing expressions.
    val rows = ctx.expression.asScala.map { e =>
      expression(e) match {
        // inline table comes in two styles:
        // style 1: values (1), (2), (3)  -- multiple columns are supported
        // style 2: values 1, 2, 3  -- only a single column is supported here
        case struct: CreateNamedStruct => struct.valExprs // style 1
        case child => Seq(child)                          // style 2
      }
    }

    val aliases = if (ctx.tableAlias.identifierList != null) {
      visitIdentifierList(ctx.tableAlias.identifierList)
    } else {
      Seq.tabulate(rows.head.size)(i => s"col${i + 1}")
    }

    val unresolvedTable = UnresolvedInlineTable(aliases, rows.toSeq)
    val table = if (conf.getConf(SQLConf.EAGER_EVAL_OF_UNRESOLVED_INLINE_TABLE_ENABLED)) {
      EvaluateUnresolvedInlineTable.evaluate(unresolvedTable)
    } else {
      unresolvedTable
    }
    table.optionalMap(ctx.tableAlias.strictIdentifier)(aliasPlan)
  }

  /**
   * Create an alias (SubqueryAlias) for a join relation. This is practically the same as
   * visitAliasedQuery and visitNamedExpression, ANTLR4 however requires us to use 3 different
   * hooks. We could add alias names for output columns, for example:
   * {{{
   *   SELECT a, b, c, d FROM (src1 s1 INNER JOIN src2 s2 ON s1.id = s2.id) dst(a, b, c, d)
   * }}}
   */
  override def visitAliasedRelation(ctx: AliasedRelationContext): LogicalPlan = withOrigin(ctx) {
    val relation = plan(ctx.relation).optionalMap(ctx.sample)(withSample)
    mayApplyAliasPlan(ctx.tableAlias, relation)
  }

  /**
   * Create an alias (SubqueryAlias) for a sub-query. This is practically the same as
   * visitAliasedRelation and visitNamedExpression, ANTLR4 however requires us to use 3 different
   * hooks. We could add alias names for output columns, for example:
   * {{{
   *   SELECT col1, col2 FROM testData AS t(col1, col2)
   * }}}
   */
  override def visitAliasedQuery(ctx: AliasedQueryContext): LogicalPlan = withOrigin(ctx) {
    val relation = plan(ctx.query).optionalMap(ctx.sample)(withSample)
    if (ctx.tableAlias.strictIdentifier == null) {
      // For un-aliased subqueries, use a default alias name that is not likely to conflict with
      // normal subquery names, so that parent operators can only access the columns in subquery by
      // unqualified names. Users can still use this special qualifier to access columns if they
      // know it, but that's not recommended.
      SubqueryAlias(SubqueryAlias.generateSubqueryName(), relation)
    } else {
      mayApplyAliasPlan(ctx.tableAlias, relation)
    }
  }

  /**
   * Create an alias ([[SubqueryAlias]]) for a [[LogicalPlan]].
   */
  private def aliasPlan(alias: ParserRuleContext, plan: LogicalPlan): LogicalPlan = {
    SubqueryAlias(alias.getText, plan)
  }

  /**
   * If aliases specified in a FROM clause, create a subquery alias ([[SubqueryAlias]]) and
   * column aliases for a [[LogicalPlan]].
   */
  private def mayApplyAliasPlan(tableAlias: TableAliasContext, plan: LogicalPlan): LogicalPlan = {
    if (tableAlias.strictIdentifier != null) {
      val alias = tableAlias.strictIdentifier.getText
      if (tableAlias.identifierList != null) {
        val columnNames = visitIdentifierList(tableAlias.identifierList)
        SubqueryAlias(alias, UnresolvedSubqueryColumnAliases(columnNames, plan))
      } else {
        SubqueryAlias(alias, plan)
      }
    } else {
      plan
    }
  }

  /**
   * Create a Sequence of Strings for a parenthesis enclosed alias list.
   */
  override def visitIdentifierList(ctx: IdentifierListContext): Seq[String] = withOrigin(ctx) {
    visitIdentifierSeq(ctx.identifierSeq)
  }

  /**
   * Create a Sequence of Strings for an identifier list.
   */
  override def visitIdentifierSeq(ctx: IdentifierSeqContext): Seq[String] = withOrigin(ctx) {
    ctx.ident.asScala.map(_.getText).toSeq
  }

  /* ********************************************************************************************
   * Table Identifier parsing
   * ******************************************************************************************** */
  /**
   * Create a [[TableIdentifier]] from a 'tableName' or 'databaseName'.'tableName' pattern.
   */
  override def visitTableIdentifier(
      ctx: TableIdentifierContext): TableIdentifier = withOrigin(ctx) {
    TableIdentifier(ctx.table.getText, Option(ctx.db).map(_.getText))
  }

  /**
   * Create a [[FunctionIdentifier]] from a 'functionName' or 'databaseName'.'functionName' pattern.
   */
  override def visitFunctionIdentifier(
      ctx: FunctionIdentifierContext): FunctionIdentifier = withOrigin(ctx) {
    FunctionIdentifier(ctx.function.getText, Option(ctx.db).map(_.getText))
  }

  /**
   * Create a multi-part identifier.
   */
  override def visitMultipartIdentifier(ctx: MultipartIdentifierContext): Seq[String] =
    withOrigin(ctx) {
      ctx.parts.asScala.map(_.getText).toSeq
    }

  /* ********************************************************************************************
   * Expression parsing
   * ******************************************************************************************** */
  /**
   * Create an expression from the given context. This method just passes the context on to the
   * visitor and only takes care of typing (We assume that the visitor returns an Expression here).
   */
  protected def expression(ctx: ParserRuleContext): Expression = typedVisit(ctx)

  /**
   * Create sequence of expressions from the given sequence of contexts.
   */
  private def expressionList(trees: java.util.List[ExpressionContext]): Seq[Expression] = {
    trees.asScala.map(expression).toSeq
  }

  /**
   * Create a star (i.e. all) expression; this selects all elements (in the specified object).
   * Both un-targeted (global) and targeted aliases are supported.
   */
  override def visitStar(ctx: StarContext): Expression = withOrigin(ctx) {
    val target = Option(ctx.qualifiedName()).map(_.identifier.asScala.map(_.getText).toSeq)

    if (ctx.exceptClause != null) {
      visitStarExcept(ctx, target)
    }
    else {
      UnresolvedStar(target)
    }
  }

  /**
   * Create a star-except (i.e. all - except list) expression; this selects all elements in the
   * specified object except those in the except list.
   * Both un-targeted (global) and targeted aliases are supported.
   */
  def visitStarExcept(ctx: StarContext, target: Option[Seq[String]]): Expression = withOrigin(ctx) {
    val exceptCols = ctx.exceptClause
      .exceptCols.multipartIdentifier.asScala.map(typedVisit[Seq[String]])
    UnresolvedStarExcept(
      target,
      exceptCols.toSeq)
  }

  /**
   * Check for the inappropriate usage of the '!' token.
   * '!' used to be a synonym for 'NOT' in the lexer, but that was too general.
   * '!' should only be a synonym for 'NOT' when used as a prefix in a logical operation.
   * We do that now explicitly.
   */
  def blockBang(ctx: ErrorCapturingNotContext): ErrorCapturingNotContext = {
    val tolerateBang = conf.getConf(LEGACY_BANG_EQUALS_NOT)
    if (ctx != null && ctx.BANG() != null && !tolerateBang) {
      withOrigin(ctx) {
        throw new ParseException(
          errorClass = "SYNTAX_DISCONTINUED.BANG_EQUALS_NOT",
          messageParameters = Map("clause" -> toSQLStmt("!")),
          ctx)
      }
    }
    ctx
  }

  /**
   * Create an aliased expression if an alias is specified. Both single and multi-aliases are
   * supported.
   */
  override def visitNamedExpression(ctx: NamedExpressionContext): Expression = withOrigin(ctx) {
    val e = expression(ctx.expression)
    if (ctx.name != null) {
      Alias(e, ctx.name.getText)()
    } else if (ctx.identifierList != null) {
      MultiAlias(e, visitIdentifierList(ctx.identifierList))
    } else {
      e
    }
  }

  /**
   * Combine a number of boolean expressions into a balanced expression tree. These expressions are
   * either combined by a logical [[And]] or a logical [[Or]].
   *
   * A balanced binary tree is created because regular left recursive trees cause considerable
   * performance degradations and can cause stack overflows.
   */
  override def visitLogicalBinary(ctx: LogicalBinaryContext): Expression = withOrigin(ctx) {
    val expressionType = ctx.operator.getType
    val expressionCombiner = expressionType match {
      case SqlBaseParser.AND => And.apply _
      case SqlBaseParser.OR => Or.apply _
    }

    // Collect all similar left hand contexts.
    val contexts = ArrayBuffer(ctx.right)
    var current = ctx.left
    def collectContexts: Boolean = current match {
      case lbc: LogicalBinaryContext if lbc.operator.getType == expressionType =>
        contexts += lbc.right
        current = lbc.left
        true
      case _ =>
        contexts += current
        false
    }
    while (collectContexts) {
      // No body - all updates take place in the collectContexts.
    }

    // Reverse the contexts to have them in the same sequence as in the SQL statement & turn them
    // into expressions.
    val expressions = contexts.reverseIterator.map(expression).to(ArrayBuffer)

    // Create a balanced tree.
    def reduceToExpressionTree(low: Int, high: Int): Expression = high - low match {
      case 0 =>
        expressions(low)
      case 1 =>
        expressionCombiner(expressions(low), expressions(high))
      case x =>
        val mid = low + x / 2
        expressionCombiner(
          reduceToExpressionTree(low, mid),
          reduceToExpressionTree(mid + 1, high))
    }
    reduceToExpressionTree(0, expressions.size - 1)
  }

  /**
   * Invert a boolean expression.
   */
  override def visitLogicalNot(ctx: LogicalNotContext): Expression = withOrigin(ctx) {
    Not(expression(ctx.booleanExpression()))
  }

  /**
   * Create a filtering correlated sub-query (EXISTS).
   */
  override def visitExists(ctx: ExistsContext): Expression = {
    Exists(plan(ctx.query))
  }

  /**
   * Create a comparison expression. This compares two expressions. The following comparison
   * operators are supported:
   * - Equal: '=' or '=='
   * - Null-safe Equal: '<=>'
   * - Not Equal: '<>' or '!='
   * - Less than: '<'
   * - Less than or Equal: '<='
   * - Greater than: '>'
   * - Greater than or Equal: '>='
   */
  override def visitComparison(ctx: ComparisonContext): Expression = withOrigin(ctx) {
    val left = expression(ctx.left)
    val right = expression(ctx.right)
    val operator = ctx.comparisonOperator().getChild(0).asInstanceOf[TerminalNode]
    operator.getSymbol.getType match {
      case SqlBaseParser.EQ =>
        EqualTo(left, right)
      case SqlBaseParser.NSEQ =>
        EqualNullSafe(left, right)
      case SqlBaseParser.NEQ | SqlBaseParser.NEQJ =>
        Not(EqualTo(left, right))
      case SqlBaseParser.LT =>
        LessThan(left, right)
      case SqlBaseParser.LTE =>
        LessThanOrEqual(left, right)
      case SqlBaseParser.GT =>
        GreaterThan(left, right)
      case SqlBaseParser.GTE =>
        GreaterThanOrEqual(left, right)
    }
  }

  /**
   * Create a predicated expression. A predicated expression is a normal expression with a
   * predicate attached to it, for example:
   * {{{
   *    a + 1 IS NULL
   * }}}
   */
  override def visitPredicated(ctx: PredicatedContext): Expression = withOrigin(ctx) {
    val e = expression(ctx.valueExpression)
    if (ctx.predicate != null) {
      withPredicate(e, ctx.predicate)
    } else {
      e
    }
  }

  /**
   * Add a predicate to the given expression. Supported expressions are:
   * - (NOT) BETWEEN
   * - (NOT) IN
   * - (NOT) (LIKE | ILIKE) (ANY | SOME | ALL)
   * - (NOT) RLIKE
   * - IS (NOT) NULL.
   * - IS (NOT) (TRUE | FALSE | UNKNOWN)
   * - IS (NOT) DISTINCT FROM
   */
  private def withPredicate(e: Expression, ctx: PredicateContext): Expression = withOrigin(ctx) {
    // Invert a predicate if it has a valid NOT clause.
    def invertIfNotDefined(e: Expression): Expression = {
      val withNot = blockBang(ctx.errorCapturingNot)
      withNot match {
        case null => e
        case _ => Not(e)
      }
    }

    def getValueExpressions(e: Expression): Seq[Expression] = e match {
      case c: CreateNamedStruct => c.valExprs
      case other => Seq(other)
    }

    def lowerLikeArgsIfNeeded(
        expr: Expression,
        patterns: Seq[UTF8String]): (Expression, Seq[UTF8String]) = ctx.kind.getType match {
      // scalastyle:off caselocale
      case SqlBaseParser.ILIKE => (Lower(expr), patterns.map(_.toLowerCase))
      // scalastyle:on caselocale
      case _ => (expr, patterns)
    }

    def getLike(expr: Expression, pattern: Expression): Expression = ctx.kind.getType match {
      case SqlBaseParser.ILIKE => new ILike(expr, pattern)
      case _ => new Like(expr, pattern)
    }

    val withNot = blockBang(ctx.errorCapturingNot)

    // Create the predicate.
    ctx.kind.getType match {
      case SqlBaseParser.BETWEEN =>
        if (!SQLConf.get.legacyDuplicateBetweenInput) {
          invertIfNotDefined(UnresolvedFunction(
            "between", Seq(e, expression(ctx.lower), expression(ctx.upper)), isDistinct = false))
        } else {
          invertIfNotDefined(And(
            GreaterThanOrEqual(e, expression(ctx.lower)),
            LessThanOrEqual(e, expression(ctx.upper))))
        }
      case SqlBaseParser.IN if ctx.query != null =>
        invertIfNotDefined(InSubquery(getValueExpressions(e), ListQuery(plan(ctx.query))))
      case SqlBaseParser.IN =>
        invertIfNotDefined(In(e, ctx.expression.asScala.map(expression).toSeq))
      case SqlBaseParser.LIKE | SqlBaseParser.ILIKE =>
        Option(ctx.quantifier).map(_.getType) match {
          case Some(SqlBaseParser.ANY) | Some(SqlBaseParser.SOME) =>
            validate(!ctx.expression.isEmpty, "Expected something between '(' and ')'.", ctx)
            val expressions = expressionList(ctx.expression)
            if (expressions.forall(_.foldable) && expressions.forall(_.dataType == StringType)) {
              // If there are many pattern expressions, will throw StackOverflowError.
              // So we use LikeAny or NotLikeAny instead.
              val patterns = expressions.map(_.eval(EmptyRow).asInstanceOf[UTF8String])
              val (expr, pat) = lowerLikeArgsIfNeeded(e, patterns)
              withNot match {
                case null => LikeAny(expr, pat)
                case _ => NotLikeAny(expr, pat)
              }
            } else {
              ctx.expression.asScala.map(expression)
                .map(p => invertIfNotDefined(getLike(e, p))).toSeq.reduceLeft(Or)
            }
          case Some(SqlBaseParser.ALL) =>
            validate(!ctx.expression.isEmpty, "Expected something between '(' and ')'.", ctx)
            val expressions = expressionList(ctx.expression)
            if (expressions.forall(_.foldable) && expressions.forall(_.dataType == StringType)) {
              // If there are many pattern expressions, will throw StackOverflowError.
              // So we use LikeAll or NotLikeAll instead.
              val patterns = expressions.map(_.eval(EmptyRow).asInstanceOf[UTF8String])
              val (expr, pat) = lowerLikeArgsIfNeeded(e, patterns)
              withNot match {
                case null => LikeAll(expr, pat)
                case _ => NotLikeAll(expr, pat)
              }
            } else {
              ctx.expression.asScala.map(expression)
                .map(p => invertIfNotDefined(getLike(e, p))).toSeq.reduceLeft(And)
            }
          case _ =>
            val escapeChar = Option(ctx.escapeChar)
              .map(stringLitCtx => string(visitStringLit(stringLitCtx))).map { str =>
              if (str.length != 1) {
                throw QueryParsingErrors.invalidEscapeStringError(str, ctx)
              }
              str.charAt(0)
            }.getOrElse('\\')
            val likeExpr = ctx.kind.getType match {
              case SqlBaseParser.ILIKE => ILike(e, expression(ctx.pattern), escapeChar)
              case _ => Like(e, expression(ctx.pattern), escapeChar)
            }
            invertIfNotDefined(likeExpr)
        }
      case SqlBaseParser.RLIKE =>
        invertIfNotDefined(RLike(e, expression(ctx.pattern)))
      case SqlBaseParser.NULL if withNot != null =>
        IsNotNull(e)
      case SqlBaseParser.NULL =>
        IsNull(e)
      case SqlBaseParser.TRUE => withNot match {
        case null => EqualNullSafe(e, Literal(true))
        case _ => Not(EqualNullSafe(e, Literal(true)))
      }
      case SqlBaseParser.FALSE => withNot match {
        case null => EqualNullSafe(e, Literal(false))
        case _ => Not(EqualNullSafe(e, Literal(false)))
      }
      case SqlBaseParser.UNKNOWN => withNot match {
        case null => IsUnknown(e)
        case _ => IsNotUnknown(e)
      }
      case SqlBaseParser.DISTINCT if withNot != null =>
        EqualNullSafe(e, expression(ctx.right))
      case SqlBaseParser.DISTINCT =>
        Not(EqualNullSafe(e, expression(ctx.right)))
    }
  }

  /**
   * Create a binary arithmetic expression. The following arithmetic operators are supported:
   * - Multiplication: '*'
   * - Division: '/'
   * - Hive Long Division: 'DIV'
   * - Modulo: '%'
   * - Addition: '+'
   * - Subtraction: '-'
   * - Binary AND: '&'
   * - Binary XOR
   * - Binary OR: '|'
   */
  override def visitArithmeticBinary(ctx: ArithmeticBinaryContext): Expression = withOrigin(ctx) {
    val left = expression(ctx.left)
    val right = expression(ctx.right)
    ctx.operator.getType match {
      case SqlBaseParser.ASTERISK =>
        Multiply(left, right)
      case SqlBaseParser.SLASH =>
        Divide(left, right)
      case SqlBaseParser.PERCENT =>
        Remainder(left, right)
      case SqlBaseParser.DIV =>
        IntegralDivide(left, right)
      case SqlBaseParser.PLUS =>
        Add(left, right)
      case SqlBaseParser.MINUS =>
        Subtract(left, right)
      case SqlBaseParser.CONCAT_PIPE =>
        Concat(left :: right :: Nil)
      case SqlBaseParser.AMPERSAND =>
        BitwiseAnd(left, right)
      case SqlBaseParser.HAT =>
        BitwiseXor(left, right)
      case SqlBaseParser.PIPE =>
        BitwiseOr(left, right)
    }
  }

  override def visitShiftExpression(ctx: ShiftExpressionContext): Expression = withOrigin(ctx) {
    val left = expression(ctx.left)
    val right = expression(ctx.right)
    val operator = ctx.shiftOperator().getChild(0).asInstanceOf[TerminalNode]
    val shift = operator.getSymbol.getType match {
      case SqlBaseParser.SHIFT_LEFT => ShiftLeft(left, right)
      case SqlBaseParser.SHIFT_RIGHT => ShiftRight(left, right)
      case SqlBaseParser.SHIFT_RIGHT_UNSIGNED => ShiftRightUnsigned(left, right)
    }
    shift.setTagValue(FUNC_ALIAS, operator.getText)
    shift
  }

  /**
   * Create a unary arithmetic expression. The following arithmetic operators are supported:
   * - Plus: '+'
   * - Minus: '-'
   * - Bitwise Not: '~'
   */
  override def visitArithmeticUnary(ctx: ArithmeticUnaryContext): Expression = withOrigin(ctx) {
    val value = expression(ctx.valueExpression)
    ctx.operator.getType match {
      case SqlBaseParser.PLUS =>
        UnaryPositive(value)
      case SqlBaseParser.MINUS =>
        UnaryMinus(value)
      case SqlBaseParser.TILDE =>
        BitwiseNot(value)
    }
  }

  override def visitCurrentLike(ctx: CurrentLikeContext): Expression = withOrigin(ctx) {
    if (conf.enforceReservedKeywords) {
      ctx.name.getType match {
        case SqlBaseParser.CURRENT_DATE =>
          CurrentDate()
        case SqlBaseParser.CURRENT_TIMESTAMP =>
          CurrentTimestamp()
        case SqlBaseParser.CURRENT_USER | SqlBaseParser.USER | SqlBaseParser.SESSION_USER =>
          CurrentUser()
      }
    } else {
      // If the parser is not in ansi mode, we should return `UnresolvedAttribute`, in case there
      // are columns named `CURRENT_DATE` or `CURRENT_TIMESTAMP`.
      UnresolvedAttribute.quoted(ctx.name.getText)
    }
  }

  /**
   * Create a [[Collate]] expression.
   */
  override def visitCollate(ctx: CollateContext): Expression = withOrigin(ctx) {
    val collationName = visitCollateClause(ctx.collateClause())
    Collate(expression(ctx.primaryExpression), collationName)
  }

  override def visitCollateClause(ctx: CollateClauseContext): String = withOrigin(ctx) {
    ctx.identifier.getText
  }

  /**
   * Create a [[Cast]] expression.
   */
  override def visitCast(ctx: CastContext): Expression = withOrigin(ctx) {
    val rawDataType = typedVisit[DataType](ctx.dataType())
    ctx.dataType() match {
      case context: PrimitiveDataTypeContext =>
        val typeCtx = context.`type`()
        if (typeCtx.start.getType == STRING) {
          typeCtx.children.asScala.toSeq match {
            case Seq(_, cctx: CollateClauseContext) =>
              throw QueryParsingErrors.dataTypeUnsupportedError(
                rawDataType.typeName,
                ctx.dataType().asInstanceOf[PrimitiveDataTypeContext])
            case _ =>
          }
        }
      case _ =>
    }
    val dataType = CharVarcharUtils.replaceCharVarcharWithStringForCast(rawDataType)
    ctx.name.getType match {
      case SqlBaseParser.CAST =>
        val cast = Cast(expression(ctx.expression), dataType)
        cast.setTagValue(Cast.USER_SPECIFIED_CAST, ())
        cast

      case SqlBaseParser.TRY_CAST =>
        val cast = Cast(expression(ctx.expression), dataType, evalMode = EvalMode.TRY)
        cast.setTagValue(Cast.USER_SPECIFIED_CAST, ())
        cast
    }
  }

  /**
   * Create a [[Cast]] expression for '::' syntax.
   */
  override def visitCastByColon(ctx: CastByColonContext): Expression = withOrigin(ctx) {
    val rawDataType = typedVisit[DataType](ctx.dataType())
    ctx.dataType() match {
      case context: PrimitiveDataTypeContext =>
        val typeCtx = context.`type`()
        if (typeCtx.start.getType == STRING) {
          typeCtx.children.asScala.toSeq match {
            case Seq(_, cctx: CollateClauseContext) =>
              throw QueryParsingErrors.dataTypeUnsupportedError(
                rawDataType.typeName,
                ctx.dataType().asInstanceOf[PrimitiveDataTypeContext])
            case _ =>
          }
        }
      case _ =>
    }
    val dataType = CharVarcharUtils.replaceCharVarcharWithStringForCast(rawDataType)
    val cast = Cast(expression(ctx.primaryExpression), dataType)
    cast.setTagValue(Cast.USER_SPECIFIED_CAST, ())
    cast
  }

  /**
   * Create a [[CreateStruct]] expression.
   */
  override def visitStruct(ctx: StructContext): Expression = withOrigin(ctx) {
    CreateStruct.create(ctx.argument.asScala.map(expression).toSeq)
  }

  /**
   * Create a [[First]] expression.
   */
  override def visitFirst(ctx: FirstContext): Expression = withOrigin(ctx) {
    val ignoreNullsExpr = ctx.IGNORE != null
    First(expression(ctx.expression), ignoreNullsExpr).toAggregateExpression()
  }

  /**
   * Create an [[AnyValue]] expression.
   */
  override def visitAny_value(ctx: Any_valueContext): Expression = withOrigin(ctx) {
    val ignoreNullsExpr = ctx.IGNORE != null
    AnyValue(expression(ctx.expression), ignoreNullsExpr).toAggregateExpression()
  }

  /**
   * Create a [[Last]] expression.
   */
  override def visitLast(ctx: LastContext): Expression = withOrigin(ctx) {
    val ignoreNullsExpr = ctx.IGNORE != null
    Last(expression(ctx.expression), ignoreNullsExpr).toAggregateExpression()
  }

  /**
   * Create a Position expression.
   */
  override def visitPosition(ctx: PositionContext): Expression = withOrigin(ctx) {
    new StringLocate(expression(ctx.substr), expression(ctx.str))
  }

  /**
   * Create a Extract expression.
   */
  override def visitExtract(ctx: ExtractContext): Expression = withOrigin(ctx) {
    val arguments = Seq(Literal(ctx.field.getText), expression(ctx.source))
    UnresolvedFunction("extract", arguments, isDistinct = false)
  }

  /**
   * Create a Substring/Substr expression.
   */
  override def visitSubstring(ctx: SubstringContext): Expression = withOrigin(ctx) {
    if (ctx.len != null) {
      Substring(expression(ctx.str), expression(ctx.pos), expression(ctx.len))
    } else {
      new Substring(expression(ctx.str), expression(ctx.pos))
    }
  }

  /**
   * Create a Trim expression.
   */
  override def visitTrim(ctx: TrimContext): Expression = withOrigin(ctx) {
    val srcStr = expression(ctx.srcStr)
    val trimStr = Option(ctx.trimStr).map(expression)
    Option(ctx.trimOption).map(_.getType).getOrElse(SqlBaseParser.BOTH) match {
      case SqlBaseParser.BOTH =>
        StringTrim(srcStr, trimStr)
      case SqlBaseParser.LEADING =>
        StringTrimLeft(srcStr, trimStr)
      case SqlBaseParser.TRAILING =>
        StringTrimRight(srcStr, trimStr)
      case other =>
        throw QueryParsingErrors.trimOptionUnsupportedError(other, ctx)
    }
  }

  /**
   * Create a Overlay expression.
   */
  override def visitOverlay(ctx: OverlayContext): Expression = withOrigin(ctx) {
    val input = expression(ctx.input)
    val replace = expression(ctx.replace)
    val position = expression(ctx.position)
    val lengthOpt = Option(ctx.length).map(expression)
    lengthOpt match {
      case Some(length) => Overlay(input, replace, position, length)
      case None => new Overlay(input, replace, position)
    }
  }

  /**
   * Create a (windowed) Function expression.
   */
  override def visitFunctionCall(ctx: FunctionCallContext): Expression = withOrigin(ctx) {
    // Create the function call.
    val name = ctx.functionName.getText
    val isDistinct = Option(ctx.setQuantifier()).exists(_.DISTINCT != null)
    // Call `toSeq`, otherwise `ctx.argument.asScala.map(expression)` is `Buffer` in Scala 2.13
    val arguments = ctx.argument.asScala.map { e =>
      extractNamedArgument(e, name)
    }.toSeq match {
      case Seq(UnresolvedStar(None))
        if name.toLowerCase(Locale.ROOT) == "count" && !isDistinct =>
        // Transform COUNT(*) into COUNT(1).
        Seq(Literal(1))
      case expressions =>
        expressions
    }
    val order = ctx.sortItem.asScala.map(visitSortItem)
    val filter = Option(ctx.where).map(expression(_))
    val ignoreNulls =
      Option(ctx.nullsOption).map(_.getType == SqlBaseParser.IGNORE).getOrElse(false)

    // Is this an IDENTIFIER clause instead of a function call?
    if (ctx.functionName.identFunc != null &&
      arguments.length == 1 && // One argument
      ctx.setQuantifier == null && // No other clause
      ctx.where == null &&
      ctx.nullsOption == null &&
      ctx.windowSpec == null) {
      new ExpressionWithUnresolvedIdentifier(arguments.head, UnresolvedAttribute(_))
    } else {
      // It's a function call
      val funcCtx = ctx.functionName
      val func: Expression = withFuncIdentClause(
        funcCtx,
        arguments ++ filter ++ order.toSeq,
        (ident, otherExprs) => {
          val orderings = otherExprs.takeRight(order.size).asInstanceOf[Seq[SortOrder]]
          val args = otherExprs.take(arguments.length)
          val filterExpr = if (filter.isDefined) {
            Some(otherExprs(args.length))
          } else {
            None
          }
          UnresolvedFunction(ident, args, isDistinct, filterExpr, ignoreNulls, orderings)
        }
      )

      // Check if the function is evaluated in a windowed context.
      ctx.windowSpec match {
        case spec: WindowRefContext =>
          UnresolvedWindowExpression(func, visitWindowRef(spec))
        case spec: WindowDefContext =>
          WindowExpression(func, visitWindowDef(spec))
        case _ => func
      }
    }
  }

  /**
   * Create a function database (optional) and name pair.
   */
  protected def visitFunctionName(ctx: QualifiedNameContext): FunctionIdentifier = {
    visitFunctionName(ctx, ctx.identifier().asScala.map(_.getText).toSeq)
  }

  /**
   * Create a function database (optional) and name pair.
   */
  private def visitFunctionName(ctx: ParserRuleContext, texts: Seq[String]): FunctionIdentifier = {
    texts match {
      case Seq(db, fn) => FunctionIdentifier(fn, Option(db))
      case Seq(fn) => FunctionIdentifier(fn, None)
      case other =>
        throw QueryParsingErrors.functionNameUnsupportedError(texts.mkString("."), ctx)
    }
  }

  protected def getFunctionMultiparts(ctx: FunctionNameContext): Seq[String] = {
    if (ctx.qualifiedName != null) {
      ctx.qualifiedName().identifier().asScala.map(_.getText).toSeq
    } else {
      Seq(ctx.getText)
    }
  }

  /**
   * Create an [[LambdaFunction]].
   */
  override def visitLambda(ctx: LambdaContext): Expression = withOrigin(ctx) {
    val arguments = ctx.identifier().asScala.map { name =>
      UnresolvedNamedLambdaVariable(UnresolvedAttribute.quoted(name.getText).nameParts)
    }
    val function = expression(ctx.expression).transformUp {
      case a: UnresolvedAttribute => UnresolvedNamedLambdaVariable(a.nameParts)
    }
    LambdaFunction(function, arguments.toSeq)
  }

  /**
   * Create a reference to a window frame, i.e. [[WindowSpecReference]].
   */
  override def visitWindowRef(ctx: WindowRefContext): WindowSpecReference = withOrigin(ctx) {
    WindowSpecReference(ctx.name.getText)
  }

  /**
   * Create a window definition, i.e. [[WindowSpecDefinition]].
   */
  override def visitWindowDef(ctx: WindowDefContext): WindowSpecDefinition = withOrigin(ctx) {
    // CLUSTER BY ... | PARTITION BY ... ORDER BY ...
    val partition = ctx.partition.asScala.map(expression)
    val order = ctx.sortItem.asScala.map(visitSortItem)

    // RANGE/ROWS BETWEEN ...
    val frameSpecOption = Option(ctx.windowFrame).map { frame =>
      val frameType = frame.frameType.getType match {
        case SqlBaseParser.RANGE => RangeFrame
        case SqlBaseParser.ROWS => RowFrame
      }

      SpecifiedWindowFrame(
        frameType,
        visitFrameBound(frame.start),
        Option(frame.end).map(visitFrameBound).getOrElse(CurrentRow))
    }

    WindowSpecDefinition(
      partition.toSeq,
      order.toSeq,
      frameSpecOption.getOrElse(UnspecifiedFrame))
  }

  /**
   * Create or resolve a frame boundary expressions.
   */
  override def visitFrameBound(ctx: FrameBoundContext): Expression = withOrigin(ctx) {
    def value: Expression = {
      val e = expression(ctx.expression)
      validate(e.resolved && e.foldable, "Frame bound value must be a literal.", ctx)
      e
    }

    ctx.boundType.getType match {
      case SqlBaseParser.PRECEDING if ctx.UNBOUNDED != null =>
        UnboundedPreceding
      case SqlBaseParser.PRECEDING =>
        UnaryMinus(value)
      case SqlBaseParser.CURRENT =>
        CurrentRow
      case SqlBaseParser.FOLLOWING if ctx.UNBOUNDED != null =>
        UnboundedFollowing
      case SqlBaseParser.FOLLOWING =>
        value
    }
  }

  /**
   * Create a [[CreateStruct]] expression.
   */
  override def visitRowConstructor(ctx: RowConstructorContext): Expression = withOrigin(ctx) {
    CreateStruct(ctx.namedExpression().asScala.map(expression).toSeq)
  }

  /**
   * Create a [[ScalarSubquery]] expression.
   */
  override def visitSubqueryExpression(
      ctx: SubqueryExpressionContext): Expression = withOrigin(ctx) {
    ScalarSubquery(plan(ctx.query))
  }

  /**
   * Create a value based [[CaseWhen]] expression. This has the following SQL form:
   * {{{
   *   CASE [expression]
   *    WHEN [value] THEN [expression]
   *    ...
   *    ELSE [expression]
   *   END
   * }}}
   */
  override def visitSimpleCase(ctx: SimpleCaseContext): Expression = withOrigin(ctx) {
    val e = expression(ctx.value)
    val branches = ctx.whenClause.asScala.map { wCtx =>
      (EqualTo(e, expression(wCtx.condition)), expression(wCtx.result))
    }
    CaseWhen(branches.toSeq, Option(ctx.elseExpression).map(expression))
  }

  /**
   * Create a condition based [[CaseWhen]] expression. This has the following SQL syntax:
   * {{{
   *   CASE
   *    WHEN [predicate] THEN [expression]
   *    ...
   *    ELSE [expression]
   *   END
   * }}}
   *
   * @param ctx the parse tree
   *    */
  override def visitSearchedCase(ctx: SearchedCaseContext): Expression = withOrigin(ctx) {
    val branches = ctx.whenClause.asScala.map { wCtx =>
      (expression(wCtx.condition), expression(wCtx.result))
    }
    CaseWhen(branches.toSeq, Option(ctx.elseExpression).map(expression))
  }

  /**
   * Currently only regex in expressions of SELECT statements are supported; in other
   * places, e.g., where `(a)?+.+` = 2, regex are not meaningful.
   */
  private def canApplyRegex(ctx: ParserRuleContext): Boolean = withOrigin(ctx) {
    var parent = ctx.getParent
    while (parent != null) {
      if (parent.isInstanceOf[NamedExpressionContext]) return true
      parent = parent.getParent
    }
    return false
  }

  /**
   * Returns whether the pattern is a regex expression (instead of a normal
   * string). Normal string is a string with all alphabets/digits and "_".
   */
  private def isRegex(pattern: String): Boolean = {
    pattern.exists(p => !Character.isLetterOrDigit(p) && p != '_')
  }

  /**
   * Create a dereference expression. The return type depends on the type of the parent.
   * If the parent is an [[UnresolvedAttribute]], it can be a [[UnresolvedAttribute]] or
   * a [[UnresolvedRegex]] for regex quoted in ``; if the parent is some other expression,
   * it can be [[UnresolvedExtractValue]].
   */
  override def visitDereference(ctx: DereferenceContext): Expression = withOrigin(ctx) {
    val attr = ctx.fieldName.getText
    expression(ctx.base) match {
      case unresolved_attr @ UnresolvedAttribute(nameParts) =>
        ctx.fieldName.getStart.getText match {
          case escapedIdentifier(columnNameRegex)
            if conf.supportQuotedRegexColumnName &&
              isRegex(columnNameRegex) && canApplyRegex(ctx) =>
            UnresolvedRegex(columnNameRegex, Some(unresolved_attr.name),
              conf.caseSensitiveAnalysis)
          case _ =>
            UnresolvedAttribute(nameParts :+ attr)
        }
      case e =>
        UnresolvedExtractValue(e, Literal(attr))
    }
  }

  /**
   * Create an [[UnresolvedAttribute]] expression or a [[UnresolvedRegex]] if it is a regex
   * quoted in ``
   */
  override def visitColumnReference(ctx: ColumnReferenceContext): Expression = withOrigin(ctx) {
    ctx.getStart.getText match {
      case escapedIdentifier(columnNameRegex)
        if conf.supportQuotedRegexColumnName &&
          isRegex(columnNameRegex) && canApplyRegex(ctx) =>
        UnresolvedRegex(columnNameRegex, None, conf.caseSensitiveAnalysis)
      case _ =>
        UnresolvedAttribute.quoted(ctx.getText)
    }

  }

  /**
   * Create an [[UnresolvedExtractValue]] expression, this is used for subscript access to an array.
   */
  override def visitSubscript(ctx: SubscriptContext): Expression = withOrigin(ctx) {
    UnresolvedExtractValue(expression(ctx.value), expression(ctx.index))
  }

  /**
   * Create an expression for an expression between parentheses. This is need because the ANTLR
   * visitor cannot automatically convert the nested context into an expression.
   */
  override def visitParenthesizedExpression(
     ctx: ParenthesizedExpressionContext): Expression = withOrigin(ctx) {
    expression(ctx.expression)
  }

  /**
   * Create a [[SortOrder]] expression.
   */
  override def visitSortItem(ctx: SortItemContext): SortOrder = withOrigin(ctx) {
    val direction = if (ctx.DESC != null) {
      Descending
    } else {
      Ascending
    }
    val nullOrdering = if (ctx.FIRST != null) {
      NullsFirst
    } else if (ctx.LAST != null) {
      NullsLast
    } else {
      direction.defaultNullOrdering
    }
    SortOrder(expression(ctx.expression), direction, nullOrdering, Seq.empty)
  }

  /**
   * Create a typed Literal expression. A typed literal has the following SQL syntax:
   * {{{
   *   [TYPE] '[VALUE]'
   * }}}
   * Currently Date, Timestamp, Interval and Binary typed literals are supported.
   */
  override def visitTypeConstructor(ctx: TypeConstructorContext): Literal = withOrigin(ctx) {
    val value = string(visitStringLit(ctx.stringLit))
    val valueType = ctx.literalType.start.getType

    def toLiteral[T](f: UTF8String => Option[T], t: DataType): Literal = {
      f(UTF8String.fromString(value)).map(Literal(_, t)).getOrElse {
        throw QueryParsingErrors.cannotParseValueTypeError(ctx.literalType.getText, value, ctx)
      }
    }

    def constructTimestampLTZLiteral(value: String): Literal = {
      val zoneId = getZoneId(conf.sessionLocalTimeZone)
      val specialTs = convertSpecialTimestamp(value, zoneId).map(Literal(_, TimestampType))
      specialTs.getOrElse(toLiteral(stringToTimestamp(_, zoneId), TimestampType))
    }

    valueType match {
      case DATE =>
        val zoneId = getZoneId(conf.sessionLocalTimeZone)
        val specialDate = convertSpecialDate(value, zoneId).map(Literal(_, DateType))
        specialDate.getOrElse(toLiteral(stringToDate, DateType))
      case TIMESTAMP_NTZ =>
        convertSpecialTimestampNTZ(value, getZoneId(conf.sessionLocalTimeZone))
          .map(Literal(_, TimestampNTZType))
          .getOrElse(toLiteral(stringToTimestampWithoutTimeZone, TimestampNTZType))
      case TIMESTAMP_LTZ =>
        constructTimestampLTZLiteral(value)
      case TIMESTAMP =>
        SQLConf.get.timestampType match {
          case TimestampNTZType =>
            convertSpecialTimestampNTZ(value, getZoneId(conf.sessionLocalTimeZone))
              .map(Literal(_, TimestampNTZType))
              .getOrElse {
                val containsTimeZonePart =
                  DateTimeUtils.parseTimestampString(UTF8String.fromString(value))._2.isDefined
                // If the input string contains time zone part, return a timestamp with local time
                // zone literal.
                if (containsTimeZonePart) {
                  constructTimestampLTZLiteral(value)
                } else {
                  toLiteral(stringToTimestampWithoutTimeZone, TimestampNTZType)
                }
              }

          case TimestampType =>
            constructTimestampLTZLiteral(value)
        }

      case INTERVAL =>
        val interval = try {
          IntervalUtils.stringToInterval(UTF8String.fromString(value))
        } catch {
          case e: IllegalArgumentException =>
            val ex = QueryParsingErrors.cannotParseValueTypeError(
              ctx.literalType.getText, value, ctx)
            ex.setStackTrace(e.getStackTrace)
            throw ex
        }
        if (!conf.legacyIntervalEnabled) {
          val units = value
            .split("\\s")
            .map(_.toLowerCase(Locale.ROOT).stripSuffix("s"))
            .filter(s => s != "interval" && s.matches("[a-z]+"))
          constructMultiUnitsIntervalLiteral(ctx, interval, units.toImmutableArraySeq)
        } else {
          Literal(interval, CalendarIntervalType)
        }
      case BINARY_HEX =>
        try {
          Literal(Hex.unhex(value), BinaryType)
        } catch {
          case e: IllegalArgumentException =>
            val ex = QueryParsingErrors.cannotParseValueTypeError("X", value, ctx)
            ex.setStackTrace(e.getStackTrace)
            throw ex
        }
      case _ =>
        throw QueryParsingErrors.literalValueTypeUnsupportedError(
          unsupportedType = ctx.literalType.getText,
          supportedTypes =
            Seq("DATE", "TIMESTAMP_NTZ", "TIMESTAMP_LTZ", "TIMESTAMP", "INTERVAL", "X"),
          ctx)
    }
  }

  /**
   * Create a NULL literal expression.
   */
  override def visitNullLiteral(ctx: NullLiteralContext): Literal = withOrigin(ctx) {
    Literal(null)
  }

  /**
   * Create a Boolean literal expression.
   */
  override def visitBooleanLiteral(ctx: BooleanLiteralContext): Literal = withOrigin(ctx) {
    if (ctx.getText.toBoolean) {
      Literal.TrueLiteral
    } else {
      Literal.FalseLiteral
    }
  }

  /**
   * Create an integral literal expression. The code selects the most narrow integral type
   * possible, either a BigDecimal, a Long or an Integer is returned.
   */
  override def visitIntegerLiteral(ctx: IntegerLiteralContext): Literal = withOrigin(ctx) {
    BigDecimal(ctx.getText) match {
      case v if v.isValidInt =>
        Literal(v.intValue)
      case v if v.isValidLong =>
        Literal(v.longValue)
      case v => Literal(v.underlying())
    }
  }

  /**
   * Create a decimal literal for a regular decimal number.
   */
  override def visitDecimalLiteral(ctx: DecimalLiteralContext): Literal = withOrigin(ctx) {
    Literal(BigDecimal(ctx.getText).underlying())
  }

  /**
   * Create a decimal literal for a regular decimal number or a scientific decimal number.
   */
  override def visitLegacyDecimalLiteral(
      ctx: LegacyDecimalLiteralContext): Literal = withOrigin(ctx) {
    Literal(BigDecimal(ctx.getText).underlying())
  }

  /**
   * Create a double literal for number with an exponent, e.g. 1E-30
   */
  override def visitExponentLiteral(ctx: ExponentLiteralContext): Literal = {
    numericLiteral(ctx, ctx.getText, /* exponent values don't have a suffix */
      Double.MinValue, Double.MaxValue, DoubleType.simpleString)(_.toDouble)
  }

  /** Create a numeric literal expression. */
  private def numericLiteral(
      ctx: NumberContext,
      rawStrippedQualifier: String,
      minValue: BigDecimal,
      maxValue: BigDecimal,
      typeName: String)(converter: String => Any): Literal = withOrigin(ctx) {
    try {
      val rawBigDecimal = BigDecimal(rawStrippedQualifier)
      if (rawBigDecimal < minValue || rawBigDecimal > maxValue) {
        throw QueryParsingErrors.invalidNumericLiteralRangeError(
          rawStrippedQualifier, minValue, maxValue, typeName, ctx)
      }
      Literal(converter(rawStrippedQualifier))
    } catch {
      case e: NumberFormatException =>
        throw new ParseException(
          errorClass = "_LEGACY_ERROR_TEMP_0060",
          messageParameters = Map("msg" -> e.getMessage),
          ctx)
    }
  }

  /**
   * Create a Byte Literal expression.
   */
  override def visitTinyIntLiteral(ctx: TinyIntLiteralContext): Literal = {
    val rawStrippedQualifier = ctx.getText.substring(0, ctx.getText.length - 1)
    numericLiteral(ctx, rawStrippedQualifier,
      Byte.MinValue, Byte.MaxValue, ByteType.simpleString)(_.toByte)
  }

  /**
   * Create a Short Literal expression.
   */
  override def visitSmallIntLiteral(ctx: SmallIntLiteralContext): Literal = {
    val rawStrippedQualifier = ctx.getText.substring(0, ctx.getText.length - 1)
    numericLiteral(ctx, rawStrippedQualifier,
      Short.MinValue, Short.MaxValue, ShortType.simpleString)(_.toShort)
  }

  /**
   * Create a Long Literal expression.
   */
  override def visitBigIntLiteral(ctx: BigIntLiteralContext): Literal = {
    val rawStrippedQualifier = ctx.getText.substring(0, ctx.getText.length - 1)
    numericLiteral(ctx, rawStrippedQualifier,
      Long.MinValue, Long.MaxValue, LongType.simpleString)(_.toLong)
  }

  /**
   * Create a Float Literal expression.
   */
  override def visitFloatLiteral(ctx: FloatLiteralContext): Literal = {
    val rawStrippedQualifier = ctx.getText.substring(0, ctx.getText.length - 1)
    numericLiteral(ctx, rawStrippedQualifier,
      Float.MinValue, Float.MaxValue, FloatType.simpleString)(_.toFloat)
  }

  /**
   * Create a Double Literal expression.
   */
  override def visitDoubleLiteral(ctx: DoubleLiteralContext): Literal = {
    val rawStrippedQualifier = ctx.getText.substring(0, ctx.getText.length - 1)
    numericLiteral(ctx, rawStrippedQualifier,
      Double.MinValue, Double.MaxValue, DoubleType.simpleString)(_.toDouble)
  }

  /**
   * Create a BigDecimal Literal expression.
   */
  override def visitBigDecimalLiteral(ctx: BigDecimalLiteralContext): Literal = {
    val raw = ctx.getText.substring(0, ctx.getText.length - 2)
    try {
      Literal(BigDecimal(raw).underlying())
    } catch {
      case e: SparkArithmeticException =>
        throw new ParseException(
          errorClass = e.getErrorClass,
          messageParameters = e.getMessageParameters.asScala.toMap,
          ctx)
    }
  }

  /**
   * Create a String literal expression.
   */
  override def visitStringLiteral(ctx: StringLiteralContext): Literal = withOrigin(ctx) {
    Literal.create(createString(ctx), conf.defaultStringType)
  }

  /**
   * Create a String from a string literal context. This supports multiple consecutive string
   * literals, these are concatenated, for example this expression "'hello' 'world'" will be
   * converted into "helloworld".
   *
   * Special characters can be escaped by using Hive/C-style escaping.
   */
  private def createString(ctx: StringLiteralContext): String = {
    if (conf.escapedStringLiterals) {
      ctx.stringLit.asScala.map(x => stringWithoutUnescape(visitStringLit(x))).mkString
    } else {
      ctx.stringLit.asScala.map(x => string(visitStringLit(x))).mkString
    }
  }

  /**
   * Create an [[UnresolvedRelation]] from an identifier reference and an options clause.
   */
  private def createUnresolvedRelation(
      ctx: IdentifierReferenceContext,
      optionsClause: Option[OptionsClauseContext] = None,
      writePrivileges: Seq[TableWritePrivilege] = Nil): LogicalPlan = withOrigin(ctx) {
    val options = resolveOptions(optionsClause)
    withIdentClause(ctx, parts => {
      val relation = new UnresolvedRelation(parts, options, isStreaming = false)
      relation.requireWritePrivileges(writePrivileges)
    })
  }

  /**
   * Create an [[UnresolvedRelation]] from a multi-part identifier.
   */
  private def createUnresolvedRelation(
      ctx: ParserRuleContext,
      ident: Seq[String],
      optionsClause: Option[OptionsClauseContext],
      writePrivileges: Seq[TableWritePrivilege]): UnresolvedRelation = withOrigin(ctx) {
    val options = resolveOptions(optionsClause)
    val relation = new UnresolvedRelation(ident, options, isStreaming = false)
    relation.requireWritePrivileges(writePrivileges)
  }

  private def resolveOptions(
      optionsClause: Option[OptionsClauseContext]): CaseInsensitiveStringMap = {
    optionsClause.map{ clause =>
      new CaseInsensitiveStringMap(visitPropertyKeyValues(clause.options).asJava)
    }.getOrElse(CaseInsensitiveStringMap.empty)
  }

  /**
   * Create an [[UnresolvedTable]] from an identifier reference.
   */
  private def createUnresolvedTable(
      ctx: IdentifierReferenceContext,
      commandName: String,
      suggestAlternative: Boolean = false): LogicalPlan = withOrigin(ctx) {
    withIdentClause(ctx, UnresolvedTable(_, commandName, suggestAlternative))
  }

  /**
   * Create an [[UnresolvedView]] from a multi-part identifier.
   */
  private def createUnresolvedView(
      ctx: IdentifierReferenceContext,
      commandName: String,
      allowTemp: Boolean = true,
      suggestAlternative: Boolean = false): LogicalPlan = withOrigin(ctx) {
    withIdentClause(ctx, UnresolvedView(_, commandName, allowTemp, suggestAlternative))
  }

  /**
   * Create an [[UnresolvedTableOrView]] from a multi-part identifier.
   */
  private def createUnresolvedTableOrView(
      ctx: IdentifierReferenceContext,
      commandName: String,
      allowTempView: Boolean = true): LogicalPlan = withOrigin(ctx) {
    withIdentClause(ctx, UnresolvedTableOrView(_, commandName, allowTempView))
  }

  private def createUnresolvedTableOrView(
      ctx: ParserRuleContext,
      ident: Seq[String],
      commandName: String,
      allowTempView: Boolean): UnresolvedTableOrView = withOrigin(ctx) {
    UnresolvedTableOrView(ident, commandName, allowTempView)
  }

  /**
   * Create an [[UnresolvedFunction]] from a multi-part identifier.
   */
  private def createUnresolvedFunctionName(
      ctx: ParserRuleContext,
      ident: Seq[String],
      commandName: String,
      requirePersistent: Boolean = false,
      funcTypeMismatchHint: Option[String] = None,
      possibleQualifiedName: Option[Seq[String]] = None): UnresolvedFunctionName = withOrigin(ctx) {
    UnresolvedFunctionName(
      ident,
      commandName,
      requirePersistent,
      funcTypeMismatchHint,
      possibleQualifiedName)
  }

  /**
   * Construct an [[Literal]] from [[CalendarInterval]] and
   * units represented as a [[Seq]] of [[String]].
   */
  private def constructMultiUnitsIntervalLiteral(
      ctx: ParserRuleContext,
      calendarInterval: CalendarInterval,
      units: Seq[String]): Literal = {
    val yearMonthFields = Set.empty[Byte]
    val dayTimeFields = Set.empty[Byte]
    for (unit <- units) {
      if (YearMonthIntervalType.stringToField.contains(unit)) {
        yearMonthFields += YearMonthIntervalType.stringToField(unit)
      } else if (DayTimeIntervalType.stringToField.contains(unit)) {
        dayTimeFields += DayTimeIntervalType.stringToField(unit)
      } else if (unit == "week") {
        dayTimeFields += DayTimeIntervalType.DAY
      } else {
        assert(unit == "millisecond" || unit == "microsecond")
        dayTimeFields += DayTimeIntervalType.SECOND
      }
    }
    if (yearMonthFields.nonEmpty) {
      if (dayTimeFields.nonEmpty) {
        val literalStr = source(ctx)
        throw QueryParsingErrors.mixedIntervalUnitsError(literalStr, ctx)
      }
      Literal(
        calendarInterval.months,
        YearMonthIntervalType(yearMonthFields.min, yearMonthFields.max)
      )
    } else {
      Literal(
        IntervalUtils.getDuration(calendarInterval, TimeUnit.MICROSECONDS),
        DayTimeIntervalType(dayTimeFields.min, dayTimeFields.max))
    }
  }

  /**
   * Create a [[CalendarInterval]] or ANSI interval literal expression.
   * Two syntaxes are supported:
   * - multiple unit value pairs, for instance: interval 2 months 2 days.
   * - from-to unit, for instance: interval '1-2' year to month.
   */
  override def visitInterval(ctx: IntervalContext): Literal = withOrigin(ctx) {
    val calendarInterval = parseIntervalLiteral(ctx)
    if (ctx.errorCapturingUnitToUnitInterval != null && !conf.legacyIntervalEnabled) {
      // Check the `to` unit to distinguish year-month and day-time intervals because
      // `CalendarInterval` doesn't have enough info. For instance, new CalendarInterval(0, 0, 0)
      // can be derived from INTERVAL '0-0' YEAR TO MONTH as well as from
      // INTERVAL '0 00:00:00' DAY TO SECOND.
      val fromUnit =
        ctx.errorCapturingUnitToUnitInterval.body.from.getText.toLowerCase(Locale.ROOT)
      val toUnit = ctx.errorCapturingUnitToUnitInterval.body.to.getText.toLowerCase(Locale.ROOT)
      if (toUnit == "month") {
        assert(calendarInterval.days == 0 && calendarInterval.microseconds == 0)
        val start = YearMonthIntervalType.stringToField(fromUnit)
        Literal(calendarInterval.months, YearMonthIntervalType(start, YearMonthIntervalType.MONTH))
      } else {
        assert(calendarInterval.months == 0)
        val micros = IntervalUtils.getDuration(calendarInterval, TimeUnit.MICROSECONDS)
        val start = DayTimeIntervalType.stringToField(fromUnit)
        val end = DayTimeIntervalType.stringToField(toUnit)
        Literal(micros, DayTimeIntervalType(start, end))
      }
    } else if (ctx.errorCapturingMultiUnitsInterval != null && !conf.legacyIntervalEnabled) {
      val units =
        ctx.errorCapturingMultiUnitsInterval.body.unit.asScala.map(
          _.getText.toLowerCase(Locale.ROOT).stripSuffix("s")).toSeq
      constructMultiUnitsIntervalLiteral(ctx, calendarInterval, units)
    } else {
      Literal(calendarInterval, CalendarIntervalType)
    }
  }

  /**
   * Create a [[CalendarInterval]] object
   */
  protected def parseIntervalLiteral(ctx: IntervalContext): CalendarInterval = withOrigin(ctx) {
    if (ctx.errorCapturingMultiUnitsInterval != null) {
      val innerCtx = ctx.errorCapturingMultiUnitsInterval
      if (innerCtx.unitToUnitInterval != null) {
        throw QueryParsingErrors.moreThanOneFromToUnitInIntervalLiteralError(
          innerCtx.unitToUnitInterval)
      }
      visitMultiUnitsInterval(innerCtx.multiUnitsInterval)
    } else {
      assert(ctx.errorCapturingUnitToUnitInterval != null)
      val innerCtx = ctx.errorCapturingUnitToUnitInterval
      if (innerCtx.error1 != null || innerCtx.error2 != null) {
        val errorCtx = if (innerCtx.error1 != null) innerCtx.error1 else innerCtx.error2
        throw QueryParsingErrors.moreThanOneFromToUnitInIntervalLiteralError(errorCtx)
      }
      visitUnitToUnitInterval(innerCtx.body)
    }
  }

  /**
   * Creates a [[CalendarInterval]] with multiple unit value pairs, e.g. 1 YEAR 2 DAYS.
   */
  override def visitMultiUnitsInterval(ctx: MultiUnitsIntervalContext): CalendarInterval = {
    withOrigin(ctx) {
      val units = ctx.unit.asScala
      val values = ctx.intervalValue().asScala
      try {
        assert(units.length == values.length)
        val kvs = units.indices.map { i =>
          val u = units(i).getText
          val v = if (values(i).stringLit() != null) {
            val value = string(visitStringLit(values(i).stringLit()))
            // SPARK-32840: For invalid cases, e.g. INTERVAL '1 day 2' hour,
            // INTERVAL 'interval 1' day, we need to check ahead before they are concatenated with
            // units and become valid ones, e.g. '1 day 2 hour'.
            // Ideally, we only ensure the value parts don't contain any units here.
            if (value.exists(Character.isLetter)) {
              throw QueryParsingErrors.invalidIntervalFormError(value, ctx)
            }
            if (values(i).MINUS() == null) {
              value
            } else if (value.startsWith("-")) {
              value.replaceFirst("-", "")
            } else {
              s"-$value"
            }
          } else {
            values(i).getText
          }
          UTF8String.fromString(" " + v + " " + u)
        }
        IntervalUtils.stringToInterval(UTF8String.concat(kvs: _*))
      } catch {
        case st: SparkThrowable => throw st
        case i: IllegalArgumentException =>
          val e = new ParseException(
            errorClass = "_LEGACY_ERROR_TEMP_0062",
            messageParameters = Map("msg" -> i.getMessage),
            ctx)
          e.setStackTrace(i.getStackTrace)
          throw e
      }
    }
  }

  /**
   * Creates a [[CalendarInterval]] with from-to unit, e.g. '2-1' YEAR TO MONTH.
   */
  override def visitUnitToUnitInterval(ctx: UnitToUnitIntervalContext): CalendarInterval = {
    withOrigin(ctx) {
      val value = Option(ctx.intervalValue().stringLit()).map(s => string(visitStringLit(s)))
        .map { interval =>
        if (ctx.intervalValue().MINUS() == null) {
          interval
        } else if (interval.startsWith("-")) {
          interval.replaceFirst("-", "")
        } else {
          s"-$interval"
        }
      }.getOrElse {
        throw QueryParsingErrors.invalidFromToUnitValueError(ctx.intervalValue)
      }
      try {
        val from = ctx.from.getText.toLowerCase(Locale.ROOT)
        val to = ctx.to.getText.toLowerCase(Locale.ROOT)
        (from, to) match {
          case ("year", "month") =>
            IntervalUtils.fromYearMonthString(value)
          case ("day", "hour") | ("day", "minute") | ("day", "second") | ("hour", "minute") |
               ("hour", "second") | ("minute", "second") =>
            IntervalUtils.fromDayTimeString(value,
              DayTimeIntervalType.stringToField(from), DayTimeIntervalType.stringToField(to))
          case _ =>
            throw QueryParsingErrors.fromToIntervalUnsupportedError(from, to, ctx)
        }
      } catch {
        // Keep error class of SparkIllegalArgumentExceptions and enrich it with query context
        case se: SparkIllegalArgumentException =>
          val pe = new ParseException(
            errorClass = se.getErrorClass,
            messageParameters = se.getMessageParameters.asScala.toMap,
            ctx)
          pe.setStackTrace(se.getStackTrace)
          throw pe
        // Handle Exceptions thrown by CalendarInterval
        case e: IllegalArgumentException =>
          val pe = new ParseException(
            errorClass = "_LEGACY_ERROR_TEMP_0063",
            messageParameters = Map("msg" -> e.getMessage),
            ctx)
          pe.setStackTrace(e.getStackTrace)
          throw pe
      }
    }
  }

  /* ********************************************************************************************
   * DataType parsing
   * ******************************************************************************************** */

  /**
   * Create top level table schema.
   */
  protected def createSchema(ctx: ColDefinitionListContext): StructType = {
    val columns = Option(ctx).toArray.flatMap(visitColDefinitionList)
    StructType(columns.map(_.toV1Column))
  }

  /**
   * Get CREATE TABLE column definitions.
   */
  override def visitColDefinitionList(
      ctx: ColDefinitionListContext): Seq[ColumnDefinition] = withOrigin(ctx) {
    ctx.colDefinition().asScala.map(visitColDefinition).toSeq
  }

  /**
   * Get a CREATE TABLE column definition.
   */
  override def visitColDefinition(
      ctx: ColDefinitionContext): ColumnDefinition = withOrigin(ctx) {
    import ctx._

    val name: String = colName.getText
    // Check that no duplicates exist among any CREATE TABLE column options specified.
    var nullable = true
    var defaultExpression: Option[DefaultExpressionContext] = None
    var generationExpression: Option[GenerationExpressionContext] = None
    var commentSpec: Option[CommentSpecContext] = None
    ctx.colDefinitionOption().asScala.foreach { option =>
      if (option.NULL != null) {
        blockBang(option.errorCapturingNot)
        if (!nullable) {
          throw QueryParsingErrors.duplicateTableColumnDescriptor(
            option, name, "NOT NULL")
        }
        nullable = false
      }
      Option(option.defaultExpression()).foreach { expr =>
        if (!conf.getConf(SQLConf.ENABLE_DEFAULT_COLUMNS)) {
          throw QueryParsingErrors.defaultColumnNotEnabledError(ctx)
        }
        if (defaultExpression.isDefined) {
          throw QueryParsingErrors.duplicateTableColumnDescriptor(
            option, name, "DEFAULT")
        }
        defaultExpression = Some(expr)
      }
      Option(option.generationExpression()).foreach { expr =>
        if (generationExpression.isDefined) {
          throw QueryParsingErrors.duplicateTableColumnDescriptor(
            option, name, "GENERATED ALWAYS AS")
        }
        generationExpression = Some(expr)
      }
      Option(option.commentSpec()).foreach { spec =>
        if (commentSpec.isDefined) {
          throw QueryParsingErrors.duplicateTableColumnDescriptor(
            option, name, "COMMENT")
        }
        commentSpec = Some(spec)
      }
    }

    ColumnDefinition(
      name = name,
      dataType = typedVisit[DataType](ctx.dataType),
      nullable = nullable,
      comment = commentSpec.map(visitCommentSpec),
      defaultValue = defaultExpression.map(visitDefaultExpression),
      generationExpression = generationExpression.map(visitGenerationExpression)
    )
  }

  /**
   * Create a location string.
   */
  override def visitLocationSpec(ctx: LocationSpecContext): String = withOrigin(ctx) {
    string(visitStringLit(ctx.stringLit))
  }

  /**
   * Create an optional location string.
   */
  protected def visitLocationSpecList(ctx: java.util.List[LocationSpecContext]): Option[String] = {
    ctx.asScala.headOption.map(visitLocationSpec)
  }

  private def getDefaultExpression(
      exprCtx: ExpressionContext,
      place: String): DefaultValueExpression = {
    // Make sure it can be converted to Catalyst expressions.
    val expr = expression(exprCtx)
    if (expr.containsPattern(PARAMETER)) {
      throw QueryParsingErrors.parameterMarkerNotAllowed(place, expr.origin)
    }
    // Extract the raw expression text so that we can save the user provided text. We don't
    // use `Expression.sql` to avoid storing incorrect text caused by bugs in any expression's
    // `sql` method. Note: `exprCtx.getText` returns a string without spaces, so we need to
    // get the text from the underlying char stream instead.
    val start = exprCtx.getStart.getStartIndex
    val end = exprCtx.getStop.getStopIndex
    val originalSQL = exprCtx.getStart.getInputStream.getText(new Interval(start, end))
    DefaultValueExpression(expr, originalSQL)
  }

  /**
   * Create `DefaultValueExpression` for a column.
   */
  override def visitDefaultExpression(ctx: DefaultExpressionContext): DefaultValueExpression =
    withOrigin(ctx) {
      getDefaultExpression(ctx.expression(), "DEFAULT")
    }

  /**
   * Create `DefaultValueExpression` for a SQL variable.
   */
  override def visitVariableDefaultExpression(
      ctx: VariableDefaultExpressionContext): DefaultValueExpression =
    withOrigin(ctx) {
      getDefaultExpression(ctx.expression(), "DEFAULT")
    }

  /**
   * Create a generation expression string.
   */
  override def visitGenerationExpression(ctx: GenerationExpressionContext): String =
    withOrigin(ctx) {
      getDefaultExpression(ctx.expression(), "GENERATED").originalSQL
    }

  /**
   * Create an optional comment string.
   */
  protected def visitCommentSpecList(ctx: java.util.List[CommentSpecContext]): Option[String] = {
    ctx.asScala.headOption.map(visitCommentSpec)
  }

  /**
   * Create a [[BucketSpec]].
   */
  override def visitBucketSpec(ctx: BucketSpecContext): BucketSpec = withOrigin(ctx) {
    BucketSpec(
      ctx.INTEGER_VALUE.getText.toInt,
      visitIdentifierList(ctx.identifierList),
      Option(ctx.orderedIdentifierList)
          .toSeq
          .flatMap(_.orderedIdentifier.asScala)
          .map { orderedIdCtx =>
            Option(orderedIdCtx.ordering).map(_.getText).foreach { dir =>
              if (dir.toLowerCase(Locale.ROOT) != "asc") {
                operationNotAllowed(s"Column ordering must be ASC, was '$dir'", ctx)
              }
            }

            orderedIdCtx.ident.getText
          })
  }

  /**
   * Create a [[ClusterBySpec]].
   */
  override def visitClusterBySpec(ctx: ClusterBySpecContext): ClusterBySpec = withOrigin(ctx) {
    val columnNames = ctx.multipartIdentifierList.multipartIdentifier.asScala
      .map(typedVisit[Seq[String]]).map(FieldReference(_)).toSeq
    ClusterBySpec(columnNames)
  }

  /**
   * Convert a property list into a key-value map.
   * This should be called through [[visitPropertyKeyValues]] or [[visitPropertyKeys]].
   */
  override def visitPropertyList(
      ctx: PropertyListContext): Map[String, String] = withOrigin(ctx) {
    val properties = ctx.property.asScala.map { property =>
      val key = visitPropertyKey(property.key)
      val value = visitPropertyValue(property.value)
      key -> value
    }
    // Check for duplicate property names.
    checkDuplicateKeys(properties.toSeq, ctx)
    properties.toMap
  }

  /**
   * Parse a key-value map from a [[PropertyListContext]], assuming all values are specified.
   */
  def visitPropertyKeyValues(ctx: PropertyListContext): Map[String, String] = {
    val props = visitPropertyList(ctx)
    val badKeys = props.collect { case (key, null) => key }
    if (badKeys.nonEmpty) {
      operationNotAllowed(
        s"Values must be specified for key(s): ${badKeys.mkString("[", ",", "]")}", ctx)
    }
    props
  }

  /**
   * Parse a list of keys from a [[PropertyListContext]], assuming no values are specified.
   */
  def visitPropertyKeys(ctx: PropertyListContext): Seq[String] = {
    val props = visitPropertyList(ctx)
    val badKeys = props.filter { case (_, v) => v != null }.keys
    if (badKeys.nonEmpty) {
      operationNotAllowed(
        s"Values should not be specified for key(s): ${badKeys.mkString("[", ",", "]")}", ctx)
    }
    props.keys.toSeq
  }

  /**
   * A property key can either be String or a collection of dot separated elements. This
   * function extracts the property key based on whether its a string literal or a property
   * identifier.
   */
  override def visitPropertyKey(key: PropertyKeyContext): String = {
    if (key.stringLit() != null) {
      string(visitStringLit(key.stringLit()))
    } else {
      key.getText
    }
  }

  /**
   * A property value can be String, Integer, Boolean or Decimal. This function extracts
   * the property value based on whether its a string, integer, boolean or decimal literal.
   */
  override def visitPropertyValue(value: PropertyValueContext): String = {
    if (value == null) {
      null
    } else if (value.stringLit() != null) {
      string(visitStringLit(value.stringLit()))
    } else if (value.booleanValue != null) {
      value.getText.toLowerCase(Locale.ROOT)
    } else {
      value.getText
    }
  }

  /**
   * Parse a key-value map from an [[ExpressionPropertyListContext]], assuming all values are
   * specified.
   */
  override def visitExpressionPropertyList(
      ctx: ExpressionPropertyListContext): OptionList = {
    val options = ctx.expressionProperty.asScala.map { property =>
      val key: String = visitPropertyKey(property.key)
      val value: Expression = Option(property.value).map(expression).getOrElse {
        operationNotAllowed(s"A value must be specified for the key: $key.", ctx)
      }
      key -> value
    }.toSeq
    OptionList(options)
  }

  /**
   * Type to keep track of a table header: (identifier, isTemporary, ifNotExists, isExternal).
   */
  type TableHeader = (IdentifierReferenceContext, Boolean, Boolean, Boolean)

  /**
   * Type to keep track of table clauses:
   * - partition transforms
   * - partition columns
   * - bucketSpec
   * - properties
   * - options
   * - location
   * - comment
   * - serde
   * - clusterBySpec
   *
   * Note: Partition transforms are based on existing table schema definition. It can be simple
   * column names, or functions like `year(date_col)`. Partition columns are column names with data
   * types like `i INT`, which should be appended to the existing table schema.
   */
  type TableClauses = (
      Seq[Transform], Seq[ColumnDefinition], Option[BucketSpec], Map[String, String],
      OptionList, Option[String], Option[String], Option[SerdeInfo], Option[ClusterBySpec])

  /**
   * Validate a create table statement and return the [[TableIdentifier]].
   */
  override def visitCreateTableHeader(
      ctx: CreateTableHeaderContext): TableHeader = withOrigin(ctx) {
    blockBang(ctx.errorCapturingNot)
    val temporary = ctx.TEMPORARY != null
    val ifNotExists = ctx.EXISTS != null
    if (temporary && ifNotExists) {
      invalidStatement("CREATE TEMPORARY TABLE ... IF NOT EXISTS", ctx)
    }
    (ctx.identifierReference(), temporary, ifNotExists, ctx.EXTERNAL != null)
  }

  /**
   * Parse a qualified name to a multipart name.
   */
  override def visitQualifiedName(ctx: QualifiedNameContext): Seq[String] = withOrigin(ctx) {
    ctx.identifier.asScala.map(_.getText).toSeq
  }

  /**
   * Parse a list of transforms or columns.
   */
  override def visitPartitionFieldList(
      ctx: PartitionFieldListContext): (Seq[Transform], Seq[ColumnDefinition]) = withOrigin(ctx) {
    val (transforms, columns) = ctx.fields.asScala.map {
      case transform: PartitionTransformContext =>
        (Some(visitPartitionTransform(transform)), None)
      case field: PartitionColumnContext =>
        val f = visitColType(field.colType)
        // The parser rule of `visitColType` only supports basic column info with comment.
        val col = ColumnDefinition(f.name, f.dataType, f.nullable, f.getComment())
        (None, Some(col))
    }.unzip

    (transforms.flatten.toSeq, columns.flatten.toSeq)
  }

  override def visitPartitionTransform(
      ctx: PartitionTransformContext): Transform = withOrigin(ctx) {
    def getFieldReference(
        ctx: ApplyTransformContext,
        arg: V2Expression): FieldReference = {
      lazy val name: String = ctx.identifier.getText
      arg match {
        case ref: FieldReference =>
          ref
        case nonRef =>
          throw QueryParsingErrors.partitionTransformNotExpectedError(name, nonRef.describe, ctx)
      }
    }

    def getSingleFieldReference(
        ctx: ApplyTransformContext,
        arguments: Seq[V2Expression]): FieldReference = {
      lazy val name: String = ctx.identifier.getText
      if (arguments.size > 1) {
        throw QueryParsingErrors.wrongNumberArgumentsForTransformError(name, arguments.size, ctx)
      } else if (arguments.isEmpty) {
        throw SparkException.internalError(s"Not enough arguments for transform $name")
      } else {
        getFieldReference(ctx, arguments.head)
      }
    }

    ctx.transform match {
      case identityCtx: IdentityTransformContext =>
        IdentityTransform(FieldReference(typedVisit[Seq[String]](identityCtx.qualifiedName)))

      case applyCtx: ApplyTransformContext =>
        val arguments = applyCtx.argument.asScala.map(visitTransformArgument).toSeq

        applyCtx.identifier.getText match {
          case "bucket" =>
            val numBuckets: Int = arguments.head match {
              case LiteralValue(shortValue, ShortType) =>
                shortValue.asInstanceOf[Short].toInt
              case LiteralValue(intValue, IntegerType) =>
                intValue.asInstanceOf[Int]
              case LiteralValue(longValue, LongType) =>
                longValue.asInstanceOf[Long].toInt
              case lit =>
                throw QueryParsingErrors.invalidBucketsNumberError(lit.describe, applyCtx)
            }

            val fields = arguments.tail.map(arg => getFieldReference(applyCtx, arg))

            BucketTransform(LiteralValue(numBuckets, IntegerType), fields)

          case "years" =>
            YearsTransform(getSingleFieldReference(applyCtx, arguments))

          case "months" =>
            MonthsTransform(getSingleFieldReference(applyCtx, arguments))

          case "days" =>
            DaysTransform(getSingleFieldReference(applyCtx, arguments))

          case "hours" =>
            HoursTransform(getSingleFieldReference(applyCtx, arguments))

          case name =>
            ApplyTransform(name, arguments)
        }
    }
  }

  /**
   * Parse an argument to a transform. An argument may be a field reference (qualified name) or
   * a value literal.
   */
  override def visitTransformArgument(ctx: TransformArgumentContext): V2Expression = {
    withOrigin(ctx) {
      val reference = Option(ctx.qualifiedName)
          .map(typedVisit[Seq[String]])
          .map(FieldReference(_))
      val literal = Option(ctx.constant)
          .map(typedVisit[Literal])
          .map(lit => LiteralValue(lit.value, lit.dataType))
      reference.orElse(literal)
          .getOrElse(throw SparkException.internalError("Invalid transform argument"))
    }
  }

  protected def cleanNamespaceProperties(
      properties: Map[String, String],
      ctx: ParserRuleContext): Map[String, String] = withOrigin(ctx) {
    import SupportsNamespaces._
    val legacyOn = conf.getConf(SQLConf.LEGACY_PROPERTY_NON_RESERVED)
    properties.filter {
      case (PROP_LOCATION, _) if !legacyOn =>
        throw QueryParsingErrors.cannotCleanReservedNamespacePropertyError(
          PROP_LOCATION, ctx, "please use the LOCATION clause to specify it")
      case (PROP_LOCATION, _) => false
      case (PROP_OWNER, _) if !legacyOn =>
        throw QueryParsingErrors.cannotCleanReservedNamespacePropertyError(
          PROP_OWNER, ctx, "it will be set to the current user")
      case (PROP_OWNER, _) => false
      case _ => true
    }
  }

  /**
   * Create a [[CreateNamespace]] command.
   *
   * For example:
   * {{{
   *   CREATE NAMESPACE [IF NOT EXISTS] ns1.ns2.ns3
   *     create_namespace_clauses;
   *
   *   create_namespace_clauses (order insensitive):
   *     [COMMENT namespace_comment]
   *     [LOCATION path]
   *     [WITH PROPERTIES (key1=val1, key2=val2, ...)]
   * }}}
   */
  override def visitCreateNamespace(ctx: CreateNamespaceContext): LogicalPlan = withOrigin(ctx) {
    import SupportsNamespaces._
    checkDuplicateClauses(ctx.commentSpec(), "COMMENT", ctx)
    checkDuplicateClauses(ctx.locationSpec, "LOCATION", ctx)
    checkDuplicateClauses(ctx.PROPERTIES, "WITH PROPERTIES", ctx)
    checkDuplicateClauses(ctx.DBPROPERTIES, "WITH DBPROPERTIES", ctx)

    if (!ctx.PROPERTIES.isEmpty && !ctx.DBPROPERTIES.isEmpty) {
      throw QueryParsingErrors.propertiesAndDbPropertiesBothSpecifiedError(ctx)
    }

    var properties = ctx.propertyList.asScala.headOption
      .map(visitPropertyKeyValues)
      .getOrElse(Map.empty)

    properties = cleanNamespaceProperties(properties, ctx)

    visitCommentSpecList(ctx.commentSpec()).foreach {
      properties += PROP_COMMENT -> _
    }

    visitLocationSpecList(ctx.locationSpec()).foreach {
      properties += PROP_LOCATION -> _
    }

    blockBang(ctx.errorCapturingNot)

    CreateNamespace(
      withIdentClause(ctx.identifierReference, UnresolvedNamespace(_)),
      ctx.EXISTS != null,
      properties)
  }

  /**
   * Create a [[DropNamespace]] command.
   *
   * For example:
   * {{{
   *   DROP (DATABASE|SCHEMA|NAMESPACE) [IF EXISTS] ns1.ns2 [RESTRICT|CASCADE];
   * }}}
   */
  override def visitDropNamespace(ctx: DropNamespaceContext): LogicalPlan = withOrigin(ctx) {
    DropNamespace(
      withIdentClause(ctx.identifierReference, UnresolvedNamespace(_)),
      ctx.EXISTS != null,
      ctx.CASCADE != null)
  }

  /**
   * Create an [[SetNamespaceProperties]] logical plan.
   *
   * For example:
   * {{{
   *   ALTER (DATABASE|SCHEMA|NAMESPACE) database
   *   SET (DBPROPERTIES|PROPERTIES) (property_name=property_value, ...);
   * }}}
   */
  override def visitSetNamespaceProperties(ctx: SetNamespacePropertiesContext): LogicalPlan = {
    withOrigin(ctx) {
      val properties = cleanNamespaceProperties(visitPropertyKeyValues(ctx.propertyList), ctx)
      SetNamespaceProperties(
        withIdentClause(ctx.identifierReference, UnresolvedNamespace(_)),
        properties)
    }
  }

  /**
   * Create an [[SetNamespaceLocation]] logical plan.
   *
   * For example:
   * {{{
   *   ALTER (DATABASE|SCHEMA|NAMESPACE) namespace SET LOCATION path;
   * }}}
   */
  override def visitSetNamespaceLocation(ctx: SetNamespaceLocationContext): LogicalPlan = {
    withOrigin(ctx) {
      SetNamespaceLocation(
        withIdentClause(ctx.identifierReference, UnresolvedNamespace(_)),
        visitLocationSpec(ctx.locationSpec))
    }
  }

  /**
   * Create a [[ShowNamespaces]] command.
   */
  override def visitShowNamespaces(ctx: ShowNamespacesContext): LogicalPlan = withOrigin(ctx) {
    val multiPart = Option(ctx.multipartIdentifier).map(visitMultipartIdentifier)
    ShowNamespaces(
      UnresolvedNamespace(multiPart.getOrElse(Seq.empty[String])),
      Option(ctx.pattern).map(x => string(visitStringLit(x))))
  }

  /**
   * Create a [[DescribeNamespace]].
   *
   * For example:
   * {{{
   *   DESCRIBE (DATABASE|SCHEMA|NAMESPACE) [EXTENDED] database;
   * }}}
   */
  override def visitDescribeNamespace(ctx: DescribeNamespaceContext): LogicalPlan =
    withOrigin(ctx) {
      DescribeNamespace(
        withIdentClause(ctx.identifierReference, UnresolvedNamespace(_)),
        ctx.EXTENDED != null)
    }

  def cleanTableProperties[ValueType](
      ctx: ParserRuleContext, properties: Map[String, ValueType]): Map[String, ValueType] = {
    import TableCatalog._
    val legacyOn = conf.getConf(SQLConf.LEGACY_PROPERTY_NON_RESERVED)
    properties.filter {
      case (PROP_PROVIDER, _) if !legacyOn =>
        throw QueryParsingErrors.cannotCleanReservedTablePropertyError(
          PROP_PROVIDER, ctx, "please use the USING clause to specify it")
      case (PROP_PROVIDER, _) => false
      case (PROP_LOCATION, _) if !legacyOn =>
        throw QueryParsingErrors.cannotCleanReservedTablePropertyError(
          PROP_LOCATION, ctx, "please use the LOCATION clause to specify it")
      case (PROP_LOCATION, _) => false
      case (PROP_OWNER, _) if !legacyOn =>
        throw QueryParsingErrors.cannotCleanReservedTablePropertyError(
          PROP_OWNER, ctx, "it will be set to the current user")
      case (PROP_OWNER, _) => false
      case (PROP_EXTERNAL, _) if !legacyOn =>
        throw QueryParsingErrors.cannotCleanReservedTablePropertyError(
          PROP_EXTERNAL, ctx, "please use CREATE EXTERNAL TABLE")
      case (PROP_EXTERNAL, _) => false
      // It's safe to set whatever table comment, so we don't make it a reserved table property.
      case (PROP_COMMENT, _) => true
      case (k, _) =>
        val isReserved = CatalogV2Util.TABLE_RESERVED_PROPERTIES.contains(k)
        if (!legacyOn && isReserved) {
          throw QueryParsingErrors.cannotCleanReservedTablePropertyError(
            k, ctx, "please remove it from the TBLPROPERTIES list.")
        }
        !isReserved
    }
  }

  def cleanTableOptions(
      ctx: ParserRuleContext,
      options: OptionList,
      location: Option[String]): (OptionList, Option[String]) = {
    var path = location
    val filtered = cleanTableProperties(ctx, options.options.toMap).filter {
      case (key, value) if key.equalsIgnoreCase("path") =>
        val newValue: String =
          if (value == null) {
            ""
          } else value match {
            case Literal(_, _: StringType) => value.toString
            case _ => throw QueryCompilationErrors.optionMustBeLiteralString(key)
          }
        if (path.nonEmpty) {
          throw QueryParsingErrors.duplicatedTablePathsFoundError(path.get, newValue, ctx)
        }
        path = Some(newValue)
        false
      case _ => true
    }
    (OptionList(filtered.toSeq), path)
  }

  /**
   * Create a [[SerdeInfo]] for creating tables.
   *
   * Format: STORED AS (name | INPUTFORMAT input_format OUTPUTFORMAT output_format)
   */
  override def visitCreateFileFormat(ctx: CreateFileFormatContext): SerdeInfo = withOrigin(ctx) {
    (ctx.fileFormat, ctx.storageHandler) match {
      // Expected format: INPUTFORMAT input_format OUTPUTFORMAT output_format
      case (c: TableFileFormatContext, null) =>
        SerdeInfo(formatClasses = Some(FormatClasses(string(visitStringLit(c.inFmt)),
          string(visitStringLit(c.outFmt)))))
      // Expected format: SEQUENCEFILE | TEXTFILE | RCFILE | ORC | PARQUET | AVRO
      case (c: GenericFileFormatContext, null) =>
        SerdeInfo(storedAs = Some(c.identifier.getText))
      case (null, storageHandler) =>
        invalidStatement("STORED BY", ctx)
      case _ =>
        throw QueryParsingErrors.storedAsAndStoredByBothSpecifiedError(ctx)
    }
  }

  /**
   * Create a [[SerdeInfo]] used for creating tables.
   *
   * Example format:
   * {{{
   *   SERDE serde_name [WITH SERDEPROPERTIES (k1=v1, k2=v2, ...)]
   * }}}
   *
   * OR
   *
   * {{{
   *   DELIMITED [FIELDS TERMINATED BY char [ESCAPED BY char]]
   *   [COLLECTION ITEMS TERMINATED BY char]
   *   [MAP KEYS TERMINATED BY char]
   *   [LINES TERMINATED BY char]
   *   [NULL DEFINED AS char]
   * }}}
   */
  def visitRowFormat(ctx: RowFormatContext): SerdeInfo = withOrigin(ctx) {
    ctx match {
      case serde: RowFormatSerdeContext => visitRowFormatSerde(serde)
      case delimited: RowFormatDelimitedContext => visitRowFormatDelimited(delimited)
    }
  }

  /**
   * Create SERDE row format name and properties pair.
   */
  override def visitRowFormatSerde(ctx: RowFormatSerdeContext): SerdeInfo = withOrigin(ctx) {
    import ctx._
    SerdeInfo(
      serde = Some(string(visitStringLit(name))),
      serdeProperties = Option(propertyList).map(visitPropertyKeyValues).getOrElse(Map.empty))
  }

  /**
   * Create a delimited row format properties object.
   */
  override def visitRowFormatDelimited(
      ctx: RowFormatDelimitedContext): SerdeInfo = withOrigin(ctx) {
    // Collect the entries if any.
    def entry(key: String, value: StringLitContext): Seq[(String, String)] = {
      Option(value).toSeq.map(x => key -> string(visitStringLit(x)))
    }
    // TODO we need proper support for the NULL format.
    val entries =
      entry("field.delim", ctx.fieldsTerminatedBy) ++
          entry("serialization.format", ctx.fieldsTerminatedBy) ++
          entry("escape.delim", ctx.escapedBy) ++
          // The following typo is inherited from Hive...
          entry("colelction.delim", ctx.collectionItemsTerminatedBy) ++
          entry("mapkey.delim", ctx.keysTerminatedBy) ++
          Option(ctx.linesSeparatedBy).toSeq.map { token =>
            val value = string(visitStringLit(token))
            validate(
              value == "\n",
              s"LINES TERMINATED BY only supports newline '\\n' right now: $value",
              ctx)
            "line.delim" -> value
          }
    SerdeInfo(serdeProperties = entries.toMap)
  }

  /**
   * Throw a [[ParseException]] if the user specified incompatible SerDes through ROW FORMAT
   * and STORED AS.
   *
   * The following are allowed. Anything else is not:
   *   ROW FORMAT SERDE ... STORED AS [SEQUENCEFILE | RCFILE | TEXTFILE]
   *   ROW FORMAT DELIMITED ... STORED AS TEXTFILE
   *   ROW FORMAT ... STORED AS INPUTFORMAT ... OUTPUTFORMAT ...
   */
  protected def validateRowFormatFileFormat(
      rowFormatCtx: RowFormatContext,
      createFileFormatCtx: CreateFileFormatContext,
      parentCtx: ParserRuleContext): Unit = {
    if (rowFormatCtx == null || createFileFormatCtx == null) {
      return
    }
    (rowFormatCtx, createFileFormatCtx.fileFormat) match {
      case (_, ffTable: TableFileFormatContext) => // OK
      case (rfSerde: RowFormatSerdeContext, ffGeneric: GenericFileFormatContext) =>
        ffGeneric.identifier.getText.toLowerCase(Locale.ROOT) match {
          case ("sequencefile" | "textfile" | "rcfile") => // OK
          case fmt =>
            operationNotAllowed(
              s"ROW FORMAT SERDE is incompatible with format '$fmt', which also specifies a serde",
              parentCtx)
        }
      case (rfDelimited: RowFormatDelimitedContext, ffGeneric: GenericFileFormatContext) =>
        ffGeneric.identifier.getText.toLowerCase(Locale.ROOT) match {
          case "textfile" => // OK
          case fmt => operationNotAllowed(
            s"ROW FORMAT DELIMITED is only compatible with 'textfile', not '$fmt'", parentCtx)
        }
      case _ =>
        // should never happen
        def str(ctx: ParserRuleContext): String = {
          (0 until ctx.getChildCount).map { i => ctx.getChild(i).getText }.mkString(" ")
        }
        operationNotAllowed(
          s"Unexpected combination of ${str(rowFormatCtx)} and ${str(createFileFormatCtx)}",
          parentCtx)
    }
  }

  protected def validateRowFormatFileFormat(
      rowFormatCtx: Seq[RowFormatContext],
      createFileFormatCtx: Seq[CreateFileFormatContext],
      parentCtx: ParserRuleContext): Unit = {
    if (rowFormatCtx.size == 1 && createFileFormatCtx.size == 1) {
      validateRowFormatFileFormat(rowFormatCtx.head, createFileFormatCtx.head, parentCtx)
    }
  }

  override def visitCreateTableClauses(ctx: CreateTableClausesContext): TableClauses = {
    checkDuplicateClauses(ctx.TBLPROPERTIES, "TBLPROPERTIES", ctx)
    checkDuplicateClauses(ctx.OPTIONS, "OPTIONS", ctx)
    checkDuplicateClauses(ctx.PARTITIONED, "PARTITIONED BY", ctx)
    checkDuplicateClauses(ctx.createFileFormat, "STORED AS/BY", ctx)
    checkDuplicateClauses(ctx.rowFormat, "ROW FORMAT", ctx)
    checkDuplicateClauses(ctx.commentSpec(), "COMMENT", ctx)
    checkDuplicateClauses(ctx.bucketSpec(), "CLUSTERED BY", ctx)
    checkDuplicateClauses(ctx.clusterBySpec(), "CLUSTER BY", ctx)
    checkDuplicateClauses(ctx.locationSpec, "LOCATION", ctx)

    if (ctx.skewSpec.size > 0) {
      invalidStatement("CREATE TABLE ... SKEWED BY", ctx)
    }

    val (partTransforms, partCols) =
      Option(ctx.partitioning).map(visitPartitionFieldList).getOrElse((Nil, Nil))
    val bucketSpec = ctx.bucketSpec().asScala.headOption.map(visitBucketSpec)
    val properties = Option(ctx.tableProps).map(visitPropertyKeyValues).getOrElse(Map.empty)
    val cleanedProperties = cleanTableProperties(ctx, properties)
    val options = Option(ctx.options).map(visitExpressionPropertyList)
      .getOrElse(OptionList(Seq.empty))
    val location = visitLocationSpecList(ctx.locationSpec())
    val (cleanedOptions, newLocation) = cleanTableOptions(ctx, options, location)
    val comment = visitCommentSpecList(ctx.commentSpec())
    val serdeInfo =
      getSerdeInfo(ctx.rowFormat.asScala.toSeq, ctx.createFileFormat.asScala.toSeq, ctx)
    val clusterBySpec = ctx.clusterBySpec().asScala.headOption.map(visitClusterBySpec)

    if (clusterBySpec.isDefined) {
      if (partCols.nonEmpty || partTransforms.nonEmpty) {
        throw QueryParsingErrors.clusterByWithPartitionedBy(ctx)
      }
      if (bucketSpec.isDefined) {
        throw QueryParsingErrors.clusterByWithBucketing(ctx)
      }
    }

    (partTransforms, partCols, bucketSpec, cleanedProperties, cleanedOptions, newLocation, comment,
      serdeInfo, clusterBySpec)
  }

  protected def getSerdeInfo(
      rowFormatCtx: Seq[RowFormatContext],
      createFileFormatCtx: Seq[CreateFileFormatContext],
      ctx: ParserRuleContext): Option[SerdeInfo] = {
    validateRowFormatFileFormat(rowFormatCtx, createFileFormatCtx, ctx)
    val rowFormatSerdeInfo = rowFormatCtx.map(visitRowFormat)
    val fileFormatSerdeInfo = createFileFormatCtx.map(visitCreateFileFormat)
    (fileFormatSerdeInfo ++ rowFormatSerdeInfo).reduceLeftOption((l, r) => l.merge(r))
  }

  private def partitionExpressions(
      partTransforms: Seq[Transform],
      partCols: Seq[ColumnDefinition],
      ctx: ParserRuleContext): Seq[Transform] = {
    if (partTransforms.nonEmpty) {
      if (partCols.nonEmpty) {
        val references = partTransforms.map(_.describe()).mkString(", ")
        val columns = partCols
          .map(column => s"${column.name} ${column.dataType.simpleString}")
          .mkString(", ")
        operationNotAllowed(
          s"""PARTITION BY: Cannot mix partition expressions and partition columns:
             |Expressions: $references
             |Columns: $columns""".stripMargin, ctx)

      }
      partTransforms
    } else {
      // columns were added to create the schema. convert to column references
      partCols.map { column =>
        IdentityTransform(FieldReference(Seq(column.name)))
      }
    }
  }

  /**
   * Create a table, returning a [[CreateTable]] or [[CreateTableAsSelect]] logical plan.
   *
   * Expected format:
   * {{{
   *   CREATE [TEMPORARY] TABLE [IF NOT EXISTS] [db_name.]table_name
   *   [USING table_provider]
   *   create_table_clauses
   *   [[AS] select_statement];
   *
   *   create_table_clauses (order insensitive):
   *     [PARTITIONED BY (partition_fields)]
   *     [OPTIONS table_property_list]
   *     [ROW FORMAT row_format]
   *     [STORED AS file_format]
   *     [CLUSTER BY (col_name, col_name, ...)]
   *     [CLUSTERED BY (col_name, col_name, ...)
   *       [SORTED BY (col_name [ASC|DESC], ...)]
   *       INTO num_buckets BUCKETS
   *     ]
   *     [LOCATION path]
   *     [COMMENT table_comment]
   *     [TBLPROPERTIES (property_name=property_value, ...)]
   *
   *   partition_fields:
   *     col_name, transform(col_name), transform(constant, col_name), ... |
   *     col_name data_type [NOT NULL] [COMMENT col_comment], ...
   * }}}
   */
  override def visitCreateTable(ctx: CreateTableContext): LogicalPlan = withOrigin(ctx) {
    val (identifierContext, temp, ifNotExists, external) =
      visitCreateTableHeader(ctx.createTableHeader)

    val columns = Option(ctx.colDefinitionList()).map(visitColDefinitionList).getOrElse(Nil)
    val provider = Option(ctx.tableProvider).map(_.multipartIdentifier.getText)
    val (partTransforms, partCols, bucketSpec, properties, options, location,
      comment, serdeInfo, clusterBySpec) = visitCreateTableClauses(ctx.createTableClauses())

    if (provider.isDefined && serdeInfo.isDefined) {
      invalidStatement(s"CREATE TABLE ... USING ... ${serdeInfo.get.describe}", ctx)
    }

    if (temp) {
      val asSelect = if (ctx.query == null) "" else " AS ..."
      operationNotAllowed(
        s"CREATE TEMPORARY TABLE ...$asSelect, use CREATE TEMPORARY VIEW instead", ctx)
    }

    val partitioning =
      partitionExpressions(partTransforms, partCols, ctx) ++
        bucketSpec.map(_.asTransform) ++
        clusterBySpec.map(_.asTransform)

    val tableSpec = UnresolvedTableSpec(properties, provider, options, location, comment,
      serdeInfo, external)

    Option(ctx.query).map(plan) match {
      case Some(_) if columns.nonEmpty =>
        operationNotAllowed(
          "Schema may not be specified in a Create Table As Select (CTAS) statement",
          ctx)

      case Some(_) if partCols.nonEmpty =>
        // non-reference partition columns are not allowed because schema can't be specified
        operationNotAllowed(
          "Partition column types may not be specified in Create Table As Select (CTAS)",
          ctx)

      case Some(query) =>
        CreateTableAsSelect(withIdentClause(identifierContext, UnresolvedIdentifier(_)),
          partitioning, query, tableSpec, Map.empty, ifNotExists)

      case _ =>
        // Note: table schema includes both the table columns list and the partition columns
        // with data type.
        val allColumns = columns ++ partCols
        CreateTable(
          withIdentClause(identifierContext, UnresolvedIdentifier(_)),
          allColumns, partitioning, tableSpec, ignoreIfExists = ifNotExists)
    }
  }

  /**
   * Replace a table, returning a [[ReplaceTable]] or [[ReplaceTableAsSelect]]
   * logical plan.
   *
   * Expected format:
   * {{{
   *   [CREATE OR] REPLACE TABLE [db_name.]table_name
   *   [USING table_provider]
   *   replace_table_clauses
   *   [[AS] select_statement];
   *
   *   replace_table_clauses (order insensitive):
   *     [OPTIONS table_property_list]
   *     [PARTITIONED BY (partition_fields)]
   *     [CLUSTER BY (col_name, col_name, ...)]
   *     [CLUSTERED BY (col_name, col_name, ...)
   *       [SORTED BY (col_name [ASC|DESC], ...)]
   *       INTO num_buckets BUCKETS
   *     ]
   *     [LOCATION path]
   *     [COMMENT table_comment]
   *     [TBLPROPERTIES (property_name=property_value, ...)]
   *
   *   partition_fields:
   *     col_name, transform(col_name), transform(constant, col_name), ... |
   *     col_name data_type [NOT NULL] [COMMENT col_comment], ...
   * }}}
   */
  override def visitReplaceTable(ctx: ReplaceTableContext): LogicalPlan = withOrigin(ctx) {
    val orCreate = ctx.replaceTableHeader().CREATE() != null
    val (partTransforms, partCols, bucketSpec, properties, options, location, comment, serdeInfo,
      clusterBySpec) = visitCreateTableClauses(ctx.createTableClauses())
    val columns = Option(ctx.colDefinitionList()).map(visitColDefinitionList).getOrElse(Nil)
    val provider = Option(ctx.tableProvider).map(_.multipartIdentifier.getText)

    if (provider.isDefined && serdeInfo.isDefined) {
      invalidStatement(s"REPLACE TABLE ... USING ... ${serdeInfo.get.describe}", ctx)
    }

    val partitioning =
      partitionExpressions(partTransforms, partCols, ctx) ++
        bucketSpec.map(_.asTransform) ++
        clusterBySpec.map(_.asTransform)

    val tableSpec = UnresolvedTableSpec(properties, provider, options, location, comment,
      serdeInfo, external = false)

    Option(ctx.query).map(plan) match {
      case Some(_) if columns.nonEmpty =>
        operationNotAllowed(
          "Schema may not be specified in a Replace Table As Select (RTAS) statement",
          ctx)

      case Some(_) if partCols.nonEmpty =>
        // non-reference partition columns are not allowed because schema can't be specified
        operationNotAllowed(
          "Partition column types may not be specified in Replace Table As Select (RTAS)",
          ctx)

      case Some(query) =>
        ReplaceTableAsSelect(
          withIdentClause(ctx.replaceTableHeader.identifierReference(), UnresolvedIdentifier(_)),
          partitioning, query, tableSpec, writeOptions = Map.empty, orCreate = orCreate)

      case _ =>
        // Note: table schema includes both the table columns list and the partition columns
        // with data type.
        val allColumns = columns ++ partCols
        ReplaceTable(
          withIdentClause(ctx.replaceTableHeader.identifierReference(), UnresolvedIdentifier(_)),
          allColumns, partitioning, tableSpec, orCreate = orCreate)
    }
  }

  /**
   * Create a [[DropTable]] command.
   */
  override def visitDropTable(ctx: DropTableContext): LogicalPlan = withOrigin(ctx) {
    // DROP TABLE works with either a table or a temporary view.
    DropTable(
      withIdentClause(ctx.identifierReference, UnresolvedIdentifier(_, allowTemp = true)),
      ctx.EXISTS != null,
      ctx.PURGE != null)
  }

  /**
   * Create a [[DropView]] command.
   */
  override def visitDropView(ctx: DropViewContext): AnyRef = withOrigin(ctx) {
    DropView(
      withIdentClause(ctx.identifierReference, UnresolvedIdentifier(_, allowTemp = true)),
      ctx.EXISTS != null)
  }

  /**
   * Create a [[SetCatalogAndNamespace]] command.
   */
  override def visitUse(ctx: UseContext): LogicalPlan = withOrigin(ctx) {
    SetCatalogAndNamespace(withIdentClause(ctx.identifierReference, UnresolvedNamespace(_)))
  }

  /**
   * Create a [[ShowTables]] command.
   */
  override def visitShowTables(ctx: ShowTablesContext): LogicalPlan = withOrigin(ctx) {
    val ns = if (ctx.identifierReference() != null) {
      withIdentClause(ctx.identifierReference, UnresolvedNamespace(_))
    } else {
      CurrentNamespace
    }
    ShowTables(ns, Option(ctx.pattern).map(x => string(visitStringLit(x))))
  }

  /**
   * Create a [[ShowTablesExtended]] or [[ShowTablePartition]] command.
   */
  override def visitShowTableExtended(
      ctx: ShowTableExtendedContext): LogicalPlan = withOrigin(ctx) {
    Option(ctx.partitionSpec).map { spec =>
      val table = withOrigin(ctx.pattern) {
        if (ctx.identifierReference() != null) {
          withIdentClause(ctx.identifierReference(), ns => {
            val names = ns :+ string(visitStringLit(ctx.pattern))
            UnresolvedTable(names, "SHOW TABLE EXTENDED ... PARTITION ...")
          })
        } else {
          val names = Seq.empty[String] :+ string(visitStringLit(ctx.pattern))
          UnresolvedTable(names, "SHOW TABLE EXTENDED ... PARTITION ...")
        }
      }
      ShowTablePartition(table, UnresolvedPartitionSpec(visitNonOptionalPartitionSpec(spec)))
    }.getOrElse {
      val ns = if (ctx.identifierReference() != null) {
        withIdentClause(ctx.identifierReference, UnresolvedNamespace(_))
      } else {
        CurrentNamespace
      }
      ShowTablesExtended(ns, string(visitStringLit(ctx.pattern)))
    }
  }

  /**
   * Create a [[ShowViews]] command.
   */
  override def visitShowViews(ctx: ShowViewsContext): LogicalPlan = withOrigin(ctx) {
    val ns = if (ctx.identifierReference() != null) {
      withIdentClause(ctx.identifierReference, UnresolvedNamespace(_))
    } else {
      CurrentNamespace
    }
    ShowViews(ns, Option(ctx.pattern).map(x => string(visitStringLit(x))))
  }

  override def visitColPosition(ctx: ColPositionContext): ColumnPosition = {
    ctx.position.getType match {
      case SqlBaseParser.FIRST => ColumnPosition.first()
      case SqlBaseParser.AFTER => ColumnPosition.after(ctx.afterCol.getText)
    }
  }

  /**
   * Parse new column info from ADD COLUMN into a QualifiedColType.
   */
  override def visitQualifiedColTypeWithPosition(
      ctx: QualifiedColTypeWithPositionContext): QualifiedColType = withOrigin(ctx) {
    val name = typedVisit[Seq[String]](ctx.name)
    // Check that no duplicates exist among any ALTER TABLE ADD|REPLACE column options specified.
    var nullable = true
    var defaultExpression: Option[DefaultExpressionContext] = None
    var commentSpec: Option[CommentSpecContext] = None
    var colPosition: Option[ColPositionContext] = None
    val columnName = name.last
    ctx.colDefinitionDescriptorWithPosition.asScala.foreach { option =>
      blockBang(option.errorCapturingNot)

      if (option.NULL != null) {
        blockBang(option.errorCapturingNot)
        if (!nullable) {
          throw QueryParsingErrors.duplicateTableColumnDescriptor(
            option, columnName, "NOT NULL", isCreate = false)
        }
        nullable = false
      }
      Option(option.defaultExpression()).foreach { expr =>
        if (defaultExpression.isDefined) {
          throw QueryParsingErrors.duplicateTableColumnDescriptor(
            option, columnName, "DEFAULT", isCreate = false)
        }
        defaultExpression = Some(expr)
      }
      Option(option.commentSpec()).foreach { spec =>
        if (commentSpec.isDefined) {
          throw QueryParsingErrors.duplicateTableColumnDescriptor(
            option, columnName, "COMMENT", isCreate = false)
        }
        commentSpec = Some(spec)
      }
      Option(option.colPosition()).foreach { spec =>
        if (colPosition.isDefined) {
          throw QueryParsingErrors.duplicateTableColumnDescriptor(
            option, columnName, "FIRST|AFTER", isCreate = false)
        }
        colPosition = Some(spec)
      }
    }

    // Add the 'DEFAULT expression' clause in the column definition, if any, to the column metadata.
    val defaultExpr = defaultExpression.map(visitDefaultExpression).map { field =>
      if (conf.getConf(SQLConf.ENABLE_DEFAULT_COLUMNS)) {
        field.originalSQL
      } else {
        throw QueryParsingErrors.defaultColumnNotEnabledError(ctx)
      }
    }

    QualifiedColType(
      path = if (name.length > 1) Some(UnresolvedFieldName(name.init)) else None,
      colName = name.last,
      dataType = typedVisit[DataType](ctx.dataType),
      nullable = nullable,
      comment = commentSpec.map(visitCommentSpec),
      position = colPosition.map( pos =>
        UnresolvedFieldPosition(typedVisit[ColumnPosition](pos))),
      default = defaultExpr)
  }

  /**
   * Parse a [[AddColumns]] command.
   *
   * For example:
   * {{{
   *   ALTER TABLE table1
   *   ADD COLUMNS (col_name data_type [COMMENT col_comment], ...);
   * }}}
   */
  override def visitAddTableColumns(ctx: AddTableColumnsContext): LogicalPlan = withOrigin(ctx) {
    val colToken = if (ctx.COLUMN() != null) "COLUMN" else "COLUMNS"
    AddColumns(
      createUnresolvedTable(ctx.identifierReference, s"ALTER TABLE ... ADD $colToken"),
      ctx.columns.qualifiedColTypeWithPosition.asScala.map(typedVisit[QualifiedColType]).toSeq
    )
  }

  /**
   * Parse a [[RenameColumn]] command.
   *
   * For example:
   * {{{
   *   ALTER TABLE table1 RENAME COLUMN a.b.c TO x
   * }}}
   */
  override def visitRenameTableColumn(
      ctx: RenameTableColumnContext): LogicalPlan = withOrigin(ctx) {
    RenameColumn(
      createUnresolvedTable(ctx.table, "ALTER TABLE ... RENAME COLUMN"),
      UnresolvedFieldName(typedVisit[Seq[String]](ctx.from)),
      ctx.to.getText)
  }

  /**
   * Parse a [[AlterColumn]] command to alter a column's property.
   *
   * For example:
   * {{{
   *   ALTER TABLE table1 ALTER COLUMN a.b.c TYPE bigint
   *   ALTER TABLE table1 ALTER COLUMN a.b.c SET NOT NULL
   *   ALTER TABLE table1 ALTER COLUMN a.b.c DROP NOT NULL
   *   ALTER TABLE table1 ALTER COLUMN a.b.c COMMENT 'new comment'
   *   ALTER TABLE table1 ALTER COLUMN a.b.c FIRST
   *   ALTER TABLE table1 ALTER COLUMN a.b.c AFTER x
   * }}}
   */
  override def visitAlterTableAlterColumn(
      ctx: AlterTableAlterColumnContext): LogicalPlan = withOrigin(ctx) {
    val action = ctx.alterColumnAction
    val verb = if (ctx.CHANGE != null) "CHANGE" else "ALTER"
    if (action == null) {
      operationNotAllowed(
        s"ALTER TABLE table $verb COLUMN requires a TYPE, a SET/DROP, a COMMENT, or a FIRST/AFTER",
        ctx)
    }
    val dataType = if (action.dataType != null) {
      Some(typedVisit[DataType](action.dataType))
    } else {
      None
    }
    val nullable = if (action.setOrDrop != null) {
      action.setOrDrop.getType match {
        case SqlBaseParser.SET => Some(false)
        case SqlBaseParser.DROP => Some(true)
      }
    } else {
      None
    }
    val comment = if (action.commentSpec != null) {
      Some(visitCommentSpec(action.commentSpec()))
    } else {
      None
    }
    val position = if (action.colPosition != null) {
      Some(UnresolvedFieldPosition(typedVisit[ColumnPosition](action.colPosition)))
    } else {
      None
    }
    val setDefaultExpression: Option[String] =
      if (action.defaultExpression != null) {
        Option(action.defaultExpression()).map(visitDefaultExpression).map(_.originalSQL)
      } else if (action.dropDefault != null) {
        Some("")
      } else {
        None
      }
    if (setDefaultExpression.isDefined && !conf.getConf(SQLConf.ENABLE_DEFAULT_COLUMNS)) {
      throw QueryParsingErrors.defaultColumnNotEnabledError(ctx)
    }

    assert(Seq(dataType, nullable, comment, position, setDefaultExpression)
      .count(_.nonEmpty) == 1)

    AlterColumn(
      createUnresolvedTable(ctx.table, s"ALTER TABLE ... $verb COLUMN"),
      UnresolvedFieldName(typedVisit[Seq[String]](ctx.column)),
      dataType = dataType,
      nullable = nullable,
      comment = comment,
      position = position,
      setDefaultExpression = setDefaultExpression)
  }

  /**
   * Parse a [[AlterColumn]] command. This is Hive SQL syntax.
   *
   * For example:
   * {{{
   *   ALTER TABLE table [PARTITION partition_spec]
   *   CHANGE [COLUMN] column_old_name column_new_name column_dataType [COMMENT column_comment]
   *   [FIRST | AFTER column_name];
   * }}}
   */
  override def visitHiveChangeColumn(ctx: HiveChangeColumnContext): LogicalPlan = withOrigin(ctx) {
    if (ctx.partitionSpec != null) {
      invalidStatement("ALTER TABLE ... PARTITION ... CHANGE COLUMN", ctx)
    }
    val columnNameParts = typedVisit[Seq[String]](ctx.colName)
    if (!conf.resolver(columnNameParts.last, ctx.colType().colName.getText)) {
      throw QueryParsingErrors.operationInHiveStyleCommandUnsupportedError("Renaming column",
        "ALTER COLUMN", ctx, Some("please run RENAME COLUMN instead"))
    }
    if (ctx.colType.NULL != null) {
      throw QueryParsingErrors.operationInHiveStyleCommandUnsupportedError(
        "NOT NULL", "ALTER COLUMN", ctx,
        Some("please run ALTER COLUMN ... SET/DROP NOT NULL instead"))
    }

    AlterColumn(
      createUnresolvedTable(ctx.table, "ALTER TABLE ... CHANGE COLUMN"),
      UnresolvedFieldName(columnNameParts),
      dataType = Option(ctx.colType().dataType()).map(typedVisit[DataType]),
      nullable = None,
      comment = Option(ctx.colType().commentSpec()).map(visitCommentSpec),
      position = Option(ctx.colPosition).map(
        pos => UnresolvedFieldPosition(typedVisit[ColumnPosition](pos))),
      setDefaultExpression = None)
  }

  override def visitHiveReplaceColumns(
      ctx: HiveReplaceColumnsContext): LogicalPlan = withOrigin(ctx) {
    if (ctx.partitionSpec != null) {
      invalidStatement("ALTER TABLE ... PARTITION ... REPLACE COLUMNS", ctx)
    }
    ReplaceColumns(
      createUnresolvedTable(ctx.table, "ALTER TABLE ... REPLACE COLUMNS"),
      ctx.columns.qualifiedColTypeWithPosition.asScala.map { colType =>
        val name = typedVisit[Seq[String]](colType.name)
        if (name.length > 1) {
          throw QueryParsingErrors.operationInHiveStyleCommandUnsupportedError(
            "Replacing with a nested column", "REPLACE COLUMNS", ctx)
        }
        var commentSpec: Option[CommentSpecContext] = None
        colType.colDefinitionDescriptorWithPosition.asScala.foreach { opt =>
          blockBang(opt.errorCapturingNot)

          if (opt.NULL != null) {
            throw QueryParsingErrors.operationInHiveStyleCommandUnsupportedError(
              "NOT NULL", "REPLACE COLUMNS", ctx)
          }
          if (opt.colPosition != null) {
            throw QueryParsingErrors.operationInHiveStyleCommandUnsupportedError(
              "Column position", "REPLACE COLUMNS", ctx)
          }
          if (Option(opt.defaultExpression()).map(visitDefaultExpression).isDefined) {
            throw QueryParsingErrors.defaultColumnNotImplementedYetError(ctx)
          }
          Option(opt.commentSpec()).foreach { spec =>
            if (commentSpec.isDefined) {
              throw QueryParsingErrors.duplicateTableColumnDescriptor(
                opt, name.last, "COMMENT", isCreate = false, alterType = "REPLACE")
            }
            commentSpec = Some(spec)
          }
        }
        QualifiedColType(
          path = None,
          colName = name.last,
          dataType = typedVisit[DataType](colType.dataType),
          nullable = true,
          comment = commentSpec.map(visitCommentSpec),
          position = None,
          default = None)
      }.toSeq
    )
  }

  /**
   * Parse a [[DropColumns]] command.
   *
   * For example:
   * {{{
   *   ALTER TABLE table1 DROP COLUMN a.b.c
   *   ALTER TABLE table1 DROP COLUMNS a.b.c, x, y
   * }}}
   */
  override def visitDropTableColumns(
      ctx: DropTableColumnsContext): LogicalPlan = withOrigin(ctx) {
    val ifExists = ctx.EXISTS() != null
    val columnsToDrop = ctx.columns.multipartIdentifier.asScala.map(typedVisit[Seq[String]])
    DropColumns(
      createUnresolvedTable(ctx.identifierReference, "ALTER TABLE ... DROP COLUMNS"),
      columnsToDrop.map(UnresolvedFieldName(_)).toSeq,
      ifExists)
  }

  /**
   * Parse a [[AlterTableClusterBy]] command.
   *
   * For example:
   * {{{
   *   ALTER TABLE table1 CLUSTER BY (a.b.c)
   *   ALTER TABLE table1 CLUSTER BY NONE
   * }}}
   */
  override def visitAlterClusterBy(ctx: AlterClusterByContext): LogicalPlan = withOrigin(ctx) {
    val table = createUnresolvedTable(ctx.identifierReference, "ALTER TABLE ... CLUSTER BY")
    if (ctx.NONE() != null) {
      AlterTableClusterBy(table, None)
    } else {
      assert(ctx.clusterBySpec() != null)
      AlterTableClusterBy(table, Some(visitClusterBySpec(ctx.clusterBySpec())))
    }
  }

  /**
   * Parse [[SetViewProperties]] or [[SetTableProperties]] commands.
   *
   * For example:
   * {{{
   *   ALTER TABLE table SET TBLPROPERTIES ('table_property' = 'property_value');
   *   ALTER VIEW view SET TBLPROPERTIES ('table_property' = 'property_value');
   * }}}
   */
  override def visitSetTableProperties(
      ctx: SetTablePropertiesContext): LogicalPlan = withOrigin(ctx) {
    val properties = visitPropertyKeyValues(ctx.propertyList)
    val cleanedTableProperties = cleanTableProperties(ctx, properties)
    if (ctx.VIEW != null) {
      SetViewProperties(
        createUnresolvedView(
          ctx.identifierReference,
          commandName = "ALTER VIEW ... SET TBLPROPERTIES",
          allowTemp = false,
          suggestAlternative = true),
        cleanedTableProperties)
    } else {
      SetTableProperties(
        createUnresolvedTable(
          ctx.identifierReference,
          "ALTER TABLE ... SET TBLPROPERTIES",
          true),
        cleanedTableProperties)
    }
  }

  /**
   * Parse [[UnsetViewProperties]] or [[UnsetTableProperties]] commands.
   *
   * For example:
   * {{{
   *   ALTER TABLE table UNSET TBLPROPERTIES [IF EXISTS] ('comment', 'key');
   *   ALTER VIEW view UNSET TBLPROPERTIES [IF EXISTS] ('comment', 'key');
   * }}}
   */
  override def visitUnsetTableProperties(
      ctx: UnsetTablePropertiesContext): LogicalPlan = withOrigin(ctx) {
    val properties = visitPropertyKeys(ctx.propertyList)
    val cleanedProperties = cleanTableProperties(ctx, properties.map(_ -> "").toMap).keys.toSeq

    val ifExists = ctx.EXISTS != null
    if (ctx.VIEW != null) {
      UnsetViewProperties(
        createUnresolvedView(
          ctx.identifierReference,
          commandName = "ALTER VIEW ... UNSET TBLPROPERTIES",
          allowTemp = false,
          suggestAlternative = true),
        cleanedProperties,
        ifExists)
    } else {
      UnsetTableProperties(
        createUnresolvedTable(
          ctx.identifierReference,
          "ALTER TABLE ... UNSET TBLPROPERTIES",
          true),
        cleanedProperties,
        ifExists)
    }
  }

  /**
   * Create an [[SetTableLocation]] command.
   *
   * For example:
   * {{{
   *   ALTER TABLE table_name [PARTITION partition_spec] SET LOCATION "loc";
   * }}}
   */
  override def visitSetTableLocation(ctx: SetTableLocationContext): LogicalPlan = withOrigin(ctx) {
    SetTableLocation(
      createUnresolvedTable(
        ctx.identifierReference,
        "ALTER TABLE ... SET LOCATION ..."),
      Option(ctx.partitionSpec).map(visitNonOptionalPartitionSpec),
      visitLocationSpec(ctx.locationSpec))
  }

  /**
   * Create a [[DescribeColumn]] or [[DescribeRelation]] commands.
   */
  override def visitDescribeRelation(ctx: DescribeRelationContext): LogicalPlan = withOrigin(ctx) {
    val isExtended = ctx.EXTENDED != null || ctx.FORMATTED != null
    val relation = createUnresolvedTableOrView(ctx.identifierReference, "DESCRIBE TABLE")
    if (ctx.describeColName != null) {
      if (ctx.partitionSpec != null) {
        throw QueryParsingErrors.descColumnForPartitionUnsupportedError(ctx)
      } else {
        DescribeColumn(
          relation,
          UnresolvedAttribute(ctx.describeColName.nameParts.asScala.map(_.getText).toSeq),
          isExtended)
      }
    } else {
      val partitionSpec = if (ctx.partitionSpec != null) {
        // According to the syntax, visitPartitionSpec returns `Map[String, Option[String]]`.
        visitPartitionSpec(ctx.partitionSpec).map {
          case (key, Some(value)) => key -> value
          case (key, _) =>
            throw QueryParsingErrors.emptyPartitionKeyError(key, ctx.partitionSpec)
        }
      } else {
        Map.empty[String, String]
      }
      DescribeRelation(relation, partitionSpec, isExtended)
    }
  }

  /**
   * Create an [[AnalyzeTable]], or an [[AnalyzeColumn]].
   * Example SQL for analyzing a table or a set of partitions :
   * {{{
   *   ANALYZE TABLE multi_part_name [PARTITION (partcol1[=val1], partcol2[=val2], ...)]
   *   COMPUTE STATISTICS [NOSCAN];
   * }}}
   *
   * Example SQL for analyzing columns :
   * {{{
   *   ANALYZE TABLE multi_part_name COMPUTE STATISTICS FOR COLUMNS column1, column2;
   * }}}
   *
   * Example SQL for analyzing all columns of a table:
   * {{{
   *   ANALYZE TABLE multi_part_name COMPUTE STATISTICS FOR ALL COLUMNS;
   * }}}
   */
  override def visitAnalyze(ctx: AnalyzeContext): LogicalPlan = withOrigin(ctx) {
    def checkPartitionSpec(): Unit = {
      if (ctx.partitionSpec != null) {
        logWarning(
          log"Partition specification is ignored when collecting column statistics: " +
            log"${MDC(PARTITION_SPECIFICATION, ctx.partitionSpec.getText)}")
      }
    }
    if (ctx.identifier != null &&
        ctx.identifier.getText.toLowerCase(Locale.ROOT) != "noscan") {
      throw QueryParsingErrors.computeStatisticsNotExpectedError(ctx.identifier())
    }

    if (ctx.ALL() != null) {
      checkPartitionSpec()
      AnalyzeColumn(
        createUnresolvedTableOrView(ctx.identifierReference, "ANALYZE TABLE ... FOR ALL COLUMNS"),
        None,
        allColumns = true)
    } else if (ctx.identifierSeq() == null) {
      val partitionSpec = if (ctx.partitionSpec != null) {
        visitPartitionSpec(ctx.partitionSpec)
      } else {
        Map.empty[String, Option[String]]
      }
      AnalyzeTable(
        createUnresolvedTableOrView(
          ctx.identifierReference,
          "ANALYZE TABLE",
          allowTempView = false),
        partitionSpec,
        noScan = ctx.identifier != null)
    } else {
      checkPartitionSpec()
      AnalyzeColumn(
        createUnresolvedTableOrView(ctx.identifierReference, "ANALYZE TABLE ... FOR COLUMNS ..."),
        Option(visitIdentifierSeq(ctx.identifierSeq())),
        allColumns = false)
    }
  }

  /**
   * Create an [[AnalyzeTables]].
   * Example SQL for analyzing all tables in default database:
   * {{{
   *   ANALYZE TABLES IN default COMPUTE STATISTICS;
   * }}}
   */
  override def visitAnalyzeTables(ctx: AnalyzeTablesContext): LogicalPlan = withOrigin(ctx) {
    if (ctx.identifier != null &&
      ctx.identifier.getText.toLowerCase(Locale.ROOT) != "noscan") {
      throw QueryParsingErrors.computeStatisticsNotExpectedError(ctx.identifier())
    }
    val ns = if (ctx.identifierReference() != null) {
      withIdentClause(ctx.identifierReference, UnresolvedNamespace(_))
    } else {
      CurrentNamespace
    }
    AnalyzeTables(ns, noScan = ctx.identifier != null)
  }

  /**
   * Create a [[RepairTable]].
   *
   * For example:
   * {{{
   *   [MSCK] REPAIR TABLE multi_part_name [{ADD|DROP|SYNC} PARTITIONS]
   * }}}
   */
  override def visitRepairTable(ctx: RepairTableContext): LogicalPlan = withOrigin(ctx) {
    val (enableAddPartitions, enableDropPartitions, option) =
      if (ctx.SYNC() != null) {
        (true, true, " ... SYNC PARTITIONS")
      } else if (ctx.DROP() != null) {
        (false, true, " ... DROP PARTITIONS")
      } else if (ctx.ADD() != null) {
        (true, false, " ... ADD PARTITIONS")
      } else {
        (true, false, "")
      }
    RepairTable(
      createUnresolvedTable(ctx.identifierReference, s"MSCK REPAIR TABLE$option"),
      enableAddPartitions,
      enableDropPartitions)
  }

  /**
   * Create a [[LoadData]].
   *
   * For example:
   * {{{
   *   LOAD DATA [LOCAL] INPATH 'filepath' [OVERWRITE] INTO TABLE multi_part_name
   *   [PARTITION (partcol1=val1, partcol2=val2 ...)]
   * }}}
   */
  override def visitLoadData(ctx: LoadDataContext): LogicalPlan = withOrigin(ctx) {
    LoadData(
      child = createUnresolvedTable(ctx.identifierReference, "LOAD DATA"),
      path = string(visitStringLit(ctx.path)),
      isLocal = ctx.LOCAL != null,
      isOverwrite = ctx.OVERWRITE != null,
      partition = Option(ctx.partitionSpec).map(visitNonOptionalPartitionSpec)
    )
  }

  /**
   * Creates a [[ShowCreateTable]]
   */
  override def visitShowCreateTable(ctx: ShowCreateTableContext): LogicalPlan = withOrigin(ctx) {
    ShowCreateTable(
      createUnresolvedTableOrView(
        ctx.identifierReference,
        "SHOW CREATE TABLE",
        allowTempView = false),
      ctx.SERDE != null)
  }

  /**
   * Create a [[CacheTable]] or [[CacheTableAsSelect]].
   *
   * For example:
   * {{{
   *   CACHE [LAZY] TABLE multi_part_name
   *   [OPTIONS tablePropertyList] [[AS] query]
   * }}}
   */
  override def visitCacheTable(ctx: CacheTableContext): LogicalPlan = withOrigin(ctx) {
    import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._

    val query = Option(ctx.query).map(plan)
    withIdentClause(ctx.identifierReference, ident => {
      if (query.isDefined && ident.length > 1) {
        val catalogAndNamespace = ident.init
        throw QueryParsingErrors.addCatalogInCacheTableAsSelectNotAllowedError(
          catalogAndNamespace.quoted, ctx)
      }
      val options = Option(ctx.options).map(visitPropertyKeyValues).getOrElse(Map.empty)
      val isLazy = ctx.LAZY != null
      if (query.isDefined) {
        // Disallow parameter markers in the query of the cache.
        // We need this limitation because we store the original query text, pre substitution.
        // To lift this we would need to reconstitute the query with parameter markers replaced with
        // the values given at CACHE TABLE time, or we would need to store the parameter values
        // alongside the text.
        // The same rule can be found in CREATE VIEW builder.
        checkInvalidParameter(query.get, "the query of CACHE TABLE")
        CacheTableAsSelect(ident.head, query.get, source(ctx.query()), isLazy, options)
      } else {
        CacheTable(
          createUnresolvedRelation(ctx.identifierReference, ident, None, writePrivileges = Nil),
          ident, isLazy, options)
      }
    })
  }

  /**
   * Create an [[UncacheTable]] logical plan.
   */
  override def visitUncacheTable(ctx: UncacheTableContext): LogicalPlan = withOrigin(ctx) {
    UncacheTable(createUnresolvedRelation(ctx.identifierReference), ctx.EXISTS != null)
  }

  /**
   * Create a [[TruncateTable]] command.
   *
   * For example:
   * {{{
   *   TRUNCATE TABLE multi_part_name [PARTITION (partcol1=val1, partcol2=val2 ...)]
   * }}}
   */
  override def visitTruncateTable(ctx: TruncateTableContext): LogicalPlan = withOrigin(ctx) {
    val table = createUnresolvedTable(ctx.identifierReference, "TRUNCATE TABLE")
    Option(ctx.partitionSpec).map { spec =>
      TruncatePartition(table, UnresolvedPartitionSpec(visitNonOptionalPartitionSpec(spec)))
    }.getOrElse(TruncateTable(table))
  }

  /**
   * A command for users to list the partition names of a table. If partition spec is specified,
   * partitions that match the spec are returned. Otherwise an empty result set is returned.
   *
   * This function creates a [[ShowPartitions]] logical plan
   *
   * The syntax of using this command in SQL is:
   * {{{
   *   SHOW PARTITIONS multi_part_name [partition_spec];
   * }}}
   */
  override def visitShowPartitions(ctx: ShowPartitionsContext): LogicalPlan = withOrigin(ctx) {
    val partitionKeys = Option(ctx.partitionSpec).map { specCtx =>
      UnresolvedPartitionSpec(visitNonOptionalPartitionSpec(specCtx), None)
    }
    ShowPartitions(
      createUnresolvedTable(ctx.identifierReference, "SHOW PARTITIONS"),
      partitionKeys)
  }

  /**
   * Create a [[RefreshTable]].
   *
   * For example:
   * {{{
   *   REFRESH TABLE multi_part_name
   * }}}
   */
  override def visitRefreshTable(ctx: RefreshTableContext): LogicalPlan = withOrigin(ctx) {
    RefreshTable(createUnresolvedTableOrView(ctx.identifierReference, "REFRESH TABLE"))
  }

  /**
   * A command for users to list the column names for a table.
   * This function creates a [[ShowColumns]] logical plan.
   *
   * The syntax of using this command in SQL is:
   * {{{
   *   SHOW COLUMNS (FROM | IN) tableName=multipartIdentifier
   *        ((FROM | IN) namespace=multipartIdentifier)?
   * }}}
   */
  override def visitShowColumns(ctx: ShowColumnsContext): LogicalPlan = withOrigin(ctx) {
    withIdentClause(ctx.table, ident => {
      val table = createUnresolvedTableOrView(
        ctx.table,
        ident,
        "SHOW COLUMNS",
        allowTempView = true)
      val namespace = Option(ctx.ns).map(visitMultipartIdentifier)
      // Use namespace only if table name doesn't specify it. If namespace is already specified
      // in the table name, it's checked against the given namespace after table/view is resolved.
      val tableWithNamespace = if (namespace.isDefined && table.multipartIdentifier.length == 1) {
        CurrentOrigin.withOrigin(table.origin) {
          table.copy(multipartIdentifier = namespace.get ++ table.multipartIdentifier)
        }
      } else {
        table
      }
      ShowColumns(tableWithNamespace, namespace)
    })
  }

  /**
   * Create an [[RecoverPartitions]]
   *
   * For example:
   * {{{
   *   ALTER TABLE multi_part_name RECOVER PARTITIONS;
   * }}}
   */
  override def visitRecoverPartitions(
      ctx: RecoverPartitionsContext): LogicalPlan = withOrigin(ctx) {
    RecoverPartitions(
      createUnresolvedTable(
        ctx.identifierReference,
        "ALTER TABLE ... RECOVER PARTITIONS"))
  }

  /**
   * Create an [[AddPartitions]].
   *
   * For example:
   * {{{
   *   ALTER TABLE multi_part_name ADD [IF NOT EXISTS] PARTITION spec [LOCATION 'loc1']
   *   ALTER VIEW multi_part_name ADD [IF NOT EXISTS] PARTITION spec
   * }}}
   *
   * ALTER VIEW ... ADD PARTITION ... is not supported because the concept of partitioning
   * is associated with physical tables
   */
  override def visitAddTablePartition(
      ctx: AddTablePartitionContext): LogicalPlan = withOrigin(ctx) {
    if (ctx.VIEW != null) {
      invalidStatement("ALTER VIEW ... ADD PARTITION", ctx)
    }
    // Create partition spec to location mapping.
    val specsAndLocs = ctx.partitionSpecLocation.asScala.map { splCtx =>
      val spec = visitNonOptionalPartitionSpec(splCtx.partitionSpec)
      val location = Option(splCtx.locationSpec).map(visitLocationSpec)
      UnresolvedPartitionSpec(spec, location)
    }
    blockBang(ctx.errorCapturingNot)
    AddPartitions(
      createUnresolvedTable(
        ctx.identifierReference,
        "ALTER TABLE ... ADD PARTITION ..."),
      specsAndLocs.toSeq,
      ctx.EXISTS != null)
  }

  /**
   * Create an [[RenamePartitions]]
   *
   * For example:
   * {{{
   *   ALTER TABLE multi_part_name PARTITION spec1 RENAME TO PARTITION spec2;
   * }}}
   */
  override def visitRenameTablePartition(
      ctx: RenameTablePartitionContext): LogicalPlan = withOrigin(ctx) {
    RenamePartitions(
      createUnresolvedTable(
        ctx.identifierReference,
        "ALTER TABLE ... RENAME TO PARTITION"),
      UnresolvedPartitionSpec(visitNonOptionalPartitionSpec(ctx.from)),
      UnresolvedPartitionSpec(visitNonOptionalPartitionSpec(ctx.to)))
  }

  /**
   * Create an [[DropPartitions]]
   *
   * For example:
   * {{{
   *   ALTER TABLE multi_part_name DROP [IF EXISTS] PARTITION spec1[, PARTITION spec2, ...]
   *     [PURGE];
   *   ALTER VIEW view DROP [IF EXISTS] PARTITION spec1[, PARTITION spec2, ...];
   * }}}
   *
   * ALTER VIEW ... DROP PARTITION ... is not supported because the concept of partitioning
   * is associated with physical tables
   */
  override def visitDropTablePartitions(
      ctx: DropTablePartitionsContext): LogicalPlan = withOrigin(ctx) {
    if (ctx.VIEW != null) {
      invalidStatement("ALTER VIEW ... DROP PARTITION", ctx)
    }
    val partSpecs = ctx.partitionSpec.asScala.map(visitNonOptionalPartitionSpec)
      .map(spec => UnresolvedPartitionSpec(spec))
    DropPartitions(
      createUnresolvedTable(
        ctx.identifierReference,
        "ALTER TABLE ... DROP PARTITION ..."),
      partSpecs.toSeq,
      ifExists = ctx.EXISTS != null,
      purge = ctx.PURGE != null)
  }

  /**
   * Create an [[SetTableSerDeProperties]]
   *
   * For example:
   * {{{
   *   ALTER TABLE multi_part_name [PARTITION spec] SET SERDE serde_name
   *     [WITH SERDEPROPERTIES props];
   *   ALTER TABLE multi_part_name [PARTITION spec] SET SERDEPROPERTIES serde_properties;
   * }}}
   */
  override def visitSetTableSerDe(ctx: SetTableSerDeContext): LogicalPlan = withOrigin(ctx) {
    SetTableSerDeProperties(
      createUnresolvedTable(
        ctx.identifierReference,
        "ALTER TABLE ... SET [SERDE|SERDEPROPERTIES]",
        true),
      Option(ctx.stringLit).map(x => string(visitStringLit(x))),
      Option(ctx.propertyList).map(visitPropertyKeyValues),
      // TODO a partition spec is allowed to have optional values. This is currently violated.
      Option(ctx.partitionSpec).map(visitNonOptionalPartitionSpec))
  }

  /**
   * Alter the query of a view. This creates a [[AlterViewAs]]
   *
   * For example:
   * {{{
   *   ALTER VIEW multi_part_name AS SELECT ...;
   * }}}
   */
  override def visitAlterViewQuery(ctx: AlterViewQueryContext): LogicalPlan = withOrigin(ctx) {
    AlterViewAs(
      createUnresolvedView(ctx.identifierReference, "ALTER VIEW ... AS"),
      originalText = source(ctx.query),
      query = plan(ctx.query))
  }

  /**
   * Defined the schema binding mode during CREATE or ALTER VIEW.
   * The method also accept a NULL context, in which case it will return the session default
   *
   * {{{
   *   WITH SCHEMA [ BINDING | COMPENSATION | TYPE EVOLUTION | EVOLUTION ]
   * }}}
   */
  override def visitSchemaBinding(ctx: SchemaBindingContext): ViewSchemaMode = {
    if (ctx == null) {
      // No schema binding specified, return the session default
      if (conf.viewSchemaBindingEnabled) {
        if (conf.viewSchemaCompensation) {
          SchemaCompensation
        } else {
          SchemaBinding
        }
      } else {
        SchemaUnsupported
      }
    } else if (!conf.viewSchemaBindingEnabled) {
      // If the feature is disabled, throw an exception
      withOrigin(ctx) {
        throw new ParseException(
          errorClass = "FEATURE_NOT_ENABLED",
          messageParameters = Map("featureName" -> "VIEW ... WITH SCHEMA ...",
            "configKey" -> "spark.sql.legacy.viewSchemaBindingMode",
            "configValue" -> "true"),
          ctx)
      }
    } else if (ctx.COMPENSATION != null) {
      SchemaCompensation
    } else if (ctx.TYPE != null) {
      SchemaTypeEvolution
    } else if (ctx.EVOLUTION != null) {
      SchemaEvolution
    } else {
      SchemaBinding
    }
  }

  /**
   * Alter the schema binding of a view. This creates a [[AlterViewSchemaBinding]]
   *
   * For example:
   * {{{
   *   ALTER VIEW multi_part_name WITH SCHEMA ...;
   * }}}
   */
  override def visitAlterViewSchemaBinding(ctx: AlterViewSchemaBindingContext): LogicalPlan
  = withOrigin(ctx) {
    AlterViewSchemaBinding(
      createUnresolvedView(ctx.identifierReference, "ALTER VIEW ... WITH SCHEMA ..."),
      viewSchemaMode = visitSchemaBinding(ctx.schemaBinding))
  }

  /**
   * Create a [[RenameTable]] command.
   *
   * For example:
   * {{{
   *   ALTER TABLE multi_part_name1 RENAME TO multi_part_name2;
   *   ALTER VIEW multi_part_name1 RENAME TO multi_part_name2;
   * }}}
   */
  override def visitRenameTable(ctx: RenameTableContext): LogicalPlan = withOrigin(ctx) {
    val isView = ctx.VIEW != null
    val relationStr = if (isView) "VIEW" else "TABLE"
    RenameTable(
      createUnresolvedTableOrView(ctx.from, s"ALTER $relationStr ... RENAME TO"),
      visitMultipartIdentifier(ctx.to),
      isView)
  }

  /**
   * A command for users to list the properties for a table. If propertyKey is specified, the value
   * for the propertyKey is returned. If propertyKey is not specified, all the keys and their
   * corresponding values are returned.
   * The syntax of using this command in SQL is:
   * {{{
   *   SHOW TBLPROPERTIES multi_part_name[('propertyKey')];
   * }}}
   */
  override def visitShowTblProperties(
      ctx: ShowTblPropertiesContext): LogicalPlan = withOrigin(ctx) {
    ShowTableProperties(
      createUnresolvedTableOrView(ctx.table, "SHOW TBLPROPERTIES"),
      Option(ctx.key).map(visitPropertyKey))
  }

  /**
   * Create a plan for a DESCRIBE FUNCTION statement.
   */
  override def visitDescribeFunction(ctx: DescribeFunctionContext): LogicalPlan = withOrigin(ctx) {
    import ctx._

    if (describeFuncName.identifierReference() == null) {
      val functionName =
        if (describeFuncName.stringLit() != null) {
          Seq(string(visitStringLit(describeFuncName.stringLit())))
        } else {
          Seq(describeFuncName.getText)
        }
      DescribeFunction(
        createUnresolvedFunctionName(
          ctx.describeFuncName(),
          functionName,
          "DESCRIBE FUNCTION",
          requirePersistent = false,
          funcTypeMismatchHint = None),
        EXTENDED != null)
    } else {
      DescribeFunction(
        withIdentClause(
          describeFuncName.identifierReference(),
          createUnresolvedFunctionName(
            describeFuncName.identifierReference,
            _,
            "DESCRIBE FUNCTION",
            requirePersistent = false,
            funcTypeMismatchHint = None)),
        EXTENDED != null)
    }
  }

  /**
   * Create a plan for a SHOW FUNCTIONS command.
   */
  override def visitShowFunctions(ctx: ShowFunctionsContext): LogicalPlan = withOrigin(ctx) {
    val (userScope, systemScope) = Option(ctx.identifier)
      .map(_.getText.toLowerCase(Locale.ROOT)) match {
        case None | Some("all") => (true, true)
        case Some("system") => (false, true)
        case Some("user") => (true, false)
        case Some(x) => throw QueryParsingErrors.showFunctionsUnsupportedError(x, ctx.identifier())
    }

    val legacy = Option(ctx.legacy).map(visitMultipartIdentifier)
    val pattern = Option(ctx.pattern).map(x => string(visitStringLit(x))).orElse(legacy.map(_.last))

    if (ctx.ns != null) {
      if (legacy.isDefined) {
        throw QueryParsingErrors.showFunctionsInvalidPatternError(ctx.legacy.getText, ctx.legacy)
      }
      ShowFunctions(
        withIdentClause(ctx.ns, UnresolvedNamespace(_)),
        userScope, systemScope, pattern)
    } else if (legacy.isDefined) {
      val ns = if (legacy.get.length > 1) {
        UnresolvedNamespace(legacy.get.dropRight(1))
      } else {
        CurrentNamespace
      }
      ShowFunctions(ns, userScope, systemScope, pattern)
    } else {
      ShowFunctions(CurrentNamespace, userScope, systemScope, pattern)
    }
  }

  override def visitRefreshFunction(ctx: RefreshFunctionContext): LogicalPlan = withOrigin(ctx) {
    RefreshFunction(
      withIdentClause(
        ctx.identifierReference,
        createUnresolvedFunctionName(
          ctx.identifierReference,
          _,
          "REFRESH FUNCTION",
          requirePersistent = true,
          funcTypeMismatchHint = None)))
  }

  override def visitCommentNamespace(ctx: CommentNamespaceContext): LogicalPlan = withOrigin(ctx) {
    val comment = visitComment(ctx.comment)
    CommentOnNamespace(withIdentClause(ctx.identifierReference, UnresolvedNamespace(_)), comment)
  }

  override def visitCommentTable(ctx: CommentTableContext): LogicalPlan = withOrigin(ctx) {
    val comment = visitComment(ctx.comment)
    CommentOnTable(createUnresolvedTable(ctx.identifierReference, "COMMENT ON TABLE"), comment)
  }

  override def visitComment (ctx: CommentContext): String = {
    Option(ctx.stringLit()).map(s => string(visitStringLit(s))).getOrElse("")
  }

  /**
   * Create an index, returning a [[CreateIndex]] logical plan.
   * For example:
   * {{{
   * CREATE INDEX index_name ON [TABLE] table_name [USING index_type] (column_index_property_list)
   *   [OPTIONS indexPropertyList]
   *   column_index_property_list: column_name [OPTIONS(indexPropertyList)]  [ ,  . . . ]
   *   indexPropertyList: index_property_name [= index_property_value] [ ,  . . . ]
   * }}}
   */
  override def visitCreateIndex(ctx: CreateIndexContext): LogicalPlan = withOrigin(ctx) {
    val (indexName, indexType) = if (ctx.identifier.size() == 1) {
      (ctx.identifier(0).getText, "")
    } else {
      (ctx.identifier(0).getText, ctx.identifier(1).getText)
    }

    val columns = ctx.columns.multipartIdentifierProperty.asScala
      .map(_.multipartIdentifier).map(typedVisit[Seq[String]]).toSeq
    val columnsProperties = ctx.columns.multipartIdentifierProperty.asScala
      .map(x => (Option(x.options).map(visitPropertyKeyValues).getOrElse(Map.empty))).toSeq
    val options = Option(ctx.options).map(visitPropertyKeyValues).getOrElse(Map.empty)

    blockBang(ctx.errorCapturingNot)

    CreateIndex(
      createUnresolvedTable(ctx.identifierReference, "CREATE INDEX"),
      indexName,
      indexType,
      ctx.EXISTS != null,
      columns.map(UnresolvedFieldName(_)).zip(columnsProperties),
      options)
  }

  /**
   * Drop an index, returning a [[DropIndex]] logical plan.
   * For example:
   * {{{
   *   DROP INDEX [IF EXISTS] index_name ON [TABLE] table_name
   * }}}
   */
  override def visitDropIndex(ctx: DropIndexContext): LogicalPlan = withOrigin(ctx) {
    val indexName = ctx.identifier.getText
    DropIndex(
      createUnresolvedTable(ctx.identifierReference, "DROP INDEX"),
      indexName,
      ctx.EXISTS != null)
  }

  /**
   * Create a TimestampAdd expression.
   */
  override def visitTimestampadd(ctx: TimestampaddContext): Expression = withOrigin(ctx) {
    if (ctx.invalidUnit != null) {
      throw QueryParsingErrors.invalidDatetimeUnitError(
        ctx,
        ctx.name.getText,
        ctx.invalidUnit.getText)
    } else {
      TimestampAdd(ctx.unit.getText, expression(ctx.unitsAmount), expression(ctx.timestamp))
    }
  }

  /**
   * Create a TimestampDiff expression.
   */
  override def visitTimestampdiff(ctx: TimestampdiffContext): Expression = withOrigin(ctx) {
    if (ctx.invalidUnit != null) {
      throw QueryParsingErrors.invalidDatetimeUnitError(
        ctx,
        ctx.name.getText,
        ctx.invalidUnit.getText)
    } else {
      TimestampDiff(ctx.unit.getText, expression(ctx.startTimestamp), expression(ctx.endTimestamp))
    }
  }

  /**
   * Create a named parameter which represents a literal with a non-bound value and unknown type.
   * */
  override def visitNamedParameterLiteral(
      ctx: NamedParameterLiteralContext): Expression = withOrigin(ctx) {
    NamedParameter(ctx.identifier().getText)
  }

  /**
   * Create a positional parameter which represents a literal
   * with a non-bound value and unknown type.
   * */
  override def visitPosParameterLiteral(
      ctx: PosParameterLiteralContext): Expression = withOrigin(ctx) {
    PosParameter(ctx.QUESTION().getSymbol.getStartIndex)
  }

  /**
   * Create a [[CreateVariable]] command.
   *
   * For example:
   * {{{
   *   DECLARE [OR REPLACE] [VARIABLE] [db_name.]variable_name
   *   [dataType] [defaultExpression];
   * }}}
   *
   * We will add CREATE VARIABLE for persisted variable definitions to this, hence the name.
   */
  override def visitCreateVariable(ctx: CreateVariableContext): LogicalPlan = withOrigin(ctx) {
    val dataTypeOpt = Option(ctx.dataType()).map(typedVisit[DataType])
    val defaultExpression = if (ctx.variableDefaultExpression() == null) {
      if (dataTypeOpt.isEmpty) {
        throw new ParseException(
          errorClass = "INVALID_SQL_SYNTAX.VARIABLE_TYPE_OR_DEFAULT_REQUIRED",
          messageParameters = Map.empty,
          ctx.identifierReference)
      }
      DefaultValueExpression(Literal(null, dataTypeOpt.get), "null")
    } else {
      val default = visitVariableDefaultExpression(ctx.variableDefaultExpression())
      dataTypeOpt.map { dt => default.copy(child = Cast(default.child, dt)) }.getOrElse(default)
    }
    CreateVariable(
      withIdentClause(ctx.identifierReference(), UnresolvedIdentifier(_)),
      defaultExpression,
      ctx.REPLACE() != null
    )
  }

  /**
   * Create a [[DropVariable]] command.
   *
   * For example:
   * {{{
   *   DROP TEMPORARY VARIABLE [IF EXISTS] variable;
   * }}}
   */
  override def visitDropVariable(ctx: DropVariableContext): LogicalPlan = withOrigin(ctx) {
    DropVariable(
      withIdentClause(ctx.identifierReference(), UnresolvedIdentifier(_)),
      ctx.EXISTS() != null
    )
  }

  private def visitSetVariableImpl(
      query: QueryContext,
      multipartIdentifierList: MultipartIdentifierListContext,
      assignmentList: AssignmentListContext): LogicalPlan = {
    if (query != null) {
      // The SET variable source is a query
      val variables = multipartIdentifierList.multipartIdentifier.asScala.map { variableIdent =>
        val varName = visitMultipartIdentifier(variableIdent)
        UnresolvedAttribute(varName)
      }.toSeq
      SetVariable(variables, visitQuery(query))
    } else {
      // The SET variable source is list of expressions.
      val (variables, values) = assignmentList.assignment().asScala.map { assign =>
        val varIdent = visitMultipartIdentifier(assign.key)
        val varExpr = expression(assign.value)
        val varNamedExpr = varExpr match {
          case n: NamedExpression => n
          case e => Alias(e, varIdent.last)()
        }
        (UnresolvedAttribute(varIdent), varNamedExpr)
      }.toSeq.unzip
      SetVariable(variables, Project(values, OneRowRelation()))
    }
  }

  /**
   * Create a [[SetVariable]] command.
   *
   * For example:
   * {{{
   *   SET VARIABLE var1 = v1, var2 = v2, ...
   *   SET VARIABLE (var1, var2, ...) = (SELECT ...)
   * }}}
   */
  override def visitSetVariable(ctx: SetVariableContext): LogicalPlan = withOrigin(ctx) {
    visitSetVariableImpl(ctx.query(), ctx.multipartIdentifierList(), ctx.assignmentList())
  }

  override def visitSetVariableWithOptionalKeyword(
      ctx: SetVariableWithOptionalKeywordContext): LogicalPlan =
    withOrigin(ctx) {
      visitSetVariableImpl(ctx.query(), ctx.multipartIdentifierList(), ctx.assignmentList())
    }

  /**
   * Check plan for any parameters.
   * If it finds any throws UNSUPPORTED_FEATURE.PARAMETER_MARKER_IN_UNEXPECTED_STATEMENT.
   * This method is used to ban parameters in some contexts.
   */
  protected def checkInvalidParameter(plan: LogicalPlan, statement: String): Unit = {
    plan.foreach { p =>
      p.expressions.foreach { expr =>
        if (expr.containsPattern(PARAMETER)) {
          throw QueryParsingErrors.parameterMarkerNotAllowed(statement, p.origin)
        }
      }
    }
    plan.children.foreach(p => checkInvalidParameter(p, statement))
    plan.innerChildren.collect {
      case child: LogicalPlan => checkInvalidParameter(child, statement)
    }
  }
}
