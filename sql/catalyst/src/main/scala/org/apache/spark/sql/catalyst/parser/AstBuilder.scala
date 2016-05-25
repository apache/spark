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

import java.sql.{Date, Timestamp}

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import org.antlr.v4.runtime.{ParserRuleContext, Token}
import org.antlr.v4.runtime.tree.{ParseTree, RuleNode, TerminalNode}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.{FunctionIdentifier, InternalRow, TableIdentifier}
import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.parser.SqlBaseParser._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.CalendarInterval
import org.apache.spark.util.random.RandomSampler

/**
 * The AstBuilder converts an ANTLR4 ParseTree into a catalyst Expression, LogicalPlan or
 * TableIdentifier.
 */
class AstBuilder extends SqlBaseBaseVisitor[AnyRef] with Logging {
  import ParserUtils._

  protected def typedVisit[T](ctx: ParseTree): T = {
    ctx.accept(this).asInstanceOf[T]
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

  override def visitSingleStatement(ctx: SingleStatementContext): LogicalPlan = withOrigin(ctx) {
    visit(ctx.statement).asInstanceOf[LogicalPlan]
  }

  override def visitSingleExpression(ctx: SingleExpressionContext): Expression = withOrigin(ctx) {
    visitNamedExpression(ctx.namedExpression)
  }

  override def visitSingleTableIdentifier(
      ctx: SingleTableIdentifierContext): TableIdentifier = withOrigin(ctx) {
    visitTableIdentifier(ctx.tableIdentifier)
  }

  override def visitSingleDataType(ctx: SingleDataTypeContext): DataType = withOrigin(ctx) {
    visit(ctx.dataType).asInstanceOf[DataType]
  }

  /* ********************************************************************************************
   * Plan parsing
   * ******************************************************************************************** */
  protected def plan(tree: ParserRuleContext): LogicalPlan = typedVisit(tree)

  /**
   * Create a top-level plan with Common Table Expressions.
   */
  override def visitQuery(ctx: QueryContext): LogicalPlan = withOrigin(ctx) {
    val query = plan(ctx.queryNoWith)

    // Apply CTEs
    query.optional(ctx.ctes) {
      val ctes = ctx.ctes.namedQuery.asScala.map {
        case nCtx =>
          val namedQuery = visitNamedQuery(nCtx)
          (namedQuery.alias, namedQuery)
      }
      // Check for duplicate names.
      checkDuplicateKeys(ctes, ctx)
      With(query, ctes.toMap)
    }
  }

  /**
   * Create a named logical plan.
   *
   * This is only used for Common Table Expressions.
   */
  override def visitNamedQuery(ctx: NamedQueryContext): SubqueryAlias = withOrigin(ctx) {
    SubqueryAlias(ctx.name.getText, plan(ctx.queryNoWith))
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
    val inserts = ctx.multiInsertQueryBody.asScala.map {
      body =>
        assert(body.querySpecification.fromClause == null,
          "Multi-Insert queries cannot have a FROM clause in their individual SELECT statements",
          body)

        withQuerySpecification(body.querySpecification, from).
          // Add organization statements.
          optionalMap(body.queryOrganization)(withQueryResultClauses).
          // Add insert.
          optionalMap(body.insertInto())(withInsertInto)
    }

    // If there are multiple INSERTS just UNION them together into one query.
    inserts match {
      case Seq(query) => query
      case queries => Union(queries)
    }
  }

  /**
   * Create a logical plan for a regular (single-insert) query.
   */
  override def visitSingleInsertQuery(
      ctx: SingleInsertQueryContext): LogicalPlan = withOrigin(ctx) {
    plan(ctx.queryTerm).
      // Add organization statements.
      optionalMap(ctx.queryOrganization)(withQueryResultClauses).
      // Add insert.
      optionalMap(ctx.insertInto())(withInsertInto)
  }

  /**
   * Add an INSERT INTO [TABLE]/INSERT OVERWRITE TABLE operation to the logical plan.
   */
  private def withInsertInto(
      ctx: InsertIntoContext,
      query: LogicalPlan): LogicalPlan = withOrigin(ctx) {
    val tableIdent = visitTableIdentifier(ctx.tableIdentifier)
    val partitionKeys = Option(ctx.partitionSpec).map(visitPartitionSpec).getOrElse(Map.empty)

    InsertIntoTable(
      UnresolvedRelation(tableIdent, None),
      partitionKeys,
      query,
      ctx.OVERWRITE != null,
      ctx.EXISTS != null)
  }

  /**
   * Create a partition specification map.
   */
  override def visitPartitionSpec(
      ctx: PartitionSpecContext): Map[String, Option[String]] = withOrigin(ctx) {
    val parts = ctx.partitionVal.asScala.map { pVal =>
      val name = pVal.identifier.getText.toLowerCase
      val value = Option(pVal.constant).map(visitStringConstant)
      name -> value
    }
    // Check for duplicate partition columns in one spec.
    checkDuplicateKeys(parts, ctx)
    parts.toMap
  }

  /**
   * Create a partition specification map without optional values.
   */
  protected def visitNonOptionalPartitionSpec(
      ctx: PartitionSpecContext): Map[String, String] = withOrigin(ctx) {
    visitPartitionSpec(ctx).mapValues(_.orNull).map(identity)
  }

  /**
   * Convert a constant of any type into a string. This is typically used in DDL commands, and its
   * main purpose is to prevent slight differences due to back to back conversions i.e.:
   * String -> Literal -> String.
   */
  protected def visitStringConstant(ctx: ConstantContext): String = withOrigin(ctx) {
    ctx match {
      case s: StringLiteralContext => createString(s)
      case o => o.getText
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
      Sort(order.asScala.map(visitSortItem), global = true, query)
    } else if (order.isEmpty && !sort.isEmpty && distributeBy.isEmpty && clusterBy.isEmpty) {
      // SORT BY ...
      Sort(sort.asScala.map(visitSortItem), global = false, query)
    } else if (order.isEmpty && sort.isEmpty && !distributeBy.isEmpty && clusterBy.isEmpty) {
      // DISTRIBUTE BY ...
      RepartitionByExpression(expressionList(distributeBy), query)
    } else if (order.isEmpty && !sort.isEmpty && !distributeBy.isEmpty && clusterBy.isEmpty) {
      // SORT BY ... DISTRIBUTE BY ...
      Sort(
        sort.asScala.map(visitSortItem),
        global = false,
        RepartitionByExpression(expressionList(distributeBy), query))
    } else if (order.isEmpty && sort.isEmpty && distributeBy.isEmpty && !clusterBy.isEmpty) {
      // CLUSTER BY ...
      val expressions = expressionList(clusterBy)
      Sort(
        expressions.map(SortOrder(_, Ascending)),
        global = false,
        RepartitionByExpression(expressions, query))
    } else if (order.isEmpty && sort.isEmpty && distributeBy.isEmpty && clusterBy.isEmpty) {
      // [EMPTY]
      query
    } else {
      throw new ParseException(
        "Combination of ORDER BY/SORT BY/DISTRIBUTE BY/CLUSTER BY is not supported", ctx)
    }

    // WINDOWS
    val withWindow = withOrder.optionalMap(windows)(withWindows)

    // LIMIT
    withWindow.optional(limit) {
      Limit(typedVisit(limit), withWindow)
    }
  }

  /**
   * Create a logical plan using a query specification.
   */
  override def visitQuerySpecification(
      ctx: QuerySpecificationContext): LogicalPlan = withOrigin(ctx) {
    val from = OneRowRelation.optional(ctx.fromClause) {
      visitFromClause(ctx.fromClause)
    }
    withQuerySpecification(ctx, from)
  }

  /**
   * Add a query specification to a logical plan. The query specification is the core of the logical
   * plan, this is where sourcing (FROM clause), transforming (SELECT TRANSFORM/MAP/REDUCE),
   * projection (SELECT), aggregation (GROUP BY ... HAVING ...) and filtering (WHERE) takes place.
   *
   * Note that query hints are ignored (both by the parser and the builder).
   */
  private def withQuerySpecification(
      ctx: QuerySpecificationContext,
      relation: LogicalPlan): LogicalPlan = withOrigin(ctx) {
    import ctx._

    // WHERE
    def filter(ctx: BooleanExpressionContext, plan: LogicalPlan): LogicalPlan = {
      Filter(expression(ctx), plan)
    }

    // Expressions.
    val expressions = Option(namedExpressionSeq).toSeq
      .flatMap(_.namedExpression.asScala)
      .map(typedVisit[Expression])

    // Create either a transform or a regular query.
    val specType = Option(kind).map(_.getType).getOrElse(SqlBaseParser.SELECT)
    specType match {
      case SqlBaseParser.MAP | SqlBaseParser.REDUCE | SqlBaseParser.TRANSFORM =>
        // Transform

        // Add where.
        val withFilter = relation.optionalMap(where)(filter)

        // Create the attributes.
        val (attributes, schemaLess) = if (colTypeList != null) {
          // Typed return columns.
          (createStructType(colTypeList).toAttributes, false)
        } else if (identifierSeq != null) {
          // Untyped return columns.
          val attrs = visitIdentifierSeq(identifierSeq).map { name =>
            AttributeReference(name, StringType, nullable = true)()
          }
          (attrs, false)
        } else {
          (Seq(AttributeReference("key", StringType)(),
            AttributeReference("value", StringType)()), true)
        }

        // Create the transform.
        ScriptTransformation(
          expressions,
          string(script),
          attributes,
          withFilter,
          withScriptIOSchema(
            ctx, inRowFormat, recordWriter, outRowFormat, recordReader, schemaLess))

      case SqlBaseParser.SELECT =>
        // Regular select

        // Add lateral views.
        val withLateralView = ctx.lateralView.asScala.foldLeft(relation)(withGenerate)

        // Add where.
        val withFilter = withLateralView.optionalMap(where)(filter)

        // Add aggregation or a project.
        val namedExpressions = expressions.map {
          case e: NamedExpression => e
          case e: Expression => UnresolvedAlias(e)
        }
        val withProject = if (aggregation != null) {
          withAggregation(aggregation, namedExpressions, withFilter)
        } else if (namedExpressions.nonEmpty) {
          Project(namedExpressions, withFilter)
        } else {
          withFilter
        }

        // Having
        val withHaving = withProject.optional(having) {
          // Note that we add a cast to non-predicate expressions. If the expression itself is
          // already boolean, the optimizer will get rid of the unnecessary cast.
          val predicate = expression(having) match {
            case p: Predicate => p
            case e => Cast(e, BooleanType)
          }
          Filter(predicate, withProject)
        }

        // Distinct
        val withDistinct = if (setQuantifier() != null && setQuantifier().DISTINCT() != null) {
          Distinct(withHaving)
        } else {
          withHaving
        }

        // Window
        withDistinct.optionalMap(windows)(withWindows)
    }
  }

  /**
   * Create a (Hive based) [[ScriptInputOutputSchema]].
   */
  protected def withScriptIOSchema(
      ctx: QuerySpecificationContext,
      inRowFormat: RowFormatContext,
      recordWriter: Token,
      outRowFormat: RowFormatContext,
      recordReader: Token,
      schemaLess: Boolean): ScriptInputOutputSchema = {
    throw new ParseException("Script Transform is not supported", ctx)
  }

  /**
   * Create a logical plan for a given 'FROM' clause. Note that we support multiple (comma
   * separated) relations here, these get converted into a single plan by condition-less inner join.
   */
  override def visitFromClause(ctx: FromClauseContext): LogicalPlan = withOrigin(ctx) {
    val from = ctx.relation.asScala.map(plan).reduceLeft(Join(_, _, Inner, None))
    ctx.lateralView.asScala.foldLeft(from)(withGenerate)
  }

  /**
   * Connect two queries by a Set operator.
   *
   * Supported Set operators are:
   * - UNION [DISTINCT]
   * - UNION ALL
   * - EXCEPT [DISTINCT]
   * - INTERSECT [DISTINCT]
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
        throw new ParseException("INTERSECT ALL is not supported.", ctx)
      case SqlBaseParser.INTERSECT =>
        Intersect(left, right)
      case SqlBaseParser.EXCEPT if all =>
        throw new ParseException("EXCEPT ALL is not supported.", ctx)
      case SqlBaseParser.EXCEPT =>
        Except(left, right)
    }
  }

  /**
   * Add a [[WithWindowDefinition]] operator to a logical plan.
   */
  private def withWindows(
      ctx: WindowsContext,
      query: LogicalPlan): LogicalPlan = withOrigin(ctx) {
    // Collect all window specifications defined in the WINDOW clause.
    val baseWindowMap = ctx.namedWindow.asScala.map {
      wCtx =>
        (wCtx.identifier.getText, typedVisit[WindowSpec](wCtx.windowSpec))
    }.toMap

    // Handle cases like
    // window w1 as (partition by p_mfgr order by p_name
    //               range between 2 preceding and 2 following),
    //        w2 as w1
    val windowMapView = baseWindowMap.mapValues {
      case WindowSpecReference(name) =>
        baseWindowMap.get(name) match {
          case Some(spec: WindowSpecDefinition) =>
            spec
          case Some(ref) =>
            throw new ParseException(s"Window reference '$name' is not a window specification", ctx)
          case None =>
            throw new ParseException(s"Cannot resolve window reference '$name'", ctx)
        }
      case spec: WindowSpecDefinition => spec
    }

    // Note that mapValues creates a view instead of materialized map. We force materialization by
    // mapping over identity.
    WithWindowDefinition(windowMapView.map(identity), query)
  }

  /**
   * Add an [[Aggregate]] to a logical plan.
   */
  private def withAggregation(
      ctx: AggregationContext,
      selectExpressions: Seq[NamedExpression],
      query: LogicalPlan): LogicalPlan = withOrigin(ctx) {
    import ctx._
    val groupByExpressions = expressionList(groupingExpressions)

    if (GROUPING != null) {
      // GROUP BY .... GROUPING SETS (...)
      val expressionMap = groupByExpressions.zipWithIndex.toMap
      val numExpressions = expressionMap.size
      val mask = (1 << numExpressions) - 1
      val masks = ctx.groupingSet.asScala.map {
        _.expression.asScala.foldLeft(mask) {
          case (bitmap, eCtx) =>
            // Find the index of the expression.
            val e = typedVisit[Expression](eCtx)
            val index = expressionMap.find(_._1.semanticEquals(e)).map(_._2).getOrElse(
              throw new ParseException(
                s"$e doesn't show up in the GROUP BY list", ctx))
            // 0 means that the column at the given index is a grouping column, 1 means it is not,
            // so we unset the bit in bitmap.
            bitmap & ~(1 << (numExpressions - 1 - index))
        }
      }
      GroupingSets(masks, groupByExpressions, query, selectExpressions)
    } else {
      // GROUP BY .... (WITH CUBE | WITH ROLLUP)?
      val mappedGroupByExpressions = if (CUBE != null) {
        Seq(Cube(groupByExpressions))
      } else if (ROLLUP != null) {
        Seq(Rollup(groupByExpressions))
      } else {
        groupByExpressions
      }
      Aggregate(mappedGroupByExpressions, selectExpressions, query)
    }
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
      join = true,
      outer = ctx.OUTER != null,
      Some(ctx.tblName.getText.toLowerCase),
      ctx.colName.asScala.map(_.getText).map(UnresolvedAttribute.apply),
      query)
  }

  /**
   * Create a joins between two or more logical plans.
   */
  override def visitJoinRelation(ctx: JoinRelationContext): LogicalPlan = withOrigin(ctx) {
    /** Build a join between two plans. */
    def join(ctx: JoinRelationContext, left: LogicalPlan, right: LogicalPlan): Join = {
      val baseJoinType = ctx.joinType match {
        case null => Inner
        case jt if jt.FULL != null => FullOuter
        case jt if jt.SEMI != null => LeftSemi
        case jt if jt.ANTI != null => LeftAnti
        case jt if jt.LEFT != null => LeftOuter
        case jt if jt.RIGHT != null => RightOuter
        case _ => Inner
      }

      // Resolve the join type and join condition
      val (joinType, condition) = Option(ctx.joinCriteria) match {
        case Some(c) if c.USING != null =>
          val columns = c.identifier.asScala.map { column =>
            UnresolvedAttribute.quoted(column.getText)
          }
          (UsingJoin(baseJoinType, columns), None)
        case Some(c) if c.booleanExpression != null =>
          (baseJoinType, Option(expression(c.booleanExpression)))
        case None if ctx.NATURAL != null =>
          (NaturalJoin(baseJoinType), None)
        case None =>
          (baseJoinType, None)
      }
      Join(left, right, joinType, condition)
    }

    // Handle all consecutive join clauses. ANTLR produces a right nested tree in which the the
    // first join clause is at the top. However fields of previously referenced tables can be used
    // in following join clauses. The tree needs to be reversed in order to make this work.
    var result = plan(ctx.left)
    var current = ctx
    while (current != null) {
      current.right match {
        case right: JoinRelationContext =>
          result = join(current, result, plan(right.left))
          current = right
        case right =>
          result = join(current, result, plan(right))
          current = null
      }
    }
    result
  }

  /**
   * Add a [[Sample]] to a logical plan.
   *
   * This currently supports the following sampling methods:
   * - TABLESAMPLE(x ROWS): Sample the table down to the given number of rows.
   * - TABLESAMPLE(x PERCENT): Sample the table down to the given percentage. Note that percentages
   * are defined as a number between 0 and 100.
   * - TABLESAMPLE(BUCKET x OUT OF y): Sample the table down to a 'x' divided by 'y' fraction.
   */
  private def withSample(ctx: SampleContext, query: LogicalPlan): LogicalPlan = withOrigin(ctx) {
    // Create a sampled plan if we need one.
    def sample(fraction: Double): Sample = {
      // The range of fraction accepted by Sample is [0, 1]. Because Hive's block sampling
      // function takes X PERCENT as the input and the range of X is [0, 100], we need to
      // adjust the fraction.
      val eps = RandomSampler.roundingEpsilon
      assert(fraction >= 0.0 - eps && fraction <= 1.0 + eps,
        s"Sampling fraction ($fraction) must be on interval [0, 1]",
        ctx)
      Sample(0.0, fraction, withReplacement = false, (math.random * 1000).toInt, query)(true)
    }

    ctx.sampleType.getType match {
      case SqlBaseParser.ROWS =>
        Limit(expression(ctx.expression), query)

      case SqlBaseParser.PERCENTLIT =>
        val fraction = ctx.percentage.getText.toDouble
        sample(fraction / 100.0d)

      case SqlBaseParser.BYTELENGTH_LITERAL =>
        throw new ParseException(
          "TABLESAMPLE(byteLengthLiteral) is not supported", ctx)

      case SqlBaseParser.BUCKET if ctx.ON != null =>
        if (ctx.identifier != null) {
          throw new ParseException(
            "TABLESAMPLE(BUCKET x OUT OF y ON colname) is not supported", ctx)
        } else {
          throw new ParseException(
            "TABLESAMPLE(BUCKET x OUT OF y ON function) is not supported", ctx)
        }

      case SqlBaseParser.BUCKET =>
        sample(ctx.numerator.getText.toDouble / ctx.denominator.getText.toDouble)
    }
  }

  /**
   * Create a logical plan for a sub-query.
   */
  override def visitSubquery(ctx: SubqueryContext): LogicalPlan = withOrigin(ctx) {
    plan(ctx.queryNoWith)
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
    UnresolvedRelation(visitTableIdentifier(ctx.tableIdentifier), None)
  }

  /**
   * Create an aliased table reference. This is typically used in FROM clauses.
   */
  override def visitTableName(ctx: TableNameContext): LogicalPlan = withOrigin(ctx) {
    val table = UnresolvedRelation(
      visitTableIdentifier(ctx.tableIdentifier),
      Option(ctx.identifier).map(_.getText))
    table.optionalMap(ctx.sample)(withSample)
  }

  /**
   * Create an inline table (a virtual table in Hive parlance).
   */
  override def visitInlineTable(ctx: InlineTableContext): LogicalPlan = withOrigin(ctx) {
    // Get the backing expressions.
    val expressions = ctx.expression.asScala.map { eCtx =>
      val e = expression(eCtx)
      assert(e.foldable, "All expressions in an inline table must be constants.", eCtx)
      e
    }

    // Validate and evaluate the rows.
    val (structType, structConstructor) = expressions.head.dataType match {
      case st: StructType =>
        (st, (e: Expression) => e)
      case dt =>
        val st = CreateStruct(Seq(expressions.head)).dataType
        (st, (e: Expression) => CreateStruct(Seq(e)))
    }
    val rows = expressions.map {
      case expression =>
        val safe = Cast(structConstructor(expression), structType)
        safe.eval().asInstanceOf[InternalRow]
    }

    // Construct attributes.
    val baseAttributes = structType.toAttributes.map(_.withNullability(true))
    val attributes = if (ctx.identifierList != null) {
      val aliases = visitIdentifierList(ctx.identifierList)
      assert(aliases.size == baseAttributes.size,
        "Number of aliases must match the number of fields in an inline table.", ctx)
      baseAttributes.zip(aliases).map(p => p._1.withName(p._2))
    } else {
      baseAttributes
    }

    // Create plan and add an alias if a name has been defined.
    LocalRelation(attributes, rows).optionalMap(ctx.identifier)(aliasPlan)
  }

  /**
   * Create an alias (SubqueryAlias) for a join relation. This is practically the same as
   * visitAliasedQuery and visitNamedExpression, ANTLR4 however requires us to use 3 different
   * hooks.
   */
  override def visitAliasedRelation(ctx: AliasedRelationContext): LogicalPlan = withOrigin(ctx) {
    plan(ctx.relation).optionalMap(ctx.sample)(withSample).optionalMap(ctx.identifier)(aliasPlan)
  }

  /**
   * Create an alias (SubqueryAlias) for a sub-query. This is practically the same as
   * visitAliasedRelation and visitNamedExpression, ANTLR4 however requires us to use 3 different
   * hooks.
   */
  override def visitAliasedQuery(ctx: AliasedQueryContext): LogicalPlan = withOrigin(ctx) {
    plan(ctx.queryNoWith).optionalMap(ctx.sample)(withSample).optionalMap(ctx.identifier)(aliasPlan)
  }

  /**
   * Create an alias (SubqueryAlias) for a LogicalPlan.
   */
  private def aliasPlan(alias: IdentifierContext, plan: LogicalPlan): LogicalPlan = {
    SubqueryAlias(alias.getText, plan)
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
    ctx.identifier.asScala.map(_.getText)
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
    trees.asScala.map(expression)
  }

  /**
   * Create a star (i.e. all) expression; this selects all elements (in the specified object).
   * Both un-targeted (global) and targeted aliases are supported.
   */
  override def visitStar(ctx: StarContext): Expression = withOrigin(ctx) {
    UnresolvedStar(Option(ctx.qualifiedName()).map(_.identifier.asScala.map(_.getText)))
  }

  /**
   * Create an aliased expression if an alias is specified. Both single and multi-aliases are
   * supported.
   */
  override def visitNamedExpression(ctx: NamedExpressionContext): Expression = withOrigin(ctx) {
    val e = expression(ctx.expression)
    if (ctx.identifier != null) {
      Alias(e, ctx.identifier.getText)()
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
    val expressions = contexts.reverse.map(expression)

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
   * - Less then or Equal: '<='
   * - Greater than: '>'
   * - Greater then or Equal: '>='
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
   * - (NOT) LIKE
   * - (NOT) RLIKE
   * - IS (NOT) NULL.
   */
  private def withPredicate(e: Expression, ctx: PredicateContext): Expression = withOrigin(ctx) {
    // Invert a predicate if it has a valid NOT clause.
    def invertIfNotDefined(e: Expression): Expression = ctx.NOT match {
      case null => e
      case not => Not(e)
    }

    // Create the predicate.
    ctx.kind.getType match {
      case SqlBaseParser.BETWEEN =>
        // BETWEEN is translated to lower <= e && e <= upper
        invertIfNotDefined(And(
          GreaterThanOrEqual(e, expression(ctx.lower)),
          LessThanOrEqual(e, expression(ctx.upper))))
      case SqlBaseParser.IN if ctx.query != null =>
        invertIfNotDefined(In(e, Seq(ListQuery(plan(ctx.query)))))
      case SqlBaseParser.IN =>
        invertIfNotDefined(In(e, ctx.expression.asScala.map(expression)))
      case SqlBaseParser.LIKE =>
        invertIfNotDefined(Like(e, expression(ctx.pattern)))
      case SqlBaseParser.RLIKE =>
        invertIfNotDefined(RLike(e, expression(ctx.pattern)))
      case SqlBaseParser.NULL if ctx.NOT != null =>
        IsNotNull(e)
      case SqlBaseParser.NULL =>
        IsNull(e)
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
        Cast(Divide(left, right), LongType)
      case SqlBaseParser.PLUS =>
        Add(left, right)
      case SqlBaseParser.MINUS =>
        Subtract(left, right)
      case SqlBaseParser.AMPERSAND =>
        BitwiseAnd(left, right)
      case SqlBaseParser.HAT =>
        BitwiseXor(left, right)
      case SqlBaseParser.PIPE =>
        BitwiseOr(left, right)
    }
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
        value
      case SqlBaseParser.MINUS =>
        UnaryMinus(value)
      case SqlBaseParser.TILDE =>
        BitwiseNot(value)
    }
  }

  /**
   * Create a [[Cast]] expression.
   */
  override def visitCast(ctx: CastContext): Expression = withOrigin(ctx) {
    Cast(expression(ctx.expression), typedVisit(ctx.dataType))
  }

  /**
   * Create a (windowed) Function expression.
   */
  override def visitFunctionCall(ctx: FunctionCallContext): Expression = withOrigin(ctx) {
    // Create the function call.
    val name = ctx.qualifiedName.getText
    val isDistinct = Option(ctx.setQuantifier()).exists(_.DISTINCT != null)
    val arguments = ctx.expression().asScala.map(expression) match {
      case Seq(UnresolvedStar(None)) if name.toLowerCase == "count" && !isDistinct =>
        // Transform COUNT(*) into COUNT(1).
        Seq(Literal(1))
      case expressions =>
        expressions
    }
    val function = UnresolvedFunction(visitFunctionName(ctx.qualifiedName), arguments, isDistinct)

    // Check if the function is evaluated in a windowed context.
    ctx.windowSpec match {
      case spec: WindowRefContext =>
        UnresolvedWindowExpression(function, visitWindowRef(spec))
      case spec: WindowDefContext =>
        WindowExpression(function, visitWindowDef(spec))
      case _ => function
    }
  }

  /**
   * Create a function database (optional) and name pair.
   */
  protected def visitFunctionName(ctx: QualifiedNameContext): FunctionIdentifier = {
    ctx.identifier().asScala.map(_.getText) match {
      case Seq(db, fn) => FunctionIdentifier(fn, Option(db))
      case Seq(fn) => FunctionIdentifier(fn, None)
      case other => throw new ParseException(s"Unsupported function name '${ctx.getText}'", ctx)
    }
  }

  /**
   * Create a reference to a window frame, i.e. [[WindowSpecReference]].
   */
  override def visitWindowRef(ctx: WindowRefContext): WindowSpecReference = withOrigin(ctx) {
    WindowSpecReference(ctx.identifier.getText)
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
      partition,
      order,
      frameSpecOption.getOrElse(UnspecifiedFrame))
  }

  /**
   * Create or resolve a [[FrameBoundary]]. Simple math expressions are allowed for Value
   * Preceding/Following boundaries. These expressions must be constant (foldable) and return an
   * integer value.
   */
  override def visitFrameBound(ctx: FrameBoundContext): FrameBoundary = withOrigin(ctx) {
    // We currently only allow foldable integers.
    def value: Int = {
      val e = expression(ctx.expression)
      assert(e.resolved && e.foldable && e.dataType == IntegerType,
        "Frame bound value must be a constant integer.",
        ctx)
      e.eval().asInstanceOf[Int]
    }

    // Create the FrameBoundary
    ctx.boundType.getType match {
      case SqlBaseParser.PRECEDING if ctx.UNBOUNDED != null =>
        UnboundedPreceding
      case SqlBaseParser.PRECEDING =>
        ValuePreceding(value)
      case SqlBaseParser.CURRENT =>
        CurrentRow
      case SqlBaseParser.FOLLOWING if ctx.UNBOUNDED != null =>
        UnboundedFollowing
      case SqlBaseParser.FOLLOWING =>
        ValueFollowing(value)
    }
  }

  /**
   * Create a [[CreateStruct]] expression.
   */
  override def visitRowConstructor(ctx: RowConstructorContext): Expression = withOrigin(ctx) {
    CreateStruct(ctx.expression.asScala.map(expression))
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
    val e = expression(ctx.valueExpression)
    val branches = ctx.whenClause.asScala.map { wCtx =>
      (EqualTo(e, expression(wCtx.condition)), expression(wCtx.result))
    }
    CaseWhen(branches, Option(ctx.elseExpression).map(expression))
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
    CaseWhen(branches, Option(ctx.elseExpression).map(expression))
  }

  /**
   * Create a dereference expression. The return type depends on the type of the parent, this can
   * either be a [[UnresolvedAttribute]] (if the parent is an [[UnresolvedAttribute]]), or an
   * [[UnresolvedExtractValue]] if the parent is some expression.
   */
  override def visitDereference(ctx: DereferenceContext): Expression = withOrigin(ctx) {
    val attr = ctx.fieldName.getText
    expression(ctx.base) match {
      case UnresolvedAttribute(nameParts) =>
        UnresolvedAttribute(nameParts :+ attr)
      case e =>
        UnresolvedExtractValue(e, Literal(attr))
    }
  }

  /**
   * Create an [[UnresolvedAttribute]] expression.
   */
  override def visitColumnReference(ctx: ColumnReferenceContext): Expression = withOrigin(ctx) {
    UnresolvedAttribute.quoted(ctx.getText)
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
    if (ctx.DESC != null) {
      SortOrder(expression(ctx.expression), Descending)
    } else {
      SortOrder(expression(ctx.expression), Ascending)
    }
  }

  /**
   * Create a typed Literal expression. A typed literal has the following SQL syntax:
   * {{{
   *   [TYPE] '[VALUE]'
   * }}}
   * Currently Date and Timestamp typed literals are supported.
   *
   * TODO what the added value of this over casting?
   */
  override def visitTypeConstructor(ctx: TypeConstructorContext): Literal = withOrigin(ctx) {
    val value = string(ctx.STRING)
    ctx.identifier.getText.toUpperCase match {
      case "DATE" =>
        Literal(Date.valueOf(value))
      case "TIMESTAMP" =>
        Literal(Timestamp.valueOf(value))
      case other =>
        throw new ParseException(s"Literals of type '$other' are currently not supported.", ctx)
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
        Literal(v.intValue())
      case v if v.isValidLong =>
        Literal(v.longValue())
      case v => Literal(v.underlying())
    }
  }

  /**
   * Create a double literal for a number denoted in scientific notation.
   */
  override def visitScientificDecimalLiteral(
      ctx: ScientificDecimalLiteralContext): Literal = withOrigin(ctx) {
    Literal(ctx.getText.toDouble)
  }

  /**
   * Create a decimal literal for a regular decimal number.
   */
  override def visitDecimalLiteral(ctx: DecimalLiteralContext): Literal = withOrigin(ctx) {
    Literal(BigDecimal(ctx.getText).underlying())
  }

  /** Create a numeric literal expression. */
  private def numericLiteral(ctx: NumberContext)(f: String => Any): Literal = withOrigin(ctx) {
    val raw = ctx.getText
    try {
      Literal(f(raw.substring(0, raw.length - 1)))
    } catch {
      case e: NumberFormatException =>
        throw new ParseException(e.getMessage, ctx)
    }
  }

  /**
   * Create a Byte Literal expression.
   */
  override def visitTinyIntLiteral(ctx: TinyIntLiteralContext): Literal = numericLiteral(ctx) {
    _.toByte
  }

  /**
   * Create a Short Literal expression.
   */
  override def visitSmallIntLiteral(ctx: SmallIntLiteralContext): Literal = numericLiteral(ctx) {
    _.toShort
  }

  /**
   * Create a Long Literal expression.
   */
  override def visitBigIntLiteral(ctx: BigIntLiteralContext): Literal = numericLiteral(ctx) {
    _.toLong
  }

  /**
   * Create a Double Literal expression.
   */
  override def visitDoubleLiteral(ctx: DoubleLiteralContext): Literal = numericLiteral(ctx) {
    _.toDouble
  }

  /**
   * Create a String literal expression.
   */
  override def visitStringLiteral(ctx: StringLiteralContext): Literal = withOrigin(ctx) {
    Literal(createString(ctx))
  }

  /**
   * Create a String from a string literal context. This supports multiple consecutive string
   * literals, these are concatenated, for example this expression "'hello' 'world'" will be
   * converted into "helloworld".
   *
   * Special characters can be escaped by using Hive/C-style escaping.
   */
  private def createString(ctx: StringLiteralContext): String = {
    ctx.STRING().asScala.map(string).mkString
  }

  /**
   * Create a [[CalendarInterval]] literal expression. An interval expression can contain multiple
   * unit value pairs, for instance: interval 2 months 2 days.
   */
  override def visitInterval(ctx: IntervalContext): Literal = withOrigin(ctx) {
    val intervals = ctx.intervalField.asScala.map(visitIntervalField)
    assert(intervals.nonEmpty, "at least one time unit should be given for interval literal", ctx)
    Literal(intervals.reduce(_.add(_)))
  }

  /**
   * Create a [[CalendarInterval]] for a unit value pair. Two unit configuration types are
   * supported:
   * - Single unit.
   * - From-To unit (only 'YEAR TO MONTH' and 'DAY TO SECOND' are supported).
   */
  override def visitIntervalField(ctx: IntervalFieldContext): CalendarInterval = withOrigin(ctx) {
    import ctx._
    val s = value.getText
    try {
      val interval = (unit.getText.toLowerCase, Option(to).map(_.getText.toLowerCase)) match {
        case (u, None) if u.endsWith("s") =>
          // Handle plural forms, e.g: yearS/monthS/weekS/dayS/hourS/minuteS/hourS/...
          CalendarInterval.fromSingleUnitString(u.substring(0, u.length - 1), s)
        case (u, None) =>
          CalendarInterval.fromSingleUnitString(u, s)
        case ("year", Some("month")) =>
          CalendarInterval.fromYearMonthString(s)
        case ("day", Some("second")) =>
          CalendarInterval.fromDayTimeString(s)
        case (from, Some(t)) =>
          throw new ParseException(s"Intervals FROM $from TO $t are not supported.", ctx)
      }
      assert(interval != null, "No interval can be constructed", ctx)
      interval
    } catch {
      // Handle Exceptions thrown by CalendarInterval
      case e: IllegalArgumentException =>
        val pe = new ParseException(e.getMessage, ctx)
        pe.setStackTrace(e.getStackTrace)
        throw pe
    }
  }

  /* ********************************************************************************************
   * DataType parsing
   * ******************************************************************************************** */
  /**
   * Resolve/create a primitive type.
   */
  override def visitPrimitiveDataType(ctx: PrimitiveDataTypeContext): DataType = withOrigin(ctx) {
    (ctx.identifier.getText.toLowerCase, ctx.INTEGER_VALUE().asScala.toList) match {
      case ("boolean", Nil) => BooleanType
      case ("tinyint" | "byte", Nil) => ByteType
      case ("smallint" | "short", Nil) => ShortType
      case ("int" | "integer", Nil) => IntegerType
      case ("bigint" | "long", Nil) => LongType
      case ("float", Nil) => FloatType
      case ("double", Nil) => DoubleType
      case ("date", Nil) => DateType
      case ("timestamp", Nil) => TimestampType
      case ("char" | "varchar" | "string", Nil) => StringType
      case ("char" | "varchar", _ :: Nil) => StringType
      case ("binary", Nil) => BinaryType
      case ("decimal", Nil) => DecimalType.USER_DEFAULT
      case ("decimal", precision :: Nil) => DecimalType(precision.getText.toInt, 0)
      case ("decimal", precision :: scale :: Nil) =>
        DecimalType(precision.getText.toInt, scale.getText.toInt)
      case (dt, params) =>
        throw new ParseException(
          s"DataType $dt${params.mkString("(", ",", ")")} is not supported.", ctx)
    }
  }

  /**
   * Create a complex DataType. Arrays, Maps and Structures are supported.
   */
  override def visitComplexDataType(ctx: ComplexDataTypeContext): DataType = withOrigin(ctx) {
    ctx.complex.getType match {
      case SqlBaseParser.ARRAY =>
        ArrayType(typedVisit(ctx.dataType(0)))
      case SqlBaseParser.MAP =>
        MapType(typedVisit(ctx.dataType(0)), typedVisit(ctx.dataType(1)))
      case SqlBaseParser.STRUCT =>
        createStructType(ctx.colTypeList())
    }
  }

  /**
   * Create a [[StructType]] from a sequence of [[StructField]]s.
   */
  protected def createStructType(ctx: ColTypeListContext): StructType = {
    StructType(Option(ctx).toSeq.flatMap(visitColTypeList))
  }

  /**
   * Create a [[StructType]] from a number of column definitions.
   */
  override def visitColTypeList(ctx: ColTypeListContext): Seq[StructField] = withOrigin(ctx) {
    ctx.colType().asScala.map(visitColType)
  }

  /**
   * Create a [[StructField]] from a column definition.
   */
  override def visitColType(ctx: ColTypeContext): StructField = withOrigin(ctx) {
    import ctx._

    // Add the comment to the metadata.
    val builder = new MetadataBuilder
    if (STRING != null) {
      builder.putString("comment", string(STRING))
    }

    StructField(identifier.getText, typedVisit(dataType), nullable = true, builder.build())
  }
}
