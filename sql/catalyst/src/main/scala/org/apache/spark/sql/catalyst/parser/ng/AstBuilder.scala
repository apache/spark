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
package org.apache.spark.sql.catalyst.parser.ng

import java.sql.{Date, Timestamp}

import scala.collection.JavaConverters._

import org.antlr.v4.runtime.{ParserRuleContext, Token}
import org.antlr.v4.runtime.tree.{ParseTree, TerminalNode}

import org.apache.spark.Logging
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.{InternalRow, TableIdentifier}
import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.parser.ParseUtils
import org.apache.spark.sql.catalyst.parser.ng.SqlBaseParser._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.trees.CurrentOrigin
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.CalendarInterval
import org.apache.spark.util.random.RandomSampler

/**
 * The AstBuilder converts an ANTLR4 ParseTree into a catalyst Expression, LogicalPlan or
 * TableIdentifier.
 */
class AstBuilder extends SqlBaseBaseVisitor[AnyRef] with Logging {
  import AstBuilder._

  protected def typedVisit[T](ctx: ParseTree): T = {
    ctx.accept(this).asInstanceOf[T]
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

  /* ********************************************************************************************
   * Plan parsing
   * ******************************************************************************************** */
  protected def plan(tree: ParserRuleContext): LogicalPlan = typedVisit(tree)

  /**
   * Create a plan for a SHOW FUNCTIONS command.
   */
  override def visitShowFunctions(ctx: ShowFunctionsContext): LogicalPlan = withOrigin(ctx) {
    import ctx._
    if (qualifiedName != null) {
      val names = qualifiedName().identifier().asScala.map(_.getText).toList
      names match {
        case db :: name :: Nil =>
          ShowFunctions(Some(db), Some(name))
        case name :: Nil =>
          ShowFunctions(None, Some(name))
        case _ =>
          notSupported("SHOW FUNCTIONS unsupported name", ctx)
      }
    } else if (pattern != null) {
      ShowFunctions(None, Some(unquote(pattern.getText)))
    } else {
      ShowFunctions(None, None)
    }
  }

  /**
   * Create a plan for a DESCRIBE FUNCTION command.
   */
  override def visitDescribeFunction(ctx: DescribeFunctionContext): LogicalPlan = withOrigin(ctx) {
    val functionName = ctx.qualifiedName().identifier().asScala.map(_.getText).mkString(".")
    DescribeFunction(functionName, ctx.EXTENDED != null)
  }

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
      }.toMap
      With(query, ctes)
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
          optionalMap(body.queryOrganization)(withQueryOrganization).
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
      optionalMap(ctx.queryOrganization)(withQueryOrganization).
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
    val partitionKeys = Option(ctx.partitionSpec).toSeq.flatMap {
      _.partitionVal.asScala.map {
        pVal => (pVal.identifier.getText, Option(pVal.constant).map(c => unquote(c.getText)))
      }
    }.toMap

    InsertIntoTable(
      UnresolvedRelation(tableIdent, None),
      partitionKeys,
      query,
      ctx.OVERWRITE != null,
      ctx.EXISTS != null)
  }

  /**
   * Add ORDER BY/SORT BY/CLUSTER BY/DISTRIBUTE BY/LIMIT/WINDOWS clauses to the logical plan.
   */
  private def withQueryOrganization(
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
      notSupported("Combination of ORDER BY/SORT BY/DISTRIBUTE BY/CLUSTER BY is not supported", ctx)
    }

    // LIMIT
    val withLimit = withOrder.optional(limit) {
      Limit(typedVisit(limit), withOrder)
    }

    // WINDOWS
    withLimit.optionalMap(windows)(withWindows)
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
    val withFilter = relation.optional(where) {
      Filter(expression(where), relation)
    }

    // Expressions.
    val expressions = selectItem.asScala.map(visit).map {
      case e: Expression => UnresolvedAlias(e)
    }

    // Create either a transform or a regular query.
    kind.getType match {
      case SqlBaseParser.MAP | SqlBaseParser.REDUCE | SqlBaseParser.TRANSFORM =>
        // Transform

        // Create the attributes.
        val attributes = if (colTypeList != null) {
          // Typed return columns.
          visitColTypeList(colTypeList).toAttributes
        } else if (columnAliasList != null) {
          // Untyped return columns.
          visitColumnAliasList(columnAliasList).map { name =>
            AttributeReference(name, StringType, nullable = true)()
          }
        } else {
          Seq.empty
        }

        // Create the transform.
        ScriptTransformation(
          expressions,
          unquote(script.getText),
          attributes,
          withFilter,
          withScriptIOSchema(inRowFormat, outRowFormat, outRecordReader))

      case SqlBaseParser.SELECT =>
        // Regular select

        // Add lateral views.
        val withLateralView = ctx.lateralView.asScala.foldLeft(withFilter)(withGenerate)

        // Add aggregation with having or a project.
        val withProject = if (aggregation != null) {
          withAggregation(aggregation, expressions, withLateralView).optionalMap(having) {
            case (h, p) => Filter(expression(h), p)
          }
        } else {
          Project(expressions, withLateralView)
        }

        // Distinct
        val withDistinct = if (setQuantifier() != null && setQuantifier().DISTINCT() != null) {
          Distinct(withProject)
        } else {
          withProject
        }

        // Window
        withDistinct.optionalMap(windows)(withWindows)
    }
  }

  /**
   * Create a (Hive based) [[ScriptInputOutputSchema]].
   */
  protected def withScriptIOSchema(
      inRowFormat: RowFormatContext,
      outRowFormat: RowFormatContext,
      outRecordReader: Token): ScriptInputOutputSchema = null

  /**
   * Create a logical plan for a given 'FROM' clause. Note that we support multiple (comma
   * separated) relations here, these get converted into a single plan by condition-less inner join.
   */
  override def visitFromClause(ctx: FromClauseContext): LogicalPlan = withOrigin(ctx) {
    ctx.relation.asScala.map(plan).reduceLeft(Join(_, _, Inner, None))
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
        notSupported("INTERSECT ALL is not supported.", ctx)
      case SqlBaseParser.INTERSECT =>
        Intersect(left, right)
      case SqlBaseParser.EXCEPT if all =>
        notSupported("EXCEPT ALL is not supported.", ctx)
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
    val windowMap = baseWindowMap.mapValues {
      case WindowSpecReference(name) => baseWindowMap(name).asInstanceOf[WindowSpecDefinition]
      case spec: WindowSpecDefinition => spec
    }
    WithWindowDefinition(windowMap, query)
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
      // TODO use new expression set here?
      val expressionMap = groupByExpressions.zipWithIndex.toMap
      val numExpressions = expressionMap.size
      val mask = (1 << numExpressions) - 1
      val masks = ctx.groupingSet.asScala.map {
        _.expression.asScala.foldLeft(mask) {
          case (bitmap, eCtx) =>
            // Find the index of the expression.
            val e = typedVisit[Expression](eCtx)
            val index = expressionMap.find(_._1.semanticEquals(e)).map(_._2).getOrElse(
              throw new AnalysisException(
                s"${e.treeString} doesn't show up in the GROUP BY list"))
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

    // Create the generator.
    // TODO Add support for other generators.
    val generator = ctx.qualifiedName.getText.toLowerCase match {
      case "explode" if expressions.size == 1 =>
        Explode(expressions.head)
      case "json_tuple" =>
        JsonTuple(expressions)
      case other =>
        notSupported(s"Generator function '$other' is not supported", ctx)
    }

    Generate(
      generator,
      join = true,
      outer = ctx.OUTER != null,
      Some(ctx.tblName.getText.toLowerCase),
      ctx.colName.asScala.map(_.getText).map(UnresolvedAttribute.apply),
      query)
  }

  /**
   * Create a join between two logical plans.
   */
  override def visitJoinRelation(ctx: JoinRelationContext): LogicalPlan = withOrigin(ctx) {
    val baseJoinType = ctx.joinType match {
      case jt if jt.FULL != null => FullOuter
      case jt if jt.SEMI != null => LeftSemi
      case jt if jt.LEFT != null => LeftOuter
      case jt if jt.RIGHT != null => RightOuter
      case _ => Inner
    }
    val joinType = if (ctx.NATURAL != null) {
      NaturalJoin(baseJoinType)
    } else {
      baseJoinType
    }

    val left = plan(ctx.left)
    val right = if (ctx.right != null) {
      plan(ctx.right)
    } else {
      plan(ctx.rightRelation)
    }
    assert(left != null, "Left side should not be null", ctx)
    assert(right != null, "Right side should not be null", ctx)
    Join(left, right, joinType, Option(ctx.booleanExpression).map(expression))
  }

  /**
   * Create a sampled relation. This returns a [[Sample]] operator when sampling is requested.
   *
   * This currently supports the following sampling methods:
   * - TABLESAMPLE(x ROWS): Sample the table down to the given number of rows.
   * - TABLESAMPLE(x PERCENT): Sample the table down to the given percentage. Note that percentages
   * are defined as a number between 0 and 100.
   * - TABLESAMPLE(BUCKET x OUT OF y): Sample the table down to a 'x' divided by 'y' fraction.
   */
  override def visitSampledRelation(ctx: SampledRelationContext): LogicalPlan = withOrigin(ctx) {
    val relation = plan(ctx.relationPrimary)

    // Create a sampled plan if we need one.
    def sample(fraction: Double): Sample = {
      Sample(0.0, fraction, withReplacement = false, (math.random * 1000).toInt, relation)(true)
    }

    // Sample the relation if we have to.
    relation.optional(ctx.sampleType) {
      ctx.sampleType.getType match {
        case SqlBaseParser.ROWS =>
          Limit(expression(ctx.expression), relation)

        case SqlBaseParser.PERCENTLIT =>
          val fraction = ctx.percentage.getText.toDouble
          // The range of fraction accepted by Sample is [0, 1]. Because Hive's block sampling
          // function takes X PERCENT as the input and the range of X is [0, 100], we need to
          // adjust the fraction.
          val eps = RandomSampler.roundingEpsilon
          require(fraction >= 0.0 - eps && fraction <= 100.0 + eps,
            s"Sampling fraction ($fraction) must be on interval [0, 100]")
          sample(fraction / 100.0d)

        case SqlBaseParser.BUCKET if ctx.ON != null =>
          notSupported("TABLESAMPLE(BUCKET x OUT OF y ON id) is not supported", ctx)

        case SqlBaseParser.BUCKET =>
          sample(ctx.numerator.getText.toDouble / ctx.denominator.getText.toDouble)
      }
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
    UnresolvedRelation(
      visitTableIdentifier(ctx.tableIdentifier),
      Option(ctx.identifier).map(_.getText))
  }

  /**
   * Create an inline table (a virtual table in Hive parlance).
   */
  override def visitInlineTable(ctx: InlineTableContext): LogicalPlan = withOrigin(ctx) {
    // Get the backing expressions.
    val expressions = ctx.expression.asScala.map(expression)

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
        assert(expression.foldable, "All expressions in an inline table must be constants.", ctx)
        val safe = Cast(structConstructor(expression), structType)
        safe.eval().asInstanceOf[InternalRow]
    }

    // Construct attributes.
    val baseAttributes = structType.toAttributes
    val attributes = if (ctx.columnAliases != null) {
      val aliases = visitColumnAliases(ctx.columnAliases)
      assert(aliases.size == baseAttributes.size,
        "Number of aliases must match the number of fields in an inline table.", ctx)
      baseAttributes.zip(aliases).map(p => p._1.withName(p._2))
    } else {
      baseAttributes
    }

    LocalRelation(attributes, rows)
  }

  /**
   * Create an alias (SubqueryAlias) for a join relation. This is practically the same as
   * visitAliasedQuery and visitNamedExpression, ANTLR4 however requires us to use 3 different
   * hooks.
   */
  override def visitAliasedRelation(ctx: AliasedRelationContext): LogicalPlan = withOrigin(ctx) {
    aliasPlan(ctx.identifier, plan(ctx.relation()))
  }

  /**
   * Create an alias (SubqueryAlias) for a sub-query. This is practically the same as
   * visitAliasedRelation and visitNamedExpression, ANTLR4 however requires us to use 3 different
   * hooks.
   */
  override def visitAliasedQuery(ctx: AliasedQueryContext): LogicalPlan = withOrigin(ctx) {
    aliasPlan(ctx.identifier, plan(ctx.queryNoWith))
  }

  /**
   * Create an alias (SubqueryAlias) for a LogicalPlan. The alias is allowed to be optional.
   */
  private def aliasPlan(alias: IdentifierContext, plan: LogicalPlan): LogicalPlan = {
    plan.optional(alias) {
      SubqueryAlias(alias.getText, plan)
    }
  }

  /**
   * Create a Sequence of Strings for a parenthesis enclosed alias list.
   */
  override def visitColumnAliases(ctx: ColumnAliasesContext): Seq[String] = withOrigin(ctx) {
    visitColumnAliasList(ctx.columnAliasList)
  }

  /**
   * Create a Sequence of Strings for an alias list.
   */
  override def visitColumnAliasList(ctx: ColumnAliasListContext): Seq[String] = withOrigin(ctx) {
    ctx.identifier.asScala.map(_.getText)
  }

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
  private def expression(tree: ParserRuleContext): Expression = typedVisit(tree)

  private def expressionList(trees: java.util.List[ExpressionContext]): Seq[Expression] = {
    trees.asScala.map(expression)
  }

  private def invertIfNotDefined(expression: Expression, not: TerminalNode): Expression = {
    if (not != null) {
      Not(expression)
    } else {
      expression
    }
  }

  override def visitSelectAll(ctx: SelectAllContext): Expression = withOrigin(ctx) {
    UnresolvedStar(Option(ctx.qualifiedName()).map(_.identifier.asScala.map(_.getText)))
  }

  override def visitNamedExpression(ctx: NamedExpressionContext): Expression = withOrigin(ctx) {
    val e = expression(ctx.expression)
    if (ctx.identifier != null) {
      Alias(e, ctx.identifier.getText)()
    } else if (ctx.columnAliases != null) {
      MultiAlias(e, visitColumnAliases(ctx.columnAliases))
    } else {
      e
    }
  }

  override def visitLogicalBinary(ctx: LogicalBinaryContext): Expression = withOrigin(ctx) {
    val left = expression(ctx.left)
    val right = expression(ctx.right)
    ctx.operator.getType match {
      case SqlBaseParser.AND =>
        And(left, right)
      case SqlBaseParser.OR =>
        Or(left, right)
    }
  }

  override def visitLogicalNot(ctx: LogicalNotContext): Expression = withOrigin(ctx) {
    Not(expression(ctx.booleanExpression()))
  }

  override def visitExists(ctx: ExistsContext): Expression = {
    notSupported("Exists is not supported.", ctx)
  }

  override def visitComparison(ctx: ComparisonContext): Expression = withOrigin(ctx) {
    val left = expression(ctx.value)
    val right = expression(ctx.right)
    val operator = ctx.comparisonOperator().getChild(0).asInstanceOf[TerminalNode]
    operator.getSymbol.getType match {
      case SqlBaseParser.EQ =>
        EqualTo(left, right)
      case SqlBaseParser.NSEQ =>
        EqualNullSafe(left, right)
      case SqlBaseParser.NEQ =>
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

  override def visitBetween(ctx: BetweenContext): Expression = withOrigin(ctx) {
    val value = expression(ctx.value)
    val between = And(
      GreaterThanOrEqual(value, expression(ctx.lower)),
      LessThanOrEqual(value, expression(ctx.upper)))
    invertIfNotDefined(between, ctx.NOT)
  }

  override def visitInList(ctx: InListContext): Expression = withOrigin(ctx) {
    val in = In(expression(ctx.value), ctx.expression().asScala.map(expression))
    invertIfNotDefined(in, ctx.NOT)
  }

  override def visitInSubquery(ctx: InSubqueryContext): Expression = {
    notSupported("IN with a Sub-query is currently not supported.", ctx)
  }

  override def visitLike(ctx: LikeContext): Expression = {
    val left = expression(ctx.value)
    val right = expression(ctx.pattern)
    val like = ctx.like.getType match {
      case SqlBaseParser.LIKE =>
        Like(left, right)
      case SqlBaseParser.RLIKE =>
        RLike(left, right)
    }
    invertIfNotDefined(like, ctx.NOT)
  }

  override def visitNullPredicate(ctx: NullPredicateContext): Expression = withOrigin(ctx) {
    val value = expression(ctx.value)
    if (ctx.NOT != null) {
      IsNotNull(value)
    } else {
      IsNull(value)
    }
  }

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
        BitwiseXor(left, right)
    }
  }

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

  override def visitCast(ctx: CastContext): Expression = withOrigin(ctx) {
    Cast(expression(ctx.expression), typedVisit(ctx.dataType))
  }

  override def visitPrimitiveDatatype(ctx: PrimitiveDatatypeContext): DataType = withOrigin(ctx) {
    (ctx.identifier.getText.toLowerCase, ctx.typeParameter().asScala.toList) match {
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
      case other => notSupported(s"DataType '$other' is not supported.", ctx)
    }
  }

  override def visitComplexDataType(ctx: ComplexDataTypeContext): DataType = withOrigin(ctx) {
    ctx.complex.getType match {
      case SqlBaseParser.ARRAY =>
        ArrayType(typedVisit(ctx.dataType(0)))
      case SqlBaseParser.MAP =>
        MapType(typedVisit(ctx.dataType(0)), typedVisit(ctx.dataType(1)))
      case SqlBaseParser.STRUCT =>
        visitColTypeList(ctx.colTypeList)
    }
  }

  override def visitColTypeList(ctx: ColTypeListContext): StructType = withOrigin(ctx) {
    StructType(ctx.colType().asScala.map(visitColType))
  }

  override def visitColType(ctx: ColTypeContext): StructField = withOrigin(ctx) {
    import ctx._

    // Add the comment to the metadata.
    val builder = new MetadataBuilder
    if (STRING != null) {
      builder.putString("comment", unquote(STRING.getText))
    }

    StructField(
      identifier.getText,
      typedVisit(dataType),
      nullable = true,
      builder.build())
  }

  override def visitFunctionCall(ctx: FunctionCallContext): Expression = withOrigin(ctx) {
    val arguments = if (ctx.ASTERISK != null) {
      Seq(Literal(1))
    } else {
      ctx.expression().asScala.map(expression)
    }

    val function = UnresolvedFunction(
      ctx.qualifiedName.getText,
      arguments,
      Option(ctx.setQuantifier()).exists(_.DISTINCT != null))

    // Check if the function is evaluated in a windowed context.
    ctx.windowSpec match {
      case spec: WindowRefContext =>
        UnresolvedWindowExpression(function, visitWindowRef(spec))
      case spec: WindowDefContext =>
        WindowExpression(function, visitWindowDef(spec))
      case _ => function
    }
  }

  override def visitWindowRef(ctx: WindowRefContext): WindowSpecReference = withOrigin(ctx) {
    WindowSpecReference(ctx.identifier.getText)
  }

  override def visitWindowDef(ctx: WindowDefContext): WindowSpecDefinition = withOrigin(ctx) {
    // PARTITION BY ... ORDER BY ...
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

  override def visitFrameBound(ctx: FrameBoundContext): FrameBoundary = withOrigin(ctx) {
    // We currently only allow foldable integers.
    def value: Int = {
      val e = expression(ctx.expression)
      assert(e.foldable && e.dataType == IntegerType,
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

  override def visitRowConstructor(ctx: RowConstructorContext): Expression = withOrigin(ctx) {
    CreateStruct(ctx.expression().asScala.map(expression))
  }

  override def visitArrayConstructor(ctx: ArrayConstructorContext): Expression = withOrigin(ctx) {
    CreateArray(ctx.expression().asScala.map(expression))
  }

  override def visitSubqueryExpression(
      ctx: SubqueryExpressionContext): Expression = withOrigin(ctx) {
    ScalarSubquery(plan(ctx.query))
  }

  override def visitSimpleCase(ctx: SimpleCaseContext): Expression = withOrigin(ctx) {
    val e = expression(ctx.valueExpression)
    val branches = ctx.whenClause.asScala.map { wCtx =>
      (EqualTo(e, expression(wCtx.condition)), expression(wCtx.result))
    }
    CaseWhen(branches, Option(ctx.elseExpression).map(expression))
  }

  override def visitSearchedCase(ctx: SearchedCaseContext): Expression = withOrigin(ctx) {
    val branches = ctx.whenClause.asScala.map { wCtx =>
      (expression(wCtx.condition), expression(wCtx.result))
    }
    CaseWhen(branches, Option(ctx.elseExpression).map(expression))
  }

  override def visitDereference(ctx: DereferenceContext): Expression = withOrigin(ctx) {
    val attr = ctx.fieldName.getText
    expression(ctx.base) match {
      case UnresolvedAttribute(nameParts) =>
        UnresolvedAttribute(nameParts :+ attr)
      case e =>
        UnresolvedExtractValue(e, Literal(attr))
    }
  }

  override def visitColumnReference(ctx: ColumnReferenceContext): Expression = withOrigin(ctx) {
    UnresolvedAttribute.quoted(ctx.getText)
  }

  override def visitSubscript(ctx: SubscriptContext): Expression = withOrigin(ctx) {
    UnresolvedExtractValue(expression(ctx.value), expression(ctx.index))
  }


  override def visitParenthesizedExpression(
     ctx: ParenthesizedExpressionContext): Expression = withOrigin(ctx) {
    expression(ctx.expression)
  }

  override def visitSortItem(ctx: SortItemContext): SortOrder = withOrigin(ctx) {
    if (ctx.DESC != null) {
      SortOrder(expression(ctx.expression), Descending)
    } else {
      SortOrder(expression(ctx.expression), Ascending)
    }
  }

  override def visitTypeConstructor(ctx: TypeConstructorContext): Literal = withOrigin(ctx) {
    val value = unquote(ctx.STRING.getText)
    ctx.identifier.getText.toUpperCase match {
      case "DATE" =>
        Literal(Date.valueOf(value))
      case "TIMESTAMP" =>
        Literal(Timestamp.valueOf(value))
      case other =>
        notSupported(s"Literals of type '$other' are currently not supported.", ctx)
    }
  }

  override def visitNullLiteral(ctx: NullLiteralContext): Literal = withOrigin(ctx) {
    Literal(null)
  }

  override def visitBooleanLiteral(ctx: BooleanLiteralContext): Literal = withOrigin(ctx) {
   Literal(ctx.getText.toBoolean)
  }

  override def visitIntegerLiteral(ctx: IntegerLiteralContext): Literal = withOrigin(ctx) {
    BigDecimal(ctx.getText) match {
      case v if v.isValidInt =>
        Literal(v.intValue())
      case v if v.isValidLong =>
        Literal(v.longValue())
      case v => Literal(v.underlying())
    }
  }

  override def visitTinyIntLiteral(ctx: TinyIntLiteralContext): Literal = withOrigin(ctx) {
    Literal(ctx.getText.toByte)
  }

  override def visitSmallIntLiteral(ctx: SmallIntLiteralContext): Literal = withOrigin(ctx) {
    Literal(ctx.getText.toShort)
  }

  override def visitBigIntLiteral(ctx: BigIntLiteralContext): Literal = withOrigin(ctx) {
    Literal(ctx.getText.toLong)
  }

  override def visitDecimalLiteral(ctx: DecimalLiteralContext): Literal = withOrigin(ctx) {
    Literal(BigDecimal(ctx.getText).underlying())
  }

  override def visitScientificDecimalLiteral(
      ctx: ScientificDecimalLiteralContext): Literal = withOrigin(ctx) {
    Literal(ctx.getText.toDouble)
  }

  override def visitDoubleLiteral(ctx: DoubleLiteralContext): Literal = withOrigin(ctx) {
    Literal(ctx.getText.toDouble)
  }

  override def visitStringLiteral(ctx: StringLiteralContext): Literal = withOrigin(ctx) {
    Literal(ctx.STRING().asScala.map(s => unquote(s.getText)).mkString)
  }

  override def visitInterval(ctx: IntervalContext): Literal = withOrigin(ctx) {
    val intervals = ctx.intervalField.asScala.map(visitIntervalField)
    assert(intervals.nonEmpty, "at least one time unit should be given for interval literal", ctx)
    Literal(intervals.reduce(_.add(_)))
  }

  override def visitIntervalField(ctx: IntervalFieldContext): CalendarInterval = withOrigin(ctx) {
    val s = ctx.value.getText
    val i = (ctx.unit.getText.toLowerCase, Option(ctx.to).map(_.getText.toLowerCase)) match {
      case (unit, None) if unit.endsWith("s") =>
        CalendarInterval.fromSingleUnitString(unit.substring(0, unit.length - 1), s)
      case (unit, None) =>
        CalendarInterval.fromSingleUnitString(unit, s)
      case ("year", Some("month")) =>
        CalendarInterval.fromYearMonthString(s)
      case ("day", Some("second")) =>
        CalendarInterval.fromDayTimeString(s)
      case (from, Some(to)) =>
        notSupported(s"Intervals FROM $from TO $to are not supported.", ctx)
    }
    assert(i != null, "No interval can be constructed", ctx)
    i
  }
}

private[spark] object AstBuilder extends Logging {

  def unquote(raw: String): String = {
    ParseUtils.unescapeSQLString(raw)
  }

  def withOrigin[T](ctx: ParserRuleContext)(f: => T): T = {
    val current = CurrentOrigin.get
    val token = ctx.getStart
    CurrentOrigin.setPosition(token.getLine, token.getCharPositionInLine)
    try {
      f
    } finally {
      CurrentOrigin.set(current)
    }
  }

  def notSupported(message: String, ctx: ParserRuleContext): Nothing = {
    val token = ctx.getStart
    throw new AnalysisException(
      message + s"\n$ctx",
      Some(token.getLine),
      Some(token.getCharPositionInLine))
  }

  def assert(f: => Boolean, message: String, ctx: ParserRuleContext): Unit = {
    if (!f) {
      notSupported(message, ctx)
    }
  }

  implicit class EnhancedLogicalPlan(val plan: LogicalPlan) extends AnyVal {
    def optional(ctx: AnyRef)(f: => LogicalPlan): LogicalPlan = {
      if (ctx != null) {
        f
      } else {
        plan
      }
    }

    def optionalMap[C <: ParserRuleContext](
        ctx: C)(
        f: (C, LogicalPlan) => LogicalPlan): LogicalPlan = {
      if (ctx != null) {
        f(ctx, plan)
      } else {
        plan
      }
    }
  }
}
