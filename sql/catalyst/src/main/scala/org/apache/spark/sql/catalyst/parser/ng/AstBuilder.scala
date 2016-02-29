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

import org.antlr.v4.runtime.ParserRuleContext
import org.antlr.v4.runtime.tree.{ParseTree, TerminalNode}

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.{InternalRow, TableIdentifier}
import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.parser.ParseUtils
import org.apache.spark.sql.catalyst.parser.ng.SqlBaseParser._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.trees.{CurrentOrigin, TreeNode}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.CalendarInterval
import org.apache.spark.util.random.RandomSampler

/**
 * The AstBuilder converts an ANTLR ParseTree into a catalyst Expression, LogicalPlan or
 * TableIdentifier.
 *
 * DDL
 * - Everything
 * - Show Functions
 * - Describe
 * - CTAS
 * - CT USING
 * - CACHE
 * - SET
 *
 * Query TODO's
 * - Multiple Inserts
 * - Transform
 * - MAP before SELECT
 *
 * Expression TODO's
 * -Hive Hintlist???
 */
class AstBuilder extends SqlBaseBaseVisitor[AnyRef] {
  import AstBuilder._

  private def typedVisit[T](ctx: ParseTree): T = {
    ctx.accept(this).asInstanceOf[T]
  }

  override def visitSingleStatement(ctx: SingleStatementContext): LogicalPlan = withOrigin(ctx) {
    typedVisit(ctx.statement)
  }

  override def visitSingleExpression(ctx: SingleExpressionContext): Expression = withOrigin(ctx) {
    visitNamedExpression(ctx.namedExpression)
  }

  override def visitSingleTableIdentifier(
      ctx: SingleTableIdentifierContext): TableIdentifier = withOrigin(ctx) {
    visitTableIdentifier(ctx.tableIdentifier)
  }

  /* --------------------------------------------------------------------------------------------
   * Plan parsing
   * -------------------------------------------------------------------------------------------- */
  private def plan(tree: ParserRuleContext): LogicalPlan = typedVisit(tree)

  override def visitQuery(ctx: QueryContext): LogicalPlan = withOrigin(ctx) {
    val query = plan(ctx.queryNoWith)

    // Apply CTEs
    val withCtes = if (ctx.ctes != null) {
      val ctes = ctx.ctes.namedQuery.asScala.map {
        case nCtx =>
          val namedQuery = visitNamedQuery(nCtx)
          (namedQuery.alias, namedQuery)
      }.toMap
      With(query, ctes)
    } else {
      query
    }

    // Apply Insert Into.
    if (ctx.insertInto() != null) {
      visitInsertInto(ctx.insertInto())(withCtes)
    } else {
      withCtes
    }
  }

  override def visitInsertInto(
      ctx: InsertIntoContext): LogicalPlan => LogicalPlan = withOrigin(ctx) {
    val tableIdent = visitTableIdentifier(ctx.tableIdentifier)
    val partitionKeys = Some(ctx.partitionSpec).toSeq.flatMap {
      _.partitionVal.asScala.map {
        pVal => (pVal.identifier.getText, Option(pVal.constant).map(c => unquote(c.getText)))
      }
    }.toMap

    InsertIntoTable(
      UnresolvedRelation(tableIdent, None),
      partitionKeys,
      _,
      ctx.OVERWRITE != null,
      ctx.EXISTS != null)
  }

  override def visitNamedQuery(ctx: NamedQueryContext): SubqueryAlias = withOrigin(ctx) {
    SubqueryAlias(ctx.name.getText, plan(ctx.query))
  }

  override def visitQueryNoWith(ctx: QueryNoWithContext): LogicalPlan = withOrigin(ctx) {
    import ctx._
    val query = plan(queryTerm)

    // Handle ORDER BY, SORT BY, DISTRIBUTE BY, and CLUSTER BY clause.
    val withOrder = if (
      !order.isEmpty && sort.isEmpty && distributeBy.isEmpty && clusterBy.isEmpty) {
      Sort(order.asScala.map(visitSortItem), global = true, query)
    } else if (order.isEmpty && !sort.isEmpty && distributeBy.isEmpty && clusterBy.isEmpty) {
      Sort(sort.asScala.map(visitSortItem), global = false, query)
    } else if (order.isEmpty && sort.isEmpty && !distributeBy.isEmpty && clusterBy.isEmpty) {
      RepartitionByExpression(expressionList(distributeBy), query)
    } else if (order.isEmpty && !sort.isEmpty && !distributeBy.isEmpty && clusterBy.isEmpty) {
      Sort(
        sort.asScala.map(visitSortItem),
        global = false,
        RepartitionByExpression(expressionList(distributeBy), query))
    } else if (order.isEmpty && sort.isEmpty && distributeBy.isEmpty && !clusterBy.isEmpty) {
      val expressions = expressionList(clusterBy)
      Sort(
        expressions.map(SortOrder(_, Ascending)),
        global = false,
        RepartitionByExpression(expressions, query))
    } else if (order.isEmpty && sort.isEmpty && distributeBy.isEmpty && clusterBy.isEmpty) {
      query
    } else {
      notSupported("Combination of ORDER BY/SORT BY/DISTRIBUTE BY/CLUSTER BY is not supported", ctx)
    }

    // LIMIT
    val withLimit = if (limit != null) {
      Limit(typedVisit(limit), withOrder)
    } else {
      withOrder
    }

    // WINDOWS
    if (ctx.windows != null) {
      WithWindowDefinition(visitWindows(ctx.windows), withLimit)
    } else {
      withLimit
    }
  }

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

  override def visitQuerySpecification(
      ctx: QuerySpecificationContext): LogicalPlan = withOrigin(ctx) {
    // SELECT ...
    val selectExpressions = ctx.selectItem.asScala.map(visit).map {
      case e: Expression => UnresolvedAlias(e)
    }

    // FROM ... JOIN ...
    val relation = ctx.relation.asScala
      .map(plan)
      .reduceLeftOption(Join(_, _, Inner, None))
      .getOrElse(OneRowRelation)

    // WHERE...
    val withWhere = if (ctx.where != null) {
      // Note that we added a cast to boolean. If the expression itself is already boolean,
      // the optimizer will get rid of the unnecessary cast.
      Filter(Cast(expression(ctx.where), BooleanType), relation)
    } else {
      relation
    }

    // TODO Apply transformation
    // LATERAL (OUTER) VIEW ...
    val withLateralView = ctx.lateralView.asScala.foldLeft(withWhere) {
      case (plan, lCtx) =>
        visitLateralView(lCtx)(plan)
    }

    // GROUP BY ...
    val withProject = if (ctx.aggregation != null) {
      visitAggregation(ctx.aggregation) match {
        case (groupingExpressions, Nil) =>
          Aggregate(groupingExpressions, selectExpressions, withLateralView)
        case (groupingExpressions, masks) =>
          GroupingSets(masks, groupingExpressions, withLateralView, selectExpressions)
      }
    } else {
      Project(selectExpressions, withLateralView)
    }

    // HAVING ...
    val withHaving = if (ctx.having != null) {
      Filter(expression(ctx.having), withProject)
    } else {
      withProject
    }

    // SELECT DISTINCT ...
    val withDistinct = if (ctx.setQuantifier() != null && ctx.setQuantifier().DISTINCT() != null) {
      Distinct(withHaving)
    } else {
      withHaving
    }

    // WINDOW ...
    if (ctx.windows != null) {
      WithWindowDefinition(visitWindows(ctx.windows), withDistinct)
    } else {
      withDistinct
    }
  }

  override def visitAggregation(
      ctx: AggregationContext): (Seq[Expression], Seq[Int]) = withOrigin(ctx) {
    val expressions = expressionList(ctx.expression)
    if (ctx.CUBE != null) {
      (Seq(Cube(expressions)), Nil)
    } else if (ctx.ROLLUP != null) {
      (Seq(Rollup(expressions)), Nil)
    } else if (ctx.GROUPING != null) {
      // TODO use new expression set here?
      val expressionMap = expressions.zipWithIndex.toMap
      val numExpressions = expressionMap.size
      val mask = (1 << numExpressions) - 1
      val masks = ctx.groupingSet.asScala.map {
        _.expression.asScala.foldLeft(mask) {
          case (bitmap, eCtx) =>
            // Find the index of the expression.
            val e = expression(eCtx)
            val index = expressionMap.find(_._1.semanticEquals(e)).map(_._2).getOrElse(
              throw new AnalysisException(
                s"${e.treeString} doesn't show up in the GROUP BY list"))
            // 0 means that the column at the given index is a grouping column, 1 means it is not,
            // so we unset the bit in bitmap.
            bitmap & ~(1 << (numExpressions - 1 - index))
        }
      }
      (expressions, masks)
    } else {
      (expressions, Nil)
    }
  }

  override def visitLateralView(
      ctx: LateralViewContext): LogicalPlan => Generate = withOrigin(ctx) {
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

    // Return the partial filled Generate constructor.
    Generate(
      generator,
      join = true,
      outer = ctx.OUTER != null,
      Some(ctx.tblName.getText.toLowerCase),
      ctx.colName.asScala.map(_.getText).map(UnresolvedAttribute.apply),
      _)
  }

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
    Join(plan(ctx.left), plan(ctx.right), joinType, Option(ctx.booleanExpression).map(expression))
  }

  override def visitSampledRelation(ctx: SampledRelationContext): LogicalPlan = withOrigin(ctx) {
    val relation = plan(ctx.relationPrimary)

    // Create a sampled plan if we need one.
    def sample(fraction: Double): Sample = {
      Sample(0.0, fraction, withReplacement = false, (math.random * 1000).toInt, relation)
    }

    // Sample the relation if we have to.
    if (ctx.sampleType != null) {
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
    } else {
      relation
    }
  }

  override def visitTable(ctx: TableContext): LogicalPlan = withOrigin(ctx) {
    UnresolvedRelation(visitTableIdentifier(ctx.tableIdentifier), None)
  }

  override def visitTableName(ctx: TableNameContext): LogicalPlan = withOrigin(ctx) {
    UnresolvedRelation(
      visitTableIdentifier(ctx.tableIdentifier),
      Option(ctx.identifier).map(_.getText))
  }

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
        assert(expression.foldable, "All expressions in an inline table must be constants.")
        val safe = Cast(structConstructor(expression), structType)
        safe.eval().asInstanceOf[InternalRow]
    }

    // Construct attributes.
    val baseAttributes = structType.toAttributes
    val attributes = if (ctx.columnAliases != null) {
      val aliases = ctx.columnAliases.identifier.asScala.map(_.getText)
      assert(aliases.size == baseAttributes.size,
        "Number of aliases must match the number of fields in an inline table.")
      baseAttributes.zip(aliases).map(p => p._1.withName(p._2))
    } else {
      baseAttributes
    }

    LocalRelation(attributes, rows)
  }

  override def visitAliasedRelation(ctx: AliasedRelationContext): LogicalPlan = withOrigin(ctx) {
    aliasPlan(ctx.identifier, plan(ctx.relation()))
  }

  override def visitAliasedQuery(ctx: AliasedQueryContext): LogicalPlan = withOrigin(ctx) {
    aliasPlan(ctx.identifier, plan(ctx.query))
  }

  private def aliasPlan(alias: IdentifierContext, plan: LogicalPlan): LogicalPlan = {
    if (alias != null) {
      SubqueryAlias(alias.getText, plan)
    } else {
      plan
    }
  }

  override def visitWindows(
      ctx: WindowsContext): Map[String, WindowSpecDefinition] = withOrigin(ctx) {
    // Collect all window specifications defined in the WINDOW clause.
    val windowMap = ctx.namedWindow.asScala.map {
      wCtx =>
        (wCtx.identifier.getText, typedVisit[WindowSpec](wCtx.windowSpec))
    }.toMap

    // Handle cases like
    // window w1 as (partition by p_mfgr order by p_name
    //               range between 2 preceding and 2 following),
    //        w2 as w1
    windowMap.mapValues {
      case WindowSpecReference(name) => windowMap(name).asInstanceOf[WindowSpecDefinition]
      case spec: WindowSpecDefinition => spec
    }
  }

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
      MultiAlias(e, ctx.columnAliases.identifier.asScala.map(_.getText))
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
        val fields = ctx.colType().asScala.map { col =>
          // Add the comment to the metadata.
          val builder = new MetadataBuilder
          if (col.STRING != null) {
            builder.putString("comment", unquote(col.STRING.getText))
          }

          StructField(
            col.identifier.getText,
            typedVisit(col.dataType),
            nullable = true,
            builder.build())
        }
        StructType(fields)
    }
  }

  override def visitFunctionCall(ctx: FunctionCallContext): Expression = withOrigin(ctx) {
    val arguments = if (ctx.ASTERISK != null) {
      Seq(UnresolvedStar(None))
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
        "Frame bound value must be a constant integer.")
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

  override def visitIntegerLiteral(ctx: IntegerLiteralContext): TreeNode[_] = withOrigin(ctx) {
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

  override def visitDtsIntervalLiteral(ctx: DtsIntervalLiteralContext): Literal = withOrigin(ctx) {
   Literal(CalendarInterval.fromDayTimeString(unquote(ctx.value.getText)))
  }

  override def visitYtmIntervalLiteral(ctx: YtmIntervalLiteralContext): Literal = withOrigin(ctx) {
    Literal(CalendarInterval.fromYearMonthString(unquote(ctx.value.getText)))
  }

  override def visitComposedIntervalLiteral(
      ctx: ComposedIntervalLiteralContext): Literal = withOrigin(ctx) {
    val intervals = ctx.intervalField().asScala.map { pCtx =>
      CalendarInterval.fromSingleUnitString(pCtx.unit.getText, unquote(pCtx.value.getText))
    }
    assert(intervals.nonEmpty, "Interval should contain at least one or more value and unit pairs")
    Literal(intervals.reduce(_.add(_)))
  }
}

private[spark] object AstBuilder {

  def unquote(raw: String): String = {
    var unquoted = raw
    val lastIndex = raw.length - 1
    if (lastIndex >= 1) {
      val first = raw(0)
      if ((first == '\'' || first == '"') && raw(lastIndex) == first) {
        unquoted = unquoted.substring(1, lastIndex)
      }
      unquoted = ParseUtils.unescapeSQLString(raw)
    }
    unquoted
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
}
