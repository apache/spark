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

package org.apache.spark.sql.connect.planner

import scala.annotation.elidable.byName
import scala.collection.JavaConverters._

import org.apache.spark.connect.proto
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAlias, UnresolvedAttribute, UnresolvedFunction, UnresolvedRelation, UnresolvedStar}
import org.apache.spark.sql.catalyst.expressions
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeReference, Expression}
import org.apache.spark.sql.catalyst.optimizer.CombineUnions
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.catalyst.plans.{logical, FullOuter, Inner, JoinType, LeftAnti, LeftOuter, LeftSemi, RightOuter, UsingJoin}
import org.apache.spark.sql.catalyst.plans.logical.{Deduplicate, Except, Intersect, LogicalPlan, Sample, SubqueryAlias, Union}
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.types._
import org.apache.spark.util.Utils

final case class InvalidPlanInput(
    private val message: String = "",
    private val cause: Throwable = None.orNull)
    extends Exception(message, cause)

class SparkConnectPlanner(plan: proto.Relation, session: SparkSession) {

  def transform(): LogicalPlan = {
    transformRelation(plan)
  }

  // The root of the query plan is a relation and we apply the transformations to it.
  private def transformRelation(rel: proto.Relation): LogicalPlan = {
    val common = if (rel.hasCommon) {
      Some(rel.getCommon)
    } else {
      None
    }

    rel.getRelTypeCase match {
      case proto.Relation.RelTypeCase.READ => transformReadRel(rel.getRead, common)
      case proto.Relation.RelTypeCase.PROJECT => transformProject(rel.getProject, common)
      case proto.Relation.RelTypeCase.FILTER => transformFilter(rel.getFilter)
      case proto.Relation.RelTypeCase.LIMIT => transformLimit(rel.getLimit)
      case proto.Relation.RelTypeCase.OFFSET => transformOffset(rel.getOffset)
      case proto.Relation.RelTypeCase.JOIN => transformJoin(rel.getJoin)
      case proto.Relation.RelTypeCase.DEDUPLICATE => transformDeduplicate(rel.getDeduplicate)
      case proto.Relation.RelTypeCase.SET_OP => transformSetOperation(rel.getSetOp)
      case proto.Relation.RelTypeCase.SORT => transformSort(rel.getSort)
      case proto.Relation.RelTypeCase.AGGREGATE => transformAggregate(rel.getAggregate)
      case proto.Relation.RelTypeCase.SQL => transformSql(rel.getSql)
      case proto.Relation.RelTypeCase.LOCAL_RELATION =>
        transformLocalRelation(rel.getLocalRelation, common)
      case proto.Relation.RelTypeCase.SAMPLE => transformSample(rel.getSample)
      case proto.Relation.RelTypeCase.RELTYPE_NOT_SET =>
        throw new IndexOutOfBoundsException("Expected Relation to be set, but is empty.")
      case _ => throw InvalidPlanInput(s"${rel.getUnknown} not supported.")
    }
  }

  private def transformSql(sql: proto.SQL): LogicalPlan = {
    session.sessionState.sqlParser.parsePlan(sql.getQuery)
  }

  /**
   * All fields of [[proto.Sample]] are optional. However, given those are proto primitive types,
   * we cannot differentiate if the field is not or set when the field's value equals to the type
   * default value. In the future if this ever become a problem, one solution could be that to
   * wrap such fields into proto messages.
   */
  private def transformSample(rel: proto.Sample): LogicalPlan = {
    Sample(
      rel.getLowerBound,
      rel.getUpperBound,
      rel.getWithReplacement,
      if (rel.hasSeed) rel.getSeed.getSeed else Utils.random.nextLong,
      transformRelation(rel.getInput))
  }

  private def transformDeduplicate(rel: proto.Deduplicate): LogicalPlan = {
    if (!rel.hasInput) {
      throw InvalidPlanInput("Deduplicate needs a plan input")
    }
    if (rel.getAllColumnsAsKeys && rel.getColumnNamesCount > 0) {
      throw InvalidPlanInput("Cannot deduplicate on both all columns and a subset of columns")
    }
    if (!rel.getAllColumnsAsKeys && rel.getColumnNamesCount == 0) {
      throw InvalidPlanInput(
        "Deduplicate requires to either deduplicate on all columns or a subset of columns")
    }
    val queryExecution = new QueryExecution(session, transformRelation(rel.getInput))
    val resolver = session.sessionState.analyzer.resolver
    val allColumns = queryExecution.analyzed.output
    if (rel.getAllColumnsAsKeys) {
      Deduplicate(allColumns, queryExecution.analyzed)
    } else {
      val toGroupColumnNames = rel.getColumnNamesList.asScala.toSeq
      val groupCols = toGroupColumnNames.flatMap { (colName: String) =>
        // It is possibly there are more than one columns with the same name,
        // so we call filter instead of find.
        val cols = allColumns.filter(col => resolver(col.name, colName))
        if (cols.isEmpty) {
          throw InvalidPlanInput(s"Invalid deduplicate column ${colName}")
        }
        cols
      }
      Deduplicate(groupCols, queryExecution.analyzed)
    }
  }

  private def transformLocalRelation(
      rel: proto.LocalRelation,
      common: Option[proto.RelationCommon]): LogicalPlan = {
    val attributes = rel.getAttributesList.asScala.map(transformAttribute(_)).toSeq
    val relation = new org.apache.spark.sql.catalyst.plans.logical.LocalRelation(attributes)
    if (common.nonEmpty && common.get.getAlias.nonEmpty) {
      logical.SubqueryAlias(identifier = common.get.getAlias, child = relation)
    } else {
      relation
    }
  }

  private def transformAttribute(exp: proto.Expression.QualifiedAttribute): Attribute = {
    AttributeReference(exp.getName, DataTypeProtoConverter.toCatalystType(exp.getType))()
  }

  private def transformReadRel(
      rel: proto.Read,
      common: Option[proto.RelationCommon]): LogicalPlan = {
    val baseRelation = rel.getReadTypeCase match {
      case proto.Read.ReadTypeCase.NAMED_TABLE =>
        val multipartIdentifier =
          CatalystSqlParser.parseMultipartIdentifier(rel.getNamedTable.getUnparsedIdentifier)
        val child = UnresolvedRelation(multipartIdentifier)
        if (common.nonEmpty && common.get.getAlias.nonEmpty) {
          SubqueryAlias(identifier = common.get.getAlias, child = child)
        } else {
          child
        }
      case proto.Read.ReadTypeCase.DATA_SOURCE =>
        if (rel.getDataSource.getFormat == "") {
          throw InvalidPlanInput("DataSource requires a format")
        }
        val localMap = CaseInsensitiveMap[String](rel.getDataSource.getOptionsMap.asScala.toMap)
        val reader = session.read
        reader.format(rel.getDataSource.getFormat)
        localMap.foreach { case (key, value) => reader.option(key, value) }
        if (rel.getDataSource.getSchema != null) {
          reader.schema(rel.getDataSource.getSchema)
        }
        reader.load().queryExecution.analyzed
      case _ => throw InvalidPlanInput("Does not support " + rel.getReadTypeCase.name())
    }
    baseRelation
  }

  private def transformFilter(rel: proto.Filter): LogicalPlan = {
    assert(rel.hasInput)
    val baseRel = transformRelation(rel.getInput)
    logical.Filter(condition = transformExpression(rel.getCondition), child = baseRel)
  }

  private def transformProject(
      rel: proto.Project,
      common: Option[proto.RelationCommon]): LogicalPlan = {
    val baseRel = transformRelation(rel.getInput)
    // TODO: support the target field for *.
    val projection =
      if (rel.getExpressionsCount == 1 && rel.getExpressions(0).hasUnresolvedStar) {
        Seq(UnresolvedStar(Option.empty))
      } else {
        rel.getExpressionsList.asScala.map(transformExpression).map(UnresolvedAlias(_))
      }
    val project = logical.Project(projectList = projection.toSeq, child = baseRel)
    if (common.nonEmpty && common.get.getAlias.nonEmpty) {
      logical.SubqueryAlias(identifier = common.get.getAlias, child = project)
    } else {
      project
    }
  }

  private def transformUnresolvedExpression(exp: proto.Expression): UnresolvedAttribute = {
    UnresolvedAttribute(exp.getUnresolvedAttribute.getUnparsedIdentifier)
  }

  private def transformExpression(exp: proto.Expression): Expression = {
    exp.getExprTypeCase match {
      case proto.Expression.ExprTypeCase.LITERAL => transformLiteral(exp.getLiteral)
      case proto.Expression.ExprTypeCase.UNRESOLVED_ATTRIBUTE =>
        transformUnresolvedExpression(exp)
      case proto.Expression.ExprTypeCase.UNRESOLVED_FUNCTION =>
        transformScalarFunction(exp.getUnresolvedFunction)
      case proto.Expression.ExprTypeCase.ALIAS => transformAlias(exp.getAlias)
      case _ => throw InvalidPlanInput()
    }
  }

  /**
   * Transforms the protocol buffers literals into the appropriate Catalyst literal expression.
   *
   * TODO(SPARK-40533): Missing support for Instant, BigDecimal, LocalDate, LocalTimestamp,
   * Duration, Period.
   * @param lit
   * @return
   *   Expression
   */
  private def transformLiteral(lit: proto.Expression.Literal): Expression = {
    lit.getLiteralTypeCase match {
      case proto.Expression.Literal.LiteralTypeCase.BOOLEAN => expressions.Literal(lit.getBoolean)
      case proto.Expression.Literal.LiteralTypeCase.I8 => expressions.Literal(lit.getI8, ByteType)
      case proto.Expression.Literal.LiteralTypeCase.I16 =>
        expressions.Literal(lit.getI16, ShortType)
      case proto.Expression.Literal.LiteralTypeCase.I32 => expressions.Literal(lit.getI32)
      case proto.Expression.Literal.LiteralTypeCase.I64 => expressions.Literal(lit.getI64)
      case proto.Expression.Literal.LiteralTypeCase.FP32 =>
        expressions.Literal(lit.getFp32, FloatType)
      case proto.Expression.Literal.LiteralTypeCase.FP64 =>
        expressions.Literal(lit.getFp64, DoubleType)
      case proto.Expression.Literal.LiteralTypeCase.STRING => expressions.Literal(lit.getString)
      case proto.Expression.Literal.LiteralTypeCase.BINARY =>
        expressions.Literal(lit.getBinary, BinaryType)
      // Microseconds since unix epoch.
      case proto.Expression.Literal.LiteralTypeCase.TIME =>
        expressions.Literal(lit.getTime, TimestampType)
      // Days since UNIX epoch.
      case proto.Expression.Literal.LiteralTypeCase.DATE =>
        expressions.Literal(lit.getDate, DateType)
      case _ =>
        throw InvalidPlanInput(
          s"Unsupported Literal Type: ${lit.getLiteralTypeCase.getNumber}" +
            s"(${lit.getLiteralTypeCase.name})")
    }
  }

  private def transformLimit(limit: proto.Limit): LogicalPlan = {
    logical.Limit(
      limitExpr = expressions.Literal(limit.getLimit, IntegerType),
      transformRelation(limit.getInput))
  }

  private def transformOffset(offset: proto.Offset): LogicalPlan = {
    logical.Offset(
      offsetExpr = expressions.Literal(offset.getOffset, IntegerType),
      transformRelation(offset.getInput))
  }

  /**
   * Translates a scalar function from proto to the Catalyst expression.
   *
   * TODO(SPARK-40546) We need to homogenize the function names for binary operators.
   *
   * @param fun
   *   Proto representation of the function call.
   * @return
   */
  private def transformScalarFunction(fun: proto.Expression.UnresolvedFunction): Expression = {
    if (fun.getPartsCount == 1 && fun.getParts(0).contains(".")) {
      throw new IllegalArgumentException(
        "Function identifier must be passed as sequence of name parts.")
    }
    UnresolvedFunction(
      fun.getPartsList.asScala.toSeq,
      fun.getArgumentsList.asScala.map(transformExpression).toSeq,
      isDistinct = false)
  }

  private def transformAlias(alias: proto.Expression.Alias): Expression = {
    Alias(transformExpression(alias.getExpr), alias.getName)()
  }

  private def transformSetOperation(u: proto.SetOperation): LogicalPlan = {
    assert(u.hasLeftInput && u.hasRightInput, "Union must have 2 inputs")

    u.getSetOpType match {
      case proto.SetOperation.SetOpType.SET_OP_TYPE_EXCEPT =>
        if (u.getByName) {
          throw InvalidPlanInput("Except does not support union_by_name")
        }
        Except(transformRelation(u.getLeftInput), transformRelation(u.getRightInput), u.getIsAll)
      case proto.SetOperation.SetOpType.SET_OP_TYPE_INTERSECT =>
        if (u.getByName) {
          throw InvalidPlanInput("Intersect does not support union_by_name")
        }
        Intersect(
          transformRelation(u.getLeftInput),
          transformRelation(u.getRightInput),
          u.getIsAll)
      case proto.SetOperation.SetOpType.SET_OP_TYPE_UNION =>
        val combinedUnion = CombineUnions(
          Union(
            Seq(transformRelation(u.getLeftInput), transformRelation(u.getRightInput)),
            byName = u.getByName))
        if (u.getIsAll) {
          combinedUnion
        } else {
          logical.Deduplicate(combinedUnion.output, combinedUnion)
        }
      case _ =>
        throw InvalidPlanInput(s"Unsupported set operation ${u.getSetOpTypeValue}")
    }
  }

  private def transformJoin(rel: proto.Join): LogicalPlan = {
    assert(rel.hasLeft && rel.hasRight, "Both join sides must be present")
    if (rel.hasJoinCondition && rel.getUsingColumnsCount > 0) {
      throw InvalidPlanInput(
        s"Using columns or join conditions cannot be set at the same time in Join")
    }
    val joinCondition =
      if (rel.hasJoinCondition) Some(transformExpression(rel.getJoinCondition)) else None
    val catalystJointype = transformJoinType(
      if (rel.getJoinType != null) rel.getJoinType else proto.Join.JoinType.JOIN_TYPE_INNER)
    val joinType = if (rel.getUsingColumnsCount > 0) {
      UsingJoin(catalystJointype, rel.getUsingColumnsList.asScala.toSeq)
    } else {
      catalystJointype
    }
    logical.Join(
      left = transformRelation(rel.getLeft),
      right = transformRelation(rel.getRight),
      joinType = joinType,
      condition = joinCondition,
      hint = logical.JoinHint.NONE)
  }

  private def transformJoinType(t: proto.Join.JoinType): JoinType = {
    t match {
      case proto.Join.JoinType.JOIN_TYPE_INNER => Inner
      case proto.Join.JoinType.JOIN_TYPE_LEFT_ANTI => LeftAnti
      case proto.Join.JoinType.JOIN_TYPE_FULL_OUTER => FullOuter
      case proto.Join.JoinType.JOIN_TYPE_LEFT_OUTER => LeftOuter
      case proto.Join.JoinType.JOIN_TYPE_RIGHT_OUTER => RightOuter
      case proto.Join.JoinType.JOIN_TYPE_LEFT_SEMI => LeftSemi
      case _ => throw InvalidPlanInput(s"Join type ${t} is not supported")
    }
  }

  private def transformSort(rel: proto.Sort): LogicalPlan = {
    assert(rel.getSortFieldsCount > 0, "'sort_fields' must be present and contain elements.")
    logical.Sort(
      child = transformRelation(rel.getInput),
      global = true,
      order = rel.getSortFieldsList.asScala.map(transformSortOrderExpression).toSeq)
  }

  private def transformSortOrderExpression(so: proto.Sort.SortField): expressions.SortOrder = {
    expressions.SortOrder(
      child = transformUnresolvedExpression(so.getExpression),
      direction = so.getDirection match {
        case proto.Sort.SortDirection.SORT_DIRECTION_DESCENDING => expressions.Descending
        case _ => expressions.Ascending
      },
      nullOrdering = so.getNulls match {
        case proto.Sort.SortNulls.SORT_NULLS_LAST => expressions.NullsLast
        case _ => expressions.NullsFirst
      },
      sameOrderExpressions = Seq.empty)
  }

  private def transformAggregate(rel: proto.Aggregate): LogicalPlan = {
    assert(rel.hasInput)

    val groupingExprs =
      rel.getGroupingExpressionsList.asScala
        .map(transformExpression)
        .map {
          case x @ UnresolvedAttribute(_) => x
          case x => UnresolvedAlias(x)
        }

    logical.Aggregate(
      child = transformRelation(rel.getInput),
      groupingExpressions = groupingExprs.toSeq,
      aggregateExpressions =
        rel.getResultExpressionsList.asScala.map(transformAggregateExpression).toSeq)
  }

  private def transformAggregateExpression(
      exp: proto.Aggregate.AggregateFunction): expressions.NamedExpression = {
    val fun = exp.getName
    UnresolvedAlias(
      UnresolvedFunction(
        name = fun,
        arguments = exp.getArgumentsList.asScala.map(transformExpression).toSeq,
        isDistinct = false))
  }

}
