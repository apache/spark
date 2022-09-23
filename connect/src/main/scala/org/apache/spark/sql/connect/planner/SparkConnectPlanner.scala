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

import scala.collection.JavaConverters._

import org.apache.spark.annotation.{Since, Unstable}
import org.apache.spark.connect.proto
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.{expressions, plans}
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAlias, UnresolvedAttribute, UnresolvedFunction, UnresolvedRelation, UnresolvedStar}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.logical
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, SubqueryAlias}
import org.apache.spark.sql.types._

final case class InvalidPlanInput(
    private val message: String = "",
    private val cause: Throwable = None.orNull)
    extends Exception(message, cause)

@Unstable
@Since("3.4.0")
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
      case proto.Relation.RelTypeCase.FETCH => transformFetch(rel.getFetch)
      case proto.Relation.RelTypeCase.JOIN => transformJoin(rel.getJoin)
      case proto.Relation.RelTypeCase.UNION => transformUnion(rel.getUnion)
      case proto.Relation.RelTypeCase.SORT => transformSort(rel.getSort)
      case proto.Relation.RelTypeCase.AGGREGATE => transformAggregate(rel.getAggregate)
      case proto.Relation.RelTypeCase.SQL => transformSql(rel.getSql)
      case proto.Relation.RelTypeCase.RELTYPE_NOT_SET =>
        throw new IndexOutOfBoundsException("Expected Relation to be set, but is empty.")
      case _ => throw InvalidPlanInput(s"${rel.getUnknown} not supported.")
    }
  }

  private def transformSql(sql: proto.SQL): LogicalPlan = {
    session.sessionState.sqlParser.parsePlan(sql.getQuery)
  }

  private def transformReadRel(
      rel: proto.Read,
      common: Option[proto.RelationCommon]): LogicalPlan = {
    val baseRelation = rel.getReadTypeCase match {
      case proto.Read.ReadTypeCase.NAMED_TABLE =>
        val child = UnresolvedRelation(rel.getNamedTable.getPartsList.asScala.toSeq)
        if (common.nonEmpty && common.get.getAlias.nonEmpty) {
          SubqueryAlias(identifier = common.get.getAlias, child = child)
        } else {
          child
        }
      case _ => throw InvalidPlanInput()
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
    val projection = if (rel.getExpressionsCount == 0) {
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
    UnresolvedAttribute(exp.getUnresolvedAttribute.getPartsList.asScala.toSeq)
  }

  private def transformExpression(exp: proto.Expression): Expression = {
    exp.getExprTypeCase match {
      case proto.Expression.ExprTypeCase.LITERAL => transformLiteral(exp.getLiteral)
      case proto.Expression.ExprTypeCase.UNRESOLVED_ATTRIBUTE =>
        transformUnresolvedExpression(exp)
      case proto.Expression.ExprTypeCase.UNRESOLVED_FUNCTION =>
        transformScalarFunction(exp.getUnresolvedFunction)
      case _ => throw InvalidPlanInput()
    }
  }

  /**
   * Transforms the protocol buffers literals into the appropriate Catalyst literal expression.
   *
   * TODO(SPARK-40533): Missing support for Instant, BigDecimal, LocalDate, LocalTimestamp,
   *   Duration, Period.
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
      case _ => throw InvalidPlanInput(
        s"Unsupported Literal Type: ${lit.getLiteralTypeCase.getNumber}" +
          s"(${lit.getLiteralTypeCase.name})")
    }
  }

  private def transformFetch(limit: proto.Fetch): LogicalPlan = {
    logical.Limit(
      child = transformRelation(limit.getInput),
      limitExpr = expressions.Literal(limit.getLimit, IntegerType))
  }

  private def lookupFunction(name: String, args: Seq[Expression]): Expression = {
    UnresolvedFunction(Seq(name), args, isDistinct = false)
  }

  /**
   * Translates a scalar function from proto to the Catalyst expression.
   *
   * TODO(SPARK-40546) We need to homogenize the function names for binary operators.
   *
   * @param fun Proto representation of the function call.
   * @return
   */
  private def transformScalarFunction(fun: proto.Expression.UnresolvedFunction): Expression = {
    val funName = fun.getPartsList.asScala.mkString(".")
    funName match {
      case "gt" =>
        assert(fun.getArgumentsCount == 2, "`gt` function must have two arguments.")
        expressions.GreaterThan(
          transformExpression(fun.getArguments(0)),
          transformExpression(fun.getArguments(1)))
      case "eq" =>
        assert(fun.getArgumentsCount == 2, "`eq` function must have two arguments.")
        expressions.EqualTo(
          transformExpression(fun.getArguments(0)),
          transformExpression(fun.getArguments(1)))
      case _ =>
        lookupFunction(funName, fun.getArgumentsList.asScala.map(transformExpression).toSeq)
    }
  }

  private def transformUnion(u: proto.Union): LogicalPlan = {
    assert(u.getInputsCount == 2, "Union must have 2 inputs")
    val plan = logical.Union(transformRelation(u.getInputs(0)), transformRelation(u.getInputs(1)))

    u.getUnionType match {
      case proto.Union.UnionType.UNION_TYPE_DISTINCT => logical.Distinct(plan)
      case proto.Union.UnionType.UNION_TYPE_ALL => plan
      case _ =>
        throw InvalidPlanInput(s"Unsupported set operation ${u.getUnionTypeValue}")
    }
  }

  private def transformJoin(rel: proto.Join): LogicalPlan = {
    assert(rel.hasLeft && rel.hasRight, "Both join sides must be present")
    logical.Join(
      left = transformRelation(rel.getLeft),
      right = transformRelation(rel.getRight),
      // TODO(SPARK-40534) Support additional join types and configuration.
      joinType = plans.Inner,
      condition = Some(transformExpression(rel.getOn)),
      hint = logical.JoinHint.NONE)
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
    assert(rel.getGroupingSetsCount == 1, "Only one grouping set is supported")

    val groupingSet = rel.getGroupingSetsList.asScala.take(1)
    val ge = groupingSet
      .flatMap(f => f.getAggregateExpressionsList.asScala)
      .map(transformExpression)
      .map {
        case x @ UnresolvedAttribute(_) => x
        case x => UnresolvedAlias(x)
      }

    logical.Aggregate(
      child = transformRelation(rel.getInput),
      groupingExpressions = ge.toSeq,
      aggregateExpressions =
        (rel.getMeasuresList.asScala.map(transformAggregateExpression) ++ ge).toSeq)
  }

  private def transformAggregateExpression(
      exp: proto.Aggregate.Measure): expressions.NamedExpression = {
    val fun = exp.getFunction.getName
    UnresolvedAlias(
      UnresolvedFunction(
        name = fun,
        arguments = exp.getFunction.getArgumentsList.asScala.map(transformExpression).toSeq,
        isDistinct = false))
  }

}
