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

package org.apache.spark.sql

import org.apache.spark.SparkFunSuite

class SubquerySQLGenerator extends SparkFunSuite with SQLQueryTestHelper {

  trait Expression

  trait NamedExpression extends Expression

  case class Attribute(name: String) extends NamedExpression {
    override def toString: String = name
  }
  case class Alias(expr: Expression, alias: String) extends NamedExpression {
    override def toString: String = f"$expr AS $alias"
  }

  trait Predicate extends Expression

  case class Equals(expr: Expression, rightSideExpr: Expression) extends Predicate {
    override def toString: String = f"$expr = $rightSideExpr"
  }
  case class LessThan(expr: Expression, rightSideExpr: Expression) extends Predicate {
    override def toString: String = f"$expr < $rightSideExpr"
  }
  case class GreaterThan(expr: Expression, rightSideExpr: Expression) extends Predicate {
    override def toString: String = f"$expr > $rightSideExpr"
  }

  trait SubqueryExpression extends Expression

  case class ScalarSubquery() extends SubqueryExpression {
    def generateSql(expr: Expression, condition: Option[Predicate]): String = {
      if (condition.isDefined) {
        f"${condition.get} $expr"
      } else {
        expr.toString
      }
    }
  }
  case class Exists() extends SubqueryExpression {
    def generateSql(expr: Expression): String = f"EXISTS ($expr)"
  }
  case class NotExists() extends SubqueryExpression {
    def generateSql(expr: Expression): String = f"NOT EXISTS ($expr)"
  }
  case class In() extends SubqueryExpression {
    def generateSql(expr: Expression, rightSideExpr: Expression): String =
      f"$expr IN ($rightSideExpr)"
  }
  case class NotIn() extends SubqueryExpression {
    def generateSql(expr: Expression, rightSideExpr: Expression): String =
      f"$expr NOT IN ($rightSideExpr)"
  }

  trait AggregateFunction extends Expression

  case class Sum(child: Expression) extends AggregateFunction {
    override def toString: String = f"SUM($child)"
  }
  case class Count(child: Expression) extends AggregateFunction {
    override def toString: String = f"COUNT($child)"
  }

  trait Operator {
    def toString: String
  }

  case class Relation(name: String, output: Seq[Attribute]) extends Operator {
    override def toString: String = name
  }

  case class Aggregate(
      resultExpressions: Seq[NamedExpression],
      groupingExpressions: Seq[Expression]) extends Operator

  object JoinType extends Enumeration {
    val INNER = Value("INNER")
    val LEFT_OUTER = Value("LEFT OUTER")
    val RIGHT_OUTER = Value("RIGHT OUTER")
  }
  case class Join(leftRelation: Operator,
      rightRelation: Operator,
      condition: Expression,
      joinType: JoinType.Value) extends Operator {

    override def toString: String =
      f"$leftRelation $joinType JOIN $rightRelation ON $condition"
  }

  object SetOperationType extends Enumeration {
    val INTERSECT = Value("INTERSECT")
    val UNION = Value("UNION")
    val EXCEPT = Value("EXCEPT")
  }
  case class SetOperation(leftRelation: Operator,
      rightRelation: Operator, setOperationType: SetOperationType.Value) extends Operator {
    override def toString: String =
      f"($leftRelation $setOperationType $rightRelation)"
  }

  case class Limit(limitValue: Int) extends Operator {
    override def toString: String = f"LIMIT $limitValue"
  }

  trait Clause

  case class SelectClause(projection: Seq[Expression], isDistinct: Boolean) extends Clause {
    override def toString: String = f"SELECT ${if (isDistinct) "DISTINCT" else {""}} $projection"
  }
  case class FromClause(relations: Seq[Operator]) extends Clause {
    override def toString: String = f"FROM $relations"
  }
  case class WhereClause(predicates: Seq[Predicate]) extends Clause {
    override def toString: String = f"WHERE " + predicates.mkString(" AND ")
  }
  case class GroupByClause(attributes: Seq[Expression]) extends Clause {
    override def toString: String = f"GROUP BY " + attributes.mkString(", ")
  }
  case class OrderByClause(attributes: Seq[NamedExpression]) extends Clause {
    override def toString: String = f"ORDER BY " +
      attributes.map(a => a + "DESC NULLS FIRST").mkString(", ")
  }
  case class LimitClause(limitValue: Int) extends Clause {
    override def toString: String = f"LIMIT $limitValue"
  }

  case class QueryOrganization(
      selectClause: SelectClause,
      fromClause: FromClause,
      whereClause: Option[WhereClause],
      groupByClause: Option[GroupByClause],
      orderByClause: Option[OrderByClause],
      limitClause: Option[LimitClause]) {

    override def toString: String =
      f"$selectClause $fromClause $whereClause $groupByClause $orderByClause $limitClause"
  }

  def generateSubquery(
      innerTable: Relation,
      subqueryExpression: SubqueryExpression,
      correlationConditions: Seq[Predicate],
      isDistinct: Boolean,
      operatorInSubquery: Operator): QueryOrganization = {

    val fromClause = FromClause(Seq(innerTable))
    val projections = operatorInSubquery match {
      case Aggregate(resultExpressions, _) => resultExpressions
      case _ => innerTable.output
    }
    val selectClause = SelectClause(projections, isDistinct = isDistinct)

    val whereClause = if (correlationConditions.nonEmpty) {
      Some(WhereClause(correlationConditions))
    } else {
      None
    }

    val groupByClause = operatorInSubquery match {
      case a: Aggregate if a.groupingExpressions.nonEmpty =>
        Some(GroupByClause(a.groupingExpressions))
      case _ => None
    }

    val requiresLimitOne = subqueryExpression.isInstanceOf[ScalarSubquery] && (
      operatorInSubquery match {
        case a: Aggregate => a.groupingExpressions.nonEmpty
        case l: Limit => l.limitValue > 1
        case _ => true
      }
    )

    val orderByClause = if (requiresLimitOne || operatorInSubquery.isInstanceOf[Limit]) {
      Some(OrderByClause(projections))
    } else {
      None
    }

    val limitClause = if (requiresLimitOne) {
      Some(LimitClause(1))
    } else operatorInSubquery match {
      case limit: Limit =>
        Some(LimitClause(limit.limitValue))
      case _ =>
        None
    }

    QueryOrganization(selectClause, fromClause, whereClause, groupByClause,
      orderByClause, limitClause)
  }

  def generateQuery(
      innerTable: Relation,
      outerTable: Relation,
      subqueryExpression: SubqueryExpression,
      correlationConditions: Seq[Predicate],
      isDistinct: Boolean,
      operatorInSubquery: Operator): String = {

  }
}


object SubquerySQLGenerator {

}