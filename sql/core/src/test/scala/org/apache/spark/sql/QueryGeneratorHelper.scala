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

trait QueryGeneratorHelper {

  trait Expression

  trait NamedExpression extends Expression {
    val name: String
  }

  case class Attribute(name: String, qualifier: Option[String] = None) extends NamedExpression {
    override def toString: String = f"${if (qualifier.isDefined) qualifier.get + "." else ""}$name"
  }
  case class Alias(child: Expression, name: String)
    extends NamedExpression {
    override def toString: String = f"$child AS $name"
  }

  trait Predicate extends Expression

  case class Equals(expr: Expression, rightSideExpr: Expression) extends Predicate {
    override def toString: String = f"$expr = $rightSideExpr"
  }
  case class LessThan(expr: Expression, rightSideExpr: Expression) extends Predicate {
    override def toString: String = f"$expr < $rightSideExpr"
  }
  case class In(expr: Expression, inner: Operator)
    extends Predicate {
    override def toString: String = f"$expr IN ($inner)"
  }
  case class NotIn(expr: Expression, inner: Operator)
    extends Predicate {
    override def toString: String = f"$expr NOT IN ($inner)"
  }

  object SubqueryType extends Enumeration {
    val SCALAR = Value
    val SCALAR_PREDICATE_EQUALS, SCALAR_PREDICATE_LESS_THAN = Value
    val IN, NOT_IN, EXISTS, NOT_EXISTS = Value
  }

  trait SubqueryExpression extends Expression {
    val inner: Operator
    override def toString: String = f"($inner)"
  }
  case class Subquery(inner: Operator) extends SubqueryExpression
  case class ScalarSubquery(inner: Operator) extends SubqueryExpression
  case class Exists(inner: Operator) extends SubqueryExpression with Predicate {
    override def toString: String = f"EXISTS ($inner)"
  }
  case class NotExists(inner: Operator) extends SubqueryExpression with Predicate {
    override def toString: String = f"NOT EXISTS ($inner)"
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

  trait Relation extends Operator {
    val name: String
    val output: Seq[Attribute]
  }

  case class TableRelation(name: String, output: Seq[Attribute]) extends Relation {
    override def toString: String = name
  }
  case class SubqueryRelation(name: String, output: Seq[Attribute], inner: Operator)
    extends Relation with SubqueryExpression {
    override def toString: String = f"($inner) AS $name"
  }

  object JoinType extends Enumeration {
    val INNER = Value("INNER")
    val LEFT_OUTER = Value("LEFT OUTER")
    val RIGHT_OUTER = Value("RIGHT OUTER")
  }
  case class JoinedRelation(
      leftRelation: Relation,
      rightRelation: Relation,
      condition: Expression,
      joinType: JoinType.Value) extends Relation {

    val output: Seq[Attribute] = leftRelation.output ++ rightRelation.output

    override def toString: String =
      f"$leftRelation $joinType JOIN $rightRelation ON $condition"
    override val name: String = toString
  }

  object SetOperationType extends Enumeration {
    val INTERSECT = Value("INTERSECT")
    val UNION = Value("UNION")
    val EXCEPT = Value("EXCEPT")
  }
  case class SetOperation(leftRelation: Operator,
      rightRelation: Operator, setOperationType: SetOperationType.Value) extends Operator {
    override def toString: String =
      f"SELECT * FROM $leftRelation $setOperationType SELECT * FROM $rightRelation"
  }

  case class Aggregate(
      resultExpressions: Seq[NamedExpression],
      groupingExpressions: Seq[Expression]) extends Operator {
    override def toString: String = f"AGGREGATE(resultExpr=[${resultExpressions.mkString(",")}]," +
      f"groupingExpr=[${groupingExpressions.mkString(",")}])"
  }

  case class Limit(limitValue: Int) extends Operator with Clause {
    override def toString: String = f"LIMIT $limitValue"
  }

  object SubqueryClause extends Enumeration {
    val SELECT, FROM, WHERE = Value
  }

  trait Clause

  case class SelectClause(projection: Seq[NamedExpression], isDistinct: Boolean = false)
    extends Clause {
    override def toString: String =
      f"SELECT${if (isDistinct) " DISTINCT " else {" "}}${projection.mkString(", ")}"
  }
  case class FromClause(relations: Seq[Operator]) extends Clause {
    override def toString: String = f"FROM ${relations.mkString(", ")}"
  }
  case class WhereClause(predicates: Seq[Predicate]) extends Clause {
    override def toString: String = f"WHERE " + predicates.mkString(" AND ")
  }
  case class GroupByClause(attributes: Seq[Expression]) extends Clause {
    override def toString: String = f"GROUP BY " + attributes.mkString(", ")
  }
  case class OrderByClause(exprs: Seq[NamedExpression]) extends Clause {
    override def toString: String = f"ORDER BY " +
      exprs.map(expr => expr.name + " DESC NULLS FIRST").mkString(", ")
  }

  case class QueryOrganization(
      selectClause: SelectClause,
      fromClause: FromClause,
      whereClause: Option[WhereClause] = None,
      groupByClause: Option[GroupByClause] = None,
      orderByClause: Option[OrderByClause] = None,
      limitClause: Option[Limit] = None
  )(comment: String = "") extends Operator {

    override def toString: String = {
      def getOptionClauseString(optionalClause: Option[Clause]): String =
        if (optionalClause.isDefined) { " " + optionalClause.get.toString } else { "" }

      val queryString = f"$selectClause $fromClause${getOptionClauseString(whereClause)}" +
        f"${getOptionClauseString(groupByClause)}${getOptionClauseString(orderByClause)}" +
        f"${getOptionClauseString(limitClause)}"
      if (comment.isEmpty) {
        queryString
      } else {
        comment + "\n" + queryString
      }
    }
  }
}
