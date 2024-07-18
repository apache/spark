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

/**
 * Simple implementation of Expressions and Operators that can be used to generate SQL for testing.
 * For example, to construct a simple SELECT query using the defined classes, follow these steps:
 *
 * 1. Define a table name, and some column names for your table.
 * {{{
 *   val tableName = "t1"
 *   val col1 = "col1"
 *   val col2 = "col2"
 * }}}
 * 2. Define named expressions representing the columns to select using the Attribute class, and
 *    define the table relation with TableRelation.
 * {{{
 *   val col1Attr = Attribute("col1", qualifier = Some(tableName))
 *   val col2Attr = Attribute("col2", qualifier = Some(tableName))
 *   val table = TableRelation(tableName, Seq(col1Attr, col2Attr))
 * }}}
 * 3. Create a SELECT clause using the SelectClause class, and specify the FROM clause with the
 *    table relations using the FromClause class.
 * {{{
 *   val selectClause = SelectClause(Seq(col1Attr))
 *   val fromClause = FromClause(Seq(table))
 * }}}
 * 4. (Optional) Add a WHERE clause using the WhereClause class for predicates. The OrderBy, Limit
 * clauses are also optional.
 * {{{
 *   val condition = Equals(col1Attr, col2Attr)
 *   val whereClause = WhereClause(Seq(condition))
 * }}}
 * 5. Construct the final query using the Query class:
 * {{{
 *   val myQuery = Query(selectClause, fromClause, Some(whereClause))()
 * }}}
 * 6. Print or use the generated SQL query:
 * {{{
 *   println(myQuery.toString)
 * }}}
 *
 * Output: SELECT t1.col1 FROM t1 WHERE t1.col1 = t1.col2
 *
 */
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

  case class Not(expr: Predicate) extends Predicate {
    override def toString: String = f"NOT $expr"
  }
  case class Equals(expr: Expression, rightSideExpr: Expression) extends Predicate {
    override def toString: String = f"$expr = $rightSideExpr"
  }
  case class NotEquals(expr: Expression, rightSideExpr: Expression) extends Predicate {
    override def toString: String = f"$expr <> $rightSideExpr"
  }
  case class LessThan(expr: Expression, rightSideExpr: Expression) extends Predicate {
    override def toString: String = f"$expr < $rightSideExpr"
  }
  case class LessThanOrEquals(expr: Expression, rightSideExpr: Expression) extends Predicate {
    override def toString: String = f"$expr <= $rightSideExpr"
  }
  case class GreaterThan(expr: Expression, rightSideExpr: Expression) extends Predicate {
    override def toString: String = f"$expr > $rightSideExpr"
  }
  case class GreaterThanOrEquals(expr: Expression, rightSideExpr: Expression) extends Predicate {
    override def toString: String = f"$expr >= $rightSideExpr"
  }

  case class In(expr: Expression, inner: Operator)
    extends Predicate {
    override def toString: String = f"$expr IN ($inner)"
  }

  object SubqueryType extends Enumeration {
    // Subquery to be treated as an Attribute.
    val ATTRIBUTE = Value
    // Subquery to be treated as a Relation.
    val RELATION = Value
    // Subquery is a Predicate - types of predicate subqueries.
    val SCALAR_PREDICATE_EQUALS, SCALAR_PREDICATE_NOT_EQUALS,
      SCALAR_PREDICATE_LESS_THAN, SCALAR_PREDICATE_LESS_THAN_OR_EQUALS,
      SCALAR_PREDICATE_GREATER_THAN, SCALAR_PREDICATE_GREATER_THAN_OR_EQUALS,
      IN, NOT_IN, EXISTS, NOT_EXISTS = Value
  }

  trait SubqueryExpression extends Expression {
    val inner: Operator
    override def toString: String = f"($inner)"
  }
  case class Subquery(inner: Query) extends SubqueryExpression
  case class Exists(inner: Query) extends SubqueryExpression with Predicate {
    override def toString: String = f"EXISTS ($inner)"
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
  case class SetOperation(leftRelation: Query,
      rightRelation: Query, setOperationType: SetOperationType.Value) extends Operator {
    override def toString: String = f"$leftRelation $setOperationType $rightRelation"
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

  object SubqueryLocation extends Enumeration {
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

  /**
   * Case class that encapsulates a Query, built with its different clauses.
   */
  case class Query(
      selectClause: SelectClause,
      fromClause: FromClause,
      whereClause: Option[WhereClause] = None,
      groupByClause: Option[GroupByClause] = None,
      orderByClause: Option[OrderByClause] = None,
      limitClause: Option[Limit] = None
  ) extends Operator {

    override def toString: String = {
      def getOptionClauseString(optionalClause: Option[Clause]): String =
        if (optionalClause.isDefined) { " " + optionalClause.get.toString } else { "" }

      f"$selectClause $fromClause${getOptionClauseString(whereClause)}" +
        f"${getOptionClauseString(groupByClause)}${getOptionClauseString(orderByClause)}" +
        f"${getOptionClauseString(limitClause)}"
    }
  }
}
