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
import org.apache.spark.sql.SubquerySQLGenerator.{SUBQUERY_ALIAS, TABLE_COMBINATIONS}

class SubquerySQLGenerator extends SparkFunSuite with SQLQueryTestHelper {

  trait Expression

  trait NamedExpression extends Expression

  case class Attribute(name: String, qualifier: Option[String] = None) extends NamedExpression {
    override def toString: String = f"${if (qualifier.isDefined) qualifier + "." else ""}.$name"
  }
  case class Alias(child: Expression, alias: String, qualifier: Option[String])
    extends NamedExpression {
    override def toString: String = f"$child AS $alias"
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

  trait SubqueryExpression extends Expression with Operator {
    val inner: Operator
    override def toString: String = f"(${inner.toString})"
  }
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

  case class Limit(limitValue: Int) extends Operator with Clause {
    override def toString: String = f"LIMIT $limitValue"
  }

  object SubqueryClause extends Enumeration {
    val SELECT, FROM, WHERE = Value
  }

  trait Clause

  case class SelectClause(projection: Seq[NamedExpression], isDistinct: Boolean = false)
    extends Clause {
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

  case class QueryOrganization(
      selectClause: SelectClause,
      fromClause: FromClause,
      whereClause: Option[WhereClause] = None,
      groupByClause: Option[GroupByClause] = None,
      orderByClause: Option[OrderByClause] = None,
      limitClause: Option[Limit] = None) extends Operator {

    override def toString: String = {
      def getOptionClauseString(optionalClause: Option[Clause]): String =
        if (optionalClause.isDefined) { optionalClause.toString + " " } else { "" }

      f"$selectClause $fromClause${getOptionClauseString(whereClause)}" +
        f"${getOptionClauseString(groupByClause)}${getOptionClauseString(orderByClause)}" +
        f"${getOptionClauseString(limitClause)}"
    }
  }

  def generateSubquery(
      innerTable: Relation,
      correlationConditions: Seq[Predicate],
      isDistinct: Boolean,
      operatorInSubquery: Operator,
      requiresLimitOne: Boolean): QueryOrganization = {

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

    val orderByClause = if (requiresLimitOne || operatorInSubquery.isInstanceOf[Limit]) {
      Some(OrderByClause(projections))
    } else {
      None
    }

    val limitClause = if (requiresLimitOne) {
      Some(Limit(1))
    } else operatorInSubquery match {
      case limit: Limit =>
        Some(limit)
      case _ =>
        None
    }

    QueryOrganization(selectClause, fromClause, whereClause, groupByClause,
      orderByClause, limitClause)
  }

  def generateQuery(
      innerTable: Relation,
      outerTable: Relation,
      subqueryClause: SubqueryClause.Value,
      subqueryType: SubqueryType.Value,
      isDistinct: Boolean,
      operatorInSubquery: Operator): String = {

    // Correlation conditions, hardcoded
    val correlationConditions = Seq(Equals(innerTable.output.head, outerTable.output.head))
    val requiresLimitOne = Seq(SubqueryType.SCALAR, SubqueryType.SCALAR_PREDICATE_EQUALS,
      SubqueryType.SCALAR_PREDICATE_LESS_THAN).contains(subqueryType) && (operatorInSubquery match {
        case a: Aggregate => a.groupingExpressions.nonEmpty
        case l: Limit => l.limitValue > 1
        case _ => true
      }
    )
    val subqueryOrganization = generateSubquery(
      innerTable, correlationConditions, isDistinct, operatorInSubquery, requiresLimitOne)

    val (queryProjection, selectClause, fromClause, whereClause) = subqueryClause match {
      case SubqueryClause.SELECT =>
        val queryProjection = outerTable.output ++ Seq(Attribute(SUBQUERY_ALIAS))
        val fromClause = FromClause(Seq(outerTable))
        val selectClause = SelectClause(queryProjection)
        (queryProjection, selectClause, fromClause, None)
      case SubqueryClause.FROM =>
        val queryProjection = subqueryOrganization.selectClause.projection
        val selectClause = SelectClause(queryProjection)
        // TODO: alias
        val fromClause = FromClause(Seq(ScalarSubquery(subqueryOrganization)))
        (queryProjection, selectClause, fromClause, None)
      case SubqueryClause.WHERE =>
        val queryProjection = outerTable.output
        val selectClause = SelectClause(queryProjection)
        val fromClause = FromClause(Seq(outerTable))
        val expr = outerTable.output.last         // Hardcoded
        val whereClausePredicate = subqueryType match {
          case SubqueryType.SCALAR_PREDICATE_EQUALS =>
            Equals(expr, ScalarSubquery(subqueryOrganization))
          case SubqueryType.SCALAR_PREDICATE_LESS_THAN =>
            LessThan(expr, ScalarSubquery(subqueryOrganization))
          case SubqueryType.EXISTS => Exists(subqueryOrganization)
          case SubqueryType.NOT_EXISTS => NotExists(subqueryOrganization)
          case SubqueryType.IN => In(expr, subqueryOrganization)
          case SubqueryType.NOT_IN => NotIn(expr, subqueryOrganization)
        }
        val whereClause = Some(WhereClause(Seq(whereClausePredicate)))
        (queryProjection, selectClause, fromClause, whereClause)
    }
    val orderByClause = Some(OrderByClause(queryProjection))

    // TODO: Query comment
    QueryOrganization(selectClause, fromClause, whereClause, groupByClause = None,
      orderByClause, limitClause = None).toString
  }

  private val subqueryTypeChoices = (subqueryClause: SubqueryClause.Value) =>
    if (subqueryClause == SubqueryClause.SELECT) {
    Seq(SubqueryType.SCALAR)
  } else {
    Seq(
      SubqueryType.SCALAR_PREDICATE_LESS_THAN,
      SubqueryType.SCALAR_PREDICATE_EQUALS,
      SubqueryType.IN,
      SubqueryType.NOT_IN,
      SubqueryType.EXISTS,
      SubqueryType.NOT_EXISTS)
  }

  def main(args: Array[String]): Unit = {
    val queries = scala.collection.mutable.ListBuffer[String]()

    for ((innerTable, outerTable) <- TABLE_COMBINATIONS) {
      for (subqueryClause <-
             Seq(SubqueryClause.WHERE, SubqueryClause.SELECT, SubqueryClause.FROM)) {
        for (subqueryType <- subqueryTypeChoices(subqueryClause)) {
          for (isDistinct <- Seq(true, false)) {
            // TODO
            for (subqueryOperator <- Seq()) {
              val generatedQuery = generateQuery(
                innerTable,
                outerTable,
                subqueryClause,
                subqueryType,
                isDistinct,
                subqueryOperator
              )
              if (!queries.contains(generatedQuery)) {
                queries += generatedQuery
              }
            }
          }
        }
      }
    }
  }
}


object SubquerySQLGenerator {

  val FIRST_COLUMN = "a"
  val SECOND_COLUMN = "b"

  val INNER_TABLE_NAME = "inner_table"
  val INNER_TABLE_SCHEMA = Seq(
    Attribute(FIRST_COLUMN, INNER_TABLE_NAME),
    Attribute(SECOND_COLUMN, INNER_TABLE_NAME))
  val INNER_TABLE = Relation(INNER_TABLE_NAME, INNER_TABLE_SCHEMA)

  val OUTER_TABLE_NAME = "outer_table"
  val OUTER_TABLE_SCHEMA = Seq(
    Attribute(FIRST_COLUMN, OUTER_TABLE_NAME),
    Attribute(SECOND_COLUMN, OUTER_TABLE_NAME))
  val OUTER_TABLE = Relation(OUTER_TABLE_NAME, OUTER_TABLE_SCHEMA)

  val NO_MATCH_TABLE_NAME = "no_match_table"
  val NO_MATCH_TABLE_SCHEMA = Seq(
    Attribute(FIRST_COLUMN, NO_MATCH_TABLE_NAME),
    Attribute(SECOND_COLUMN, NO_MATCH_TABLE_NAME))
  val NO_MATCH_TABLE = Relation(NO_MATCH_TABLE_NAME, NO_MATCH_TABLE_SCHEMA)

  val JOIN_TABLE_NAME = "join_table"
  val JOIN_TABLE_SCHEMA = Seq(
    Attribute(FIRST_COLUMN, JOIN_TABLE_NAME),
    Attribute(SECOND_COLUMN, JOIN_TABLE_NAME))
  val JOIN_TABLE = Relation(JOIN_TABLE_NAME, JOIN_TABLE_SCHEMA)

  val NULL_TABLE_NAME = "null_table"
  val NULL_TABLE_SCHEMA = Seq(
    Attribute(FIRST_COLUMN, NULL_TABLE_NAME),
    Attribute(SECOND_COLUMN, NULL_TABLE_NAME))
  val NULL_TABLE = Relation(NULL_TABLE_NAME, NULL_TABLE_SCHEMA)

  // TODO: inner table combinations to include join, set ops with JOIN_TABLE
  val TABLE_COMBINATIONS = Seq(
    (INNER_TABLE, OUTER_TABLE),
    (INNER_TABLE, NULL_TABLE),
    (NULL_TABLE, OUTER_TABLE),
    (NO_MATCH_TABLE, OUTER_TABLE),
    (INNER_TABLE, NO_MATCH_TABLE)
  )

  val SUBQUERY_ALIAS = "subqueryAlias"
}