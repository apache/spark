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

  trait NamedExpression extends Expression {
    val name: String
  }

  case class Attribute(name: String, qualifier: Option[String] = None) extends NamedExpression {
    override def toString: String = f"${if (qualifier.isDefined) qualifier.get + "." else ""}$name"
  }
  case class Alias(child: Expression, name: String, qualifier: Option[String])
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
      groupingExpressions: Seq[Expression]) extends Operator

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
      exprs.map(a => a.name + " DESC NULLS FIRST").mkString(", ")
  }

  case class QueryOrganization(
      selectClause: SelectClause,
      fromClause: FromClause,
      whereClause: Option[WhereClause] = None,
      groupByClause: Option[GroupByClause] = None,
      orderByClause: Option[OrderByClause] = None,
      limitClause: Option[Limit] = None,
      comment: String = "") extends Operator {

    override def toString: String = {
      def getOptionClauseString(optionalClause: Option[Clause]): String =
        if (optionalClause.isDefined) { " " + optionalClause.get.toString } else { "" }

      comment + f"$selectClause $fromClause${getOptionClauseString(whereClause)}" +
        f"${getOptionClauseString(groupByClause)}${getOptionClauseString(orderByClause)}" +
        f"${getOptionClauseString(limitClause)}"
    }
  }

  def generateSubquery(
      innerTable: Relation,
      correlationConditions: Seq[Predicate],
      isDistinct: Boolean,
      operatorInSubquery: Operator,
      isScalarSubquery: Boolean): QueryOrganization = {

    val fromClause = FromClause(Seq(innerTable))
    val projections = operatorInSubquery match {
      case Aggregate(resultExpressions, _) => resultExpressions
      case _ => Seq(innerTable.output.head)
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

    val requiresLimitOne = isScalarSubquery && (operatorInSubquery match {
      case a: Aggregate => a.groupingExpressions.nonEmpty
      case l: Limit => l.limitValue > 1
      case _ => true
    })

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
      isCorrelated: Boolean,
      isDistinct: Boolean,
      operatorInSubquery: Operator): String = {

    // Correlation conditions, hardcoded
    val correlationConditions = if (isCorrelated) {
      Seq(Equals(innerTable.output.head, outerTable.output.head))
    } else {
      Seq()
    }
    val isScalarSubquery = Seq(SubqueryType.SCALAR, SubqueryType.SCALAR_PREDICATE_EQUALS,
      SubqueryType.SCALAR_PREDICATE_LESS_THAN).contains(subqueryType)
    val subqueryOrganization = generateSubquery(
      innerTable, correlationConditions, isDistinct, operatorInSubquery, isScalarSubquery)

    // TODO: Clean up
    val (queryProjection, selectClause, fromClause, whereClause) = subqueryClause match {
      case SubqueryClause.SELECT =>
        val queryProjection = outerTable.output ++ Seq(Attribute(SUBQUERY_ALIAS))
        val fromClause = FromClause(Seq(outerTable))
        val selectClause = SelectClause(outerTable.output ++
          Seq(Alias(Subquery(subqueryOrganization), SUBQUERY_ALIAS, None)))
        (queryProjection, selectClause, fromClause, None)
      case SubqueryClause.FROM =>
        val subqueryProjection = subqueryOrganization.selectClause.projection
        val subqueryOutput = subqueryProjection.map {
          case a: Attribute => Attribute(name = a.name, qualifier = Some(SUBQUERY_ALIAS))
          case a: Alias => Attribute(name = a.name, qualifier = Some(SUBQUERY_ALIAS))
        }
        val selectClause = SelectClause(subqueryOutput)
        val subqueryRelation = SubqueryRelation(SUBQUERY_ALIAS, subqueryOutput,
          subqueryOrganization)
        val fromClause = FromClause(Seq(subqueryRelation))
        (subqueryOutput, selectClause, fromClause, None)
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

    val comment = f"""
                     |/*
                     |  inner_table=$innerTable,
                     |  outer_table=$outerTable,
                     |  subqueryClause=$subqueryClause,
                     |  subquery_type=$subqueryType,
                     |  is_correlated=$isCorrelated,
                     |  distinct=$isDistinct",
                     |  subquery_operator=$operatorInSubquery
                     |*/
                     |""".stripMargin
    QueryOrganization(selectClause, fromClause, whereClause, groupByClause = None,
      orderByClause, limitClause = None,
      comment = comment).toString
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

  val FIRST_COLUMN = "a"
  val SECOND_COLUMN = "b"

  val INNER_TABLE_NAME = "inner_table"
  val INNER_TABLE_SCHEMA = Seq(
    Attribute(FIRST_COLUMN, Some(INNER_TABLE_NAME)),
    Attribute(SECOND_COLUMN, Some(INNER_TABLE_NAME)))
  val INNER_TABLE = TableRelation(INNER_TABLE_NAME, INNER_TABLE_SCHEMA)

  val OUTER_TABLE_NAME = "outer_table"
  val OUTER_TABLE_SCHEMA = Seq(
    Attribute(FIRST_COLUMN, Some(OUTER_TABLE_NAME)),
    Attribute(SECOND_COLUMN, Some(OUTER_TABLE_NAME)))
  val OUTER_TABLE = TableRelation(OUTER_TABLE_NAME, OUTER_TABLE_SCHEMA)

  val NO_MATCH_TABLE_NAME = "no_match_table"
  val NO_MATCH_TABLE_SCHEMA = Seq(
    Attribute(FIRST_COLUMN, Some(NO_MATCH_TABLE_NAME)),
    Attribute(SECOND_COLUMN, Some(NO_MATCH_TABLE_NAME)))
  val NO_MATCH_TABLE = TableRelation(NO_MATCH_TABLE_NAME, NO_MATCH_TABLE_SCHEMA)

  val JOIN_TABLE_NAME = "join_table"
  val JOIN_TABLE_SCHEMA = Seq(
    Attribute(FIRST_COLUMN, Some(JOIN_TABLE_NAME)),
    Attribute(SECOND_COLUMN, Some(JOIN_TABLE_NAME)))
  val JOIN_TABLE = TableRelation(JOIN_TABLE_NAME, JOIN_TABLE_SCHEMA)

  val NULL_TABLE_NAME = "null_table"
  val NULL_TABLE_SCHEMA = Seq(
    Attribute(FIRST_COLUMN, Some(NULL_TABLE_NAME)),
    Attribute(SECOND_COLUMN, Some(NULL_TABLE_NAME)))
  val NULL_TABLE = TableRelation(NULL_TABLE_NAME, NULL_TABLE_SCHEMA)

  val TABLE_COMBINATIONS = Seq(
    (INNER_TABLE, OUTER_TABLE),
    (INNER_TABLE, NULL_TABLE),
    (NULL_TABLE, OUTER_TABLE),
    (NO_MATCH_TABLE, OUTER_TABLE),
    (INNER_TABLE, NO_MATCH_TABLE)
  )

  val INNER_SUBQUERY_ALIAS = "innerSubqueryAlias"
  val SUBQUERY_ALIAS = "subqueryAlias"
  val AGGREGATE_FUNCTION_ALIAS = "aggFunctionAlias"

  val ALL_COMBINATIONS = TABLE_COMBINATIONS.flatMap {
    case (innerTable, outerTable) =>
      val joins = Seq(JoinType.INNER, JoinType.LEFT_OUTER, JoinType.RIGHT_OUTER)
        .map(joinType => JoinedRelation(
          leftRelation = innerTable,
          rightRelation = JOIN_TABLE,
          condition = Equals(innerTable.output.head, JOIN_TABLE.output.head), // hardcoded
          joinType = joinType))
      val setOps = Seq(SetOperationType.UNION, SetOperationType.EXCEPT, SetOperationType.INTERSECT)
        .map(setOp => SetOperation(innerTable, JOIN_TABLE, setOp))
        .map(plan => {
          val output = innerTable.output.map(a => a.copy(qualifier = Some(INNER_SUBQUERY_ALIAS)))
          SubqueryRelation(name = INNER_SUBQUERY_ALIAS, output = output, inner = plan)
        })
      (joins ++ setOps ++ Seq(innerTable)).map(inner => (inner, outerTable))
  }

  // TODO: Add aggregates


  val queries = scala.collection.mutable.ListBuffer[String]()

  for ((innerTable, outerTable) <- ALL_COMBINATIONS) {
    for (subqueryClause <-
           Seq(SubqueryClause.WHERE, SubqueryClause.SELECT, SubqueryClause.FROM)) {
      for (subqueryType <- subqueryTypeChoices(subqueryClause)) {
        for (isDistinct <- Seq(true, false)) {
          val correlationChoices = if (subqueryClause == SubqueryClause.FROM) {
            Seq(false)
          } else {
            Seq(true, false)
          }
          for (isCorrelated <- correlationChoices) {
            // hardcoded
            val (aggColumn, groupByColumn) = innerTable.output.head -> innerTable.output(1)
            val aggFunctions = Seq(Sum(aggColumn), Count(aggColumn))
              .map(af => Alias(af, AGGREGATE_FUNCTION_ALIAS, None))
            val groupByOptions = Seq(true, false)
            val combinations =
              aggFunctions.flatMap(agg => groupByOptions.map(groupBy => (agg, groupBy)))
            val aggregates = combinations.map {
              case (af, groupBy) => Aggregate(Seq(af), if (groupBy) Seq(groupByColumn) else Seq())
            }
            val SUBQUERY_OPERATORS = Seq(Limit(1), Limit(10)) ++ aggregates
            for (subqueryOperator <- SUBQUERY_OPERATORS) {
              val generatedQuery = generateQuery(
                innerTable,
                outerTable,
                subqueryClause,
                subqueryType,
                isCorrelated,
                isDistinct,
                subqueryOperator
              )
              if (!queries.contains(generatedQuery)) {
                queries += generatedQuery + ";"
              }
            }
          }
        }
      }
    }
  }
}
