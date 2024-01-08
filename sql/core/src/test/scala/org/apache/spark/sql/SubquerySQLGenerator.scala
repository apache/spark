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

  case class Attribute(name: String) extends Expression {
    override def toString: String = name
  }
  case class Alias(expr: Expression, alias: String) extends Expression {
    override def toString: String = f"$expr AS $alias"
  }

  trait Predicate extends Expression

  case class Exists(expr: Expression) extends Predicate {
    override def toString: String = f"EXISTS ($expr)"
  }
  case class NotExists(expr: Expression) extends Predicate {
    override def toString: String = f"NOT EXISTS ($expr)"
  }
  case class In(expr: Expression, rightSideExpr: Expression) extends Predicate {
    override def toString: String = f"$expr IN ($rightSideExpr)"
  }
  case class NotIn(expr: Expression, rightSideExpr: Expression) extends Predicate {
    override def toString: String = f"$expr NOT IN ($rightSideExpr)"
  }
  case class Equals(expr: Expression, rightSideExpr: Expression) extends Predicate {
    override def toString: String = f"$expr = $rightSideExpr"
  }

  trait AggregateFunction extends Expression

  case class Sum() extends AggregateFunction {
    override def toString: String = "SUM"
  }
  case class Count() extends AggregateFunction {
    override def toString: String = "COUNT"
  }

  trait Operator {
    def toString: String
  }

  case class Relation(name: String) extends Operator {
    override def toString: String = name
  }

  case class Aggregate(aggregateFunction: AggregateFunction,
      resultExpressions: Seq[Attribute],
      groupingExpressions: Seq[Attribute]) extends Operator {
    override def toString: String = ""
  }

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

  case class SelectClause(projection: Seq[Attribute], isDistinct: Boolean) extends Clause {
    override def toString: String = f"SELECT ${if (isDistinct) "DISTINCT" else {""}} $projection"
  }
  case class FromClause(relations: Seq[Operator]) extends Clause {
    override def toString: String = f"FROM $relations"
  }
  case class WhereClause(predicates: Seq[Predicate]) extends Clause {
    override def toString: String = f"WHERE " + predicates.mkString(" AND ")
  }
  case class OrderByClause(attributes: Seq[Attribute]) extends Clause {
    override def toString: String = f"ORDER BY " +
      attributes.map(a => a + "DESC NULLS FIRST").mkString(", ")
  }
  case class LimitClause(limitValue: Int) extends Clause {
    override def toString: String = f"LIMIT $limitValue"
  }

  case class QueryOrganization(
      selectClause: SelectClause,
      fromClause: FromClause,
      whereClause: WhereClause,
      orderByClause: OrderByClause,
      limitClause: LimitClause) {

    override def toString: String =
      f"$selectClause $fromClause $whereClause $orderByClause $limitClause"
  }

  def generateSubquery(
      innerTable: Relation,
      outerTable: Relation,
      subqueryPredicate: Option[Predicate],
      isCorrelated: Boolean,
      isDistinct: Boolean,
      operatorInSubquery: Operator): QueryOrganization = {

  }
}


object SubquerySQLGenerator {

}