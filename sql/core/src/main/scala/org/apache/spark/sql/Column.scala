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

import scala.language.implicitConversions

import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, Star}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.{Literal => LiteralExpr}
import org.apache.spark.sql.catalyst.plans.logical.{Project, LogicalPlan}
import org.apache.spark.sql.types._


object Column {
  def unapply(col: Column): Option[Expression] = Some(col.expr)

  def apply(colName: String): Column = new Column(colName)
}


/**
 * A column in a [[DataFrame]].
 *
 * `Column` instances can be created by:
 * {{{
 *   // 1. Select a column out of a DataFrame
 *   df("colName")
 *
 *   // 2. Create a literal expression
 *   Literal(1)
 *
 *   // 3. Create new columns from
 * }}}
 *
 */
// TODO: Improve documentation.
class Column(
    sqlContext: Option[SQLContext],
    plan: Option[LogicalPlan],
    val expr: Expression)
  extends DataFrame(sqlContext, plan) with ExpressionApi {

  /** Turn a Catalyst expression into a `Column`. */
  protected[sql] def this(expr: Expression) = this(None, None, expr)

  /**
   * Create a new `Column` expression based on a column or attribute name.
   * The resolution of this is the same as SQL. For example:
   *
   * - "colName" becomes an expression selecting the column named "colName".
   * - "*" becomes an expression selecting all columns.
   * - "df.*" becomes an expression selecting all columns in data frame "df".
   */
  def this(name: String) = this(name match {
    case "*" => Star(None)
    case _ if name.endsWith(".*") => Star(Some(name.substring(0, name.length - 2)))
    case _ => UnresolvedAttribute(name)
  })

  override def isComputable: Boolean = sqlContext.isDefined && plan.isDefined

  /**
   * An implicit conversion function internal to this class. This function creates a new Column
   * based on an expression. If the expression itself is not named, it aliases the expression
   * by calling it "col".
   */
  private[this] implicit def toColumn(expr: Expression): Column = {
    val projectedPlan = plan.map { p =>
      Project(Seq(expr match {
        case named: NamedExpression => named
        case unnamed: Expression => Alias(unnamed, "col")()
      }), p)
    }
    new Column(sqlContext, projectedPlan, expr)
  }

  /**
   * Unary minus, i.e. negate the expression.
   * {{{
   *   // Select the amount column and negates all values.
   *   df.select( -df("amount") )
   * }}}
   */
  override def unary_- : Column = UnaryMinus(expr)

  /**
   * Bitwise NOT.
   * {{{
   *   // Select the flags column and negate every bit.
   *   df.select( ~df("flags") )
   * }}}
   */
  override def unary_~ : Column = BitwiseNot(expr)

  /**
   * Invert a boolean expression, i.e. NOT.
   * {{
   *   // Select rows that are not active (isActive === false)
   *   df.select( !df("isActive") )
   * }}
   */
  override def unary_! : Column = Not(expr)


  /**
   * Equality test with an expression.
   * {{{
   *   // The following two both select rows in which colA equals colB.
   *   df.select( df("colA") === df("colB") )
   *   df.select( df("colA".equalTo(df("colB")) )
   * }}}
   */
  override def === (other: Column): Column = EqualTo(expr, other.expr)

  /**
   * Equality test with a literal value.
   * {{{
   *   // The following two both select rows in which colA is "Zaharia".
   *   df.select( df("colA") === "Zaharia")
   *   df.select( df("colA".equalTo("Zaharia") )
   * }}}
   */
  override def === (literal: Any): Column = this === Literal.anyToLiteral(literal)

  /**
   * Equality test with an expression.
   * {{{
   *   // The following two both select rows in which colA equals colB.
   *   df.select( df("colA") === df("colB") )
   *   df.select( df("colA".equalTo(df("colB")) )
   * }}}
   */
  override def equalTo(other: Column): Column = this === other

  /**
   * Equality test with a literal value.
   * {{{
   *   // The following two both select rows in which colA is "Zaharia".
   *   df.select( df("colA") === "Zaharia")
   *   df.select( df("colA".equalTo("Zaharia") )
   * }}}
   */
  override def equalTo(literal: Any): Column = this === literal

  /**
   * Inequality test with an expression.
   * {{{
   *   // The following two both select rows in which colA does not equal colB.
   *   df.select( df("colA") !== df("colB") )
   *   df.select( !(df("colA") === df("colB")) )
   * }}}
   */
  override def !== (other: Column): Column = Not(EqualTo(expr, other.expr))

  /**
   * Inequality test with a literal value.
   * {{{
   *   // The following two both select rows in which colA does not equal equal 15.
   *   df.select( df("colA") !== 15 )
   *   df.select( !(df("colA") === 15) )
   * }}}
   */
  override def !== (literal: Any): Column = this !== Literal.anyToLiteral(literal)

  /**
   * Greater than an expression.
   * {{{
   *   // The following selects people older than 21.
   *   people.select( people("age") > Literal(21) )
   * }}}
   */
  override def > (other: Column): Column = GreaterThan(expr, other.expr)

  /**
   * Greater than a literal value.
   * {{{
   *   // The following selects people older than 21.
   *   people.select( people("age") > 21 )
   * }}}
   */
  override def > (literal: Any): Column = this > Literal.anyToLiteral(literal)

  /**
   * Less than an expression.
   * {{{
   *   // The following selects people younger than 21.
   *   people.select( people("age") < Literal(21) )
   * }}}
   */
  override def < (other: Column): Column = LessThan(expr, other.expr)

  /**
   * Less than a literal value.
   * {{{
   *   // The following selects people younger than 21.
   *   people.select( people("age") < 21 )
   * }}}
   */
  override def < (literal: Any): Column = this < Literal.anyToLiteral(literal)

  /**
   * Less than or equal to an expression.
   * {{{
   *   // The following selects people age 21 or younger than 21.
   *   people.select( people("age") <= Literal(21) )
   * }}}
   */
  override def <= (other: Column): Column = LessThanOrEqual(expr, other.expr)

  /**
   * Less than or equal to a literal value.
   * {{{
   *   // The following selects people age 21 or younger than 21.
   *   people.select( people("age") <= 21 )
   * }}}
   */
  override def <= (literal: Any): Column = this <= Literal.anyToLiteral(literal)

  /**
   * Greater than or equal to an expression.
   * {{{
   *   // The following selects people age 21 or older than 21.
   *   people.select( people("age") >= Literal(21) )
   * }}}
   */
  override def >= (other: Column): Column = GreaterThanOrEqual(expr, other.expr)

  /**
   * Greater than or equal to a literal value.
   * {{{
   *   // The following selects people age 21 or older than 21.
   *   people.select( people("age") >= 21 )
   * }}}
   */
  override def >= (literal: Any): Column = this >= Literal.anyToLiteral(literal)

  /**
   * Equality test with an expression that is safe for null values.
   */
  override def <=> (other: Column): Column = EqualNullSafe(expr, other.expr)

  /**
   * Equality test with a literal value that is safe for null values.
   */
  override def <=> (literal: Any): Column = this <=> Literal.anyToLiteral(literal)

  /**
   * True if the current expression is null.
   */
  override def isNull: Column = IsNull(expr)

  /**
   * True if the current expression is NOT null.
   */
  override def isNotNull: Column = IsNotNull(expr)

  /**
   * Boolean OR with an expression.
   * {{{
   *   // The following selects people that are in school or employed.
   *   people.select( people("inSchool") || people("isEmployed") )
   * }}}
   */
  override def || (other: Column): Column = Or(expr, other.expr)

  /**
   * Boolean OR with a literal value.
   * {{{
   *   // The following selects everything.
   *   people.select( people("inSchool") || true )
   * }}}
   */
  override def || (literal: Boolean): Column = this || Literal.anyToLiteral(literal)

  /**
   * Boolean AND with an expression.
   * {{{
   *   // The following selects people that are in school and employed at the same time.
   *   people.select( people("inSchool") && people("isEmployed") )
   * }}}
   */
  override def && (other: Column): Column = And(expr, other.expr)

  /**
   * Boolean AND with a literal value.
   * {{{
   *   // The following selects people that are in school.
   *   people.select( people("inSchool") && true )
   * }}}
   */
  override def && (literal: Boolean): Column = this && Literal.anyToLiteral(literal)

  /**
   * Bitwise AND with an expression.
   */
  override def & (other: Column): Column = BitwiseAnd(expr, other.expr)

  /**
   * Bitwise AND with a literal value.
   */
  override def & (literal: Any): Column = this & Literal.anyToLiteral(literal)

  /**
   * Bitwise OR with an expression.
   */
  override def | (other: Column): Column = BitwiseOr(expr, other.expr)

  /**
   * Bitwise OR with a literal value.
   */
  override def | (literal: Any): Column = this | Literal.anyToLiteral(literal)

  /**
   * Bitwise XOR with an expression.
   */
  override def ^ (other: Column): Column = BitwiseXor(expr, other.expr)

  /**
   * Bitwise XOR with a literal value.
   */
  override def ^ (literal: Any): Column = this ^ Literal.anyToLiteral(literal)

  /**
   * Sum of this expression and another expression.
   * {{{
   *   // The following selects the sum of a person's height and weight.
   *   people.select( people("height") + people("weight") )
   * }}}
   */
  override def + (other: Column): Column = Add(expr, other.expr)

  /**
   * Sum of this expression and another expression.
   * {{{
   *   // The following selects the sum of a person's height and 10.
   *   people.select( people("height") + 10 )
   * }}}
   */
  override def + (literal: Any): Column = this + Literal.anyToLiteral(literal)

  /**
   * Subtraction. Substract the other expression from this expression.
   * {{{
   *   // The following selects the difference between people's height and their weight.
   *   people.select( people("height") - people("weight") )
   * }}}
   */
  override def - (other: Column): Column = Subtract(expr, other.expr)

  /**
   * Subtraction. Substract a literal value from this expression.
   * {{{
   *   // The following selects a person's height and substract it by 10.
   *   people.select( people("height") - 10 )
   * }}}
   */
  override def - (literal: Any): Column = this - Literal.anyToLiteral(literal)

  /**
   * Multiply this expression and another expression.
   * {{{
   *   // The following multiplies a person's height by their weight.
   *   people.select( people("height") * people("weight") )
   * }}}
   */
  override def * (other: Column): Column = Multiply(expr, other.expr)

  /**
   * Multiply this expression and a literal value.
   * {{{
   *   // The following multiplies a person's height by 10.
   *   people.select( people("height") * 10 )
   * }}}
   */
  override def * (literal: Any): Column = this * Literal.anyToLiteral(literal)

  /**
   * Divide this expression by another expression.
   * {{{
   *   // The following divides a person's height by their weight.
   *   people.select( people("height") / people("weight") )
   * }}}
   */
  override def / (other: Column): Column = Divide(expr, other.expr)

  /**
   * Divide this expression by a literal value.
   * {{{
   *   // The following divides a person's height by 10.
   *   people.select( people("height") / 10 )
   * }}}
   */
  override def / (literal: Any): Column = this / Literal.anyToLiteral(literal)

  /**
   * Modulo (a.k.a. remainder) expression.
   */
  override def % (other: Column): Column = Remainder(expr, other.expr)

  /**
   * Modulo (a.k.a. remainder) expression.
   */
  override def % (literal: Any): Column = this % Literal.anyToLiteral(literal)


  /**
   * A boolean expression that is evaluated to true if the value of this expression is contained
   * by the evaluated values of the arguments.
   */
  @scala.annotation.varargs
  override def in(list: Column*): Column = In(expr, list.map(_.expr))

  override def like(other: Column): Column = Like(expr, other.expr)

  override def like(literal: String): Column = this.like(Literal.anyToLiteral(literal))

  override def rlike(other: Column): Column = RLike(expr, other.expr)

  override def rlike(literal: String): Column = this.rlike(Literal.anyToLiteral(literal))


  override def getItem(ordinal: Int): Column = GetItem(expr, LiteralExpr(ordinal))

  override def getItem(ordinal: Column): Column = GetItem(expr, ordinal.expr)

  override def getField(fieldName: String): Column = GetField(expr, fieldName)


  override def substr(startPos: Column, len: Column): Column =
    Substring(expr, startPos.expr, len.expr)

  override def substr(startPos: Int, len: Int): Column =
    this.substr(Literal.anyToLiteral(startPos), Literal.anyToLiteral(len))

  override def contains(other: Column): Column = Contains(expr, other.expr)

  override def contains(literal: Any): Column = this.contains(Literal.anyToLiteral(literal))


  override def startsWith(other: Column): Column = StartsWith(expr, other.expr)

  override def startsWith(literal: String): Column = this.startsWith(Literal.anyToLiteral(literal))

  override def endsWith(other: Column): Column = EndsWith(expr, other.expr)

  override def endsWith(literal: String): Column = this.endsWith(Literal.anyToLiteral(literal))

  override def as(alias: String): Column = Alias(expr, alias)()

  override def cast(to: DataType): Column = Cast(expr, to)

  override def desc: Column = SortOrder(expr, Descending)

  override def asc: Column = SortOrder(expr, Ascending)
}


class ColumnName(name: String) extends Column(name) {

  /** Creates a new AttributeReference of type boolean */
  def boolean: StructField = StructField(name, BooleanType)

  /** Creates a new AttributeReference of type byte */
  def byte: StructField = StructField(name, ByteType)

  /** Creates a new AttributeReference of type short */
  def short: StructField = StructField(name, ShortType)

  /** Creates a new AttributeReference of type int */
  def int: StructField = StructField(name, IntegerType)

  /** Creates a new AttributeReference of type long */
  def long: StructField = StructField(name, LongType)

  /** Creates a new AttributeReference of type float */
  def float: StructField = StructField(name, FloatType)

  /** Creates a new AttributeReference of type double */
  def double: StructField = StructField(name, DoubleType)

  /** Creates a new AttributeReference of type string */
  def string: StructField = StructField(name, StringType)

  /** Creates a new AttributeReference of type date */
  def date: StructField = StructField(name, DateType)

  /** Creates a new AttributeReference of type decimal */
  def decimal: StructField = StructField(name, DecimalType.Unlimited)

  /** Creates a new AttributeReference of type decimal */
  def decimal(precision: Int, scale: Int): StructField =
    StructField(name, DecimalType(precision, scale))

  /** Creates a new AttributeReference of type timestamp */
  def timestamp: StructField = StructField(name, TimestampType)

  /** Creates a new AttributeReference of type binary */
  def binary: StructField = StructField(name, BinaryType)

  /** Creates a new AttributeReference of type array */
  def array(dataType: DataType): StructField = StructField(name, ArrayType(dataType))

  /** Creates a new AttributeReference of type map */
  def map(keyType: DataType, valueType: DataType): StructField =
    map(MapType(keyType, valueType))

  def map(mapType: MapType): StructField = StructField(name, mapType)

  /** Creates a new AttributeReference of type struct */
  def struct(fields: StructField*): StructField = struct(StructType(fields))

  def struct(structType: StructType): StructField = StructField(name, structType)
}
