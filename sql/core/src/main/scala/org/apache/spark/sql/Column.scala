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

import scala.annotation.tailrec
import scala.language.implicitConversions

import org.apache.spark.sql.Dsl.lit
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.{Subquery, Project, LogicalPlan}
import org.apache.spark.sql.types._


private[sql] object Column {

  def apply(colName: String): Column = new IncomputableColumn(colName)

  def apply(expr: Expression): Column = new IncomputableColumn(expr)

  def apply(sqlContext: SQLContext, plan: LogicalPlan, expr: Expression): Column = {
    new ComputableColumn(sqlContext, plan, expr)
  }

  def unapply(col: Column): Option[Expression] = Some(col.expr)
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
trait Column extends DataFrame {

  protected[sql] def expr: Expression

  /**
   * Returns true iff the [[Column]] is computable.
   */
  def isComputable: Boolean

  private def constructColumn(other: Column)(newExpr: Expression): Column = {
    // Removes all the top level projection and subquery so we can get to the underlying plan.
    @tailrec def stripProject(p: LogicalPlan): LogicalPlan = p match {
      case Project(_, child) => stripProject(child)
      case Subquery(_, child) => stripProject(child)
      case _ => p
    }

    def computableCol(baseCol: ComputableColumn, expr: Expression) = {
      val plan = Project(Seq(expr match {
        case named: NamedExpression => named
        case unnamed: Expression => Alias(unnamed, "col")()
      }), baseCol.plan)
      Column(baseCol.sqlContext, plan, expr)
    }

    (this, other) match {
      case (left: ComputableColumn, right: ComputableColumn) =>
        if (stripProject(left.plan).sameResult(stripProject(right.plan))) {
          computableCol(right, newExpr)
        } else {
          Column(newExpr)
        }
      case (left: ComputableColumn, _) => computableCol(left, newExpr)
      case (_, right: ComputableColumn) => computableCol(right, newExpr)
      case (_, _) => Column(newExpr)
    }
  }

  /**
   * Unary minus, i.e. negate the expression.
   * {{{
   *   // Select the amount column and negates all values.
   *   df.select( -df("amount") )
   * }}}
   */
  def unary_- : Column = constructColumn(null) { UnaryMinus(expr) }

  /**
   * Bitwise NOT.
   * {{{
   *   // Select the flags column and negate every bit.
   *   df.select( ~df("flags") )
   * }}}
   */
  def unary_~ : Column = constructColumn(null) { BitwiseNot(expr) }

  /**
   * Inversion of boolean expression, i.e. NOT.
   * {{
   *   // Select rows that are not active (isActive === false)
   *   df.select( !df("isActive") )
   * }}
   */
  def unary_! : Column = constructColumn(null) { Not(expr) }


  /**
   * Equality test with an expression.
   * {{{
   *   // The following two both select rows in which colA equals colB.
   *   df.select( df("colA") === df("colB") )
   *   df.select( df("colA".equalTo(df("colB")) )
   * }}}
   */
  def === (other: Column): Column = constructColumn(other) {
    EqualTo(expr, other.expr)
  }

  /**
   * Equality test with a literal value.
   * {{{
   *   // The following two both select rows in which colA is "Zaharia".
   *   df.select( df("colA") === "Zaharia")
   *   df.select( df("colA".equalTo("Zaharia") )
   * }}}
   */
  def === (literal: Any): Column = this === lit(literal)

  /**
   * Equality test with an expression.
   * {{{
   *   // The following two both select rows in which colA equals colB.
   *   df.select( df("colA") === df("colB") )
   *   df.select( df("colA".equalTo(df("colB")) )
   * }}}
   */
  def equalTo(other: Column): Column = this === other

  /**
   * Equality test with a literal value.
   * {{{
   *   // The following two both select rows in which colA is "Zaharia".
   *   df.select( df("colA") === "Zaharia")
   *   df.select( df("colA".equalTo("Zaharia") )
   * }}}
   */
  def equalTo(literal: Any): Column = this === literal

  /**
   * Inequality test with an expression.
   * {{{
   *   // The following two both select rows in which colA does not equal colB.
   *   df.select( df("colA") !== df("colB") )
   *   df.select( !(df("colA") === df("colB")) )
   * }}}
   */
  def !== (other: Column): Column = constructColumn(other) {
    Not(EqualTo(expr, other.expr))
  }

  /**
   * Inequality test with a literal value.
   * {{{
   *   // The following two both select rows in which colA does not equal equal 15.
   *   df.select( df("colA") !== 15 )
   *   df.select( !(df("colA") === 15) )
   * }}}
   */
  def !== (literal: Any): Column = this !== lit(literal)

  /**
   * Greater than an expression.
   * {{{
   *   // The following selects people older than 21.
   *   people.select( people("age") > Literal(21) )
   * }}}
   */
  def > (other: Column): Column =  constructColumn(other) {
    GreaterThan(expr, other.expr)
  }

  /**
   * Greater than a literal value.
   * {{{
   *   // The following selects people older than 21.
   *   people.select( people("age") > 21 )
   * }}}
   */
  def > (literal: Any): Column = this > lit(literal)

  /**
   * Less than an expression.
   * {{{
   *   // The following selects people younger than 21.
   *   people.select( people("age") < Literal(21) )
   * }}}
   */
  def < (other: Column): Column =  constructColumn(other) {
    LessThan(expr, other.expr)
  }

  /**
   * Less than a literal value.
   * {{{
   *   // The following selects people younger than 21.
   *   people.select( people("age") < 21 )
   * }}}
   */
  def < (literal: Any): Column = this < lit(literal)

  /**
   * Less than or equal to an expression.
   * {{{
   *   // The following selects people age 21 or younger than 21.
   *   people.select( people("age") <= Literal(21) )
   * }}}
   */
  def <= (other: Column): Column = constructColumn(other) {
    LessThanOrEqual(expr, other.expr)
  }

  /**
   * Less than or equal to a literal value.
   * {{{
   *   // The following selects people age 21 or younger than 21.
   *   people.select( people("age") <= 21 )
   * }}}
   */
  def <= (literal: Any): Column = this <= lit(literal)

  /**
   * Greater than or equal to an expression.
   * {{{
   *   // The following selects people age 21 or older than 21.
   *   people.select( people("age") >= Literal(21) )
   * }}}
   */
  def >= (other: Column): Column =  constructColumn(other) {
    GreaterThanOrEqual(expr, other.expr)
  }

  /**
   * Greater than or equal to a literal value.
   * {{{
   *   // The following selects people age 21 or older than 21.
   *   people.select( people("age") >= 21 )
   * }}}
   */
  def >= (literal: Any): Column = this >= lit(literal)

  /**
   * Equality test with an expression that is safe for null values.
   */
  def <=> (other: Column): Column = constructColumn(other) {
    other match {
      case null => EqualNullSafe(expr, lit(null).expr)
      case _ => EqualNullSafe(expr, other.expr)
    }
  }

  /**
   * Equality test with a literal value that is safe for null values.
   */
  def <=> (literal: Any): Column = this <=> lit(literal)

  /**
   * True if the current expression is null.
   */
  def isNull: Column = constructColumn(null) { IsNull(expr) }

  /**
   * True if the current expression is NOT null.
   */
  def isNotNull: Column = constructColumn(null) { IsNotNull(expr) }

  /**
   * Boolean OR with an expression.
   * {{{
   *   // The following selects people that are in school or employed.
   *   people.select( people("inSchool") || people("isEmployed") )
   * }}}
   */
  def || (other: Column): Column = constructColumn(other) {
    Or(expr, other.expr)
  }

  /**
   * Boolean OR with a literal value.
   * {{{
   *   // The following selects everything.
   *   people.select( people("inSchool") || true )
   * }}}
   */
  def || (literal: Boolean): Column = this || lit(literal)

  /**
   * Boolean AND with an expression.
   * {{{
   *   // The following selects people that are in school and employed at the same time.
   *   people.select( people("inSchool") && people("isEmployed") )
   * }}}
   */
  def && (other: Column): Column = constructColumn(other) {
    And(expr, other.expr)
  }

  /**
   * Boolean AND with a literal value.
   * {{{
   *   // The following selects people that are in school.
   *   people.select( people("inSchool") && true )
   * }}}
   */
  def && (literal: Boolean): Column = this && lit(literal)

  /**
   * Bitwise AND with an expression.
   */
  def & (other: Column): Column = constructColumn(other) {
    BitwiseAnd(expr, other.expr)
  }

  /**
   * Bitwise AND with a literal value.
   */
  def & (literal: Any): Column = this & lit(literal)

  /**
   * Bitwise OR with an expression.
   */
  def | (other: Column): Column = constructColumn(other) {
    BitwiseOr(expr, other.expr)
  }

  /**
   * Bitwise OR with a literal value.
   */
  def | (literal: Any): Column = this | lit(literal)

  /**
   * Bitwise XOR with an expression.
   */
  def ^ (other: Column): Column = constructColumn(other) {
    BitwiseXor(expr, other.expr)
  }

  /**
   * Bitwise XOR with a literal value.
   */
  def ^ (literal: Any): Column = this ^ lit(literal)

  /**
   * Sum of this expression and another expression.
   * {{{
   *   // The following selects the sum of a person's height and weight.
   *   people.select( people("height") + people("weight") )
   * }}}
   */
  def + (other: Column): Column = constructColumn(other) {
    Add(expr, other.expr)
  }

  /**
   * Sum of this expression and another expression.
   * {{{
   *   // The following selects the sum of a person's height and 10.
   *   people.select( people("height") + 10 )
   * }}}
   */
  def + (literal: Any): Column = this + lit(literal)

  /**
   * Subtraction. Subtract the other expression from this expression.
   * {{{
   *   // The following selects the difference between people's height and their weight.
   *   people.select( people("height") - people("weight") )
   * }}}
   */
  def - (other: Column): Column = constructColumn(other) {
    Subtract(expr, other.expr)
  }

  /**
   * Subtraction. Subtract a literal value from this expression.
   * {{{
   *   // The following selects a person's height and subtract it by 10.
   *   people.select( people("height") - 10 )
   * }}}
   */
  def - (literal: Any): Column = this - lit(literal)

  /**
   * Multiplication of this expression and another expression.
   * {{{
   *   // The following multiplies a person's height by their weight.
   *   people.select( people("height") * people("weight") )
   * }}}
   */
  def * (other: Column): Column = constructColumn(other) {
    Multiply(expr, other.expr)
  }

  /**
   * Multiplication this expression and a literal value.
   * {{{
   *   // The following multiplies a person's height by 10.
   *   people.select( people("height") * 10 )
   * }}}
   */
  def * (literal: Any): Column = this * lit(literal)

  /**
   * Division this expression by another expression.
   * {{{
   *   // The following divides a person's height by their weight.
   *   people.select( people("height") / people("weight") )
   * }}}
   */
  def / (other: Column): Column = constructColumn(other) {
    Divide(expr, other.expr)
  }

  /**
   * Division this expression by a literal value.
   * {{{
   *   // The following divides a person's height by 10.
   *   people.select( people("height") / 10 )
   * }}}
   */
  def / (literal: Any): Column = this / lit(literal)

  /**
   * Modulo (a.k.a. remainder) expression.
   */
  def % (other: Column): Column = constructColumn(other) {
    Remainder(expr, other.expr)
  }

  /**
   * Modulo (a.k.a. remainder) expression.
   */
  def % (literal: Any): Column = this % lit(literal)


  /**
   * A boolean expression that is evaluated to true if the value of this expression is contained
   * by the evaluated values of the arguments.
   */
  @scala.annotation.varargs
  def in(list: Column*): Column = {
    new IncomputableColumn(In(expr, list.map(_.expr)))
  }

  def like(literal: String): Column = constructColumn(null) {
    Like(expr, lit(literal).expr)
  }

  def rlike(literal: String): Column = constructColumn(null) {
    RLike(expr, lit(literal).expr)
  }

  /**
   * An expression that gets an item at position `ordinal` out of an array.
   */
  def getItem(ordinal: Int): Column = constructColumn(null) {
    GetItem(expr, Literal(ordinal))
  }

  /**
   * An expression that gets a field by name in a [[StructField]].
   */
  def getField(fieldName: String): Column = constructColumn(null) {
    GetField(expr, fieldName)
  }

  /**
   * An expression that returns a substring.
   * @param startPos expression for the starting position.
   * @param len expression for the length of the substring.
   */
  def substr(startPos: Column, len: Column): Column = {
    new IncomputableColumn(Substring(expr, startPos.expr, len.expr))
  }

  /**
   * An expression that returns a substring.
   * @param startPos starting position.
   * @param len length of the substring.
   */
  def substr(startPos: Int, len: Int): Column = this.substr(lit(startPos), lit(len))

  def contains(other: Column): Column = constructColumn(other) {
    Contains(expr, other.expr)
  }

  def contains(literal: Any): Column = this.contains(lit(literal))

  def startsWith(other: Column): Column = constructColumn(other) {
    StartsWith(expr, other.expr)
  }

  def startsWith(literal: String): Column = this.startsWith(lit(literal))

  def endsWith(other: Column): Column = constructColumn(other) {
    EndsWith(expr, other.expr)
  }

  def endsWith(literal: String): Column = this.endsWith(lit(literal))

  /**
   * Gives the column an alias.
   * {{{
   *   // Renames colA to colB in select output.
   *   df.select($"colA".as("colB"))
   * }}}
   */
  override def as(alias: String): Column = constructColumn(null) { Alias(expr, alias)() }

  /**
   * Casts the column to a different data type.
   * {{{
   *   // Casts colA to IntegerType.
   *   import org.apache.spark.sql.types.IntegerType
   *   df.select(df("colA").cast(IntegerType))
   *
   *   // equivalent to
   *   df.select(df("colA").cast("int"))
   * }}}
   */
  def cast(to: DataType): Column = constructColumn(null) { Cast(expr, to) }

  /**
   * Casts the column to a different data type, using the canonical string representation
   * of the type. The supported types are: `string`, `boolean`, `byte`, `short`, `int`, `long`,
   * `float`, `double`, `decimal`, `date`, `timestamp`.
   * {{{
   *   // Casts colA to integer.
   *   df.select(df("colA").cast("int"))
   * }}}
   */
  def cast(to: String): Column = constructColumn(null) {
    Cast(expr, to.toLowerCase match {
      case "string" => StringType
      case "boolean" => BooleanType
      case "byte" => ByteType
      case "short" => ShortType
      case "int" => IntegerType
      case "long" => LongType
      case "float" => FloatType
      case "double" => DoubleType
      case "decimal" => DecimalType.Unlimited
      case "date" => DateType
      case "timestamp" => TimestampType
      case _ => throw new RuntimeException(s"""Unsupported cast type: "$to"""")
    })
  }

  def desc: Column = constructColumn(null) { SortOrder(expr, Descending) }

  def asc: Column = constructColumn(null) { SortOrder(expr, Ascending) }
}


class ColumnName(name: String) extends IncomputableColumn(name) {

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
