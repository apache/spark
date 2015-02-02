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

import org.apache.spark.sql.Dsl.lit
import org.apache.spark.sql.catalyst.analysis.{UnresolvedStar, UnresolvedAttribute}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.{Project, LogicalPlan}
import org.apache.spark.sql.types._


object Column {
  /**
   * Creates a [[Column]] based on the given column name. Same as [[Dsl.col]].
   */
  def apply(colName: String): Column = new Column(colName)

  /** For internal pattern matching. */
  private[sql] def unapply(col: Column): Option[Expression] = Some(col.expr)
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
    protected[sql] val expr: Expression)
  extends DataFrame(sqlContext, plan) with ExpressionApi {

  /** Turns a Catalyst expression into a `Column`. */
  protected[sql] def this(expr: Expression) = this(None, None, expr)

  /**
   * Creates a new `Column` expression based on a column or attribute name.
   * The resolution of this is the same as SQL. For example:
   *
   * - "colName" becomes an expression selecting the column named "colName".
   * - "*" becomes an expression selecting all columns.
   * - "df.*" becomes an expression selecting all columns in data frame "df".
   */
  def this(name: String) = this(name match {
    case "*" => UnresolvedStar(None)
    case _ if name.endsWith(".*") => UnresolvedStar(Some(name.substring(0, name.length - 2)))
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
   * Inversion of boolean expression, i.e. NOT.
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
  override def === (literal: Any): Column = this === lit(literal)

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
  override def !== (literal: Any): Column = this !== lit(literal)

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
  override def > (literal: Any): Column = this > lit(literal)

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
  override def < (literal: Any): Column = this < lit(literal)

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
  override def <= (literal: Any): Column = this <= lit(literal)

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
  override def >= (literal: Any): Column = this >= lit(literal)

  /**
   * Equality test with an expression that is safe for null values.
   */
  override def <=> (other: Column): Column = other match {
    case null => EqualNullSafe(expr, lit(null).expr)
    case _ => EqualNullSafe(expr, other.expr)
  }

  /**
   * Equality test with a literal value that is safe for null values.
   */
  override def <=> (literal: Any): Column = this <=> lit(literal)

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
  override def || (literal: Boolean): Column = this || lit(literal)

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
  override def && (literal: Boolean): Column = this && lit(literal)

  /**
   * Bitwise AND with an expression.
   */
  override def & (other: Column): Column = BitwiseAnd(expr, other.expr)

  /**
   * Bitwise AND with a literal value.
   */
  override def & (literal: Any): Column = this & lit(literal)

  /**
   * Bitwise OR with an expression.
   */
  override def | (other: Column): Column = BitwiseOr(expr, other.expr)

  /**
   * Bitwise OR with a literal value.
   */
  override def | (literal: Any): Column = this | lit(literal)

  /**
   * Bitwise XOR with an expression.
   */
  override def ^ (other: Column): Column = BitwiseXor(expr, other.expr)

  /**
   * Bitwise XOR with a literal value.
   */
  override def ^ (literal: Any): Column = this ^ lit(literal)

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
  override def + (literal: Any): Column = this + lit(literal)

  /**
   * Subtraction. Subtract the other expression from this expression.
   * {{{
   *   // The following selects the difference between people's height and their weight.
   *   people.select( people("height") - people("weight") )
   * }}}
   */
  override def - (other: Column): Column = Subtract(expr, other.expr)

  /**
   * Subtraction. Subtract a literal value from this expression.
   * {{{
   *   // The following selects a person's height and subtract it by 10.
   *   people.select( people("height") - 10 )
   * }}}
   */
  override def - (literal: Any): Column = this - lit(literal)

  /**
   * Multiplication of this expression and another expression.
   * {{{
   *   // The following multiplies a person's height by their weight.
   *   people.select( people("height") * people("weight") )
   * }}}
   */
  override def * (other: Column): Column = Multiply(expr, other.expr)

  /**
   * Multiplication this expression and a literal value.
   * {{{
   *   // The following multiplies a person's height by 10.
   *   people.select( people("height") * 10 )
   * }}}
   */
  override def * (literal: Any): Column = this * lit(literal)

  /**
   * Division this expression by another expression.
   * {{{
   *   // The following divides a person's height by their weight.
   *   people.select( people("height") / people("weight") )
   * }}}
   */
  override def / (other: Column): Column = Divide(expr, other.expr)

  /**
   * Division this expression by a literal value.
   * {{{
   *   // The following divides a person's height by 10.
   *   people.select( people("height") / 10 )
   * }}}
   */
  override def / (literal: Any): Column = this / lit(literal)

  /**
   * Modulo (a.k.a. remainder) expression.
   */
  override def % (other: Column): Column = Remainder(expr, other.expr)

  /**
   * Modulo (a.k.a. remainder) expression.
   */
  override def % (literal: Any): Column = this % lit(literal)


  /**
   * A boolean expression that is evaluated to true if the value of this expression is contained
   * by the evaluated values of the arguments.
   */
  @scala.annotation.varargs
  override def in(list: Column*): Column = In(expr, list.map(_.expr))

  override def like(literal: String): Column = Like(expr, lit(literal).expr)

  override def rlike(literal: String): Column = RLike(expr, lit(literal).expr)

  /**
   * An expression that gets an item at position `ordinal` out of an array.
   */
  override def getItem(ordinal: Int): Column = GetItem(expr, Literal(ordinal))

  /**
   * An expression that gets a field by name in a [[StructField]].
   */
  override def getField(fieldName: String): Column = GetField(expr, fieldName)

  /**
   * An expression that returns a substring.
   * @param startPos expression for the starting position.
   * @param len expression for the length of the substring.
   */
  override def substr(startPos: Column, len: Column): Column =
    Substring(expr, startPos.expr, len.expr)

  /**
   * An expression that returns a substring.
   * @param startPos starting position.
   * @param len length of the substring.
   */
  override def substr(startPos: Int, len: Int): Column = this.substr(lit(startPos), lit(len))

  override def contains(other: Column): Column = Contains(expr, other.expr)

  override def contains(literal: Any): Column = this.contains(lit(literal))


  override def startsWith(other: Column): Column = StartsWith(expr, other.expr)

  override def startsWith(literal: String): Column = this.startsWith(lit(literal))

  override def endsWith(other: Column): Column = EndsWith(expr, other.expr)

  override def endsWith(literal: String): Column = this.endsWith(lit(literal))

  /**
   * Gives the column an alias.
   * {{{
   *   // Renames colA to colB in select output.
   *   df.select($"colA".as("colB"))
   * }}}
   */
  override def as(alias: String): Column = Alias(expr, alias)()

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
  override def cast(to: DataType): Column = Cast(expr, to)

  /**
   * Casts the column to a different data type, using the canonical string representation
   * of the type. The supported types are: `string`, `boolean`, `byte`, `short`, `int`, `long`,
   * `float`, `double`, `decimal`, `date`, `timestamp`.
   * {{{
   *   // Casts colA to integer.
   *   df.select(df("colA").cast("int"))
   * }}}
   */
  override def cast(to: String): Column = Cast(expr, to.toLowerCase match {
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
