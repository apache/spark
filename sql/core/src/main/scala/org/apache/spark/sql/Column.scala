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

import org.apache.spark.annotation.Experimental
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedStar, UnresolvedGetField}
import org.apache.spark.sql.types._


private[sql] object Column {

  def apply(colName: String): Column = new Column(colName)

  def apply(expr: Expression): Column = new Column(expr)

  def unapply(col: Column): Option[Expression] = Some(col.expr)
}


/**
 * :: Experimental ::
 * A column in a [[DataFrame]].
 *
 * @groupname java_expr_ops Java-specific expression operators.
 * @groupname expr_ops Expression operators.
 * @groupname df_ops DataFrame functions.
 * @groupname Ungrouped Support functions for DataFrames.
 */
@Experimental
class Column(protected[sql] val expr: Expression) {

  def this(name: String) = this(name match {
    case "*" => UnresolvedStar(None)
    case _ if name.endsWith(".*") => UnresolvedStar(Some(name.substring(0, name.length - 2)))
    case _ => UnresolvedAttribute(name)
  })

  /** Creates a column based on the given expression. */
  implicit private def exprToColumn(newExpr: Expression): Column = new Column(newExpr)

  override def toString: String = expr.prettyString

  /**
   * Unary minus, i.e. negate the expression.
   * {{{
   *   // Scala: select the amount column and negates all values.
   *   df.select( -df("amount") )
   *
   *   // Java:
   *   import static org.apache.spark.sql.functions.*;
   *   df.select( negate(col("amount") );
   * }}}
   *
   * @group expr_ops
   */
  def unary_- : Column = UnaryMinus(expr)

  /**
   * Inversion of boolean expression, i.e. NOT.
   * {{
   *   // Scala: select rows that are not active (isActive === false)
   *   df.filter( !df("isActive") )
   *
   *   // Java:
   *   import static org.apache.spark.sql.functions.*;
   *   df.filter( not(df.col("isActive")) );
   * }}
   *
   * @group expr_ops
   */
  def unary_! : Column = Not(expr)

  /**
   * Equality test.
   * {{{
   *   // Scala:
   *   df.filter( df("colA") === df("colB") )
   *
   *   // Java
   *   import static org.apache.spark.sql.functions.*;
   *   df.filter( col("colA").equalTo(col("colB")) );
   * }}}
   *
   * @group expr_ops
   */
  def === (other: Any): Column = EqualTo(expr, lit(other).expr)

  /**
   * Equality test.
   * {{{
   *   // Scala:
   *   df.filter( df("colA") === df("colB") )
   *
   *   // Java
   *   import static org.apache.spark.sql.functions.*;
   *   df.filter( col("colA").equalTo(col("colB")) );
   * }}}
   *
   * @group expr_ops
   */
  def equalTo(other: Any): Column = this === other

  /**
   * Inequality test.
   * {{{
   *   // Scala:
   *   df.select( df("colA") !== df("colB") )
   *   df.select( !(df("colA") === df("colB")) )
   *
   *   // Java:
   *   import static org.apache.spark.sql.functions.*;
   *   df.filter( col("colA").notEqual(col("colB")) );
   * }}}
   *
   * @group expr_ops
   */
  def !== (other: Any): Column = Not(EqualTo(expr, lit(other).expr))

  /**
   * Inequality test.
   * {{{
   *   // Scala:
   *   df.select( df("colA") !== df("colB") )
   *   df.select( !(df("colA") === df("colB")) )
   *
   *   // Java:
   *   import static org.apache.spark.sql.functions.*;
   *   df.filter( col("colA").notEqual(col("colB")) );
   * }}}
   *
   * @group java_expr_ops
   */
  def notEqual(other: Any): Column = Not(EqualTo(expr, lit(other).expr))

  /**
   * Greater than.
   * {{{
   *   // Scala: The following selects people older than 21.
   *   people.select( people("age") > 21 )
   *
   *   // Java:
   *   import static org.apache.spark.sql.functions.*;
   *   people.select( people("age").gt(21) );
   * }}}
   *
   * @group expr_ops
   */
  def > (other: Any): Column = GreaterThan(expr, lit(other).expr)

  /**
   * Greater than.
   * {{{
   *   // Scala: The following selects people older than 21.
   *   people.select( people("age") > lit(21) )
   *
   *   // Java:
   *   import static org.apache.spark.sql.functions.*;
   *   people.select( people("age").gt(21) );
   * }}}
   *
   * @group java_expr_ops
   */
  def gt(other: Any): Column = this > other

  /**
   * Less than.
   * {{{
   *   // Scala: The following selects people younger than 21.
   *   people.select( people("age") < 21 )
   *
   *   // Java:
   *   people.select( people("age").lt(21) );
   * }}}
   *
   * @group expr_ops
   */
  def < (other: Any): Column = LessThan(expr, lit(other).expr)

  /**
   * Less than.
   * {{{
   *   // Scala: The following selects people younger than 21.
   *   people.select( people("age") < 21 )
   *
   *   // Java:
   *   people.select( people("age").lt(21) );
   * }}}
   *
   * @group java_expr_ops
   */
  def lt(other: Any): Column = this < other

  /**
   * Less than or equal to.
   * {{{
   *   // Scala: The following selects people age 21 or younger than 21.
   *   people.select( people("age") <= 21 )
   *
   *   // Java:
   *   people.select( people("age").leq(21) );
   * }}}
   *
   * @group expr_ops
   */
  def <= (other: Any): Column = LessThanOrEqual(expr, lit(other).expr)

  /**
   * Less than or equal to.
   * {{{
   *   // Scala: The following selects people age 21 or younger than 21.
   *   people.select( people("age") <= 21 )
   *
   *   // Java:
   *   people.select( people("age").leq(21) );
   * }}}
   *
   * @group java_expr_ops
   */
  def leq(other: Any): Column = this <= other

  /**
   * Greater than or equal to an expression.
   * {{{
   *   // Scala: The following selects people age 21 or older than 21.
   *   people.select( people("age") >= 21 )
   *
   *   // Java:
   *   people.select( people("age").geq(21) )
   * }}}
   *
   * @group expr_ops
   */
  def >= (other: Any): Column = GreaterThanOrEqual(expr, lit(other).expr)

  /**
   * Greater than or equal to an expression.
   * {{{
   *   // Scala: The following selects people age 21 or older than 21.
   *   people.select( people("age") >= 21 )
   *
   *   // Java:
   *   people.select( people("age").geq(21) )
   * }}}
   *
   * @group java_expr_ops
   */
  def geq(other: Any): Column = this >= other

  /**
   * Equality test that is safe for null values.
   *
   * @group expr_ops
   */
  def <=> (other: Any): Column = EqualNullSafe(expr, lit(other).expr)

  /**
   * Equality test that is safe for null values.
   *
   * @group java_expr_ops
   */
  def eqNullSafe(other: Any): Column = this <=> other

  /**
   * True if the current expression is null.
   *
   * @group expr_ops
   */
  def isNull: Column = IsNull(expr)

  /**
   * True if the current expression is NOT null.
   *
   * @group expr_ops
   */
  def isNotNull: Column = IsNotNull(expr)

  /**
   * Boolean OR.
   * {{{
   *   // Scala: The following selects people that are in school or employed.
   *   people.filter( people("inSchool") || people("isEmployed") )
   *
   *   // Java:
   *   people.filter( people("inSchool").or(people("isEmployed")) );
   * }}}
   *
   * @group expr_ops
   */
  def || (other: Any): Column = Or(expr, lit(other).expr)

  /**
   * Boolean OR.
   * {{{
   *   // Scala: The following selects people that are in school or employed.
   *   people.filter( people("inSchool") || people("isEmployed") )
   *
   *   // Java:
   *   people.filter( people("inSchool").or(people("isEmployed")) );
   * }}}
   *
   * @group java_expr_ops
   */
  def or(other: Column): Column = this || other

  /**
   * Boolean AND.
   * {{{
   *   // Scala: The following selects people that are in school and employed at the same time.
   *   people.select( people("inSchool") && people("isEmployed") )
   *
   *   // Java:
   *   people.select( people("inSchool").and(people("isEmployed")) );
   * }}}
   *
   * @group expr_ops
   */
  def && (other: Any): Column = And(expr, lit(other).expr)

  /**
   * Boolean AND.
   * {{{
   *   // Scala: The following selects people that are in school and employed at the same time.
   *   people.select( people("inSchool") && people("isEmployed") )
   *
   *   // Java:
   *   people.select( people("inSchool").and(people("isEmployed")) );
   * }}}
   *
   * @group java_expr_ops
   */
  def and(other: Column): Column = this && other

  /**
   * Sum of this expression and another expression.
   * {{{
   *   // Scala: The following selects the sum of a person's height and weight.
   *   people.select( people("height") + people("weight") )
   *
   *   // Java:
   *   people.select( people("height").plus(people("weight")) );
   * }}}
   *
   * @group expr_ops
   */
  def + (other: Any): Column = Add(expr, lit(other).expr)

  /**
   * Sum of this expression and another expression.
   * {{{
   *   // Scala: The following selects the sum of a person's height and weight.
   *   people.select( people("height") + people("weight") )
   *
   *   // Java:
   *   people.select( people("height").plus(people("weight")) );
   * }}}
   *
   * @group java_expr_ops
   */
  def plus(other: Any): Column = this + other

  /**
   * Subtraction. Subtract the other expression from this expression.
   * {{{
   *   // Scala: The following selects the difference between people's height and their weight.
   *   people.select( people("height") - people("weight") )
   *
   *   // Java:
   *   people.select( people("height").minus(people("weight")) );
   * }}}
   *
   * @group expr_ops
   */
  def - (other: Any): Column = Subtract(expr, lit(other).expr)

  /**
   * Subtraction. Subtract the other expression from this expression.
   * {{{
   *   // Scala: The following selects the difference between people's height and their weight.
   *   people.select( people("height") - people("weight") )
   *
   *   // Java:
   *   people.select( people("height").minus(people("weight")) );
   * }}}
   *
   * @group java_expr_ops
   */
  def minus(other: Any): Column = this - other

  /**
   * Multiplication of this expression and another expression.
   * {{{
   *   // Scala: The following multiplies a person's height by their weight.
   *   people.select( people("height") * people("weight") )
   *
   *   // Java:
   *   people.select( people("height").multiply(people("weight")) );
   * }}}
   *
   * @group expr_ops
   */
  def * (other: Any): Column = Multiply(expr, lit(other).expr)

  /**
   * Multiplication of this expression and another expression.
   * {{{
   *   // Scala: The following multiplies a person's height by their weight.
   *   people.select( people("height") * people("weight") )
   *
   *   // Java:
   *   people.select( people("height").multiply(people("weight")) );
   * }}}
   *
   * @group java_expr_ops
   */
  def multiply(other: Any): Column = this * other

  /**
   * Division this expression by another expression.
   * {{{
   *   // Scala: The following divides a person's height by their weight.
   *   people.select( people("height") / people("weight") )
   *
   *   // Java:
   *   people.select( people("height").divide(people("weight")) );
   * }}}
   *
   * @group expr_ops
   */
  def / (other: Any): Column = Divide(expr, lit(other).expr)

  /**
   * Division this expression by another expression.
   * {{{
   *   // Scala: The following divides a person's height by their weight.
   *   people.select( people("height") / people("weight") )
   *
   *   // Java:
   *   people.select( people("height").divide(people("weight")) );
   * }}}
   *
   * @group java_expr_ops
   */
  def divide(other: Any): Column = this / other

  /**
   * Modulo (a.k.a. remainder) expression.
   *
   * @group expr_ops
   */
  def % (other: Any): Column = Remainder(expr, lit(other).expr)

  /**
   * Modulo (a.k.a. remainder) expression.
   *
   * @group java_expr_ops
   */
  def mod(other: Any): Column = this % other

  /**
   * A boolean expression that is evaluated to true if the value of this expression is contained
   * by the evaluated values of the arguments.
   *
   * @group expr_ops
   */
  @scala.annotation.varargs
  def in(list: Column*): Column = In(expr, list.map(_.expr))

  /**
   * SQL like expression.
   *
   * @group expr_ops
   */
  def like(literal: String): Column = Like(expr, lit(literal).expr)

  /**
   * SQL RLIKE expression (LIKE with Regex).
   *
   * @group expr_ops
   */
  def rlike(literal: String): Column = RLike(expr, lit(literal).expr)

  /**
   * An expression that gets an item at position `ordinal` out of an array.
   *
   * @group expr_ops
   */
  def getItem(ordinal: Int): Column = GetItem(expr, Literal(ordinal))

  /**
   * An expression that gets a field by name in a [[StructField]].
   *
   * @group expr_ops
   */
  def getField(fieldName: String): Column = UnresolvedGetField(expr, fieldName)

  /**
   * An expression that returns a substring.
   * @param startPos expression for the starting position.
   * @param len expression for the length of the substring.
   *
   * @group expr_ops
   */
  def substr(startPos: Column, len: Column): Column = Substring(expr, startPos.expr, len.expr)

  /**
   * An expression that returns a substring.
   * @param startPos starting position.
   * @param len length of the substring.
   *
   * @group expr_ops
   */
  def substr(startPos: Int, len: Int): Column = Substring(expr, lit(startPos).expr, lit(len).expr)

  /**
   * Contains the other element.
   *
   * @group expr_ops
   */
  def contains(other: Any): Column = Contains(expr, lit(other).expr)

  /**
   * String starts with.
   *
   * @group expr_ops
   */
  def startsWith(other: Column): Column = StartsWith(expr, lit(other).expr)

  /**
   * String starts with another string literal.
   *
   * @group expr_ops
   */
  def startsWith(literal: String): Column = this.startsWith(lit(literal))

  /**
   * String ends with.
   *
   * @group expr_ops
   */
  def endsWith(other: Column): Column = EndsWith(expr, lit(other).expr)

  /**
   * String ends with another string literal.
   *
   * @group expr_ops
   */
  def endsWith(literal: String): Column = this.endsWith(lit(literal))

  /**
   * Gives the column an alias.
   * {{{
   *   // Renames colA to colB in select output.
   *   df.select($"colA".as("colB"))
   * }}}
   *
   * @group expr_ops
   */
  def as(alias: String): Column = Alias(expr, alias)()

  /**
   * Gives the column an alias.
   * {{{
   *   // Renames colA to colB in select output.
   *   df.select($"colA".as('colB))
   * }}}
   *
   * @group expr_ops
   */
  def as(alias: Symbol): Column = Alias(expr, alias.name)()

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
   *
   * @group expr_ops
   */
  def cast(to: DataType): Column = Cast(expr, to)

  /**
   * Casts the column to a different data type, using the canonical string representation
   * of the type. The supported types are: `string`, `boolean`, `byte`, `short`, `int`, `long`,
   * `float`, `double`, `decimal`, `date`, `timestamp`.
   * {{{
   *   // Casts colA to integer.
   *   df.select(df("colA").cast("int"))
   * }}}
   *
   * @group expr_ops
   */
  def cast(to: String): Column = Cast(expr, to.toLowerCase match {
    case "string" | "str" => StringType
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

  /**
   * Returns an ordering used in sorting.
   * {{{
   *   // Scala: sort a DataFrame by age column in descending order.
   *   df.sort(df("age").desc)
   *
   *   // Java
   *   df.sort(df.col("age").desc());
   * }}}
   *
   * @group expr_ops
   */
  def desc: Column = SortOrder(expr, Descending)

  /**
   * Returns an ordering used in sorting.
   * {{{
   *   // Scala: sort a DataFrame by age column in ascending order.
   *   df.sort(df("age").asc)
   *
   *   // Java
   *   df.sort(df.col("age").asc());
   * }}}
   *
   * @group expr_ops
   */
  def asc: Column = SortOrder(expr, Ascending)

  /**
   * Prints the expression to the console for debugging purpose.
   *
   * @group df_ops
   */
  def explain(extended: Boolean): Unit = {
    if (extended) {
      println(expr)
    } else {
      println(expr.prettyString)
    }
  }
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
