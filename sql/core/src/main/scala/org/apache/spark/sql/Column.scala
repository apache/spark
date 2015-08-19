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
import org.apache.spark.Logging
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.analysis._
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
 *
 * @since 1.3.0
 */
@Experimental
class Column(protected[sql] val expr: Expression) extends Logging {

  def this(name: String) = this(name match {
    case "*" => UnresolvedStar(None)
    case _ if name.endsWith(".*") => UnresolvedStar(Some(name.substring(0, name.length - 2)))
    case _ => UnresolvedAttribute.quotedString(name)
  })

  /** Creates a column based on the given expression. */
  implicit private def exprToColumn(newExpr: Expression): Column = new Column(newExpr)

  override def toString: String = expr.prettyString

  override def equals(that: Any): Boolean = that match {
    case that: Column => that.expr.equals(this.expr)
    case _ => false
  }

  override def hashCode: Int = this.expr.hashCode

  /**
   * Extracts a value or values from a complex type.
   * The following types of extraction are supported:
   * - Given an Array, an integer ordinal can be used to retrieve a single value.
   * - Given a Map, a key of the correct type can be used to retrieve an individual value.
   * - Given a Struct, a string fieldName can be used to extract that field.
   * - Given an Array of Structs, a string fieldName can be used to extract filed
   *   of every struct in that array, and return an Array of fields
   *
   * @group expr_ops
   * @since 1.4.0
   */
  def apply(extraction: Any): Column = UnresolvedExtractValue(expr, lit(extraction).expr)

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
   * @since 1.3.0
   */
  def unary_- : Column = UnaryMinus(expr)

  /**
   * Inversion of boolean expression, i.e. NOT.
   * {{{
   *   // Scala: select rows that are not active (isActive === false)
   *   df.filter( !df("isActive") )
   *
   *   // Java:
   *   import static org.apache.spark.sql.functions.*;
   *   df.filter( not(df.col("isActive")) );
   * }}}
   *
   * @group expr_ops
   * @since 1.3.0
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
   * @since 1.3.0
   */
  def === (other: Any): Column = {
    val right = lit(other).expr
    if (this.expr == right) {
      logWarning(
        s"Constructing trivially true equals predicate, '${this.expr} = $right'. " +
          "Perhaps you need to use aliases.")
    }
    EqualTo(expr, right)
  }

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
   * @since 1.3.0
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
   * @since 1.3.0
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
   * @since 1.3.0
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
   * @since 1.3.0
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
   * @since 1.3.0
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
   * @since 1.3.0
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
   * @since 1.3.0
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
   * @since 1.3.0
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
   * @since 1.3.0
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
   * @since 1.3.0
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
   * @since 1.3.0
   */
  def geq(other: Any): Column = this >= other

  /**
   * Equality test that is safe for null values.
   *
   * @group expr_ops
   * @since 1.3.0
   */
  def <=> (other: Any): Column = EqualNullSafe(expr, lit(other).expr)

  /**
   * Equality test that is safe for null values.
   *
   * @group java_expr_ops
   * @since 1.3.0
   */
  def eqNullSafe(other: Any): Column = this <=> other

  /**
   * Evaluates a list of conditions and returns one of multiple possible result expressions.
   * If otherwise is not defined at the end, null is returned for unmatched conditions.
   *
   * {{{
   *   // Example: encoding gender string column into integer.
   *
   *   // Scala:
   *   people.select(when(people("gender") === "male", 0)
   *     .when(people("gender") === "female", 1)
   *     .otherwise(2))
   *
   *   // Java:
   *   people.select(when(col("gender").equalTo("male"), 0)
   *     .when(col("gender").equalTo("female"), 1)
   *     .otherwise(2))
   * }}}
   *
   * @group expr_ops
   * @since 1.4.0
   */
  def when(condition: Column, value: Any): Column = this.expr match {
    case CaseWhen(branches: Seq[Expression]) =>
      CaseWhen(branches ++ Seq(lit(condition).expr, lit(value).expr))
    case _ =>
      throw new IllegalArgumentException(
        "when() can only be applied on a Column previously generated by when() function")
  }

  /**
   * Evaluates a list of conditions and returns one of multiple possible result expressions.
   * If otherwise is not defined at the end, null is returned for unmatched conditions.
   *
   * {{{
   *   // Example: encoding gender string column into integer.
   *
   *   // Scala:
   *   people.select(when(people("gender") === "male", 0)
   *     .when(people("gender") === "female", 1)
   *     .otherwise(2))
   *
   *   // Java:
   *   people.select(when(col("gender").equalTo("male"), 0)
   *     .when(col("gender").equalTo("female"), 1)
   *     .otherwise(2))
   * }}}
   *
   * @group expr_ops
   * @since 1.4.0
   */
  def otherwise(value: Any): Column = this.expr match {
    case CaseWhen(branches: Seq[Expression]) =>
      if (branches.size % 2 == 0) {
        CaseWhen(branches :+ lit(value).expr)
      } else {
        throw new IllegalArgumentException(
          "otherwise() can only be applied once on a Column previously generated by when()")
      }
    case _ =>
      throw new IllegalArgumentException(
        "otherwise() can only be applied on a Column previously generated by when()")
  }

  /**
   * True if the current column is between the lower bound and upper bound, inclusive.
   *
   * @group java_expr_ops
   * @since 1.4.0
   */
  def between(lowerBound: Any, upperBound: Any): Column = {
    (this >= lowerBound) && (this <= upperBound)
  }

  /**
   * True if the current expression is NaN.
   *
   * @group expr_ops
   * @since 1.5.0
   */
  def isNaN: Column = IsNaN(expr)

  /**
   * True if the current expression is null.
   *
   * @group expr_ops
   * @since 1.3.0
   */
  def isNull: Column = IsNull(expr)

  /**
   * True if the current expression is NOT null.
   *
   * @group expr_ops
   * @since 1.3.0
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
   * @since 1.3.0
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
   * @since 1.3.0
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
   * @since 1.3.0
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
   * @since 1.3.0
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
   * @since 1.3.0
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
   * @since 1.3.0
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
   * @since 1.3.0
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
   * @since 1.3.0
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
   * @since 1.3.0
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
   * @since 1.3.0
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
   * @since 1.3.0
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
   * @since 1.3.0
   */
  def divide(other: Any): Column = this / other

  /**
   * Modulo (a.k.a. remainder) expression.
   *
   * @group expr_ops
   * @since 1.3.0
   */
  def % (other: Any): Column = Remainder(expr, lit(other).expr)

  /**
   * Modulo (a.k.a. remainder) expression.
   *
   * @group java_expr_ops
   * @since 1.3.0
   */
  def mod(other: Any): Column = this % other

  /**
   * A boolean expression that is evaluated to true if the value of this expression is contained
   * by the evaluated values of the arguments.
   *
   * @group expr_ops
   * @since 1.3.0
   */
  @deprecated("use isin", "1.5.0")
  @scala.annotation.varargs
  def in(list: Any*): Column = isin(list : _*)

  /**
   * A boolean expression that is evaluated to true if the value of this expression is contained
   * by the evaluated values of the arguments.
   *
   * @group expr_ops
   * @since 1.5.0
   */
  @scala.annotation.varargs
  def isin(list: Any*): Column = In(expr, list.map(lit(_).expr))

  /**
   * SQL like expression.
   *
   * @group expr_ops
   * @since 1.3.0
   */
  def like(literal: String): Column = Like(expr, lit(literal).expr)

  /**
   * SQL RLIKE expression (LIKE with Regex).
   *
   * @group expr_ops
   * @since 1.3.0
   */
  def rlike(literal: String): Column = RLike(expr, lit(literal).expr)

  /**
   * An expression that gets an item at position `ordinal` out of an array,
   * or gets a value by key `key` in a [[MapType]].
   *
   * @group expr_ops
   * @since 1.3.0
   */
  def getItem(key: Any): Column = UnresolvedExtractValue(expr, Literal(key))

  /**
   * An expression that gets a field by name in a [[StructType]].
   *
   * @group expr_ops
   * @since 1.3.0
   */
  def getField(fieldName: String): Column = UnresolvedExtractValue(expr, Literal(fieldName))

  /**
   * An expression that returns a substring.
   * @param startPos expression for the starting position.
   * @param len expression for the length of the substring.
   *
   * @group expr_ops
   * @since 1.3.0
   */
  def substr(startPos: Column, len: Column): Column = Substring(expr, startPos.expr, len.expr)

  /**
   * An expression that returns a substring.
   * @param startPos starting position.
   * @param len length of the substring.
   *
   * @group expr_ops
   * @since 1.3.0
   */
  def substr(startPos: Int, len: Int): Column = Substring(expr, lit(startPos).expr, lit(len).expr)

  /**
   * Contains the other element.
   *
   * @group expr_ops
   * @since 1.3.0
   */
  def contains(other: Any): Column = Contains(expr, lit(other).expr)

  /**
   * String starts with.
   *
   * @group expr_ops
   * @since 1.3.0
   */
  def startsWith(other: Column): Column = StartsWith(expr, lit(other).expr)

  /**
   * String starts with another string literal.
   *
   * @group expr_ops
   * @since 1.3.0
   */
  def startsWith(literal: String): Column = this.startsWith(lit(literal))

  /**
   * String ends with.
   *
   * @group expr_ops
   * @since 1.3.0
   */
  def endsWith(other: Column): Column = EndsWith(expr, lit(other).expr)

  /**
   * String ends with another string literal.
   *
   * @group expr_ops
   * @since 1.3.0
   */
  def endsWith(literal: String): Column = this.endsWith(lit(literal))

  /**
   * Gives the column an alias. Same as `as`.
   * {{{
   *   // Renames colA to colB in select output.
   *   df.select($"colA".alias("colB"))
   * }}}
   *
   * @group expr_ops
   * @since 1.4.0
   */
  def alias(alias: String): Column = as(alias)

  /**
   * Gives the column an alias.
   * {{{
   *   // Renames colA to colB in select output.
   *   df.select($"colA".as("colB"))
   * }}}
   *
   * If the current column has metadata associated with it, this metadata will be propagated
   * to the new column.  If this not desired, use `as` with explicitly empty metadata.
   *
   * @group expr_ops
   * @since 1.3.0
   */
  def as(alias: String): Column = expr match {
    case ne: NamedExpression => Alias(expr, alias)(explicitMetadata = Some(ne.metadata))
    case other => Alias(other, alias)()
  }

  /**
   * (Scala-specific) Assigns the given aliases to the results of a table generating function.
   * {{{
   *   // Renames colA to colB in select output.
   *   df.select(explode($"myMap").as("key" :: "value" :: Nil))
   * }}}
   *
   * @group expr_ops
   * @since 1.4.0
   */
  def as(aliases: Seq[String]): Column = MultiAlias(expr, aliases)

  /**
   * Assigns the given aliases to the results of a table generating function.
   * {{{
   *   // Renames colA to colB in select output.
   *   df.select(explode($"myMap").as("key" :: "value" :: Nil))
   * }}}
   *
   * @group expr_ops
   * @since 1.4.0
   */
  def as(aliases: Array[String]): Column = MultiAlias(expr, aliases)

  /**
   * Gives the column an alias.
   * {{{
   *   // Renames colA to colB in select output.
   *   df.select($"colA".as('colB))
   * }}}
   *
   * If the current column has metadata associated with it, this metadata will be propagated
   * to the new column.  If this not desired, use `as` with explicitly empty metadata.
   *
   * @group expr_ops
   * @since 1.3.0
   */
  def as(alias: Symbol): Column = expr match {
    case ne: NamedExpression => Alias(expr, alias.name)(explicitMetadata = Some(ne.metadata))
    case other => Alias(other, alias.name)()
  }

  /**
   * Gives the column an alias with metadata.
   * {{{
   *   val metadata: Metadata = ...
   *   df.select($"colA".as("colB", metadata))
   * }}}
   *
   * @group expr_ops
   * @since 1.3.0
   */
  def as(alias: String, metadata: Metadata): Column = {
    Alias(expr, alias)(explicitMetadata = Some(metadata))
  }

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
   * @since 1.3.0
   */
  def cast(to: DataType): Column = expr match {
    // Lift alias out of cast so we can support col.as("name").cast(IntegerType)
    case Alias(childExpr, name) => Alias(Cast(childExpr, to), name)()
    case _ => Cast(expr, to)
  }

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
   * @since 1.3.0
   */
  def cast(to: String): Column = cast(DataTypeParser.parse(to))

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
   * @since 1.3.0
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
   * @since 1.3.0
   */
  def asc: Column = SortOrder(expr, Ascending)

  /**
   * Prints the expression to the console for debugging purpose.
   *
   * @group df_ops
   * @since 1.3.0
   */
  def explain(extended: Boolean): Unit = {
    // scalastyle:off println
    if (extended) {
      println(expr)
    } else {
      println(expr.prettyString)
    }
    // scalastyle:on println
  }

  /**
   * Compute bitwise OR of this expression with another expression.
   * {{{
   *   df.select($"colA".bitwiseOR($"colB"))
   * }}}
   *
   * @group expr_ops
   * @since 1.4.0
   */
  def bitwiseOR(other: Any): Column = BitwiseOr(expr, lit(other).expr)

  /**
   * Compute bitwise AND of this expression with another expression.
   * {{{
   *   df.select($"colA".bitwiseAND($"colB"))
   * }}}
   *
   * @group expr_ops
   * @since 1.4.0
   */
  def bitwiseAND(other: Any): Column = BitwiseAnd(expr, lit(other).expr)

  /**
   * Compute bitwise XOR of this expression with another expression.
   * {{{
   *   df.select($"colA".bitwiseXOR($"colB"))
   * }}}
   *
   * @group expr_ops
   * @since 1.4.0
   */
  def bitwiseXOR(other: Any): Column = BitwiseXor(expr, lit(other).expr)

  /**
   * Define a windowing column.
   *
   * {{{
   *   val w = Window.partitionBy("name").orderBy("id")
   *   df.select(
   *     sum("price").over(w.rangeBetween(Long.MinValue, 2)),
   *     avg("price").over(w.rowsBetween(0, 4))
   *   )
   * }}}
   *
   * @group expr_ops
   * @since 1.4.0
   */
  def over(window: expressions.WindowSpec): Column = window.withAggregate(this)

}


/**
 * :: Experimental ::
 * A convenient class used for constructing schema.
 *
 * @since 1.3.0
 */
@Experimental
class ColumnName(name: String) extends Column(name) {

  /**
   * Creates a new [[StructField]] of type boolean.
   * @since 1.3.0
   */
  def boolean: StructField = StructField(name, BooleanType)

  /**
   * Creates a new [[StructField]] of type byte.
   * @since 1.3.0
   */
  def byte: StructField = StructField(name, ByteType)

  /**
   * Creates a new [[StructField]] of type short.
   * @since 1.3.0
   */
  def short: StructField = StructField(name, ShortType)

  /**
   * Creates a new [[StructField]] of type int.
   * @since 1.3.0
   */
  def int: StructField = StructField(name, IntegerType)

  /**
   * Creates a new [[StructField]] of type long.
   * @since 1.3.0
   */
  def long: StructField = StructField(name, LongType)

  /**
   * Creates a new [[StructField]] of type float.
   * @since 1.3.0
   */
  def float: StructField = StructField(name, FloatType)

  /**
   * Creates a new [[StructField]] of type double.
   * @since 1.3.0
   */
  def double: StructField = StructField(name, DoubleType)

  /**
   * Creates a new [[StructField]] of type string.
   * @since 1.3.0
   */
  def string: StructField = StructField(name, StringType)

  /**
   * Creates a new [[StructField]] of type date.
   * @since 1.3.0
   */
  def date: StructField = StructField(name, DateType)

  /**
   * Creates a new [[StructField]] of type decimal.
   * @since 1.3.0
   */
  def decimal: StructField = StructField(name, DecimalType.USER_DEFAULT)

  /**
   * Creates a new [[StructField]] of type decimal.
   * @since 1.3.0
   */
  def decimal(precision: Int, scale: Int): StructField =
    StructField(name, DecimalType(precision, scale))

  /**
   * Creates a new [[StructField]] of type timestamp.
   * @since 1.3.0
   */
  def timestamp: StructField = StructField(name, TimestampType)

  /**
   * Creates a new [[StructField]] of type binary.
   * @since 1.3.0
   */
  def binary: StructField = StructField(name, BinaryType)

  /**
   * Creates a new [[StructField]] of type array.
   * @since 1.3.0
   */
  def array(dataType: DataType): StructField = StructField(name, ArrayType(dataType))

  /**
   * Creates a new [[StructField]] of type map.
   * @since 1.3.0
   */
  def map(keyType: DataType, valueType: DataType): StructField =
    map(MapType(keyType, valueType))

  def map(mapType: MapType): StructField = StructField(name, mapType)

  /**
   * Creates a new [[StructField]] of type struct.
   * @since 1.3.0
   */
  def struct(fields: StructField*): StructField = struct(StructType(fields))

  /**
   * Creates a new [[StructField]] of type struct.
   * @since 1.3.0
   */
  def struct(structType: StructType): StructField = StructField(name, structType)
}
