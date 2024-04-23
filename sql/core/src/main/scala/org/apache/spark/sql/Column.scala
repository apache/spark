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

import scala.jdk.CollectionConverters._

import org.apache.spark.annotation.Stable
import org.apache.spark.internal.{Logging, MDC}
import org.apache.spark.internal.LogKey.{LEFT_EXPR, RIGHT_EXPR}
import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.encoders.{encoderFor, ExpressionEncoder}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.catalyst.util.{toPrettySQL, CharVarcharUtils}
import org.apache.spark.sql.execution.aggregate.TypedAggregateExpression
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.internal.TypedAggUtils
import org.apache.spark.sql.types._
import org.apache.spark.util.ArrayImplicits._

private[sql] object Column {

  def apply(colName: String): Column = new Column(colName)

  def apply(expr: Expression): Column = new Column(expr)

  def unapply(col: Column): Option[Expression] = Some(col.expr)

  private[sql] def generateAlias(e: Expression): String = {
    e match {
      case a: AggregateExpression if a.aggregateFunction.isInstanceOf[TypedAggregateExpression] =>
        a.aggregateFunction.toString
      case expr => toPrettySQL(expr)
    }
  }

  private[sql] def stripColumnReferenceMetadata(a: AttributeReference): AttributeReference = {
    val metadataWithoutId = new MetadataBuilder()
      .withMetadata(a.metadata)
      .remove(Dataset.DATASET_ID_KEY)
      .remove(Dataset.COL_POS_KEY)
      .build()
    a.withMetadata(metadataWithoutId)
  }

  private[sql] def fn(name: String, inputs: Column*): Column = {
    fn(name, isDistinct = false, ignoreNulls = false, inputs: _*)
  }

  private[sql] def fn(name: String, isDistinct: Boolean, inputs: Column*): Column = {
    fn(name, isDistinct = isDistinct, ignoreNulls = false, inputs: _*)
  }

  private[sql] def fn(
      name: String,
      isDistinct: Boolean,
      ignoreNulls: Boolean,
      inputs: Column*): Column = withOrigin {
    Column {
      UnresolvedFunction(Seq(name), inputs.map(_.expr), isDistinct, ignoreNulls = ignoreNulls)
    }
  }
}

/**
 * A [[Column]] where an [[Encoder]] has been given for the expected input and return type.
 * To create a [[TypedColumn]], use the `as` function on a [[Column]].
 *
 * @tparam T The input type expected for this expression.  Can be `Any` if the expression is type
 *           checked by the analyzer instead of the compiler (i.e. `expr("sum(...)")`).
 * @tparam U The output type of this column.
 *
 * @since 1.6.0
 */
@Stable
class TypedColumn[-T, U](
    expr: Expression,
    private[sql] val encoder: ExpressionEncoder[U])
  extends Column(expr) {

  /**
   * Inserts the specific input type and schema into any expressions that are expected to operate
   * on a decoded object.
   */
  private[sql] def withInputType(
      inputEncoder: ExpressionEncoder[_],
      inputAttributes: Seq[Attribute]): TypedColumn[T, U] = {
    val newExpr = TypedAggUtils.withInputType(expr, inputEncoder, inputAttributes)
    new TypedColumn[T, U](newExpr, encoder)
  }

  /**
   * Gives the [[TypedColumn]] a name (alias).
   * If the current `TypedColumn` has metadata associated with it, this metadata will be propagated
   * to the new column.
   *
   * @group expr_ops
   * @since 2.0.0
   */
  override def name(alias: String): TypedColumn[T, U] =
    new TypedColumn[T, U](super.name(alias).expr, encoder)

}

/**
 * A column that will be computed based on the data in a `DataFrame`.
 *
 * A new column can be constructed based on the input columns present in a DataFrame:
 *
 * {{{
 *   df("columnName")            // On a specific `df` DataFrame.
 *   col("columnName")           // A generic column not yet associated with a DataFrame.
 *   col("columnName.field")     // Extracting a struct field
 *   col("`a.column.with.dots`") // Escape `.` in column names.
 *   $"columnName"               // Scala short hand for a named column.
 * }}}
 *
 * [[Column]] objects can be composed to form complex expressions:
 *
 * {{{
 *   $"a" + 1
 *   $"a" === $"b"
 * }}}
 *
 * @note The internal Catalyst expression can be accessed via [[expr]], but this method is for
 * debugging purposes only and can change in any future Spark releases.
 *
 * @groupname java_expr_ops Java-specific expression operators
 * @groupname expr_ops Expression operators
 * @groupname df_ops DataFrame functions
 * @groupname Ungrouped Support functions for DataFrames
 *
 * @since 1.3.0
 */
@Stable
class Column(val expr: Expression) extends Logging {

  def this(name: String) = this(withOrigin {
    name match {
      case "*" => UnresolvedStar(None)
      case _ if name.endsWith(".*") =>
        val parts = UnresolvedAttribute.parseAttributeName(name.dropRight(2))
        UnresolvedStar(Some(parts))
      case _ => UnresolvedAttribute.quotedString(name)
    }
  })

  private def fn(name: String): Column = {
    Column.fn(name, this)
  }
  private def fn(name: String, other: Column): Column = {
    Column.fn(name, this, other)
  }
  private def fn(name: String, other: Any): Column = {
    Column.fn(name, this, lit(other))
  }

  override def toString: String = toPrettySQL(expr)

  override def equals(that: Any): Boolean = that match {
    case that: Column => that.normalizedExpr() == this.normalizedExpr()
    case _ => false
  }

  override def hashCode: Int = this.normalizedExpr().hashCode()

  private def normalizedExpr(): Expression = expr transform {
    case a: AttributeReference => Column.stripColumnReferenceMetadata(a)
  }

  /** Creates a column based on the given expression. */
  private def withExpr(newExpr: => Expression): Column = withOrigin {
    new Column(newExpr)
  }

  /**
   * Returns the expression for this column either with an existing or auto assigned name.
   */
  private[sql] def named: NamedExpression = expr match {
    case expr: NamedExpression => expr

    // Leave an unaliased generator with an empty list of names since the analyzer will generate
    // the correct defaults after the nested expression's type has been resolved.
    case g: Generator => MultiAlias(g, Nil)

    // If we have a top level Cast, there is a chance to give it a better alias, if there is a
    // NamedExpression under this Cast.
    case c: Cast =>
      c.transformUp {
        case c @ Cast(_: NamedExpression, _, _, _) => UnresolvedAlias(c)
      } match {
        case ne: NamedExpression => ne
        case _ => UnresolvedAlias(expr, Some(Column.generateAlias))
      }

    case expr: Expression => UnresolvedAlias(expr, Some(Column.generateAlias))
  }

  /**
   * Provides a type hint about the expected return value of this column.  This information can
   * be used by operations such as `select` on a [[Dataset]] to automatically convert the
   * results into the correct JVM types.
   * @since 1.6.0
   */
  def as[U : Encoder]: TypedColumn[Any, U] = new TypedColumn[Any, U](expr, encoderFor[U])

  /**
   * Extracts a value or values from a complex type.
   * The following types of extraction are supported:
   * <ul>
   * <li>Given an Array, an integer ordinal can be used to retrieve a single value.</li>
   * <li>Given a Map, a key of the correct type can be used to retrieve an individual value.</li>
   * <li>Given a Struct, a string fieldName can be used to extract that field.</li>
   * <li>Given an Array of Structs, a string fieldName can be used to extract filed
   *    of every struct in that array, and return an Array of fields.</li>
   * </ul>
   * @group expr_ops
   * @since 1.4.0
   */
  def apply(extraction: Any): Column = withExpr {
    UnresolvedExtractValue(expr, lit(extraction).expr)
  }

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
  def unary_- : Column = fn("negative")

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
  def unary_! : Column = fn("!")

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
  def ===(other: Any): Column = {
    val right = lit(other).expr
    if (this.expr == right) {
      logWarning(
        log"Constructing trivially true equals predicate, " +
          log"'${MDC(LEFT_EXPR, this.expr)} = ${MDC(RIGHT_EXPR, right)}'. " +
          log"Perhaps you need to use aliases.")
    }
    fn("=", other)
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
   *   df.select( df("colA") =!= df("colB") )
   *   df.select( !(df("colA") === df("colB")) )
   *
   *   // Java:
   *   import static org.apache.spark.sql.functions.*;
   *   df.filter( col("colA").notEqual(col("colB")) );
   * }}}
   *
   * @group expr_ops
   * @since 2.0.0
    */
  def =!= (other: Any): Column = !(this === other)

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
  @deprecated("!== does not have the same precedence as ===, use =!= instead", "2.0.0")
  def !== (other: Any): Column = this =!= other

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
  def notEqual(other: Any): Column = this =!= other

  /**
   * Greater than.
   * {{{
   *   // Scala: The following selects people older than 21.
   *   people.select( people("age") > 21 )
   *
   *   // Java:
   *   import static org.apache.spark.sql.functions.*;
   *   people.select( people.col("age").gt(21) );
   * }}}
   *
   * @group expr_ops
   * @since 1.3.0
   */
  def >(other: Any): Column = fn(">", other)

  /**
   * Greater than.
   * {{{
   *   // Scala: The following selects people older than 21.
   *   people.select( people("age") > lit(21) )
   *
   *   // Java:
   *   import static org.apache.spark.sql.functions.*;
   *   people.select( people.col("age").gt(21) );
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
   *   people.select( people.col("age").lt(21) );
   * }}}
   *
   * @group expr_ops
   * @since 1.3.0
   */
  def <(other: Any): Column = fn("<", other)

  /**
   * Less than.
   * {{{
   *   // Scala: The following selects people younger than 21.
   *   people.select( people("age") < 21 )
   *
   *   // Java:
   *   people.select( people.col("age").lt(21) );
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
   *   people.select( people.col("age").leq(21) );
   * }}}
   *
   * @group expr_ops
   * @since 1.3.0
   */
  def <=(other: Any): Column = fn("<=", other)

  /**
   * Less than or equal to.
   * {{{
   *   // Scala: The following selects people age 21 or younger than 21.
   *   people.select( people("age") <= 21 )
   *
   *   // Java:
   *   people.select( people.col("age").leq(21) );
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
   *   people.select( people.col("age").geq(21) )
   * }}}
   *
   * @group expr_ops
   * @since 1.3.0
   */
  def >=(other: Any): Column = fn(">=", other)

  /**
   * Greater than or equal to an expression.
   * {{{
   *   // Scala: The following selects people age 21 or older than 21.
   *   people.select( people("age") >= 21 )
   *
   *   // Java:
   *   people.select( people.col("age").geq(21) )
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
  def <=>(other: Any): Column = {
    val right = lit(other).expr
    if (this.expr == right) {
      logWarning(
        log"Constructing trivially true equals predicate, " +
          log"'${MDC(LEFT_EXPR, this.expr)} <=> ${MDC(RIGHT_EXPR, right)}'. " +
          log"Perhaps you need to use aliases.")
    }
    fn("<=>", other)
  }

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
  def when(condition: Column, value: Any): Column = withExpr {
    this.expr match {
      case CaseWhen(branches, None) =>
        CaseWhen(branches :+ ((condition.expr, lit(value).expr)))
      case CaseWhen(_, Some(_)) =>
        throw new IllegalArgumentException(
          "when() cannot be applied once otherwise() is applied")
      case _ =>
        throw new IllegalArgumentException(
          "when() can only be applied on a Column previously generated by when() function")
    }
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
  def otherwise(value: Any): Column = withExpr {
    this.expr match {
      case CaseWhen(branches, None) =>
        CaseWhen(branches, Option(lit(value).expr))
      case CaseWhen(_, Some(_)) =>
        throw new IllegalArgumentException(
          "otherwise() can only be applied once on a Column previously generated by when()")
      case _ =>
        throw new IllegalArgumentException(
          "otherwise() can only be applied on a Column previously generated by when()")
    }
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
  def isNaN: Column = fn("isNaN")

  /**
   * True if the current expression is null.
   *
   * @group expr_ops
   * @since 1.3.0
   */
  def isNull: Column = fn("isNull")

  /**
   * True if the current expression is NOT null.
   *
   * @group expr_ops
   * @since 1.3.0
   */
  def isNotNull: Column = fn("isNotNull")

  /**
   * Boolean OR.
   * {{{
   *   // Scala: The following selects people that are in school or employed.
   *   people.filter( people("inSchool") || people("isEmployed") )
   *
   *   // Java:
   *   people.filter( people.col("inSchool").or(people.col("isEmployed")) );
   * }}}
   *
   * @group expr_ops
   * @since 1.3.0
   */
  def ||(other: Any): Column = fn("or", other)

  /**
   * Boolean OR.
   * {{{
   *   // Scala: The following selects people that are in school or employed.
   *   people.filter( people("inSchool") || people("isEmployed") )
   *
   *   // Java:
   *   people.filter( people.col("inSchool").or(people.col("isEmployed")) );
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
   *   people.select( people.col("inSchool").and(people.col("isEmployed")) );
   * }}}
   *
   * @group expr_ops
   * @since 1.3.0
   */
  def &&(other: Any): Column = fn("and", other)

  /**
   * Boolean AND.
   * {{{
   *   // Scala: The following selects people that are in school and employed at the same time.
   *   people.select( people("inSchool") && people("isEmployed") )
   *
   *   // Java:
   *   people.select( people.col("inSchool").and(people.col("isEmployed")) );
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
   *   people.select( people.col("height").plus(people.col("weight")) );
   * }}}
   *
   * @group expr_ops
   * @since 1.3.0
   */
  def +(other: Any): Column = fn("+", other)

  /**
   * Sum of this expression and another expression.
   * {{{
   *   // Scala: The following selects the sum of a person's height and weight.
   *   people.select( people("height") + people("weight") )
   *
   *   // Java:
   *   people.select( people.col("height").plus(people.col("weight")) );
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
   *   people.select( people.col("height").minus(people.col("weight")) );
   * }}}
   *
   * @group expr_ops
   * @since 1.3.0
   */
  def -(other: Any): Column = fn("-", other)

  /**
   * Subtraction. Subtract the other expression from this expression.
   * {{{
   *   // Scala: The following selects the difference between people's height and their weight.
   *   people.select( people("height") - people("weight") )
   *
   *   // Java:
   *   people.select( people.col("height").minus(people.col("weight")) );
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
   *   people.select( people.col("height").multiply(people.col("weight")) );
   * }}}
   *
   * @group expr_ops
   * @since 1.3.0
   */
  def *(other: Any): Column = fn("*", other)

  /**
   * Multiplication of this expression and another expression.
   * {{{
   *   // Scala: The following multiplies a person's height by their weight.
   *   people.select( people("height") * people("weight") )
   *
   *   // Java:
   *   people.select( people.col("height").multiply(people.col("weight")) );
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
   *   people.select( people.col("height").divide(people.col("weight")) );
   * }}}
   *
   * @group expr_ops
   * @since 1.3.0
   */
  def /(other: Any): Column = fn("/", other)

  /**
   * Division this expression by another expression.
   * {{{
   *   // Scala: The following divides a person's height by their weight.
   *   people.select( people("height") / people("weight") )
   *
   *   // Java:
   *   people.select( people.col("height").divide(people.col("weight")) );
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
  def %(other: Any): Column = fn("%", other)

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
   * Note: Since the type of the elements in the list are inferred only during the run time,
   * the elements will be "up-casted" to the most common type for comparison.
   * For eg:
   *   1) In the case of "Int vs String", the "Int" will be up-casted to "String" and the
   * comparison will look like "String vs String".
   *   2) In the case of "Float vs Double", the "Float" will be up-casted to "Double" and the
   * comparison will look like "Double vs Double"
   *
   * @group expr_ops
   * @since 1.5.0
   */
  @scala.annotation.varargs
  def isin(list: Any*): Column = withExpr { In(expr, list.map(lit(_).expr)) }

  /**
   * A boolean expression that is evaluated to true if the value of this expression is contained
   * by the provided collection.
   *
   * Note: Since the type of the elements in the collection are inferred only during the run time,
   * the elements will be "up-casted" to the most common type for comparison.
   * For eg:
   *   1) In the case of "Int vs String", the "Int" will be up-casted to "String" and the
   * comparison will look like "String vs String".
   *   2) In the case of "Float vs Double", the "Float" will be up-casted to "Double" and the
   * comparison will look like "Double vs Double"
   *
   * @group expr_ops
   * @since 2.4.0
   */
  def isInCollection(values: scala.collection.Iterable[_]): Column = isin(values.toSeq: _*)

  /**
   * A boolean expression that is evaluated to true if the value of this expression is contained
   * by the provided collection.
   *
   * Note: Since the type of the elements in the collection are inferred only during the run time,
   * the elements will be "up-casted" to the most common type for comparison.
   * For eg:
   *   1) In the case of "Int vs String", the "Int" will be up-casted to "String" and the
   * comparison will look like "String vs String".
   *   2) In the case of "Float vs Double", the "Float" will be up-casted to "Double" and the
   * comparison will look like "Double vs Double"
   *
   * @group java_expr_ops
   * @since 2.4.0
   */
  def isInCollection(values: java.lang.Iterable[_]): Column = isInCollection(values.asScala)

  /**
   * SQL like expression. Returns a boolean column based on a SQL LIKE match.
   *
   * @group expr_ops
   * @since 1.3.0
   */
  def like(literal: String): Column = fn("like", literal)

  /**
   * SQL RLIKE expression (LIKE with Regex). Returns a boolean column based on a regex
   * match.
   *
   * @group expr_ops
   * @since 1.3.0
   */
  def rlike(literal: String): Column = fn("rlike", literal)

  /**
   * SQL ILIKE expression (case insensitive LIKE).
   *
   * @group expr_ops
   * @since 3.3.0
   */
  def ilike(literal: String): Column = fn("ilike", literal)

  /**
   * An expression that gets an item at position `ordinal` out of an array,
   * or gets a value by key `key` in a `MapType`.
   *
   * @group expr_ops
   * @since 1.3.0
   */
  def getItem(key: Any): Column = apply(key)

  // scalastyle:off line.size.limit
  /**
   * An expression that adds/replaces field in `StructType` by name.
   *
   * {{{
   *   val df = sql("SELECT named_struct('a', 1, 'b', 2) struct_col")
   *   df.select($"struct_col".withField("c", lit(3)))
   *   // result: {"a":1,"b":2,"c":3}
   *
   *   val df = sql("SELECT named_struct('a', 1, 'b', 2) struct_col")
   *   df.select($"struct_col".withField("b", lit(3)))
   *   // result: {"a":1,"b":3}
   *
   *   val df = sql("SELECT CAST(NULL AS struct<a:int,b:int>) struct_col")
   *   df.select($"struct_col".withField("c", lit(3)))
   *   // result: null of type struct<a:int,b:int,c:int>
   *
   *   val df = sql("SELECT named_struct('a', 1, 'b', 2, 'b', 3) struct_col")
   *   df.select($"struct_col".withField("b", lit(100)))
   *   // result: {"a":1,"b":100,"b":100}
   *
   *   val df = sql("SELECT named_struct('a', named_struct('a', 1, 'b', 2)) struct_col")
   *   df.select($"struct_col".withField("a.c", lit(3)))
   *   // result: {"a":{"a":1,"b":2,"c":3}}
   *
   *   val df = sql("SELECT named_struct('a', named_struct('b', 1), 'a', named_struct('c', 2)) struct_col")
   *   df.select($"struct_col".withField("a.c", lit(3)))
   *   // result: org.apache.spark.sql.AnalysisException: Ambiguous reference to fields
   * }}}
   *
   * This method supports adding/replacing nested fields directly e.g.
   *
   * {{{
   *   val df = sql("SELECT named_struct('a', named_struct('a', 1, 'b', 2)) struct_col")
   *   df.select($"struct_col".withField("a.c", lit(3)).withField("a.d", lit(4)))
   *   // result: {"a":{"a":1,"b":2,"c":3,"d":4}}
   * }}}
   *
   * However, if you are going to add/replace multiple nested fields, it is more optimal to extract
   * out the nested struct before adding/replacing multiple fields e.g.
   *
   * {{{
   *   val df = sql("SELECT named_struct('a', named_struct('a', 1, 'b', 2)) struct_col")
   *   df.select($"struct_col".withField("a", $"struct_col.a".withField("c", lit(3)).withField("d", lit(4))))
   *   // result: {"a":{"a":1,"b":2,"c":3,"d":4}}
   * }}}
   *
   * @group expr_ops
   * @since 3.1.0
   */
  // scalastyle:on line.size.limit
  def withField(fieldName: String, col: Column): Column = withExpr {
    require(fieldName != null, "fieldName cannot be null")
    require(col != null, "col cannot be null")
    UpdateFields(expr, fieldName, col.expr)
  }

  // scalastyle:off line.size.limit
  /**
   * An expression that drops fields in `StructType` by name.
   * This is a no-op if schema doesn't contain field name(s).
   *
   * {{{
   *   val df = sql("SELECT named_struct('a', 1, 'b', 2) struct_col")
   *   df.select($"struct_col".dropFields("b"))
   *   // result: {"a":1}
   *
   *   val df = sql("SELECT named_struct('a', 1, 'b', 2) struct_col")
   *   df.select($"struct_col".dropFields("c"))
   *   // result: {"a":1,"b":2}
   *
   *   val df = sql("SELECT named_struct('a', 1, 'b', 2, 'c', 3) struct_col")
   *   df.select($"struct_col".dropFields("b", "c"))
   *   // result: {"a":1}
   *
   *   val df = sql("SELECT named_struct('a', 1, 'b', 2) struct_col")
   *   df.select($"struct_col".dropFields("a", "b"))
   *   // result: org.apache.spark.sql.AnalysisException: [DATATYPE_MISMATCH.CANNOT_DROP_ALL_FIELDS] Cannot resolve "update_fields(struct_col, dropfield(), dropfield())" due to data type mismatch: Cannot drop all fields in struct.;
   *
   *   val df = sql("SELECT CAST(NULL AS struct<a:int,b:int>) struct_col")
   *   df.select($"struct_col".dropFields("b"))
   *   // result: null of type struct<a:int>
   *
   *   val df = sql("SELECT named_struct('a', 1, 'b', 2, 'b', 3) struct_col")
   *   df.select($"struct_col".dropFields("b"))
   *   // result: {"a":1}
   *
   *   val df = sql("SELECT named_struct('a', named_struct('a', 1, 'b', 2)) struct_col")
   *   df.select($"struct_col".dropFields("a.b"))
   *   // result: {"a":{"a":1}}
   *
   *   val df = sql("SELECT named_struct('a', named_struct('b', 1), 'a', named_struct('c', 2)) struct_col")
   *   df.select($"struct_col".dropFields("a.c"))
   *   // result: org.apache.spark.sql.AnalysisException: Ambiguous reference to fields
   * }}}
   *
   * This method supports dropping multiple nested fields directly e.g.
   *
   * {{{
   *   val df = sql("SELECT named_struct('a', named_struct('a', 1, 'b', 2)) struct_col")
   *   df.select($"struct_col".dropFields("a.b", "a.c"))
   *   // result: {"a":{"a":1}}
   * }}}
   *
   * However, if you are going to drop multiple nested fields, it is more optimal to extract
   * out the nested struct before dropping multiple fields from it e.g.
   *
   * {{{
   *   val df = sql("SELECT named_struct('a', named_struct('a', 1, 'b', 2)) struct_col")
   *   df.select($"struct_col".withField("a", $"struct_col.a".dropFields("b", "c")))
   *   // result: {"a":{"a":1}}
   * }}}
   *
   * @group expr_ops
   * @since 3.1.0
   */
  // scalastyle:on line.size.limit
  def dropFields(fieldNames: String*): Column = withExpr {
    fieldNames.tail.foldLeft(UpdateFields(expr, fieldNames.head)) {
      (resExpr, fieldName) => UpdateFields(resExpr, fieldName)
    }
  }

  /**
   * An expression that gets a field by name in a `StructType`.
   *
   * @group expr_ops
   * @since 1.3.0
   */
  def getField(fieldName: String): Column = apply(fieldName)

  /**
   * An expression that returns a substring.
   * @param startPos expression for the starting position.
   * @param len expression for the length of the substring.
   *
   * @group expr_ops
   * @since 1.3.0
   */
  def substr(startPos: Column, len: Column): Column = Column.fn("substr", this, startPos, len)

  /**
   * An expression that returns a substring.
   * @param startPos starting position.
   * @param len length of the substring.
   *
   * @group expr_ops
   * @since 1.3.0
   */
  def substr(startPos: Int, len: Int): Column = substr(lit(startPos), lit(len))

  /**
   * Contains the other element. Returns a boolean column based on a string match.
   *
   * @group expr_ops
   * @since 1.3.0
   */
  def contains(other: Any): Column = fn("contains", other)

  /**
   * String starts with. Returns a boolean column based on a string match.
   *
   * @group expr_ops
   * @since 1.3.0
   */
  def startsWith(other: Column): Column = fn("startswith", other)

  /**
   * String starts with another string literal. Returns a boolean column based on a string match.
   *
   * @group expr_ops
   * @since 1.3.0
   */
  def startsWith(literal: String): Column = startsWith(lit(literal))

  /**
   * String ends with. Returns a boolean column based on a string match.
   *
   * @group expr_ops
   * @since 1.3.0
   */
  def endsWith(other: Column): Column = fn("endswith", other)

  /**
   * String ends with another string literal. Returns a boolean column based on a string match.
   *
   * @group expr_ops
   * @since 1.3.0
   */
  def endsWith(literal: String): Column = endsWith(lit(literal))

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
  def alias(alias: String): Column = name(alias)

  /**
   * Gives the column an alias.
   * {{{
   *   // Renames colA to colB in select output.
   *   df.select($"colA".as("colB"))
   * }}}
   *
   * If the current column has metadata associated with it, this metadata will be propagated
   * to the new column. If this not desired, use the API `as(alias: String, metadata: Metadata)`
   * with explicit metadata.
   *
   * @group expr_ops
   * @since 1.3.0
   */
  def as(alias: String): Column = name(alias)

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
  def as(aliases: Seq[String]): Column = withExpr { MultiAlias(expr, aliases) }

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
  def as(aliases: Array[String]): Column = withExpr {
    MultiAlias(expr, aliases.toImmutableArraySeq)
  }

  /**
   * Gives the column an alias.
   * {{{
   *   // Renames colA to colB in select output.
   *   df.select($"colA".as("colB"))
   * }}}
   *
   * If the current column has metadata associated with it, this metadata will be propagated
   * to the new column. If this not desired, use the API `as(alias: String, metadata: Metadata)`
   * with explicit metadata.
   *
   * @group expr_ops
   * @since 1.3.0
   */
  def as(alias: Symbol): Column = name(alias.name)

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
  def as(alias: String, metadata: Metadata): Column = withExpr {
    Alias(expr, alias)(explicitMetadata = Some(metadata))
  }

  /**
   * Gives the column a name (alias).
   * {{{
   *   // Renames colA to colB in select output.
   *   df.select($"colA".name("colB"))
   * }}}
   *
   * If the current column has metadata associated with it, this metadata will be propagated
   * to the new column. If this not desired, use the API `as(alias: String, metadata: Metadata)`
   * with explicit metadata.
   *
   * @group expr_ops
   * @since 2.0.0
   */
  def name(alias: String): Column = withExpr {
    // SPARK-33536: an alias is no longer a column reference. Therefore,
    // we should not inherit the column reference related metadata in an alias
    // so that it is not caught as a column reference in DetectAmbiguousSelfJoin.
    Alias(expr, alias)(
      nonInheritableMetadataKeys = Seq(Dataset.DATASET_ID_KEY, Dataset.COL_POS_KEY))
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
  def cast(to: DataType): Column = withExpr {
    val cast = Cast(expr, CharVarcharUtils.replaceCharVarcharWithStringForCast(to))
    cast.setTagValue(Cast.USER_SPECIFIED_CAST, ())
    cast
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
  def cast(to: String): Column = cast(CatalystSqlParser.parseDataType(to))

  /**
   * Casts the column to a different data type and the result is null on failure.
   * {{{
   *   // Casts colA to IntegerType.
   *   import org.apache.spark.sql.types.IntegerType
   *   df.select(df("colA").try_cast(IntegerType))
   *
   *   // equivalent to
   *   df.select(df("colA").try_cast("int"))
   * }}}
   *
   * @group expr_ops
   * @since 4.0.0
   */
  def try_cast(to: DataType): Column = withExpr {
    val cast = Cast(
      child = expr,
      dataType = CharVarcharUtils.replaceCharVarcharWithStringForCast(to),
      evalMode = EvalMode.TRY)
    cast.setTagValue(Cast.USER_SPECIFIED_CAST, ())
    cast
  }

  /**
   * Casts the column to a different data type and the result is null on failure.
   * {{{
   *   // Casts colA to integer.
   *   df.select(df("colA").try_cast("int"))
   * }}}
   *
   * @group expr_ops
   * @since 4.0.0
   */
  def try_cast(to: String): Column = {
    try_cast(CatalystSqlParser.parseDataType(to))
  }

  /**
   * Returns a sort expression based on the descending order of the column.
   * {{{
   *   // Scala
   *   df.sort(df("age").desc)
   *
   *   // Java
   *   df.sort(df.col("age").desc());
   * }}}
   *
   * @group expr_ops
   * @since 1.3.0
   */
  def desc: Column = withExpr { SortOrder(expr, Descending) }

  /**
   * Returns a sort expression based on the descending order of the column,
   * and null values appear before non-null values.
   * {{{
   *   // Scala: sort a DataFrame by age column in descending order and null values appearing first.
   *   df.sort(df("age").desc_nulls_first)
   *
   *   // Java
   *   df.sort(df.col("age").desc_nulls_first());
   * }}}
   *
   * @group expr_ops
   * @since 2.1.0
   */
  def desc_nulls_first: Column = withExpr { SortOrder(expr, Descending, NullsFirst, Seq.empty) }

  /**
   * Returns a sort expression based on the descending order of the column,
   * and null values appear after non-null values.
   * {{{
   *   // Scala: sort a DataFrame by age column in descending order and null values appearing last.
   *   df.sort(df("age").desc_nulls_last)
   *
   *   // Java
   *   df.sort(df.col("age").desc_nulls_last());
   * }}}
   *
   * @group expr_ops
   * @since 2.1.0
   */
  def desc_nulls_last: Column = withExpr { SortOrder(expr, Descending, NullsLast, Seq.empty) }

  /**
   * Returns a sort expression based on ascending order of the column.
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
  def asc: Column = withExpr { SortOrder(expr, Ascending) }

  /**
   * Returns a sort expression based on ascending order of the column,
   * and null values return before non-null values.
   * {{{
   *   // Scala: sort a DataFrame by age column in ascending order and null values appearing first.
   *   df.sort(df("age").asc_nulls_first)
   *
   *   // Java
   *   df.sort(df.col("age").asc_nulls_first());
   * }}}
   *
   * @group expr_ops
   * @since 2.1.0
   */
  def asc_nulls_first: Column = withExpr { SortOrder(expr, Ascending, NullsFirst, Seq.empty) }

  /**
   * Returns a sort expression based on ascending order of the column,
   * and null values appear after non-null values.
   * {{{
   *   // Scala: sort a DataFrame by age column in ascending order and null values appearing last.
   *   df.sort(df("age").asc_nulls_last)
   *
   *   // Java
   *   df.sort(df.col("age").asc_nulls_last());
   * }}}
   *
   * @group expr_ops
   * @since 2.1.0
   */
  def asc_nulls_last: Column = withExpr { SortOrder(expr, Ascending, NullsLast, Seq.empty) }

  /**
   * Prints the expression to the console for debugging purposes.
   *
   * @group df_ops
   * @since 1.3.0
   */
  def explain(extended: Boolean): Unit = {
    // scalastyle:off println
    if (extended) {
      println(expr)
    } else {
      println(expr.sql)
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
  def bitwiseOR(other: Any): Column = fn("|", other)

  /**
   * Compute bitwise AND of this expression with another expression.
   * {{{
   *   df.select($"colA".bitwiseAND($"colB"))
   * }}}
   *
   * @group expr_ops
   * @since 1.4.0
   */
  def bitwiseAND(other: Any): Column = fn("&", other)

  /**
   * Compute bitwise XOR of this expression with another expression.
   * {{{
   *   df.select($"colA".bitwiseXOR($"colB"))
   * }}}
   *
   * @group expr_ops
   * @since 1.4.0
   */
  def bitwiseXOR(other: Any): Column = fn("^", other)

  /**
   * Defines a windowing column.
   *
   * {{{
   *   val w = Window.partitionBy("name").orderBy("id")
   *   df.select(
   *     sum("price").over(w.rangeBetween(Window.unboundedPreceding, 2)),
   *     avg("price").over(w.rowsBetween(Window.currentRow, 4))
   *   )
   * }}}
   *
   * @group expr_ops
   * @since 1.4.0
   */
  def over(window: expressions.WindowSpec): Column = withOrigin {
    window.withAggregate(this)
  }

  /**
   * Defines an empty analytic clause. In this case the analytic function is applied
   * and presented for all rows in the result set.
   *
   * {{{
   *   df.select(
   *     sum("price").over(),
   *     avg("price").over()
   *   )
   * }}}
   *
   * @group expr_ops
   * @since 2.0.0
   */
  def over(): Column = over(Window.spec)

}


/**
 * A convenient class used for constructing schema.
 *
 * @since 1.3.0
 */
@Stable
class ColumnName(name: String) extends Column(name) {

  /**
   * Creates a new `StructField` of type boolean.
   * @since 1.3.0
   */
  def boolean: StructField = StructField(name, BooleanType)

  /**
   * Creates a new `StructField` of type byte.
   * @since 1.3.0
   */
  def byte: StructField = StructField(name, ByteType)

  /**
   * Creates a new `StructField` of type short.
   * @since 1.3.0
   */
  def short: StructField = StructField(name, ShortType)

  /**
   * Creates a new `StructField` of type int.
   * @since 1.3.0
   */
  def int: StructField = StructField(name, IntegerType)

  /**
   * Creates a new `StructField` of type long.
   * @since 1.3.0
   */
  def long: StructField = StructField(name, LongType)

  /**
   * Creates a new `StructField` of type float.
   * @since 1.3.0
   */
  def float: StructField = StructField(name, FloatType)

  /**
   * Creates a new `StructField` of type double.
   * @since 1.3.0
   */
  def double: StructField = StructField(name, DoubleType)

  /**
   * Creates a new `StructField` of type string.
   * @since 1.3.0
   */
  def string: StructField = StructField(name, StringType)

  /**
   * Creates a new `StructField` of type date.
   * @since 1.3.0
   */
  def date: StructField = StructField(name, DateType)

  /**
   * Creates a new `StructField` of type decimal.
   * @since 1.3.0
   */
  def decimal: StructField = StructField(name, DecimalType.USER_DEFAULT)

  /**
   * Creates a new `StructField` of type decimal.
   * @since 1.3.0
   */
  def decimal(precision: Int, scale: Int): StructField =
    StructField(name, DecimalType(precision, scale))

  /**
   * Creates a new `StructField` of type timestamp.
   * @since 1.3.0
   */
  def timestamp: StructField = StructField(name, TimestampType)

  /**
   * Creates a new `StructField` of type binary.
   * @since 1.3.0
   */
  def binary: StructField = StructField(name, BinaryType)

  /**
   * Creates a new `StructField` of type array.
   * @since 1.3.0
   */
  def array(dataType: DataType): StructField = StructField(name, ArrayType(dataType))

  /**
   * Creates a new `StructField` of type map.
   * @since 1.3.0
   */
  def map(keyType: DataType, valueType: DataType): StructField =
    map(MapType(keyType, valueType))

  def map(mapType: MapType): StructField = StructField(name, mapType)

  /**
   * Creates a new `StructField` of type struct.
   * @since 1.3.0
   */
  def struct(fields: StructField*): StructField = struct(StructType(fields))

  /**
   * Creates a new `StructField` of type struct.
   * @since 1.3.0
   */
  def struct(structType: StructType): StructField = StructField(name, structType)
}
