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

import org.apache.spark.annotation.{DeveloperApi, Since}
import org.apache.spark.connect.proto
import org.apache.spark.connect.proto.Expression.SortOrder.NullOrdering
import org.apache.spark.connect.proto.Expression.SortOrder.SortDirection
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.encoders.AgnosticEncoder
import org.apache.spark.sql.catalyst.parser.DataTypeParser
import org.apache.spark.sql.connect.common.DataTypeProtoConverter
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types._
import org.apache.spark.util.ArrayImplicits._

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
 * }}}
 *
 * @since 3.4.0
 */
class Column(@DeveloperApi private val expr: proto.Expression) extends Logging {

  private[sql] def this(name: String, planId: Option[Long]) =
    this(Column.nameToExpression(name, planId))

  private[sql] def this(name: String) =
    this(name, None)

  private def fn(name: String): Column = Column.fn(name, this)
  private def fn(name: String, other: Column): Column = Column.fn(name, this, other)
  private def fn(name: String, other: Any): Column = Column.fn(name, this, lit(other))

  override def toString: String = expr.toString

  override def equals(that: Any): Boolean = that match {
    case that: Column => expr == that.expr
    case _ => false
  }

  override def hashCode: Int = expr.hashCode()

  /**
   * Provides a type hint about the expected return value of this column. This information can be
   * used by operations such as `select` on a [[Dataset]] to automatically convert the results
   * into the correct JVM types.
   * @since 3.4.0
   */
  def as[U: Encoder]: TypedColumn[Any, U] = {
    val encoder = implicitly[Encoder[U]].asInstanceOf[AgnosticEncoder[U]]
    new TypedColumn[Any, U](expr, encoder)
  }

  private[sql] def secret(): proto.Expression = expr

  /**
   * Extracts a value or values from a complex type. The following types of extraction are
   * supported:
   *   - Given an Array, an integer ordinal can be used to retrieve a single value.
   *   - Given a Map, a key of the correct type can be used to retrieve an individual value.
   *   - Given a Struct, a string fieldName can be used to extract that field.
   *   - Given an Array of Structs, a string fieldName can be used to extract filed of every
   *     struct in that array, and return an Array of fields.
   * @group expr_ops
   * @since 3.4.0
   */
  def apply(extraction: Any): Column = Column { builder =>
    builder.getUnresolvedExtractValueBuilder
      .setChild(expr)
      .setExtraction(lit(extraction).expr)
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
   * @since 3.4.0
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
   * @since 3.4.0
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
   * @since 3.4.0
   */
  def ===(other: Any): Column = fn("=", other)

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
   * @since 3.4.0
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
   * @since 3.4.0
   */
  def =!=(other: Any): Column = !(this === other)

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
   * @since 3.4.0
   */
  @deprecated("!== does not have the same precedence as ===, use =!= instead", "2.0.0")
  def !==(other: Any): Column = this =!= other

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
   * @since 3.4.0
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
   * @since 3.4.0
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
   * @since 3.4.0
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
   * @since 3.4.0
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
   * @since 3.4.0
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
   * @since 3.4.0
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
   * @since 3.4.0
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
   * @since 3.4.0
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
   * @since 3.4.0
   */
  def geq(other: Any): Column = this >= other

  /**
   * Equality test that is safe for null values.
   *
   * @group expr_ops
   * @since 3.4.0
   */
  def <=>(other: Any): Column = fn("<=>", other)

  /**
   * Equality test that is safe for null values.
   *
   * @group java_expr_ops
   * @since 3.4.0
   */
  def eqNullSafe(other: Any): Column = this <=> other

  private def extractWhen(name: String): java.util.List[proto.Expression] = {
    def fail(): Nothing = {
      throw new IllegalArgumentException(
        s"$name() can only be applied on a Column previously generated by when() function")
    }
    if (!expr.hasUnresolvedFunction) {
      fail()
    }
    val parentFn = expr.getUnresolvedFunction
    if (parentFn.getFunctionName != "when") {
      fail()
    }
    parentFn.getArgumentsList
  }

  /**
   * Evaluates a list of conditions and returns one of multiple possible result expressions. If
   * otherwise is not defined at the end, null is returned for unmatched conditions.
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
   * @since 3.4.0
   */
  def when(condition: Column, value: Any): Column = {
    val expressions = extractWhen("when")
    if (expressions.size() % 2 == 1) {
      throw new IllegalArgumentException("when() cannot be applied once otherwise() is applied")
    }
    Column { builder =>
      builder.getUnresolvedFunctionBuilder
        .setFunctionName("when")
        .addAllArguments(expressions)
        .addArguments(condition.expr)
        .addArguments(lit(value).expr)
    }
  }

  /**
   * Evaluates a list of conditions and returns one of multiple possible result expressions. If
   * otherwise is not defined at the end, null is returned for unmatched conditions.
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
   * @since 3.4.0
   */
  def otherwise(value: Any): Column = {
    val expressions = extractWhen("otherwise")
    if (expressions.size() % 2 == 1) {
      throw new IllegalArgumentException(
        "otherwise() can only be applied once on a Column previously generated by when()")
    }
    Column { builder =>
      builder.getUnresolvedFunctionBuilder
        .setFunctionName("when")
        .addAllArguments(expressions)
        .addArguments(lit(value).expr)
    }
  }

  /**
   * True if the current column is between the lower bound and upper bound, inclusive.
   *
   * @group java_expr_ops
   * @since 3.4.0
   */
  def between(lowerBound: Any, upperBound: Any): Column = {
    (this >= lowerBound) && (this <= upperBound)
  }

  /**
   * True if the current expression is NaN.
   *
   * @group expr_ops
   * @since 3.4.0
   */
  def isNaN: Column = fn("isNaN")

  /**
   * True if the current expression is null.
   *
   * @group expr_ops
   * @since 3.4.0
   */
  def isNull: Column = fn("isNull")

  /**
   * True if the current expression is NOT null.
   *
   * @group expr_ops
   * @since 3.4.0
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
   * @since 3.4.0
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
   * @since 3.4.0
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
   * @since 3.4.0
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
   * @since 3.4.0
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
   * @since 3.4.0
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
   * @since 3.4.0
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
   * @since 3.4.0
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
   * @since 3.4.0
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
   * @since 3.4.0
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
   * @since 3.4.0
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
   * @since 3.4.0
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
   * @since 3.4.0
   */
  def divide(other: Any): Column = this / other

  /**
   * Modulo (a.k.a. remainder) expression.
   *
   * @group expr_ops
   * @since 3.4.0
   */
  def %(other: Any): Column = fn("%", other)

  /**
   * Modulo (a.k.a. remainder) expression.
   *
   * @group java_expr_ops
   * @since 3.4.0
   */
  def mod(other: Any): Column = this % other

  /**
   * A boolean expression that is evaluated to true if the value of this expression is contained
   * by the evaluated values of the arguments.
   *
   * Note: Since the type of the elements in the list are inferred only during the run time, the
   * elements will be "up-casted" to the most common type for comparison. For eg: 1) In the case
   * of "Int vs String", the "Int" will be up-casted to "String" and the comparison will look like
   * "String vs String". 2) In the case of "Float vs Double", the "Float" will be up-casted to
   * "Double" and the comparison will look like "Double vs Double"
   *
   * @group expr_ops
   * @since 3.4.0
   */
  @scala.annotation.varargs
  def isin(list: Any*): Column = Column.fn("in", this +: list.map(lit): _*)

  /**
   * A boolean expression that is evaluated to true if the value of this expression is contained
   * by the provided collection.
   *
   * Note: Since the type of the elements in the collection are inferred only during the run time,
   * the elements will be "up-casted" to the most common type for comparison. For eg: 1) In the
   * case of "Int vs String", the "Int" will be up-casted to "String" and the comparison will look
   * like "String vs String". 2) In the case of "Float vs Double", the "Float" will be up-casted
   * to "Double" and the comparison will look like "Double vs Double"
   *
   * @group expr_ops
   * @since 3.4.0
   */
  def isInCollection(values: scala.collection.Iterable[_]): Column = isin(values.toSeq: _*)

  /**
   * A boolean expression that is evaluated to true if the value of this expression is contained
   * by the provided collection.
   *
   * Note: Since the type of the elements in the collection are inferred only during the run time,
   * the elements will be "up-casted" to the most common type for comparison. For eg: 1) In the
   * case of "Int vs String", the "Int" will be up-casted to "String" and the comparison will look
   * like "String vs String". 2) In the case of "Float vs Double", the "Float" will be up-casted
   * to "Double" and the comparison will look like "Double vs Double"
   *
   * @group java_expr_ops
   * @since 3.4.0
   */
  def isInCollection(values: java.lang.Iterable[_]): Column = isInCollection(values.asScala)

  /**
   * SQL like expression. Returns a boolean column based on a SQL LIKE match.
   *
   * @group expr_ops
   * @since 3.4.0
   */
  def like(literal: String): Column = fn("like", literal)

  /**
   * SQL RLIKE expression (LIKE with Regex). Returns a boolean column based on a regex match.
   *
   * @group expr_ops
   * @since 3.4.0
   */
  def rlike(literal: String): Column = fn("rlike", literal)

  /**
   * SQL ILIKE expression (case insensitive LIKE).
   *
   * @group expr_ops
   * @since 3.4.0
   */
  def ilike(literal: String): Column = fn("ilike", literal)

  /**
   * An expression that gets an item at position `ordinal` out of an array, or gets a value by key
   * `key` in a `MapType`.
   *
   * @group expr_ops
   * @since 3.4.0
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
   * However, if you are going to add/replace multiple nested fields, it is more optimal to
   * extract out the nested struct before adding/replacing multiple fields e.g.
   *
   * {{{
   *   val df = sql("SELECT named_struct('a', named_struct('a', 1, 'b', 2)) struct_col")
   *   df.select($"struct_col".withField("a", $"struct_col.a".withField("c", lit(3)).withField("d", lit(4))))
   *   // result: {"a":{"a":1,"b":2,"c":3,"d":4}}
   * }}}
   *
   * @group expr_ops
   * @since 3.4.0
   */
  // scalastyle:on line.size.limit
  def withField(fieldName: String, col: Column): Column = {
    require(fieldName != null, "fieldName cannot be null")
    require(col != null, "col cannot be null")
    Column { builder =>
      builder.getUpdateFieldsBuilder
        .setStructExpression(expr)
        .setFieldName(fieldName)
        .setValueExpression(col.expr)
    }
  }

  // scalastyle:off line.size.limit
  /**
   * An expression that drops fields in `StructType` by name. This is a no-op if schema doesn't
   * contain field name(s).
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
   * However, if you are going to drop multiple nested fields, it is more optimal to extract out
   * the nested struct before dropping multiple fields from it e.g.
   *
   * {{{
   *   val df = sql("SELECT named_struct('a', named_struct('a', 1, 'b', 2)) struct_col")
   *   df.select($"struct_col".withField("a", $"struct_col.a".dropFields("b", "c")))
   *   // result: {"a":{"a":1}}
   * }}}
   *
   * @group expr_ops
   * @since 3.4.0
   */
  // scalastyle:on line.size.limit
  def dropFields(fieldNames: String*): Column = {
    fieldNames.foldLeft(this) { case (column, fieldName) =>
      Column { builder =>
        builder.getUpdateFieldsBuilder
          .setStructExpression(column.expr)
          .setFieldName(fieldName)
      }
    }
  }

  /**
   * An expression that gets a field by name in a `StructType`.
   *
   * @group expr_ops
   * @since 3.4.0
   */
  def getField(fieldName: String): Column = apply(fieldName)

  /**
   * An expression that returns a substring.
   * @param startPos
   *   expression for the starting position.
   * @param len
   *   expression for the length of the substring.
   *
   * @group expr_ops
   * @since 3.4.0
   */
  def substr(startPos: Column, len: Column): Column = Column.fn("substr", this, startPos, len)

  /**
   * An expression that returns a substring.
   * @param startPos
   *   starting position.
   * @param len
   *   length of the substring.
   *
   * @group expr_ops
   * @since 3.4.0
   */
  def substr(startPos: Int, len: Int): Column = substr(lit(startPos), lit(len))

  /**
   * Contains the other element. Returns a boolean column based on a string match.
   *
   * @group expr_ops
   * @since 3.4.0
   */
  def contains(other: Any): Column = fn("contains", other)

  /**
   * String starts with. Returns a boolean column based on a string match.
   *
   * @group expr_ops
   * @since 3.4.0
   */
  def startsWith(other: Column): Column = fn("startswith", other)

  /**
   * String starts with another string literal. Returns a boolean column based on a string match.
   *
   * @group expr_ops
   * @since 3.4.0
   */
  def startsWith(literal: String): Column = startsWith(lit(literal))

  /**
   * String ends with. Returns a boolean column based on a string match.
   *
   * @group expr_ops
   * @since 3.4.0
   */
  def endsWith(other: Column): Column = fn("endswith", other)

  /**
   * String ends with another string literal. Returns a boolean column based on a string match.
   *
   * @group expr_ops
   * @since 3.4.0
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
   * @since 3.4.0
   */
  def alias(alias: String): Column = name(alias)

  /**
   * Gives the column an alias.
   * {{{
   *   // Renames colA to colB in select output.
   *   df.select($"colA".as("colB"))
   * }}}
   *
   * If the current column has metadata associated with it, this metadata will be propagated to
   * the new column. If this not desired, use the API `as(alias: String, metadata: Metadata)` with
   * explicit metadata.
   *
   * @group expr_ops
   * @since 3.4.0
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
   * @since 3.4.0
   */
  def as(aliases: Seq[String]): Column = Column { builder =>
    builder.getAliasBuilder.setExpr(expr).addAllName(aliases.asJava)
  }

  /**
   * Assigns the given aliases to the results of a table generating function.
   * {{{
   *   // Renames colA to colB in select output.
   *   df.select(explode($"myMap").as("key" :: "value" :: Nil))
   * }}}
   *
   * @group expr_ops
   * @since 3.4.0
   */
  def as(aliases: Array[String]): Column = as(aliases.toImmutableArraySeq)

  /**
   * Gives the column an alias.
   * {{{
   *   // Renames colA to colB in select output.
   *   df.select($"colA".as("colB"))
   * }}}
   *
   * If the current column has metadata associated with it, this metadata will be propagated to
   * the new column. If this not desired, use the API `as(alias: String, metadata: Metadata)` with
   * explicit metadata.
   *
   * @group expr_ops
   * @since 3.4.0
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
   * @since 3.4.0
   */
  def as(alias: String, metadata: Metadata): Column = Column { builder =>
    builder.getAliasBuilder
      .setExpr(expr)
      .addName(alias)
      .setMetadata(metadata.json)
  }

  /**
   * Gives the column a name (alias).
   * {{{
   *   // Renames colA to colB in select output.
   *   df.select($"colA".name("colB"))
   * }}}
   *
   * If the current column has metadata associated with it, this metadata will be propagated to
   * the new column. If this not desired, use the API `as(alias: String, metadata: Metadata)` with
   * explicit metadata.
   *
   * @group expr_ops
   * @since 3.4.0
   */
  def name(alias: String): Column = as(alias :: Nil)

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
   * @since 3.4.0
   */
  def cast(to: DataType): Column = Column { builder =>
    builder.getCastBuilder
      .setExpr(expr)
      .setType(DataTypeProtoConverter.toConnectProtoType(to))
  }

  /**
   * Casts the column to a different data type, using the canonical string representation of the
   * type. The supported types are: `string`, `boolean`, `byte`, `short`, `int`, `long`, `float`,
   * `double`, `decimal`, `date`, `timestamp`.
   * {{{
   *   // Casts colA to integer.
   *   df.select(df("colA").cast("int"))
   * }}}
   *
   * @group expr_ops
   * @since 3.4.0
   */
  def cast(to: String): Column = cast(DataTypeParser.parseDataType(to))

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
  def try_cast(to: DataType): Column = Column { builder =>
    builder.getCastBuilder
      .setExpr(expr)
      .setType(DataTypeProtoConverter.toConnectProtoType(to))
      .setEvalMode(proto.Expression.Cast.EvalMode.EVAL_MODE_TRY)
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
    try_cast(DataTypeParser.parseDataType(to))
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
   * @since 3.4.0
   */
  def desc: Column = desc_nulls_last

  /**
   * Returns a sort expression based on the descending order of the column, and null values appear
   * before non-null values.
   * {{{
   *   // Scala: sort a DataFrame by age column in descending order and null values appearing first.
   *   df.sort(df("age").desc_nulls_first)
   *
   *   // Java
   *   df.sort(df.col("age").desc_nulls_first());
   * }}}
   *
   * @group expr_ops
   * @since 3.4.0
   */
  def desc_nulls_first: Column =
    buildSortOrder(SortDirection.SORT_DIRECTION_DESCENDING, NullOrdering.SORT_NULLS_FIRST)

  /**
   * Returns a sort expression based on the descending order of the column, and null values appear
   * after non-null values.
   * {{{
   *   // Scala: sort a DataFrame by age column in descending order and null values appearing last.
   *   df.sort(df("age").desc_nulls_last)
   *
   *   // Java
   *   df.sort(df.col("age").desc_nulls_last());
   * }}}
   *
   * @group expr_ops
   * @since 3.4.0
   */
  def desc_nulls_last: Column =
    buildSortOrder(SortDirection.SORT_DIRECTION_DESCENDING, NullOrdering.SORT_NULLS_LAST)

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
   * @since 3.4.0
   */
  def asc: Column = asc_nulls_first

  /**
   * Returns a sort expression based on ascending order of the column, and null values return
   * before non-null values.
   * {{{
   *   // Scala: sort a DataFrame by age column in ascending order and null values appearing first.
   *   df.sort(df("age").asc_nulls_first)
   *
   *   // Java
   *   df.sort(df.col("age").asc_nulls_first());
   * }}}
   *
   * @group expr_ops
   * @since 3.4.0
   */
  def asc_nulls_first: Column =
    buildSortOrder(SortDirection.SORT_DIRECTION_ASCENDING, NullOrdering.SORT_NULLS_FIRST)

  /**
   * Returns a sort expression based on ascending order of the column, and null values appear
   * after non-null values.
   * {{{
   *   // Scala: sort a DataFrame by age column in ascending order and null values appearing last.
   *   df.sort(df("age").asc_nulls_last)
   *
   *   // Java
   *   df.sort(df.col("age").asc_nulls_last());
   * }}}
   *
   * @group expr_ops
   * @since 3.4.0
   */
  def asc_nulls_last: Column =
    buildSortOrder(SortDirection.SORT_DIRECTION_ASCENDING, NullOrdering.SORT_NULLS_LAST)

  private def buildSortOrder(sortDirection: SortDirection, nullOrdering: NullOrdering): Column = {
    Column { builder =>
      builder.getSortOrderBuilder
        .setChild(expr)
        .setDirection(sortDirection)
        .setNullOrdering(nullOrdering)
    }
  }

  private[sql] def sortOrder: proto.Expression.SortOrder = {
    val base = if (expr.hasSortOrder) {
      expr
    } else {
      asc.expr
    }
    base.getSortOrder
  }

  /**
   * Prints the expression to the console for debugging purposes.
   *
   * @group df_ops
   * @since 3.4.0
   */
  def explain(extended: Boolean): Unit = {
    // scalastyle:off println
    if (extended) {
      println(expr)
    } else {
      println(toString)
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
   * @since 3.4.0
   */
  def bitwiseOR(other: Any): Column = fn("|", other)

  /**
   * Compute bitwise AND of this expression with another expression.
   * {{{
   *   df.select($"colA".bitwiseAND($"colB"))
   * }}}
   *
   * @group expr_ops
   * @since 3.4.0
   */
  def bitwiseAND(other: Any): Column = fn("&", other)

  /**
   * Compute bitwise XOR of this expression with another expression.
   * {{{
   *   df.select($"colA".bitwiseXOR($"colB"))
   * }}}
   *
   * @group expr_ops
   * @since 3.4.0
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
   * @since 3.4.0
   */
  def over(window: expressions.WindowSpec): Column = window.withAggregate(this)

  /**
   * Defines an empty analytic clause. In this case the analytic function is applied and presented
   * for all rows in the result set.
   *
   * {{{
   *   df.select(
   *     sum("price").over(),
   *     avg("price").over()
   *   )
   * }}}
   *
   * @group expr_ops
   * @since 3.4.0
   */
  def over(): Column = over(Window.spec)
}

object Column {

  private[sql] def apply(name: String): Column = new Column(name)

  private[sql] def apply(name: String, planId: Option[Long]): Column = new Column(name, planId)

  private[sql] def nameToExpression(
      name: String,
      planId: Option[Long] = None): proto.Expression = {
    val builder = proto.Expression.newBuilder()
    name match {
      case "*" =>
        val starBuilder = builder.getUnresolvedStarBuilder
        planId.foreach(starBuilder.setPlanId)
      case _ if name.endsWith(".*") =>
        builder.getUnresolvedStarBuilder.setUnparsedTarget(name)
      case _ =>
        val attributeBuilder = builder.getUnresolvedAttributeBuilder.setUnparsedIdentifier(name)
        planId.foreach(attributeBuilder.setPlanId)
    }
    builder.build()
  }

  @Since("4.0.0")
  @DeveloperApi
  def apply(f: proto.Expression.Builder => Unit): Column = {
    val builder = proto.Expression.newBuilder()
    f(builder)
    new Column(builder.build())
  }

  private[sql] def fn(name: String, inputs: Column*): Column = {
    fn(name, isDistinct = false, inputs: _*)
  }

  private[sql] def fn(name: String, isDistinct: Boolean, inputs: Column*): Column = Column {
    builder =>
      builder.getUnresolvedFunctionBuilder
        .setFunctionName(name)
        .setIsDistinct(isDistinct)
        .addAllArguments(inputs.map(_.expr).asJava)
  }
}

/**
 * A convenient class used for constructing schema.
 *
 * @since 3.4.0
 */
class ColumnName(name: String) extends Column(name) {

  /**
   * Creates a new `StructField` of type boolean.
   * @since 3.4.0
   */
  def boolean: StructField = StructField(name, BooleanType)

  /**
   * Creates a new `StructField` of type byte.
   * @since 3.4.0
   */
  def byte: StructField = StructField(name, ByteType)

  /**
   * Creates a new `StructField` of type short.
   * @since 3.4.0
   */
  def short: StructField = StructField(name, ShortType)

  /**
   * Creates a new `StructField` of type int.
   * @since 3.4.0
   */
  def int: StructField = StructField(name, IntegerType)

  /**
   * Creates a new `StructField` of type long.
   * @since 3.4.0
   */
  def long: StructField = StructField(name, LongType)

  /**
   * Creates a new `StructField` of type float.
   * @since 3.4.0
   */
  def float: StructField = StructField(name, FloatType)

  /**
   * Creates a new `StructField` of type double.
   * @since 3.4.0
   */
  def double: StructField = StructField(name, DoubleType)

  /**
   * Creates a new `StructField` of type string.
   * @since 3.4.0
   */
  def string: StructField = StructField(name, StringType)

  /**
   * Creates a new `StructField` of type date.
   * @since 3.4.0
   */
  def date: StructField = StructField(name, DateType)

  /**
   * Creates a new `StructField` of type decimal.
   * @since 3.4.0
   */
  def decimal: StructField = StructField(name, DecimalType.USER_DEFAULT)

  /**
   * Creates a new `StructField` of type decimal.
   * @since 3.4.0
   */
  def decimal(precision: Int, scale: Int): StructField =
    StructField(name, DecimalType(precision, scale))

  /**
   * Creates a new `StructField` of type timestamp.
   * @since 3.4.0
   */
  def timestamp: StructField = StructField(name, TimestampType)

  /**
   * Creates a new `StructField` of type binary.
   * @since 3.4.0
   */
  def binary: StructField = StructField(name, BinaryType)

  /**
   * Creates a new `StructField` of type array.
   * @since 3.4.0
   */
  def array(dataType: DataType): StructField = StructField(name, ArrayType(dataType))

  /**
   * Creates a new `StructField` of type map.
   * @since 3.4.0
   */
  def map(keyType: DataType, valueType: DataType): StructField =
    map(MapType(keyType, valueType))

  /**
   * Creates a new `StructField` of type map.
   * @since 3.4.0
   */
  def map(mapType: MapType): StructField = StructField(name, mapType)

  /**
   * Creates a new `StructField` of type struct.
   * @since 3.4.0
   */
  def struct(fields: StructField*): StructField = struct(StructType(fields))

  /**
   * Creates a new `StructField` of type struct.
   * @since 3.4.0
   */
  def struct(structType: StructType): StructField = StructField(name, structType)
}

/**
 * A [[Column]] where an [[Encoder]] has been given for the expected input and return type. To
 * create a [[TypedColumn]], use the `as` function on a [[Column]].
 *
 * @tparam T
 *   The input type expected for this expression. Can be `Any` if the expression is type checked
 *   by the analyzer instead of the compiler (i.e. `expr("sum(...)")`).
 * @tparam U
 *   The output type of this column.
 *
 * @since 3.4.0
 */
class TypedColumn[-T, U] private[sql] (
    expr: proto.Expression,
    private[sql] val encoder: AgnosticEncoder[U])
    extends Column(expr) {

  /**
   * Gives the [[TypedColumn]] a name (alias). If the current `TypedColumn` has metadata
   * associated with it, this metadata will be propagated to the new column.
   *
   * @group expr_ops
   * @since 3.4.0
   */
  override def name(alias: String): TypedColumn[T, U] =
    new TypedColumn[T, U](super.name(alias).expr, encoder)
}
