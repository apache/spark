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

  private def computableCol(baseCol: ComputableColumn, expr: Expression) = {
    val plan = Project(Seq(expr match {
      case named: NamedExpression => named
      case unnamed: Expression => Alias(unnamed, "col")()
    }), baseCol.plan)
    Column(baseCol.sqlContext, plan, expr)
  }

  private def constructColumn(otherValue: Any)(newExpr: Column => Expression): Column = {
    // Removes all the top level projection and subquery so we can get to the underlying plan.
    @tailrec def stripProject(p: LogicalPlan): LogicalPlan = p match {
      case Project(_, child) => stripProject(child)
      case Subquery(_, child) => stripProject(child)
      case _ => p
    }

    (this, lit(otherValue)) match {
      case (left: ComputableColumn, right: ComputableColumn) =>
        if (stripProject(left.plan).sameResult(stripProject(right.plan))) {
          computableCol(right, newExpr(right))
        } else {
          Column(newExpr(right))
        }
      case (left: ComputableColumn, right) => computableCol(left, newExpr(right))
      case (_, right: ComputableColumn) => computableCol(right, newExpr(right))
      case (_, right) => Column(newExpr(right))
    }
  }

  /** Creates a column based on the given expression. */
  private def exprToColumn(newExpr: Expression, computable: Boolean = true): Column = {
    this match {
      case c: ComputableColumn if computable => computableCol(c, newExpr)
      case _ => Column(newExpr)
    }
  }

  /**
   * Unary minus, i.e. negate the expression.
   * {{{
   *   // Scala: select the amount column and negates all values.
   *   df.select( -df("amount") )
   *
   *   // Java:
   *   import static org.apache.spark.sql.Dsl.*;
   *   df.select( negate(col("amount") );
   * }}}
   */
  def unary_- : Column = exprToColumn(UnaryMinus(expr))

  /**
   * Inversion of boolean expression, i.e. NOT.
   * {{
   *   // Scala: select rows that are not active (isActive === false)
   *   df.filter( !df("isActive") )
   *
   *   // Java:
   *   import static org.apache.spark.sql.Dsl.*;
   *   df.filter( not(df.col("isActive")) );
   * }}
   */
  def unary_! : Column = exprToColumn(Not(expr))


  /**
   * Equality test.
   * {{{
   *   // Scala:
   *   df.filter( df("colA") === df("colB") )
   *
   *   // Java
   *   import static org.apache.spark.sql.Dsl.*;
   *   df.filter( col("colA").equalTo(col("colB")) );
   * }}}
   */
  def === (other: Any): Column = constructColumn(other) { o =>
    EqualTo(expr, o.expr)
  }

  /**
   * Equality test.
   * {{{
   *   // Scala:
   *   df.filter( df("colA") === df("colB") )
   *
   *   // Java
   *   import static org.apache.spark.sql.Dsl.*;
   *   df.filter( col("colA").equalTo(col("colB")) );
   * }}}
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
   *   import static org.apache.spark.sql.Dsl.*;
   *   df.filter( not(col("colA").equalTo(col("colB"))) );
   * }}}
   */
  def !== (other: Any): Column = constructColumn(other) { o =>
    Not(EqualTo(expr, o.expr))
  }

  /**
   * Greater than.
   * {{{
   *   // Scala: The following selects people older than 21.
   *   people.select( people("age") > 21 )
   *
   *   // Java:
   *   import static org.apache.spark.sql.Dsl.*;
   *   people.select( people("age").gt(21) );
   * }}}
   */
  def > (other: Any): Column = constructColumn(other) { o =>
    GreaterThan(expr, o.expr)
  }

  /**
   * Greater than.
   * {{{
   *   // Scala: The following selects people older than 21.
   *   people.select( people("age") > lit(21) )
   *
   *   // Java:
   *   import static org.apache.spark.sql.Dsl.*;
   *   people.select( people("age").gt(21) );
   * }}}
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
   */
  def < (other: Any): Column = constructColumn(other) { o =>
    LessThan(expr, o.expr)
  }

  /**
   * Less than.
   * {{{
   *   // Scala: The following selects people younger than 21.
   *   people.select( people("age") < 21 )
   *
   *   // Java:
   *   people.select( people("age").lt(21) );
   * }}}
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
   */
  def <= (other: Any): Column = constructColumn(other) { o =>
    LessThanOrEqual(expr, o.expr)
  }

  /**
   * Less than or equal to.
   * {{{
   *   // Scala: The following selects people age 21 or younger than 21.
   *   people.select( people("age") <= 21 )
   *
   *   // Java:
   *   people.select( people("age").leq(21) );
   * }}}
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
   */
  def >= (other: Any): Column = constructColumn(other) { o =>
    GreaterThanOrEqual(expr, o.expr)
  }

  /**
   * Greater than or equal to an expression.
   * {{{
   *   // Scala: The following selects people age 21 or older than 21.
   *   people.select( people("age") >= 21 )
   *
   *   // Java:
   *   people.select( people("age").geq(21) )
   * }}}
   */
  def geq(other: Any): Column = this >= other

  /**
   * Equality test that is safe for null values.
   */
  def <=> (other: Any): Column = constructColumn(other) { o =>
    EqualNullSafe(expr, o.expr)
  }

  /**
   * Equality test that is safe for null values.
   */
  def eqNullSafe(other: Any): Column = this <=> other

  /**
   * True if the current expression is null.
   */
  def isNull: Column = exprToColumn(IsNull(expr))

  /**
   * True if the current expression is NOT null.
   */
  def isNotNull: Column = exprToColumn(IsNotNull(expr))

  /**
   * Boolean OR.
   * {{{
   *   // Scala: The following selects people that are in school or employed.
   *   people.filter( people("inSchool") || people("isEmployed") )
   *
   *   // Java:
   *   people.filter( people("inSchool").or(people("isEmployed")) );
   * }}}
   */
  def || (other: Any): Column = constructColumn(other) { o =>
    Or(expr, o.expr)
  }

  /**
   * Boolean OR.
   * {{{
   *   // Scala: The following selects people that are in school or employed.
   *   people.filter( people("inSchool") || people("isEmployed") )
   *
   *   // Java:
   *   people.filter( people("inSchool").or(people("isEmployed")) );
   * }}}
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
   */
  def && (other: Any): Column = constructColumn(other) { o =>
    And(expr, o.expr)
  }

  /**
   * Boolean AND.
   * {{{
   *   // Scala: The following selects people that are in school and employed at the same time.
   *   people.select( people("inSchool") && people("isEmployed") )
   *
   *   // Java:
   *   people.select( people("inSchool").and(people("isEmployed")) );
   * }}}
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
   */
  def + (other: Any): Column = constructColumn(other) { o =>
    Add(expr, o.expr)
  }

  /**
   * Sum of this expression and another expression.
   * {{{
   *   // Scala: The following selects the sum of a person's height and weight.
   *   people.select( people("height") + people("weight") )
   *
   *   // Java:
   *   people.select( people("height").plus(people("weight")) );
   * }}}
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
   */
  def - (other: Any): Column = constructColumn(other) { o =>
    Subtract(expr, o.expr)
  }

  /**
   * Subtraction. Subtract the other expression from this expression.
   * {{{
   *   // Scala: The following selects the difference between people's height and their weight.
   *   people.select( people("height") - people("weight") )
   *
   *   // Java:
   *   people.select( people("height").minus(people("weight")) );
   * }}}
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
   */
  def * (other: Any): Column = constructColumn(other) { o =>
    Multiply(expr, o.expr)
  }

  /**
   * Multiplication of this expression and another expression.
   * {{{
   *   // Scala: The following multiplies a person's height by their weight.
   *   people.select( people("height") * people("weight") )
   *
   *   // Java:
   *   people.select( people("height").multiply(people("weight")) );
   * }}}
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
   */
  def / (other: Any): Column = constructColumn(other) { o =>
    Divide(expr, o.expr)
  }

  /**
   * Division this expression by another expression.
   * {{{
   *   // Scala: The following divides a person's height by their weight.
   *   people.select( people("height") / people("weight") )
   *
   *   // Java:
   *   people.select( people("height").divide(people("weight")) );
   * }}}
   */
  def divide(other: Any): Column = this / other

  /**
   * Modulo (a.k.a. remainder) expression.
   */
  def % (other: Any): Column = constructColumn(other) { o =>
    Remainder(expr, o.expr)
  }

  /**
   * Modulo (a.k.a. remainder) expression.
   */
  def mod(other: Any): Column = this % other

  /**
   * A boolean expression that is evaluated to true if the value of this expression is contained
   * by the evaluated values of the arguments.
   */
  @scala.annotation.varargs
  def in(list: Column*): Column = {
    new IncomputableColumn(In(expr, list.map(_.expr)))
  }

  def like(literal: String): Column = exprToColumn(Like(expr, lit(literal).expr))

  def rlike(literal: String): Column = exprToColumn(RLike(expr, lit(literal).expr))

  /**
   * An expression that gets an item at position `ordinal` out of an array.
   */
  def getItem(ordinal: Int): Column = exprToColumn(GetItem(expr, Literal(ordinal)))

  /**
   * An expression that gets a field by name in a [[StructField]].
   */
  def getField(fieldName: String): Column = exprToColumn(GetField(expr, fieldName))

  /**
   * An expression that returns a substring.
   * @param startPos expression for the starting position.
   * @param len expression for the length of the substring.
   */
  def substr(startPos: Column, len: Column): Column =
    exprToColumn(Substring(expr, startPos.expr, len.expr), computable = false)

  /**
   * An expression that returns a substring.
   * @param startPos starting position.
   * @param len length of the substring.
   */
  def substr(startPos: Int, len: Int): Column =
    exprToColumn(Substring(expr, lit(startPos).expr, lit(len).expr))

  def contains(other: Any): Column = constructColumn(other) { o =>
    Contains(expr, o.expr)
  }

  def startsWith(other: Column): Column = constructColumn(other) { o =>
    StartsWith(expr, o.expr)
  }

  def startsWith(literal: String): Column = this.startsWith(lit(literal))

  def endsWith(other: Column): Column = constructColumn(other) { o =>
    EndsWith(expr, o.expr)
  }

  def endsWith(literal: String): Column = this.endsWith(lit(literal))

  /**
   * Gives the column an alias.
   * {{{
   *   // Renames colA to colB in select output.
   *   df.select($"colA".as("colB"))
   * }}}
   */
  override def as(alias: String): Column = exprToColumn(Alias(expr, alias)())

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
  def cast(to: DataType): Column = exprToColumn(Cast(expr, to))

  /**
   * Casts the column to a different data type, using the canonical string representation
   * of the type. The supported types are: `string`, `boolean`, `byte`, `short`, `int`, `long`,
   * `float`, `double`, `decimal`, `date`, `timestamp`.
   * {{{
   *   // Casts colA to integer.
   *   df.select(df("colA").cast("int"))
   * }}}
   */
  def cast(to: String): Column = exprToColumn(
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
  )

  def desc: Column = exprToColumn(SortOrder(expr, Descending), computable = false)

  def asc: Column = exprToColumn(SortOrder(expr, Ascending), computable = false)
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
