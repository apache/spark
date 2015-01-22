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

import org.apache.spark.sql.catalyst.analysis
import org.apache.spark.sql.catalyst.analysis.Star

import scala.language.implicitConversions

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.{Literal => LiteralExpr}
import org.apache.spark.sql.catalyst.plans.logical.{Project, LogicalPlan}
import org.apache.spark.sql.types._


object Literal {
  def apply(literal: Boolean): Column = new Column(LiteralExpr(literal))
  def apply(literal: Byte): Column = new Column(LiteralExpr(literal))
  def apply(literal: Short): Column = new Column(LiteralExpr(literal))
  def apply(literal: Int): Column = new Column(LiteralExpr(literal))
  def apply(literal: Long): Column = new Column(LiteralExpr(literal))
  def apply(literal: Float): Column = new Column(LiteralExpr(literal))
  def apply(literal: Double): Column = new Column(LiteralExpr(literal))
  def apply(literal: String): Column = new Column(LiteralExpr(literal))
  def apply(literal: BigDecimal): Column = new Column(LiteralExpr(literal))
  def apply(literal: java.math.BigDecimal): Column = new Column(LiteralExpr(literal))
  def apply(literal: java.sql.Timestamp): Column = new Column(LiteralExpr(literal))
  def apply(literal: java.sql.Date): Column = new Column(LiteralExpr(literal))
  def apply(literal: Array[Byte]): Column = new Column(LiteralExpr(literal))
  def apply(literal: Null): Column = new Column(LiteralExpr(null))
}


object Column {
  def unapply(col: Column): Option[Expression] = Some(col.expr)
}


class Column(
    sqlContext: Option[SQLContext],
    plan: Option[LogicalPlan],
    val expr: Expression)
  extends DataFrame(sqlContext, plan) with ExpressionApi[Column] {

  def this(expr: Expression) = this(None, None, expr)

  def this(name: String) = this(name match {
    case "*" => Star(None)
    case _ if name.endsWith(".*") => Star(Some(name.substring(0, name.length - 2)))
    case _ => analysis.UnresolvedAttribute(name)
  })

  private[this] implicit def toColumn(expr: Expression): Column = {
    val projectedPlan = plan.map { p =>
      Project(Seq(expr match {
        case named: NamedExpression => named
        case unnamed: Expression => Alias(unnamed, "col")()
      }), p)
    }
    new Column(sqlContext, projectedPlan, expr)
  }

  override def unary_- : Column = UnaryMinus(expr)

  override def ||(other: Column): Column = Or(expr, other.expr)

  override def unary_~ : Column = BitwiseNot(expr)

  override def !==(other: Column): Column = Not(EqualTo(expr, other.expr))

  override def >(other: Column): Column = GreaterThan(expr, other.expr)

  override def unary_! : Column = Not(expr)

  override def &(other: Column): Column = BitwiseAnd(expr, other.expr)

  override def /(other: Column): Column = Divide(expr, other.expr)

  override def &&(other: Column): Column = And(expr, other.expr)

  override def |(other: Column): Column = BitwiseOr(expr, other.expr)

  override def ^(other: Column): Column = BitwiseXor(expr, other.expr)

  override def <=>(other: Column): Column = EqualNullSafe(expr, other.expr)

  override def ===(other: Column): Column = EqualTo(expr, other.expr)

  override def equalTo(other: Column): Column = this === other

  override def +(other: Column): Column = Add(expr, other.expr)

  override def rlike(other: Column): Column = RLike(expr, other.expr)

  override def %(other: Column): Column = Remainder(expr, other.expr)

  override def in(list: Column*): Column = In(expr, list.map(_.expr))

  override def getItem(ordinal: Int): Column = GetItem(expr, LiteralExpr(ordinal))

  override def getItem(ordinal: Column): Column = GetItem(expr, ordinal.expr)

  override def <=(other: Column): Column = LessThanOrEqual(expr, other.expr)

  override def like(other: Column): Column = Like(expr, other.expr)

  override def getField(fieldName: String): Column = GetField(expr, fieldName)

  override def isNotNull: Column = IsNotNull(expr)

  override def substr(startPos: Column, len: Column): Column =
    Substring(expr, startPos.expr, len.expr)

  override def <(other: Column): Column = LessThan(expr, other.expr)

  override def isNull: Column = IsNull(expr)

  override def contains(other: Column): Column = Contains(expr, other.expr)

  override def -(other: Column): Column = Subtract(expr, other.expr)

  override def desc: Column = SortOrder(expr, Descending)

  override def >=(other: Column): Column = GreaterThanOrEqual(expr, other.expr)

  override def asc: Column = SortOrder(expr, Ascending)

  override def endsWith(other: Column): Column = EndsWith(expr, other.expr)

  override def *(other: Column): Column = Multiply(expr, other.expr)

  override def startsWith(other: Column): Column = StartsWith(expr, other.expr)

  override def as(alias: String): Column = Alias(expr, alias)()

  override def cast(to: DataType): Column = Cast(expr, to)
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
