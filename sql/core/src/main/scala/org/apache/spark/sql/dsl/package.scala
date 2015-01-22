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

import java.sql.{Timestamp, Date}

import scala.language.implicitConversions

import org.apache.spark.sql.catalyst.expressions._


package object dsl {

  implicit def symbolToColumn(s: Symbol): ColumnName = new ColumnName(s.name)

  /** Converts $"col name" into an [[Column]]. */
  implicit class StringToColumn(val sc: StringContext) extends AnyVal {
    def $(args: Any*): ColumnName = {
      new ColumnName(sc.s(args :_*))
    }
  }

  private[this] implicit def toColumn(expr: Expression): Column = new Column(expr)

  def sum(e: Column): Column = Sum(e.expr)
  def sumDistinct(e: Column): Column = SumDistinct(e.expr)
  def count(e: Column): Column = Count(e.expr)

  @scala.annotation.varargs
  def countDistinct(expr: Column, exprs: Column*): Column =
    CountDistinct((expr +: exprs).map(_.expr))

  def avg(e: Column): Column = Average(e.expr)
  def first(e: Column): Column = First(e.expr)
  def last(e: Column): Column = Last(e.expr)
  def min(e: Column): Column = Min(e.expr)
  def max(e: Column): Column = Max(e.expr)
  def upper(e: Column): Column = Upper(e.expr)
  def lower(e: Column): Column = Lower(e.expr)
  def sqrt(e: Column): Column = Sqrt(e.expr)
  def abs(e: Column): Column = Abs(e.expr)


  object literals {

    implicit def booleanToLiteral(b: Boolean): Column = Literal(b)

    implicit def byteToLiteral(b: Byte): Column = Literal(b)

    implicit def shortToLiteral(s: Short): Column = Literal(s)

    implicit def intToLiteral(i: Int): Column = Literal(i)

    implicit def longToLiteral(l: Long): Column = Literal(l)

    implicit def floatToLiteral(f: Float): Column = Literal(f)

    implicit def doubleToLiteral(d: Double): Column = Literal(d)

    implicit def stringToLiteral(s: String): Column = Literal(s)

    implicit def dateToLiteral(d: Date): Column = Literal(d)

    implicit def bigDecimalToLiteral(d: BigDecimal): Column = Literal(d.underlying())

    implicit def bigDecimalToLiteral(d: java.math.BigDecimal): Column = Literal(d)

    implicit def timestampToLiteral(t: Timestamp): Column = Literal(t)

    implicit def binaryToLiteral(a: Array[Byte]): Column = Literal(a)
  }
}
