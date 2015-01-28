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

import org.apache.spark.sql.catalyst.expressions.{Literal => LiteralExpr}
import org.apache.spark.sql.types._

object Literal {

  /** Return a new boolean literal. */
  def apply(literal: Boolean): Column = new Column(LiteralExpr(literal))

  /** Return a new byte literal. */
  def apply(literal: Byte): Column = new Column(LiteralExpr(literal))

  /** Return a new short literal. */
  def apply(literal: Short): Column = new Column(LiteralExpr(literal))

  /** Return a new int literal. */
  def apply(literal: Int): Column = new Column(LiteralExpr(literal))

  /** Return a new long literal. */
  def apply(literal: Long): Column = new Column(LiteralExpr(literal))

  /** Return a new float literal. */
  def apply(literal: Float): Column = new Column(LiteralExpr(literal))

  /** Return a new double literal. */
  def apply(literal: Double): Column = new Column(LiteralExpr(literal))

  /** Return a new string literal. */
  def apply(literal: String): Column = new Column(LiteralExpr(literal))

  /** Return a new decimal literal. */
  def apply(literal: BigDecimal): Column = new Column(LiteralExpr(literal))

  /** Return a new decimal literal. */
  def apply(literal: java.math.BigDecimal): Column = new Column(LiteralExpr(literal))

  /** Return a new timestamp literal. */
  def apply(literal: java.sql.Timestamp): Column = new Column(LiteralExpr(literal))

  /** Return a new date literal. */
  def apply(literal: java.sql.Date): Column = new Column(LiteralExpr(literal))

  /** Return a new binary (byte array) literal. */
  def apply(literal: Array[Byte]): Column = new Column(LiteralExpr(literal))

  /** Return a new null literal. */
  def apply(literal: Null): Column = new Column(LiteralExpr(null))

  /**
   * Return a Column expression representing the literal value. Throws an exception if the
   * data type is not supported by SparkSQL.
   */
  protected[sql] def anyToLiteral(literal: Any): Column = {
    // If the literal is a symbol, convert it into a Column.
    if (literal.isInstanceOf[Symbol]) {
      return dsl.symbolToColumn(literal.asInstanceOf[Symbol])
    }

    val literalExpr = literal match {
      case v: Int => LiteralExpr(v, IntegerType)
      case v: Long => LiteralExpr(v, LongType)
      case v: Double => LiteralExpr(v, DoubleType)
      case v: Float => LiteralExpr(v, FloatType)
      case v: Byte => LiteralExpr(v, ByteType)
      case v: Short => LiteralExpr(v, ShortType)
      case v: String => LiteralExpr(v, StringType)
      case v: Boolean => LiteralExpr(v, BooleanType)
      case v: BigDecimal => LiteralExpr(Decimal(v), DecimalType.Unlimited)
      case v: java.math.BigDecimal => LiteralExpr(Decimal(v), DecimalType.Unlimited)
      case v: Decimal => LiteralExpr(v, DecimalType.Unlimited)
      case v: java.sql.Timestamp => LiteralExpr(v, TimestampType)
      case v: java.sql.Date => LiteralExpr(v, DateType)
      case v: Array[Byte] => LiteralExpr(v, BinaryType)
      case null => LiteralExpr(null, NullType)
      case _ =>
        throw new RuntimeException("Unsupported literal type " + literal.getClass + " " + literal)
    }
    new Column(literalExpr)
  }
}
