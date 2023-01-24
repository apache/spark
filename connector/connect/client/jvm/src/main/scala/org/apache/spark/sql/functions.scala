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

import java.math.{BigDecimal => JBigDecimal}
import java.time.LocalDate

import com.google.protobuf.ByteString

import org.apache.spark.connect.proto
import org.apache.spark.sql.connect.client.unsupported

/**
 * Commonly used functions available for DataFrame operations.
 *
 * @since 3.4.0
 */
// scalastyle:off
object functions {
// scalastyle:on

  private def createLiteral(f: proto.Expression.Literal.Builder => Unit): Column = Column {
    builder =>
      val literalBuilder = proto.Expression.Literal.newBuilder()
      f(literalBuilder)
      builder.setLiteral(literalBuilder)
  }

  private def createDecimalLiteral(precision: Int, scale: Int, value: String): Column =
    createLiteral { builder =>
      builder.getDecimalBuilder
        .setPrecision(precision)
        .setScale(scale)
        .setValue(value)
    }

  /**
   * Creates a [[Column]] of literal value.
   *
   * The passed in object is returned directly if it is already a [[Column]]. If the object is a
   * Scala Symbol, it is converted into a [[Column]] also. Otherwise, a new [[Column]] is created
   * to represent the literal value.
   *
   * @since 3.4.0
   */
  def lit(literal: Any): Column = {
    literal match {
      case c: Column => c
      case s: Symbol => Column(s.name)
      case v: Boolean => createLiteral(_.setBoolean(v))
      case v: Byte => createLiteral(_.setByte(v))
      case v: Short => createLiteral(_.setShort(v))
      case v: Int => createLiteral(_.setInteger(v))
      case v: Long => createLiteral(_.setLong(v))
      case v: Float => createLiteral(_.setFloat(v))
      case v: Double => createLiteral(_.setDouble(v))
      case v: BigDecimal => createDecimalLiteral(v.precision, v.scale, v.toString)
      case v: JBigDecimal => createDecimalLiteral(v.precision, v.scale, v.toString)
      case v: String => createLiteral(_.setString(v))
      case v: Char => createLiteral(_.setString(v.toString))
      case v: Array[Char] => createLiteral(_.setString(String.valueOf(v)))
      case v: Array[Byte] => createLiteral(_.setBinary(ByteString.copyFrom(v)))
      case v: collection.mutable.WrappedArray[_] => lit(v.array)
      case v: LocalDate => createLiteral(_.setDate(v.toEpochDay.toInt))
      case null => unsupported("Null literals not supported yet.")
      case _ => unsupported(s"literal $literal not supported (yet).")
    }
  }
}
