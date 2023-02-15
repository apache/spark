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

import scala.reflect.runtime.universe.{typeTag, TypeTag}

import com.google.protobuf.ByteString

import org.apache.spark.connect.proto
import org.apache.spark.sql.connect.client.unsupported
import org.apache.spark.sql.expressions.{ScalarUserDefinedFunction, UserDefinedFunction}

/**
 * Commonly used functions available for DataFrame operations.
 *
 * @since 3.4.0
 */
// scalastyle:off
object functions {
// scalastyle:on

  /**
   * Returns a [[Column]] based on the given column name.
   *
   * @group normal_funcs
   * @since 3.4.0
   */
  def col(colName: String): Column = Column(colName)

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

  /**
   * Parses the expression string into the column that it represents, similar to
   * [[Dataset#selectExpr]].
   * {{{
   *   // get the number of words of each length
   *   df.groupBy(expr("length(word)")).count()
   * }}}
   *
   * @group normal_funcs
   */
  def expr(expr: String): Column = Column { builder =>
    builder.getExpressionStringBuilder.setExpression(expr)
  }

  // scalastyle:off line.size.limit

  /**
   * Defines a Scala closure of 0 arguments as user-defined function (UDF). The data types are
   * automatically inferred based on the Scala closure's signature. By default the returned UDF is
   * deterministic. To change it to nondeterministic, call the API
   * `UserDefinedFunction.asNondeterministic()`.
   *
   * @group udf_funcs
   * @since 3.4.0
   */
  def udf[RT: TypeTag](f: () => RT): UserDefinedFunction = {
    ScalarUserDefinedFunction(f, typeTag[RT])
  }

  /**
   * Defines a Scala closure of 1 arguments as user-defined function (UDF). The data types are
   * automatically inferred based on the Scala closure's signature. By default the returned UDF is
   * deterministic. To change it to nondeterministic, call the API
   * `UserDefinedFunction.asNondeterministic()`.
   *
   * @group udf_funcs
   * @since 3.4.0
   */
  def udf[RT: TypeTag, A1: TypeTag](f: A1 => RT): UserDefinedFunction = {
    ScalarUserDefinedFunction(f, typeTag[RT], typeTag[A1])
  }

  /**
   * Defines a Scala closure of 2 arguments as user-defined function (UDF). The data types are
   * automatically inferred based on the Scala closure's signature. By default the returned UDF is
   * deterministic. To change it to nondeterministic, call the API
   * `UserDefinedFunction.asNondeterministic()`.
   *
   * @group udf_funcs
   * @since 3.4.0
   */
  def udf[RT: TypeTag, A1: TypeTag, A2: TypeTag](f: (A1, A2) => RT): UserDefinedFunction = {
    ScalarUserDefinedFunction(f, typeTag[RT], typeTag[A1], typeTag[A2])
  }

  /**
   * Defines a Scala closure of 3 arguments as user-defined function (UDF). The data types are
   * automatically inferred based on the Scala closure's signature. By default the returned UDF is
   * deterministic. To change it to nondeterministic, call the API
   * `UserDefinedFunction.asNondeterministic()`.
   *
   * @group udf_funcs
   * @since 3.4.0
   */
  def udf[RT: TypeTag, A1: TypeTag, A2: TypeTag, A3: TypeTag](
      f: (A1, A2, A3) => RT): UserDefinedFunction = {
    ScalarUserDefinedFunction(f, typeTag[RT], typeTag[A1], typeTag[A2], typeTag[A3])
  }

  /**
   * Defines a Scala closure of 4 arguments as user-defined function (UDF). The data types are
   * automatically inferred based on the Scala closure's signature. By default the returned UDF is
   * deterministic. To change it to nondeterministic, call the API
   * `UserDefinedFunction.asNondeterministic()`.
   *
   * @group udf_funcs
   * @since 3.4.0
   */
  def udf[RT: TypeTag, A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag](
      f: (A1, A2, A3, A4) => RT): UserDefinedFunction = {
    ScalarUserDefinedFunction(f, typeTag[RT], typeTag[A1], typeTag[A2], typeTag[A3], typeTag[A4])
  }

  /**
   * Defines a Scala closure of 5 arguments as user-defined function (UDF). The data types are
   * automatically inferred based on the Scala closure's signature. By default the returned UDF is
   * deterministic. To change it to nondeterministic, call the API
   * `UserDefinedFunction.asNondeterministic()`.
   *
   * @group udf_funcs
   * @since 3.4.0
   */
  def udf[RT: TypeTag, A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag, A5: TypeTag](
      f: (A1, A2, A3, A4, A5) => RT): UserDefinedFunction = {
    ScalarUserDefinedFunction(
      f,
      typeTag[RT],
      typeTag[A1],
      typeTag[A2],
      typeTag[A3],
      typeTag[A4],
      typeTag[A5])
  }

  /**
   * Defines a Scala closure of 6 arguments as user-defined function (UDF). The data types are
   * automatically inferred based on the Scala closure's signature. By default the returned UDF is
   * deterministic. To change it to nondeterministic, call the API
   * `UserDefinedFunction.asNondeterministic()`.
   *
   * @group udf_funcs
   * @since 3.4.0
   */
  def udf[
      RT: TypeTag,
      A1: TypeTag,
      A2: TypeTag,
      A3: TypeTag,
      A4: TypeTag,
      A5: TypeTag,
      A6: TypeTag](f: (A1, A2, A3, A4, A5, A6) => RT): UserDefinedFunction = {
    ScalarUserDefinedFunction(
      f,
      typeTag[RT],
      typeTag[A1],
      typeTag[A2],
      typeTag[A3],
      typeTag[A4],
      typeTag[A5],
      typeTag[A6])
  }

  /**
   * Defines a Scala closure of 7 arguments as user-defined function (UDF). The data types are
   * automatically inferred based on the Scala closure's signature. By default the returned UDF is
   * deterministic. To change it to nondeterministic, call the API
   * `UserDefinedFunction.asNondeterministic()`.
   *
   * @group udf_funcs
   * @since 3.4.0
   */
  def udf[
      RT: TypeTag,
      A1: TypeTag,
      A2: TypeTag,
      A3: TypeTag,
      A4: TypeTag,
      A5: TypeTag,
      A6: TypeTag,
      A7: TypeTag](f: (A1, A2, A3, A4, A5, A6, A7) => RT): UserDefinedFunction = {
    ScalarUserDefinedFunction(
      f,
      typeTag[RT],
      typeTag[A1],
      typeTag[A2],
      typeTag[A3],
      typeTag[A4],
      typeTag[A5],
      typeTag[A6],
      typeTag[A7])
  }

  /**
   * Defines a Scala closure of 8 arguments as user-defined function (UDF). The data types are
   * automatically inferred based on the Scala closure's signature. By default the returned UDF is
   * deterministic. To change it to nondeterministic, call the API
   * `UserDefinedFunction.asNondeterministic()`.
   *
   * @group udf_funcs
   * @since 3.4.0
   */
  def udf[
      RT: TypeTag,
      A1: TypeTag,
      A2: TypeTag,
      A3: TypeTag,
      A4: TypeTag,
      A5: TypeTag,
      A6: TypeTag,
      A7: TypeTag,
      A8: TypeTag](f: (A1, A2, A3, A4, A5, A6, A7, A8) => RT): UserDefinedFunction = {
    ScalarUserDefinedFunction(
      f,
      typeTag[RT],
      typeTag[A1],
      typeTag[A2],
      typeTag[A3],
      typeTag[A4],
      typeTag[A5],
      typeTag[A6],
      typeTag[A7],
      typeTag[A8])
  }

  /**
   * Defines a Scala closure of 9 arguments as user-defined function (UDF). The data types are
   * automatically inferred based on the Scala closure's signature. By default the returned UDF is
   * deterministic. To change it to nondeterministic, call the API
   * `UserDefinedFunction.asNondeterministic()`.
   *
   * @group udf_funcs
   * @since 3.4.0
   */
  def udf[
      RT: TypeTag,
      A1: TypeTag,
      A2: TypeTag,
      A3: TypeTag,
      A4: TypeTag,
      A5: TypeTag,
      A6: TypeTag,
      A7: TypeTag,
      A8: TypeTag,
      A9: TypeTag](f: (A1, A2, A3, A4, A5, A6, A7, A8, A9) => RT): UserDefinedFunction = {
    ScalarUserDefinedFunction(
      f,
      typeTag[RT],
      typeTag[A1],
      typeTag[A2],
      typeTag[A3],
      typeTag[A4],
      typeTag[A5],
      typeTag[A6],
      typeTag[A7],
      typeTag[A8],
      typeTag[A9])
  }

  /**
   * Defines a Scala closure of 10 arguments as user-defined function (UDF). The data types are
   * automatically inferred based on the Scala closure's signature. By default the returned UDF is
   * deterministic. To change it to nondeterministic, call the API
   * `UserDefinedFunction.asNondeterministic()`.
   *
   * @group udf_funcs
   * @since 3.4.0
   */
  def udf[
      RT: TypeTag,
      A1: TypeTag,
      A2: TypeTag,
      A3: TypeTag,
      A4: TypeTag,
      A5: TypeTag,
      A6: TypeTag,
      A7: TypeTag,
      A8: TypeTag,
      A9: TypeTag,
      A10: TypeTag](f: (A1, A2, A3, A4, A5, A6, A7, A8, A9, A10) => RT): UserDefinedFunction = {
    ScalarUserDefinedFunction(
      f,
      typeTag[RT],
      typeTag[A1],
      typeTag[A2],
      typeTag[A3],
      typeTag[A4],
      typeTag[A5],
      typeTag[A6],
      typeTag[A7],
      typeTag[A8],
      typeTag[A9],
      typeTag[A10])
  }
  // scalastyle:off line.size.limit

}
