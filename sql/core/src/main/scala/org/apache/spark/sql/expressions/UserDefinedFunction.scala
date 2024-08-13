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

package org.apache.spark.sql.expressions

import org.apache.spark.annotation.Stable
import org.apache.spark.sql.{Column, Encoder}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.{Expression, ScalaUDF}
import org.apache.spark.sql.execution.aggregate.ScalaAggregator
import org.apache.spark.sql.internal.UserDefinedFunctionLike
import org.apache.spark.sql.types.DataType

/**
 * A user-defined function. To create one, use the `udf` functions in `functions`.
 *
 * As an example:
 * {{{
 *   // Define a UDF that returns true or false based on some numeric score.
 *   val predict = udf((score: Double) => score > 0.5)
 *
 *   // Projects a column that adds a prediction column based on the score column.
 *   df.select( predict(df("score")) )
 * }}}
 *
 * @since 1.3.0
 */
@Stable
sealed abstract class UserDefinedFunction extends UserDefinedFunctionLike {

  /**
   * Returns true when the UDF can return a nullable value.
   *
   * @since 2.3.0
   */
  def nullable: Boolean

  /**
   * Returns true iff the UDF is deterministic, i.e. the UDF produces the same output given the same
   * input.
   *
   * @since 2.3.0
   */
  def deterministic: Boolean

  /**
   * Returns an expression that invokes the UDF, using the given arguments.
   *
   * @since 1.3.0
   */
  @scala.annotation.varargs
  def apply(exprs: Column*): Column

  /**
   * Updates UserDefinedFunction with a given name.
   *
   * @since 2.3.0
   */
  def withName(name: String): UserDefinedFunction

  /**
   * Updates UserDefinedFunction to non-nullable.
   *
   * @since 2.3.0
   */
  def asNonNullable(): UserDefinedFunction

  /**
   * Updates UserDefinedFunction to nondeterministic.
   *
   * @since 2.3.0
   */
  def asNondeterministic(): UserDefinedFunction
}

private[spark] case class SparkUserDefinedFunction(
    f: AnyRef,
    dataType: DataType,
    inputEncoders: Seq[Option[ExpressionEncoder[_]]] = Nil,
    outputEncoder: Option[ExpressionEncoder[_]] = None,
    givenName: Option[String] = None,
    nullable: Boolean = true,
    deterministic: Boolean = true) extends UserDefinedFunction {

  @scala.annotation.varargs
  override def apply(exprs: Column*): Column = {
    Column(createScalaUDF(exprs.map(_.expr)))
  }

  private[sql] def createScalaUDF(exprs: Seq[Expression]): ScalaUDF = {
    ScalaUDF(
      f,
      dataType,
      exprs,
      inputEncoders,
      outputEncoder,
      udfName = givenName,
      nullable = nullable,
      udfDeterministic = deterministic)
  }

  override def withName(name: String): SparkUserDefinedFunction = {
    copy(givenName = Option(name))
  }

  override def asNonNullable(): SparkUserDefinedFunction = {
    if (!nullable) {
      this
    } else {
      copy(nullable = false)
    }
  }

  override def asNondeterministic(): SparkUserDefinedFunction = {
    if (!deterministic) {
      this
    } else {
      copy(deterministic = false)
    }
  }

  override def name: String = givenName.getOrElse("UDF")
}

private[sql] case class UserDefinedAggregator[IN, BUF, OUT](
    aggregator: Aggregator[IN, BUF, OUT],
    inputEncoder: Encoder[IN],
    givenName: Option[String] = None,
    nullable: Boolean = true,
    deterministic: Boolean = true) extends UserDefinedFunction {

  @scala.annotation.varargs
  def apply(exprs: Column*): Column = {
    Column(scalaAggregator(exprs.map(_.expr)).toAggregateExpression())
  }

  // This is also used by udf.register(...) when it detects a UserDefinedAggregator
  def scalaAggregator(exprs: Seq[Expression]): ScalaAggregator[IN, BUF, OUT] = {
    val iEncoder = inputEncoder.asInstanceOf[ExpressionEncoder[IN]]
    val bEncoder = aggregator.bufferEncoder.asInstanceOf[ExpressionEncoder[BUF]]
    ScalaAggregator(
      exprs, aggregator, iEncoder, bEncoder, nullable, deterministic, aggregatorName = givenName)
  }

  override def withName(name: String): UserDefinedAggregator[IN, BUF, OUT] = {
    copy(givenName = Option(name))
  }

  override def asNonNullable(): UserDefinedAggregator[IN, BUF, OUT] = {
    if (!nullable) {
      this
    } else {
      copy(nullable = false)
    }
  }

  override def asNondeterministic(): UserDefinedAggregator[IN, BUF, OUT] = {
    if (!deterministic) {
      this
    } else {
      copy(deterministic = false)
    }
  }

  override def name: String = givenName.getOrElse(aggregator.name)
}
