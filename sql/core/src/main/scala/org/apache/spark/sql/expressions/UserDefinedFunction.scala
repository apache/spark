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
import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.catalyst.expressions.{Expression, ScalaUDF}
import org.apache.spark.sql.types.{AnyDataType, DataType}

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
sealed abstract class UserDefinedFunction {

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

private[sql] case class SparkUserDefinedFunction(
    f: AnyRef,
    dataType: DataType,
    inputSchemas: Seq[Option[ScalaReflection.Schema]],
    name: Option[String] = None,
    nullable: Boolean = true,
    deterministic: Boolean = true) extends UserDefinedFunction {

  @scala.annotation.varargs
  override def apply(exprs: Column*): Column = {
    Column(createScalaUDF(exprs.map(_.expr)))
  }

  private[sql] def createScalaUDF(exprs: Seq[Expression]): ScalaUDF = {
    // It's possible that some of the inputs don't have a specific type(e.g. `Any`),  skip type
    // check.
    val inputTypes = inputSchemas.map(_.map(_.dataType).getOrElse(AnyDataType))
    // `ScalaReflection.Schema.nullable` is false iff the type is primitive. Also `Any` is not
    // primitive.
    val inputsPrimitive = inputSchemas.map(_.map(!_.nullable).getOrElse(false))
    ScalaUDF(
      f,
      dataType,
      exprs,
      inputsPrimitive,
      inputTypes,
      udfName = name,
      nullable = nullable,
      udfDeterministic = deterministic)
  }

  override def withName(name: String): SparkUserDefinedFunction = {
    copy(name = Option(name))
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
}
