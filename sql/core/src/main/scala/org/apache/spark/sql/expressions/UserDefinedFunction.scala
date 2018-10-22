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

import org.apache.spark.annotation.InterfaceStability
import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.catalyst.expressions.ScalaUDF
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
@InterfaceStability.Stable
case class UserDefinedFunction protected[sql] (
    f: AnyRef,
    dataType: DataType,
    inputTypes: Option[Seq[DataType]]) {

  private var _nameOption: Option[String] = None
  private var _nullable: Boolean = true
  private var _deterministic: Boolean = true

  // This is a `var` instead of in the constructor for backward compatibility of this case class.
  // TODO: revisit this case class in Spark 3.0, and narrow down the public surface.
  private[sql] var nullableTypes: Option[Seq[Boolean]] = None

  /**
   * Returns true when the UDF can return a nullable value.
   *
   * @since 2.3.0
   */
  def nullable: Boolean = _nullable

  /**
   * Returns true iff the UDF is deterministic, i.e. the UDF produces the same output given the same
   * input.
   *
   * @since 2.3.0
   */
  def deterministic: Boolean = _deterministic

  /**
   * Returns an expression that invokes the UDF, using the given arguments.
   *
   * @since 1.3.0
   */
  @scala.annotation.varargs
  def apply(exprs: Column*): Column = {
    // TODO: make sure this class is only instantiated through `SparkUserDefinedFunction.create()`
    // and `nullableTypes` is always set.
    if (nullableTypes.isEmpty) {
      nullableTypes = Some(ScalaReflection.getParameterTypeNullability(f))
    }
    if (inputTypes.isDefined) {
      assert(inputTypes.get.length == nullableTypes.get.length)
    }

    Column(ScalaUDF(
      f,
      dataType,
      exprs.map(_.expr),
      nullableTypes.get,
      inputTypes.getOrElse(Nil),
      udfName = _nameOption,
      nullable = _nullable,
      udfDeterministic = _deterministic))
  }

  private def copyAll(): UserDefinedFunction = {
    val udf = copy()
    udf._nameOption = _nameOption
    udf._nullable = _nullable
    udf._deterministic = _deterministic
    udf.nullableTypes = nullableTypes
    udf
  }

  /**
   * Updates UserDefinedFunction with a given name.
   *
   * @since 2.3.0
   */
  def withName(name: String): UserDefinedFunction = {
    val udf = copyAll()
    udf._nameOption = Option(name)
    udf
  }

  /**
   * Updates UserDefinedFunction to non-nullable.
   *
   * @since 2.3.0
   */
  def asNonNullable(): UserDefinedFunction = {
    if (!nullable) {
      this
    } else {
      val udf = copyAll()
      udf._nullable = false
      udf
    }
  }

  /**
   * Updates UserDefinedFunction to nondeterministic.
   *
   * @since 2.3.0
   */
  def asNondeterministic(): UserDefinedFunction = {
    if (!_deterministic) {
      this
    } else {
      val udf = copyAll()
      udf._deterministic = false
      udf
    }
  }
}

// We have to use a name different than `UserDefinedFunction` here, to avoid breaking the binary
// compatibility of the auto-generate UserDefinedFunction object.
private[sql] object SparkUserDefinedFunction {

  def create(
      f: AnyRef,
      dataType: DataType,
      inputSchemas: Seq[Option[ScalaReflection.Schema]]): UserDefinedFunction = {
    val inputTypes = if (inputSchemas.contains(None)) {
      None
    } else {
      Some(inputSchemas.map(_.get.dataType))
    }
    val udf = new UserDefinedFunction(f, dataType, inputTypes)
    udf.nullableTypes = Some(inputSchemas.map(_.map(_.nullable).getOrElse(true)))
    udf
  }
}
