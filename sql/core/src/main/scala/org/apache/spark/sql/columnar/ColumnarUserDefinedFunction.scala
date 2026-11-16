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

package org.apache.spark.sql.columnar

import org.apache.spark.annotation.Evolving
import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.expressions.{ColumnarUDF, Expression}
import org.apache.spark.sql.classic.ClassicConversions._
import org.apache.spark.sql.classic.ExpressionUtils.expression
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.types.DataType

/**
 * A columnar user-defined function: a scalar function that is evaluated one batch at a time,
 * with columnar input and output, by an operator of its own.
 *
 * A function has any number of implementations, and whoever provides them makes sure that they
 * return the same results:
 *  - `function` is the one that Spark runs, in the JVM;
 *  - `engineImplementations` are for engines that replace Spark's operators, such as native
 *    engines, which run them with their own operator.
 *
 * A function may have no `function`. A query that calls it then fails when Spark has to run
 * the call itself.
 *
 * {{{
 *   val addOne = ColumnarUserDefinedFunction("add_one", Seq(LongType), LongType, Some(new AddOne))
 *   df.select(addOne(col("id")))
 *   spark.udf.registerColumnar(addOne)
 *   spark.sql("SELECT add_one(id) FROM t")
 * }}}
 *
 * @param name the name of the function
 * @param inputTypes the types of the arguments, which the arguments of a call are cast to
 * @param dataType the type of the result
 * @param function the implementation that Spark runs, if there is one
 * @param engineImplementations the implementations for other engines
 * @param deterministic whether the function returns the same result for the same arguments
 * @param nullable whether the function can return null
 * @since 4.4.0
 */
@Evolving
case class ColumnarUserDefinedFunction(
    name: String,
    inputTypes: Seq[DataType],
    dataType: DataType,
    function: Option[ColumnarScalarFunction] = None,
    engineImplementations: Seq[ColumnarUDFImplementation] = Nil,
    deterministic: Boolean = true,
    nullable: Boolean = true) {

  private[sql] def builder(children: Seq[Expression]): Expression = {
    if (children.length != inputTypes.length) {
      throw QueryCompilationErrors.wrongNumArgsError(
        name, Seq(inputTypes.length), children.length)
    }
    ColumnarUDF(
      name, function, engineImplementations, inputTypes, dataType, children, deterministic,
      nullable)
  }

  /** Returns a [[Column]] that evaluates this function with the given arguments. */
  @scala.annotation.varargs
  def apply(cols: Column*): Column = Column(builder(cols.map(expression)))
}
