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
import org.apache.spark.sql.catalyst.expressions.{ColumnarUDF, Expression, NamedArgumentExpression}
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
 * A function can be applied to columns, or registered to be called from SQL. The registration
 * is a method of the classic `UDFRegistration`, which is the one of a session that runs in the
 * same JVM as the driver:
 *
 * {{{
 *   val addOne = ColumnarUserDefinedFunction("add_one", Seq(LongType), LongType, Some(new AddOne))
 *   df.select(addOne(col("id")))
 *   spark.udf.asInstanceOf[org.apache.spark.sql.classic.UDFRegistration].registerColumnar(addOne)
 *   spark.sql("SELECT add_one(id) FROM t")
 * }}}
 *
 * The operator reads the arguments of a call as columns of its child, and evaluates together
 * the calls of one projection or filter. They are evaluated without leaving the columnar
 * format when the child is columnar and the arguments of all of them are columns of it, of the
 * types that the functions declare. If one argument of one of these calls is anything else,
 * including a column that is cast to the declared type, a row-based projection computes it
 * first, and its output is converted back to columns for all of these calls. A row-based
 * operator below the calls, such as a filter over the scan, has the same effect.
 *
 * Limitations:
 *  - Calls with named arguments, and calls inside the lambda function of a higher-order
 *    function, are not supported.
 *  - A call that reads columns of both sides of a join is supported in the condition of an
 *    inner join only. This includes the joins that correlated EXISTS and IN subqueries become.
 *  - A conditional expression does not guard a call. A call in a branch of CASE, IF or
 *    COALESCE, or on one side of AND or OR, is evaluated for every row, and so are the
 *    expressions that compute its arguments, which can then fail for rows that the branch
 *    would have skipped.
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
    if (children.exists(_.isInstanceOf[NamedArgumentExpression])) {
      throw QueryCompilationErrors.namedArgumentsNotSupported(name)
    }
    if (children.length != inputTypes.length) {
      throw QueryCompilationErrors.wrongNumArgsError(
        name, Seq(inputTypes.length), children.length)
    }
    ColumnarUDF(
      name, function, engineImplementations, inputTypes, dataType, children, deterministic,
      nullable)
  }

  /** Returns a `Column` that evaluates this function with the given arguments. */
  @scala.annotation.varargs
  def apply(cols: Column*): Column = Column(builder(cols.map(expression)))
}
