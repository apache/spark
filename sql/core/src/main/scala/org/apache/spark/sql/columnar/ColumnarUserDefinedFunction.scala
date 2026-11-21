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
import org.apache.spark.sql.catalyst.util.CharVarcharUtils
import org.apache.spark.sql.classic.ClassicConversions._
import org.apache.spark.sql.classic.ExpressionUtils.expression
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.types.DataType
import org.apache.spark.util.ArrayImplicits._

/**
 * A columnar user-defined function: a scalar function that is evaluated one batch at a time,
 * with columnar input and output, by an operator of its own.
 *
 * A function has any number of implementations, and whoever provides them makes sure that they
 * return the same results:
 *  - a `ColumnarScalarFunction` is the one that Spark runs, in the JVM;
 *  - the `ColumnarUDFImplementation`s are for engines that replace Spark's operators, such as
 *    native engines, which run them with their own operator.
 *
 * A function may have no implementation that Spark runs. A query that calls it then fails when
 * Spark has to run the call itself.
 *
 * A function is created with `ColumnarUserDefinedFunction.create`, and is not changed
 * afterwards: `withEngineImplementation`, `asNondeterministic` and `asNonNullable` return a new
 * definition.
 *
 * A function can be applied to columns, or registered to be called from SQL. The registration
 * is a method of the classic `UDFRegistration`, which is the one of a session that runs in the
 * same JVM as the driver:
 *
 * {{{
 *   val addOne = ColumnarUserDefinedFunction.create("add_one", Seq(LongType), LongType, new AddOne)
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
 *  - The same holds for a conditional expression that the optimizer adds. A correlated
 *    subquery with an aggregate function that is not null over no rows, such as `count`, is
 *    evaluated as an outer join, where the result of the aggregate is chosen by a condition.
 *    A call over that result, as in `SELECT sum(x) FROM (SELECT f(count(*)) AS x ...)`, is
 *    evaluated for every outer row, also for the value over no rows when the row has matching
 *    rows, and can then fail for a value that the query does not need.
 *
 * @since 4.5.0
 */
@Evolving
final class ColumnarUserDefinedFunction private (
    /** The name of the function. */
    val name: String,
    /** The types of the arguments, which the arguments of a call are cast to. */
    val inputTypes: Seq[DataType],
    /** The type of the result. */
    val dataType: DataType,
    private[sql] val function: Option[ColumnarScalarFunction],
    private[sql] val engineImplementations: Seq[ColumnarUDFImplementation],
    /** Whether the function returns the same result for the same arguments. */
    val deterministic: Boolean,
    /** Whether the function can return null. */
    val nullable: Boolean) extends Serializable {

  /**
   * Returns this function with one more implementation for an engine other than Spark.
   */
  def withEngineImplementation(
      implementation: ColumnarUDFImplementation): ColumnarUserDefinedFunction = {
    require(implementation != null,
      "An implementation of a columnar user-defined function cannot be null.")
    new ColumnarUserDefinedFunction(
      name, inputTypes, dataType, function, engineImplementations :+ implementation,
      deterministic, nullable)
  }

  /**
   * Returns this function as one that may return different results for the same arguments.
   */
  def asNondeterministic(): ColumnarUserDefinedFunction = {
    new ColumnarUserDefinedFunction(
      name, inputTypes, dataType, function, engineImplementations, false, nullable)
  }

  /**
   * Returns this function as one that never returns null.
   */
  def asNonNullable(): ColumnarUserDefinedFunction = {
    new ColumnarUserDefinedFunction(
      name, inputTypes, dataType, function, engineImplementations, deterministic, false)
  }

  private[sql] def builder(children: Seq[Expression]): Expression = {
    if (function.isEmpty && engineImplementations.isEmpty) {
      throw QueryCompilationErrors.columnarUDFNoImplementationError(name)
    }
    if (children.exists(_.isInstanceOf[NamedArgumentExpression])) {
      throw QueryCompilationErrors.namedArgumentsNotSupported(name)
    }
    if (children.length != inputTypes.length) {
      throw QueryCompilationErrors.wrongNumArgsError(
        name, Seq(inputTypes.length), children.length)
    }
    // As for the other kinds of functions, CHAR and VARCHAR are not types of a function, unless
    // they are first-class types: they fail, or are replaced with STRING when the legacy conf
    // says so. This reads the conf of the session that builds the call.
    val checkedInputTypes = inputTypes.map(CharVarcharUtils.failIfHasCharVarchar)
    val checkedDataType = CharVarcharUtils.failIfHasCharVarchar(dataType)
    ColumnarUDF(
      name, function, engineImplementations, checkedInputTypes, checkedDataType, children,
      deterministic, nullable)
  }

  /** Returns a `Column` that evaluates this function with the given arguments. */
  @scala.annotation.varargs
  def apply(cols: Column*): Column = Column(builder(cols.map(expression)))

  override def toString: String = {
    s"ColumnarUserDefinedFunction($name, ${inputTypes.map(_.sql).mkString("(", ", ", ")")}, " +
      s"${dataType.sql})"
  }
}

/**
 * @since 4.5.0
 */
@Evolving
object ColumnarUserDefinedFunction {

  /**
   * Creates a deterministic function that can return null, with an implementation that Spark
   * runs.
   *
   * @param name the name of the function
   * @param inputTypes the types of the arguments, which the arguments of a call are cast to
   * @param dataType the type of the result
   * @param function the implementation that Spark runs, in the JVM
   */
  def create(
      name: String,
      inputTypes: Seq[DataType],
      dataType: DataType,
      function: ColumnarScalarFunction): ColumnarUserDefinedFunction = {
    checkSignature(name, inputTypes, dataType)
    require(function != null, "The function of a columnar user-defined function cannot be null.")
    new ColumnarUserDefinedFunction(name, inputTypes, dataType, Some(function), Nil, true, true)
  }

  /**
   * Creates a deterministic function that can return null, with an implementation that Spark
   * runs. This is the variant for Java, which copies the array.
   */
  def create(
      name: String,
      inputTypes: Array[DataType],
      dataType: DataType,
      function: ColumnarScalarFunction): ColumnarUserDefinedFunction = {
    create(name, copyOf(inputTypes), dataType, function)
  }

  /**
   * Creates a deterministic function that can return null, with no implementation that Spark
   * runs. Its implementations for other engines are added with `withEngineImplementation`: a
   * function without any implementation cannot be called. A query that calls a function with
   * such implementations only fails when Spark has to run the call itself.
   *
   * @param name the name of the function
   * @param inputTypes the types of the arguments, which the arguments of a call are cast to
   * @param dataType the type of the result
   */
  def create(
      name: String,
      inputTypes: Seq[DataType],
      dataType: DataType): ColumnarUserDefinedFunction = {
    checkSignature(name, inputTypes, dataType)
    new ColumnarUserDefinedFunction(name, inputTypes, dataType, None, Nil, true, true)
  }

  /**
   * Creates a deterministic function that can return null, with no implementation that Spark
   * runs. This is the variant for Java, which copies the array.
   */
  def create(
      name: String,
      inputTypes: Array[DataType],
      dataType: DataType): ColumnarUserDefinedFunction = {
    create(name, copyOf(inputTypes), dataType)
  }

  private def checkSignature(name: String, inputTypes: Seq[DataType], dataType: DataType): Unit = {
    require(name != null, "The name of a columnar user-defined function cannot be null.")
    require(inputTypes != null && !inputTypes.contains(null),
      "The argument types of a columnar user-defined function cannot be null.")
    require(dataType != null,
      "The result type of a columnar user-defined function cannot be null.")
  }

  // A copy, so that a later change of the array does not change the function.
  private def copyOf(inputTypes: Array[DataType]): Seq[DataType] = {
    require(inputTypes != null,
      "The argument types of a columnar user-defined function cannot be null.")
    inputTypes.clone().toImmutableArraySeq
  }
}
