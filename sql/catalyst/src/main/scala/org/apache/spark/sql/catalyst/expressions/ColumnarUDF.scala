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

package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.catalyst.trees.TreePattern.{COLUMNAR_UDF, TreePattern}
import org.apache.spark.sql.columnar.{ColumnarScalarFunction, ColumnarUDFImplementation}
import org.apache.spark.sql.types.{AbstractDataType, DataType}

/**
 * A call of a columnar user-defined function: a scalar function that is evaluated one batch at
 * a time, by the `EvalColumnarUDFs` operator that `ExtractColumnarUDFs` plans for it.
 *
 * A function has any number of implementations. Spark can run `function`, in the JVM. The
 * others, `engineImplementations`, are for engines that replace Spark's operators, and Spark
 * only carries them. A function without `function` fails when Spark has to run it.
 *
 * @param name the name of the function
 * @param function the implementation that Spark runs, if there is one
 * @param engineImplementations the implementations for other engines
 * @param inputTypes the types of the arguments, which the analyzer casts the arguments to
 * @param dataType the type of the result
 * @param children the arguments
 * @param udfDeterministic whether the function returns the same result for the same arguments
 * @param udfNullable whether the function can return null
 * @param resultId the id of the call, which tells two calls of a function that is not
 *                 deterministic apart
 */
case class ColumnarUDF(
    name: String,
    function: Option[ColumnarScalarFunction],
    engineImplementations: Seq[ColumnarUDFImplementation],
    inputTypes: Seq[AbstractDataType],
    dataType: DataType,
    children: Seq[Expression],
    udfDeterministic: Boolean = true,
    udfNullable: Boolean = true,
    resultId: ExprId = NamedExpression.newExprId)
  extends Expression with Unevaluable with ImplicitCastInputTypes with UserDefinedExpression {

  override lazy val deterministic: Boolean = udfDeterministic && children.forall(_.deterministic)

  override def nullable: Boolean = udfNullable

  // A call runs user code, so the optimizer does not move a filter below the projection that
  // computes it, which would evaluate the call a second time.
  override def expensive: Boolean = true

  override lazy val canonicalized: Expression = {
    // `resultId` does not affect the result, so that equal calls of a deterministic function
    // are evaluated once.
    copy(resultId = ExprId(-1)).withNewChildren(children.map(_.canonicalized))
  }

  final override val nodePatterns: Seq[TreePattern] = Seq(COLUMNAR_UDF)

  override def prettyName: String = name

  override def toString: String = s"$name(${children.mkString(", ")})#${resultId.id}"

  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[Expression]): ColumnarUDF = copy(children = newChildren)
}
