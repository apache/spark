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
package org.apache.spark.sql.ml

import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.stat._
import org.apache.spark.sql.{SparkSessionExtensions, SparkSessionExtensionsProvider}
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry
import org.apache.spark.sql.catalyst.expressions.{Expression, StringLiteral, UnwrapUDT}
import org.apache.spark.sql.catalyst.expressions.ml.VectorToArray
import org.apache.spark.sql.classic.UserDefinedFunctionUtils.toScalaUDF
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.expressions.{SparkUserDefinedFunction, UserDefinedFunction}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.{DoubleType, FloatType}

/**
 * Register ML vector conversion functions in the internal function registry.
 *
 * This is a bit of a hack because we use the [[SparkSessionExtensions]] mechanism to register
 * functions in a global registry. The use of a Scala object makes sure we only do this once.
 */
object InternalFunctionRegistration {
  def apply(): Unit = ()

  private def invokeUdf(udf: UserDefinedFunction, e: Expression): Expression = {
    toScalaUDF(udf.asInstanceOf[SparkUserDefinedFunction], e :: Nil)
  }

  private def registerFunction(name: String)(builder: Seq[Expression] => Expression): Unit = {
    FunctionRegistry.internal.registerFunction(
      FunctionIdentifier(name), builder, "internal")
  }

  registerFunction("vector_to_array") {
    case Seq(input, StringLiteral("float64")) =>
      VectorToArray(UnwrapUDT(input), DoubleType)
    case Seq(input, StringLiteral("float32")) =>
      VectorToArray(UnwrapUDT(input), FloatType)
    case Seq(_, invalid @ StringLiteral(_)) =>
      throw QueryCompilationErrors.invalidParameter("DTYPE", "vector_to_array", "dtype", invalid)
    case Seq(_, invalid) =>
      throw QueryCompilationErrors.invalidStringParameter("vector_to_array", "dtype", invalid)
    case exprs =>
      throw QueryCompilationErrors.wrongNumArgsError("vector_to_array", "2", exprs.size)
  }

  private val arrayToVectorUdf = udf { array: Seq[Double] =>
    Vectors.dense(array.toArray)
  }

  registerFunction("array_to_vector") {
    case Seq(input) =>
      invokeUdf(arrayToVectorUdf, input)
    case exprs =>
      throw QueryCompilationErrors.wrongNumArgsError("array_to_vector", "1", exprs.size)
  }

  FunctionRegistry
    .registerInternalExpression[SummaryBuilderImpl.MetricsAggregate]("aggregate_metrics")
}

class InternalFunctionRegistration extends SparkSessionExtensionsProvider {
  override def apply(e: SparkSessionExtensions): Unit = InternalFunctionRegistration()
}
