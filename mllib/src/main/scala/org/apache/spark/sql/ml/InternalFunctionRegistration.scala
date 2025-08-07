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

import org.apache.spark.ml.linalg.{SparseVector, Vector, Vectors}
import org.apache.spark.ml.stat._
import org.apache.spark.mllib.linalg.{SparseVector => OldSparseVector, Vector => OldVector}
import org.apache.spark.sql.{SparkSessionExtensions, SparkSessionExtensionsProvider}
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry
import org.apache.spark.sql.catalyst.expressions.{Expression, StringLiteral}
import org.apache.spark.sql.classic.UserDefinedFunctionUtils.toScalaUDF
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.expressions.{SparkUserDefinedFunction, UserDefinedFunction}
import org.apache.spark.sql.functions.udf

/**
 * Register a couple ML vector conversion UDFs in the internal function registry.
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
    FunctionRegistry.internal.createOrReplaceTempFunction(name, builder, "internal")
  }

  private val vectorToArrayUdf = udf { vec: Any =>
    vec match {
      case v: Vector => v.toArray
      case v: OldVector => v.toArray
      case v => throw new IllegalArgumentException(
        "function vector_to_array requires a non-null input argument and input type must be " +
          "`org.apache.spark.ml.linalg.Vector` or `org.apache.spark.mllib.linalg.Vector`, " +
          s"but got ${if (v == null) "null" else v.getClass.getName}.")
    }
  }.asNonNullable()

  private val vectorToArrayFloatUdf = udf { vec: Any =>
    vec match {
      case v: SparseVector =>
        val data = new Array[Float](v.size)
        v.foreachNonZero { (index, value) => data(index) = value.toFloat }
        data
      case v: Vector => v.toArray.map(_.toFloat)
      case v: OldSparseVector =>
        val data = new Array[Float](v.size)
        v.foreachNonZero { (index, value) => data(index) = value.toFloat }
        data
      case v: OldVector => v.toArray.map(_.toFloat)
      case v => throw new IllegalArgumentException(
        "function vector_to_array requires a non-null input argument and input type must be " +
          "`org.apache.spark.ml.linalg.Vector` or `org.apache.spark.mllib.linalg.Vector`, " +
          s"but got ${if (v == null) "null" else v.getClass.getName}.")
    }
  }.asNonNullable()

  registerFunction("vector_to_array") {
    case Seq(input, StringLiteral("float64")) =>
      invokeUdf(vectorToArrayUdf, input)
    case Seq(input, StringLiteral("float32")) =>
      invokeUdf(vectorToArrayFloatUdf, input)
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
