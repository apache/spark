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
package org.apache.spark.sql.execution.externalUDF

import org.apache.spark.SparkConf
import org.apache.spark.annotation.Experimental
import org.apache.spark.resource.ResourceProfile
import org.apache.spark.sql.catalyst.expressions.{Expression,
  ExternalUserDefinedFunction, PythonUDF}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan,
  MapInArrow, MapInPandas, MapPartitionsExternalUDF}
import org.apache.spark.sql.catalyst.types.DataTypeUtils.toAttributes
import org.apache.spark.sql.types.StructType

/**
 * Strategy for converting UDF calls into logical plan nodes.
 * Implementations decide whether to use the classic
 * language-specific runner or the unified external UDF worker
 * framework.
 *
 * Wired into [[org.apache.spark.sql.internal.SessionState]] via
 * [[org.apache.spark.sql.internal.BaseSessionStateBuilder]].
 */
trait ExternalUDFPlanner {

  /**
   * Creates the logical plan node for a Python mapInPandas
   * operation.
   */
  def planPythonMapInPandas(
      func: Expression,
      child: LogicalPlan,
      isBarrier: Boolean,
      profile: Option[ResourceProfile]): LogicalPlan

  /**
   * Creates the logical plan node for a Python mapInArrow
   * operation.
   */
  def planPythonMapInArrow(
      func: Expression,
      child: LogicalPlan,
      isBarrier: Boolean,
      profile: Option[ResourceProfile]): LogicalPlan
}

/**
 * Classic [[ExternalUDFPlanner]] that uses the built-in Python
 * runner.
 */
class ClassicExternalUDFPlanner extends ExternalUDFPlanner {

  override def planPythonMapInPandas(
      func: Expression,
      child: LogicalPlan,
      isBarrier: Boolean,
      profile: Option[ResourceProfile]): LogicalPlan = {
    val output = toAttributes(
      func.dataType.asInstanceOf[StructType])
    MapInPandas(func, output, child, isBarrier, profile)
  }

  override def planPythonMapInArrow(
      func: Expression,
      child: LogicalPlan,
      isBarrier: Boolean,
      profile: Option[ResourceProfile]): LogicalPlan = {
    val output = toAttributes(
      func.dataType.asInstanceOf[StructType])
    MapInArrow(func, output, child, isBarrier, profile)
  }
}

/**
 * :: Experimental ::
 * Unified [[ExternalUDFPlanner]] that uses the language-agnostic
 * external UDF worker framework.
 */
@Experimental
class UnifiedExternalUDFPlanner(
    private val conf: SparkConf) extends ExternalUDFPlanner {

  override def planPythonMapInPandas(
      func: Expression,
      child: LogicalPlan,
      isBarrier: Boolean,
      profile: Option[ResourceProfile]): LogicalPlan = {
    val pythonUdf = func.asInstanceOf[PythonUDF]
    val workerSpec =
      PythonUDFWorkerSpecification.fromPythonFunction(
        pythonUdf.func, conf)
    val udf = ExternalUserDefinedFunction(
      name = Some(pythonUdf.name),
      payload = pythonUdf.func.command.toArray,
      dataType = pythonUdf.dataType,
      children = Seq.empty,
      udfDeterministic = pythonUdf.udfDeterministic,
      udfNullable = true)
    MapPartitionsExternalUDF(workerSpec, udf, isBarrier, child)
  }

  override def planPythonMapInArrow(
      func: Expression,
      child: LogicalPlan,
      isBarrier: Boolean,
      profile: Option[ResourceProfile]): LogicalPlan = {
    // TODO [SPARK-55278]: Implement unified mapInArrow support.
    // For now, fall back to the classic path.
    val output = toAttributes(
      func.dataType.asInstanceOf[StructType])
    MapInArrow(func, output, child, isBarrier, profile)
  }
}
