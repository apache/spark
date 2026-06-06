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

import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._
import scala.reflect.ClassTag

import org.apache.spark.SparkConf
import org.apache.spark.api.python.{PythonEvalType, SimplePythonFunction}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan,
  MapInPandas, MapPartitionsExternalUDF}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.python.{MapInPandasExec,
  UserDefinedPythonFunction}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{IntegerType, StringType,
  StructField, StructType}

/**
 * Shared UDF fixtures and assertion helpers for planning tests.
 */
trait ExternalUDFPlanningTestBase extends SharedSparkSession {
  import testImplicits._

  protected val outputSchema: StructType = StructType(Seq(
    StructField("a", StringType),
    StructField("b", IntegerType)))

  protected def dummyPythonFunction: SimplePythonFunction =
    new SimplePythonFunction(
      command = Array.emptyByteArray,
      envVars = Map.empty[String, String].asJava,
      pythonIncludes = ArrayBuffer.empty[String].asJava,
      pythonExec = "python3",
      pythonVer = "3.12",
      broadcastVars = null,
      accumulator = null)

  protected val mapInPandasUDF: UserDefinedPythonFunction =
    UserDefinedPythonFunction(
      name = "dummyMapInPandas",
      func = dummyPythonFunction,
      dataType = outputSchema,
      pythonEvalType = PythonEvalType.SQL_MAP_PANDAS_ITER_UDF,
      udfDeterministic = true)

  protected def applyMapInPandas(): DataFrame = {
    val inputDF = Seq(("hello", 1)).toDF("a", "b")
    inputDF.mapInPandas(mapInPandasUDF(col("a"), col("b")))
  }

  protected def assertLogicalNode[T <: LogicalPlan: ClassTag](
      df: DataFrame): Unit = {
    val tag = implicitly[ClassTag[T]]
    val node = df.queryExecution.analyzed.collectFirst {
      case n if tag.runtimeClass.isInstance(n) => n
    }
    assert(node.isDefined,
      s"Expected ${tag.runtimeClass.getSimpleName}" +
        " in logical plan")
  }

  protected def assertPhysicalNode[T <: SparkPlan: ClassTag](
      df: DataFrame): Unit = {
    val tag = implicitly[ClassTag[T]]
    val node =
      df.queryExecution.executedPlan.collectFirst {
        case n if tag.runtimeClass.isInstance(n) => n
      }
    assert(node.isDefined,
      s"Expected ${tag.runtimeClass.getSimpleName}" +
        " in physical plan")
  }
}

/**
 * Tests that the classic Python runner path is used when the
 * unified UDF execution config is disabled (default).
 */
class ClassicUDFPlanningSuite
    extends ExternalUDFPlanningTestBase {

  test("mapInPandas uses MapInPandas logical node") {
    val result = applyMapInPandas()
    assertLogicalNode[MapInPandas](result)
  }

  test("mapInPandas uses MapInPandasExec physical node") {
    val result = applyMapInPandas()
    assertPhysicalNode[MapInPandasExec](result)
  }
}

/**
 * Tests that the unified external UDF worker framework is used
 * when the config is enabled.
 */
class UnifiedUDFPlanningSuite
    extends ExternalUDFPlanningTestBase {

  override def sparkConf: SparkConf =
    super.sparkConf.set(
      SQLConf.UNIFIED_UDF_EXECUTION_ENABLED.key, "true")

  test("mapInPandas uses MapPartitionsExternalUDF logical node") {
    val result = applyMapInPandas()
    assertLogicalNode[MapPartitionsExternalUDF](result)
  }

  test("mapInPandas uses MapPartitionsExternalUDFExec" +
      " physical node") {
    val result = applyMapInPandas()
    assertPhysicalNode[MapPartitionsExternalUDFExec](result)
  }
}
