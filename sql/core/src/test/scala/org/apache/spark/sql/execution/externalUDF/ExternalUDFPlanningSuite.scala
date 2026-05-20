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

import org.apache.spark.api.python.{PythonEvalType, SimplePythonFunction}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, MapInPandas,
  MapPartitionsExternalUDF}
import org.apache.spark.sql.execution.{SparkPlan}
import org.apache.spark.sql.execution.python.{MapInPandasExec,
  UserDefinedPythonFunction}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{IntegerType, StringType,
  StructField, StructType}

/**
 * Tests that the unified UDF execution config gates the correct
 * logical and physical plan nodes for mapInPandas.
 */
class ExternalUDFPlanningSuite extends SharedSparkSession {
  import testImplicits._

  private val outputSchema = StructType(Seq(
    StructField("a", StringType),
    StructField("b", IntegerType)))

  private def dummyPythonFunction: SimplePythonFunction =
    new SimplePythonFunction(
      command = Array.emptyByteArray,
      envVars = Map.empty[String, String].asJava,
      pythonIncludes = ArrayBuffer.empty[String].asJava,
      pythonExec = "python3",
      pythonVer = "3.12",
      broadcastVars = null,
      accumulator = null)

  private val mapInPandasUDF = UserDefinedPythonFunction(
    name = "dummyMapInPandas",
    func = dummyPythonFunction,
    dataType = outputSchema,
    pythonEvalType = PythonEvalType.SQL_MAP_PANDAS_ITER_UDF,
    udfDeterministic = true)

  private def applyMapInPandas(): org.apache.spark.sql.DataFrame = {
    val inputDF = Seq(("hello", 1)).toDF("a", "b")
    val udfColumn = mapInPandasUDF(
      inputDF.columns.toIndexedSeq.map(inputDF.col): _*)
    inputDF.mapInPandas(udfColumn)
  }

  /**
   * Asserts that the logical plan contains a node of the expected
   * type when the unified UDF execution config is set as specified.
   */
  private def assertLogicalNode[T <: LogicalPlan: ClassTag](
      enabled: Boolean): Unit = {
    val configValue = enabled.toString
    withSQLConf(
        SQLConf.UNIFIED_UDF_EXECUTION_ENABLED.key -> configValue) {
      val result = applyMapInPandas()
      val tag = implicitly[ClassTag[T]]
      val node = result.queryExecution.analyzed.collectFirst {
        case n if tag.runtimeClass.isInstance(n) => n
      }
      assert(node.isDefined,
        s"Expected ${tag.runtimeClass.getSimpleName} in" +
          " logical plan")
    }
  }

  /**
   * Asserts that the physical plan contains a node of the expected
   * type when the unified UDF execution config is set as specified.
   */
  private def assertPhysicalNode[T <: SparkPlan: ClassTag](
      enabled: Boolean): Unit = {
    val configValue = enabled.toString
    withSQLConf(
        SQLConf.UNIFIED_UDF_EXECUTION_ENABLED.key -> configValue) {
      val result = applyMapInPandas()
      val tag = implicitly[ClassTag[T]]
      val node = result.queryExecution.executedPlan.collectFirst {
        case n if tag.runtimeClass.isInstance(n) => n
      }
      assert(node.isDefined,
        s"Expected ${tag.runtimeClass.getSimpleName} in" +
          " physical plan")
    }
  }

  // -- Logical plan tests ---------------------------------------------------

  test("mapInPandas uses MapInPandas logical node" +
      " when config disabled") {
    assertLogicalNode[MapInPandas](enabled = false)
  }

  test("mapInPandas uses MapPartitionsExternalUDF logical node" +
      " when config enabled") {
    assertLogicalNode[MapPartitionsExternalUDF](enabled = true)
  }

  // -- Physical plan tests --------------------------------------------------

  test("mapInPandas uses MapInPandasExec physical node" +
      " when config disabled") {
    assertPhysicalNode[MapInPandasExec](enabled = false)
  }

  test("mapInPandas uses MapPartitionsExternalUDFExec physical" +
      " node when config enabled") {
    assertPhysicalNode[MapPartitionsExternalUDFExec](
      enabled = true)
  }
}
