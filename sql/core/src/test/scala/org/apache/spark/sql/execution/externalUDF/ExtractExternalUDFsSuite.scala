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

import java.nio.charset.StandardCharsets

import org.apache.spark.{SparkConf, SparkUnsupportedOperationException}
import org.apache.spark.sql.catalyst.expressions.{Add, Alias, AttributeReference,
  ExternalUserDefinedFunction, Expression}
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.{EvalExternalUDF, LocalRelation,
  LogicalPlan, Project}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.udf.worker.{DirectWorker, ProcessCallable, UDFWorkerProperties,
  UDFWorkerSpecification, WorkerCapabilities, WorkerEnvironment}

class ExtractExternalUDFsSuite extends PlanTest {

  private val input = AttributeReference("input", IntegerType, nullable = false)()
  private val relation = LocalRelation(Seq(input))
  private val extractRule = new ExtractExternalUDFs(new SparkConf(false))

  private def workerSpec(name: String, supportsChaining: Boolean): UDFWorkerSpecification = {
    val capabilities = WorkerCapabilities.newBuilder()
      .setSupportsUdfChaining(supportsChaining)
    val direct = DirectWorker.newBuilder()
      .setRunner(ProcessCallable.newBuilder().addCommand(name))
      .setProperties(UDFWorkerProperties.newBuilder())

    UDFWorkerSpecification.newBuilder()
      .setEnvironment(WorkerEnvironment.newBuilder())
      .setCapabilities(capabilities)
      .setDirect(direct)
      .build()
  }

  private def udf(
      name: String,
      spec: UDFWorkerSpecification,
      children: Seq[Expression]): ExternalUserDefinedFunction = {
    ExternalUserDefinedFunction(
      name = Some(name),
      workerSpec = spec,
      payload = name.getBytes(StandardCharsets.UTF_8),
      dataType = IntegerType,
      children = children,
      inputTypes = None,
      udfDeterministic = true,
      udfNullable = false)
  }

  private def extract(expression: Expression): LogicalPlan = {
    withSQLConf(SQLConf.UNIFIED_UDF_EXECUTION_ENABLED.key -> "true") {
      extractRule(Project(Seq(Alias(expression, "result")()), relation))
    }
  }

  private def evalNodes(plan: LogicalPlan): Seq[EvalExternalUDF] = {
    plan.collect { case eval: EvalExternalUDF => eval }
  }

  private def callCount(expression: Expression): Int = {
    expression.collect { case _: ExternalUserDefinedFunction => 1 }.size
  }

  test("a worker without chaining support gets one UDF call per node") {
    val spec = workerSpec("single", supportsChaining = false)
    val expression = udf("outer", spec,
      Seq(udf("middle", spec, Seq(udf("inner", spec, Seq(input))))))

    val nodes = evalNodes(extract(expression))
    assert(nodes.size == 3)
    assert(nodes.forall(_.udfs.size == 1))
    assert(nodes.forall(_.udfs.forall(callCount(_) == 1)))
  }

  test("a chaining worker evaluates a unary UDF chain in one node") {
    val spec = workerSpec("chained", supportsChaining = true)
    val expression = udf("outer", spec,
      Seq(udf("middle", spec, Seq(udf("inner", spec, Seq(input))))))

    val nodes = evalNodes(extract(expression))
    assert(nodes.size == 1)
    assert(nodes.head.udfs.size == 1)
    assert(callCount(nodes.head.udfs.head) == 3)
  }

  test("a chaining worker fuses independent UDF roots in one node") {
    val spec = workerSpec("fused", supportsChaining = true)
    val expression = Add(udf("left", spec, Seq(input)), udf("right", spec, Seq(input)))

    val nodes = evalNodes(extract(expression))
    assert(nodes.size == 1)
    assert(nodes.head.udfs.size == 2)
    assert(nodes.head.udfs.forall(callCount(_) == 1))
  }

  test("dependent branches use separate session nodes") {
    val spec = workerSpec("branched", supportsChaining = true)
    val left = udf("left", spec, Seq(input))
    val right = udf("right", spec, Seq(input))
    val expression = udf("outer", spec, Seq(Add(left, right)))

    val nodes = evalNodes(extract(expression))
    assert(nodes.size == 2)
    assert(nodes.map(_.udfs.size).sorted == Seq(1, 2))
    assert(nodes.flatMap(_.udfs).map(callCount).sorted == Seq(1, 1, 1))
  }

  test("UDFs with different worker specifications use separate nodes") {
    val outerSpec = workerSpec("outer-worker", supportsChaining = true)
    val innerSpec = workerSpec("inner-worker", supportsChaining = true)
    val expression = udf("outer", outerSpec, Seq(udf("inner", innerSpec, Seq(input))))

    val nodes = evalNodes(extract(expression))
    assert(nodes.size == 2)
    assert(nodes.map(_.workerSpec).toSet == Set(outerSpec, innerSpec))
    assert(nodes.forall(_.udfs.forall(callCount(_) == 1)))
  }

  test("independent UDFs are separate sessions when chaining is unsupported") {
    val spec = workerSpec("single", supportsChaining = false)
    val expression = Add(udf("left", spec, Seq(input)), udf("right", spec, Seq(input)))

    val nodes = evalNodes(extract(expression))
    assert(nodes.size == 2)
    assert(nodes.forall(_.udfs.size == 1))
  }

  test("external UDF expressions are rejected when unified execution is disabled") {
    val spec = workerSpec("disabled", supportsChaining = true)
    val plan = Project(Seq(Alias(udf("external", spec, Seq(input)), "result")()), relation)

    val exception = withSQLConf(SQLConf.UNIFIED_UDF_EXECUTION_ENABLED.key -> "false") {
      intercept[SparkUnsupportedOperationException] {
        extractRule(plan)
      }
    }
    checkError(
      exception = exception,
      condition = "UNSUPPORTED_FEATURE.EXTERNAL_UDF",
      parameters = Map("config" -> SQLConf.UNIFIED_UDF_EXECUTION_ENABLED.key))
  }
}
