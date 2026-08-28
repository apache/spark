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

import org.apache.spark.SparkUnsupportedOperationException
import org.apache.spark.sql.{AnalysisException, QueryTest}
import org.apache.spark.sql.catalyst.QueryPlanningTracker
import org.apache.spark.sql.catalyst.expressions.{Add, Alias, And, ArrayTransform, Attribute,
  AttributeReference, CreateArray, EqualTo, Expression, ExternalUserDefinedFunction, GreaterThan,
  Lag, LambdaFunction, Literal, NamedLambdaVariable, UnspecifiedFrame, WindowExpression,
  WindowSpecDefinition}
import org.apache.spark.sql.catalyst.expressions.aggregate.Sum
import org.apache.spark.sql.catalyst.plans.{Inner, LeftOuter}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, BinaryNode, ExecuteExternalUDF,
  Filter, Join, JoinHint, LocalLimit, LocalRelation, LogicalPlan, Project, Range, Window}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{BooleanType, DataType, IntegerType}
import org.apache.spark.udf.worker.{DirectWorker, ProcessCallable, UDFWorkerProperties,
  UDFWorkerSpecification, WorkerEnvironment}

class PlanExternalUDFsSuite extends QueryTest with SharedSparkSession {

  private case class TestBinaryPlan(
      expression: Expression,
      left: LogicalPlan,
      right: LogicalPlan) extends BinaryNode {

    override def output: Seq[Attribute] = left.output ++ right.output

    override protected def withNewChildrenInternal(
        newLeft: LogicalPlan,
        newRight: LogicalPlan): TestBinaryPlan = copy(left = newLeft, right = newRight)
  }

  private val input = AttributeReference("input", IntegerType, nullable = false)()
  private val relation = LocalRelation(Seq(input))
  private def workerSpec(name: String): UDFWorkerSpecification = {
    val direct = DirectWorker.newBuilder()
      .setRunner(ProcessCallable.newBuilder().addCommand(name))
      .setProperties(UDFWorkerProperties.newBuilder())

    UDFWorkerSpecification.newBuilder()
      .setEnvironment(WorkerEnvironment.newBuilder())
      .setDirect(direct)
      .build()
  }

  private def udf(
      name: String,
      spec: UDFWorkerSpecification,
      children: Seq[Expression],
      dataType: DataType = IntegerType,
      deterministic: Boolean = true,
      inputTypes: Option[Seq[DataType]] = None): ExternalUserDefinedFunction = {
    ExternalUserDefinedFunction(
      name = Some(name),
      workerSpec = spec,
      payload = name.getBytes(StandardCharsets.UTF_8),
      dataType = dataType,
      children = children,
      inputTypes = inputTypes,
      udfDeterministic = deterministic,
      udfNullable = false)
  }

  private def extract(plan: LogicalPlan): LogicalPlan = {
    withSQLConf(SQLConf.UNIFIED_UDF_EXECUTION_ENABLED.key -> "true") {
      PlanExternalUDFs(ExtractExternalUDFFromWindow(plan))
    }
  }

  private def extract(expression: Expression): LogicalPlan = {
    extract(Project(Seq(Alias(expression, "result")()), relation))
  }

  private def optimize(plan: LogicalPlan): LogicalPlan = {
    withSQLConf(SQLConf.UNIFIED_UDF_EXECUTION_ENABLED.key -> "true") {
      spark.sessionState.optimizer.execute(plan)
    }
  }

  private def evalNodes(plan: LogicalPlan): Seq[ExecuteExternalUDF] = {
    plan.collect { case eval: ExecuteExternalUDF => eval }
  }

  private def callCount(expression: Expression): Int = {
    expression.collect { case _: ExternalUserDefinedFunction => 1 }.size
  }

  test("nested UDF expressions use separate nodes") {
    val spec = workerSpec("nested")
    val expression = udf("outer", spec,
      Seq(udf("middle", spec, Seq(udf("inner", spec, Seq(input))))))

    val nodes = evalNodes(extract(expression))
    assert(nodes.size == 3)
    assert(nodes.forall(node => callCount(node.udf) == 1))
  }

  test("independent UDF expressions use separate nodes") {
    val spec = workerSpec("independent")
    val expression = Add(udf("left", spec, Seq(input)), udf("right", spec, Seq(input)))

    val nodes = evalNodes(extract(expression))
    assert(nodes.size == 2)
    assert(nodes.forall(node => callCount(node.udf) == 1))
  }

  test("equivalent deterministic UDF expressions use separate nodes") {
    val spec = workerSpec("deterministic")
    val expression = Add(
      udf("duplicate", spec, Seq(input)),
      udf("duplicate", spec, Seq(input)))

    val nodes = evalNodes(extract(expression))
    assert(nodes.size == 2)
  }

  test("equivalent non-deterministic UDF roots remain separate") {
    val spec = workerSpec("non-deterministic")
    val expression = Add(
      udf("duplicate", spec, Seq(input), deterministic = false),
      udf("duplicate", spec, Seq(input), deterministic = false))

    val nodes = evalNodes(extract(expression))
    assert(nodes.size == 2)
  }

  test("dependent branches use separate nodes") {
    val spec = workerSpec("branched")
    val left = udf("left", spec, Seq(input))
    val right = udf("right", spec, Seq(input))
    val expression = udf("outer", spec, Seq(Add(left, right)))

    val nodes = evalNodes(extract(expression))
    assert(nodes.size == 3)
    assert(nodes.forall(node => callCount(node.udf) == 1))
  }

  test("UDFs with different worker specifications use separate nodes") {
    val outerSpec = workerSpec("outer-worker")
    val innerSpec = workerSpec("inner-worker")
    val expression = udf("outer", outerSpec, Seq(udf("inner", innerSpec, Seq(input))))

    val nodes = evalNodes(extract(expression))
    assert(nodes.size == 2)
    assert(nodes.map(_.workerSpec).toSet == Set(outerSpec, innerSpec))
    assert(nodes.forall(node => callCount(node.udf) == 1))
  }

  test("external UDF input types are validated during analysis") {
    val spec = workerSpec("input-types")
    val validPlan = Project(Seq(Alias(
      udf("valid", spec, Seq(input), inputTypes = Some(Seq(IntegerType))), "result")()), relation)
    spark.sessionState.analyzer.executeAndCheck(validPlan, new QueryPlanningTracker)

    val invalidPlan = Project(Seq(Alias(
      udf("invalid", spec, Seq(input), inputTypes = Some(Seq(BooleanType))), "result")()), relation)
    val exception = intercept[AnalysisException] {
      spark.sessionState.analyzer.executeAndCheck(invalidPlan, new QueryPlanningTracker)
    }
    assert(exception.getCondition == "DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE")
  }

  test("an external UDF in a higher-order function lambda is rejected") {
    val spec = workerSpec("lambda")
    val lambdaVariable = NamedLambdaVariable("x", IntegerType, nullable = false)
    val transform = ArrayTransform(
      CreateArray(Seq(input)),
      LambdaFunction(
        udf("lambda", spec, Seq(lambdaVariable)),
        Seq(lambdaVariable)))
    val plan = Project(Seq(Alias(transform, "result")()), relation)

    val exception = intercept[AnalysisException] {
      spark.sessionState.analyzer.executeAndCheck(plan, new QueryPlanningTracker)
    }
    assert(exception.getCondition ==
      "UNSUPPORTED_FEATURE.LAMBDA_FUNCTION_WITH_EXTERNAL_UDF")
  }

  test("an external UDF over an aggregate expression is evaluated after aggregate") {
    val spec = workerSpec("aggregate")
    val sum = Sum(input).toAggregateExpression()
    val plan = Aggregate(
      groupingExpressions = Seq.empty,
      aggregateExpressions = Seq(Alias(udf("aggregate", spec, Seq(sum)), "result")()),
      child = relation)

    val nodes = evalNodes(extract(plan))
    assert(nodes.size == 1)
    assert(nodes.head.child.isInstanceOf[Aggregate])
  }

  test("a zero-argument non-deterministic UDF is evaluated after aggregate") {
    val spec = workerSpec("zero-argument")
    val plan = Aggregate(
      groupingExpressions = Seq.empty,
      aggregateExpressions = Seq(Alias(
        udf("zero-argument", spec, Seq.empty, deterministic = false), "result")()),
      child = relation)

    val extracted = extract(plan)
    val nodes = evalNodes(extracted)
    assert(nodes.size == 1)
    assert(nodes.head.child.isInstanceOf[Aggregate])
    assert(extracted.output == plan.output)
  }

  test("an external UDF used as a grouping key is evaluated before aggregate") {
    val spec = workerSpec("grouping")
    val plan = Aggregate(
      groupingExpressions = Seq(udf("grouping", spec, Seq(input))),
      aggregateExpressions = Seq(Alias(udf("grouping", spec, Seq(input)), "result")()),
      child = relation)

    val extracted = extract(plan)
    val aggregate = extracted.collectFirst { case node: Aggregate => node }.get
    assert(aggregate.child.collect { case node: ExecuteExternalUDF => node }.size == 1)
  }

  test("an external UDF over a window expression is evaluated after window") {
    val spec = workerSpec("over-window")
    val windowSpec = WindowSpecDefinition(Seq.empty, Seq.empty, UnspecifiedFrame)
    val windowExpression = WindowExpression(new Lag(input), windowSpec)
    val plan = Window(
      windowExpressions = Seq(Alias(udf("over-window", spec, Seq(windowExpression)), "result")()),
      partitionSpec = Seq.empty,
      orderSpec = Seq.empty,
      child = relation)

    val extracted = extract(plan)
    val evaluation = evalNodes(extracted).head
    assert(evaluation.child.isInstanceOf[Window])
    assert(!evaluation.udf.exists(_.isInstanceOf[WindowExpression]))
    assert(evaluation.udf.references.subsetOf(evaluation.child.outputSet))
  }

  test("an external UDF used by a window expression is evaluated before window") {
    val spec = workerSpec("under-window")
    val windowSpec = WindowSpecDefinition(Seq.empty, Seq.empty, UnspecifiedFrame)
    val windowExpression = WindowExpression(
      new Lag(udf("under-window", spec, Seq(input))),
      windowSpec)
    val plan = Window(
      windowExpressions = Seq(Alias(windowExpression, "result")()),
      partitionSpec = Seq.empty,
      orderSpec = Seq.empty,
      child = relation)

    val extracted = extract(plan)
    val window = extracted.collectFirst { case node: Window => node }.get
    val evaluation = window.child.asInstanceOf[ExecuteExternalUDF]
    assert(evaluation.child == relation)
  }

  test("an external UDF join condition referencing both sides is evaluated after join") {
    val spec = workerSpec("join")
    val rightInput = AttributeReference("right", IntegerType, nullable = false)()
    val right = LocalRelation(Seq(rightInput))
    val condition = udf("join", spec, Seq(input, rightInput), dataType = BooleanType)
    val plan = Join(relation, right, Inner, Some(condition), JoinHint.NONE)

    val extracted = extract(plan)
    val filter = extracted.collectFirst { case node: Filter => node }.get
    val evaluation = filter.child.asInstanceOf[ExecuteExternalUDF]
    val join = evaluation.child.asInstanceOf[Join]
    assert(join.condition.isEmpty)
  }

  test("an external UDF join condition rejects unsupported join types") {
    val spec = workerSpec("join")
    val rightInput = AttributeReference("right", IntegerType, nullable = false)()
    val right = LocalRelation(Seq(rightInput))
    val condition = udf("join", spec, Seq(input, rightInput), dataType = BooleanType)
    val plan = Join(relation, right, LeftOuter, Some(condition), JoinHint.NONE)

    val exception = intercept[AnalysisException] {
      extract(plan)
    }
    checkError(
      exception = exception,
      condition = "UNSUPPORTED_FEATURE.EXTERNAL_UDF_IN_ON_CLAUSE",
      parameters = Map("joinType" -> LeftOuter.sql))
  }

  test("an external UDF referencing multiple children reports an unsupported feature") {
    val spec = workerSpec("multiple-children")
    val rightInput = AttributeReference("right", IntegerType, nullable = false)()
    val right = LocalRelation(Seq(rightInput))
    val plan = TestBinaryPlan(
      udf("multiple-children", spec, Seq(input, rightInput)),
      relation,
      right)

    val exception = intercept[AnalysisException] {
      extract(plan)
    }
    checkError(
      exception = exception,
      condition = "UNSUPPORTED_FEATURE.EXTERNAL_UDF_WITH_MULTIPLE_CHILDREN",
      parameters = Map.empty)
  }

  test("external UDF expressions are rejected when unified execution is disabled") {
    val spec = workerSpec("disabled")
    val plan = Project(Seq(Alias(udf("external", spec, Seq(input)), "result")()), relation)

    val exception = withSQLConf(SQLConf.UNIFIED_UDF_EXECUTION_ENABLED.key -> "false") {
      intercept[SparkUnsupportedOperationException] {
        PlanExternalUDFs(plan)
      }
    }
    checkError(
      exception = exception,
      condition = "UNSUPPORTED_FEATURE.EXTERNAL_UDF",
      parameters = Map("config" -> SQLConf.UNIFIED_UDF_EXECUTION_ENABLED.key))
  }

  test("optimizer pushes a local limit through an external UDF node") {
    val spec = workerSpec("limit")
    val range = Range(0, 10, 1, 1)
    val function = udf("limit", spec, Seq(range.output.head))
    val resultAttr = AttributeReference("externalUDF", IntegerType, nullable = false)()
    val plan = LocalLimit(Literal(1), ExecuteExternalUDF(function, resultAttr, range))

    val optimized = optimize(plan)
    val evaluation = optimized.collectFirst { case node: ExecuteExternalUDF => node }.get
    assert(evaluation.child.isInstanceOf[LocalLimit])
  }

  test("strategy plans an external UDF execution node") {
    val spec = workerSpec("physical-planning")
    val range = Range(0, 10, 1, 1)
    val function = udf("physical-planning", spec, Seq(range.output.head))
    val resultAttr = AttributeReference("externalUDF", IntegerType, nullable = false)()
    val logicalPlan = ExecuteExternalUDF(function, resultAttr, range)

    val physicalPlan = spark.sessionState.planner.plan(logicalPlan).next()
    val execution = physicalPlan.asInstanceOf[ExecuteExternalUDFExec]
    assert(execution.udf == function)
    assert(execution.resultAttr == resultAttr)
    assert(execution.workerSpec == spec)
  }

  test("optimizer pushes child-only predicates through an external UDF node") {
    val spec = workerSpec("predicate")
    val range = Range(0, 10, 1, 1)
    val function = udf("predicate", spec, Seq(range.output.head))
    val resultAttr = AttributeReference("externalUDF", IntegerType, nullable = false)()
    val childPredicate = GreaterThan(range.output.head, Literal(0L))
    val resultPredicate = EqualTo(resultAttr, Literal(1))
    val plan = Filter(
      And(childPredicate, resultPredicate),
      ExecuteExternalUDF(function, resultAttr, range))

    val optimized = optimize(plan)
    val evaluation = optimized.collectFirst { case node: ExecuteExternalUDF => node }.get
    val pushedPredicate = evaluation.child.collectFirst {
      case Filter(condition, _) => condition
    }.get
    assert(pushedPredicate.semanticEquals(childPredicate))
  }

  test("optimizer checks cartesian products after external UDF extraction") {
    val spec = workerSpec("cartesian")
    val left = Range(0, 10, 1, 1)
    val right = Range(0, 10, 1, 1)
    val condition = udf(
      "cartesian",
      spec,
      Seq(left.output.head, right.output.head),
      dataType = BooleanType)
    val plan = Join(left, right, Inner, Some(condition), JoinHint.NONE)

    val exception = withSQLConf(
        SQLConf.UNIFIED_UDF_EXECUTION_ENABLED.key -> "true",
        SQLConf.CROSS_JOINS_ENABLED.key -> "false") {
      intercept[AnalysisException] {
        spark.sessionState.optimizer.execute(plan)
      }
    }
    assert(exception.getMessage.startsWith("Detected implicit cartesian product"))
  }
}
