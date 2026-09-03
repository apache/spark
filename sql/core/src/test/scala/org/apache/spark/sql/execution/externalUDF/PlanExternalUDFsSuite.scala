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
  AttributeReference, AttributeSet, CreateArray, EqualTo, Expression,
  ExternalUserDefinedFunction, GreaterThan, Lag, LambdaFunction, Literal, NamedLambdaVariable,
  UnspecifiedFrame, WindowExpression, WindowSpecDefinition}
import org.apache.spark.sql.catalyst.expressions.aggregate.Sum
import org.apache.spark.sql.catalyst.plans.{Inner, LeftOuter}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, BinaryNode, ExecuteExternalUDF,
  Filter, Join, JoinHint, LocalLimit, LocalRelation, LogicalPlan, MapPartitionsExternalUDF, Project,
  Range, Window}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{BooleanType, DataType, IntegerType, StructField, StructType}
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
  private def workerSpec(
      name: String,
      environmentVariables: Map[String, String] = Map.empty): UDFWorkerSpecification = {
    val runner = ProcessCallable.newBuilder().addCommand(name)
    environmentVariables.foreach { case (key, value) =>
      runner.putEnvironmentVariables(key, value)
    }
    val direct = DirectWorker.newBuilder()
      .setRunner(runner)
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
    val plan = Project(Seq(Alias(expression, "result")()), relation)
    val extracted = extract(plan)
    assert(extracted.output == plan.output)
    extracted
  }

  private def optimize(plan: LogicalPlan): LogicalPlan = {
    withSQLConf(SQLConf.UNIFIED_UDF_EXECUTION_ENABLED.key -> "true") {
      spark.sessionState.optimizer.execute(plan)
    }
  }

  private def evalNodes(plan: LogicalPlan): Seq[ExecuteExternalUDF] = {
    plan.collect { case eval: ExecuteExternalUDF => eval }
  }

  private def singleEvalNode(plan: LogicalPlan): ExecuteExternalUDF = {
    evalNodes(plan) match {
      case Seq(node) => node
      case nodes => fail(s"Expected one ExecuteExternalUDF node, found ${nodes.size}:\n$plan")
    }
  }

  private def singleOutput(plan: LogicalPlan): Attribute = {
    plan.output match {
      case Seq(attribute) => attribute
      case output => fail(s"Expected one output attribute, found ${output.size}: $output")
    }
  }

  private def singleProjectExpression(plan: LogicalPlan): Expression = {
    plan match {
      case Project(Seq(alias: Alias), _) => alias.child
      case other => fail(s"Expected a Project with one Alias, found:\n$other")
    }
  }

  private def normalizeRenderedExpressionIds(rendered: String): String = {
    rendered.replaceAll("#[0-9]+", "#x")
  }

  private def callCount(expression: Expression): Int = {
    expression.collect { case _: ExternalUserDefinedFunction => 1 }.size
  }

  test("nested UDF expressions use separate nodes") {
    val spec = workerSpec("nested")
    val expression = udf("outer", spec,
      Seq(udf("middle", spec, Seq(udf("inner", spec, Seq(input))))))

    val extracted = extract(expression)
    val nodes = evalNodes(extracted)
    nodes match {
      case Seq(outer, middle, inner) =>
        assert(outer.udf.children == Seq(middle.resultAttr))
        assert(middle.udf.children == Seq(inner.resultAttr))
        assert(inner.udf.children == Seq(input))
        assert(inner.child == relation)
        assert(singleProjectExpression(extracted).semanticEquals(outer.resultAttr))
        assert(Seq(outer, middle, inner).forall(node => callCount(node.udf) == 1))
      case _ =>
        fail(
          s"Expected outer, middle, and inner evaluation nodes, found:\n${nodes.mkString("\n")}")
    }
  }

  test("independent UDF expressions use separate nodes") {
    val spec = workerSpec("independent")
    val expression = Add(udf("left", spec, Seq(input)), udf("right", spec, Seq(input)))

    val extracted = extract(expression)
    val nodes = evalNodes(extracted)
    nodes match {
      case Seq(upper, lower) =>
        assert(upper.child == lower)
        assert(lower.child == relation)
        assert(nodes.forall(_.udf.children == Seq(input)))
        assert(nodes.forall(node => callCount(node.udf) == 1))
        val projectExpression = singleProjectExpression(extracted)
        assert(nodes.forall(node => projectExpression.references.contains(node.resultAttr)))
      case _ => fail(s"Expected two independent evaluation nodes, found:\n${nodes.mkString("\n")}")
    }
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

    val extracted = extract(expression)
    val nodes = evalNodes(extracted)
    nodes match {
      case Seq(outer, upperBranch, lowerBranch) =>
        assert(outer.child == upperBranch)
        assert(upperBranch.child == lowerBranch)
        assert(lowerBranch.child == relation)
        assert(outer.udf.references ==
          AttributeSet(Seq(upperBranch.resultAttr, lowerBranch.resultAttr)))
        assert(singleProjectExpression(extracted).semanticEquals(outer.resultAttr))
        assert(nodes.forall(node => callCount(node.udf) == 1))
      case _ => fail(s"Expected three branched evaluation nodes, found:\n${nodes.mkString("\n")}")
    }
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

  test("external UDF rendering hides worker execution details") {
    val sensitiveValue = "sensitive-worker-value"
    val spec = workerSpec("safe-rendering", Map("UDF_SECRET" -> sensitiveValue))
    val function = udf("safeRendering", spec, Seq(input))
    val resultAttr = AttributeReference("externalUDF", IntegerType, nullable = false)()
    val logicalPlan = ExecuteExternalUDF(function, resultAttr, relation)
    // Structured logging renders MDC values with toString, including join conditions.
    val loggedCondition = EqualTo(function, Literal(1)).toString
    val physicalPlan = spark.sessionState.planner.plan(logicalPlan).toSeq match {
      case Seq(node: ExecuteExternalUDFExec) => node
      case other =>
        fail(s"Expected one ExecuteExternalUDFExec node, found:\n${other.mkString("\n")}")
    }

    assert(function.sql == "safeRendering(input)")
    assert(normalizeRenderedExpressionIds(function.toString) ==
      "safeRendering(input#x)#x")
    assert(normalizeRenderedExpressionIds(loggedCondition) ==
      "(safeRendering(input#x)#x = 1)")
    assert(normalizeRenderedExpressionIds(logicalPlan.treeString) ==
      """ExecuteExternalUDF safeRendering(input#x)#x, externalUDF#x: int
        |+- LocalRelation <empty>, [input#x]
        |""".stripMargin)
    assert(normalizeRenderedExpressionIds(physicalPlan.treeString) ==
      """ExecuteExternalUDF safeRendering(input#x)#x, externalUDF#x: int
        |+- LocalTableScan <empty>, [input#x]
        |""".stripMargin)
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

  test("external UDF input types reject fewer declarations than arguments") {
    val spec = workerSpec("too-few-input-types")
    val plan = Project(Seq(Alias(
      udf(
        "tooFewInputTypes",
        spec,
        Seq(input, input),
        inputTypes = Some(Seq(IntegerType))),
      "result")()), relation)

    val exception = intercept[AnalysisException] {
      spark.sessionState.analyzer.executeAndCheck(plan, new QueryPlanningTracker)
    }
    checkError(
      exception = exception,
      condition = "WRONG_NUM_ARGS.WITHOUT_SUGGESTION",
      parameters = Map(
        "functionName" -> "`tooFewInputTypes`",
        "expectedNum" -> "1",
        "actualNum" -> "2",
        "docroot" -> "https://spark.apache.org/docs/latest"))
  }

  test("external UDF input types reject more declarations than arguments") {
    val spec = workerSpec("too-many-input-types")
    val plan = Project(Seq(Alias(
      udf(
        "tooManyInputTypes",
        spec,
        Seq(input),
        inputTypes = Some(Seq(IntegerType, IntegerType))),
      "result")()), relation)

    val exception = intercept[AnalysisException] {
      spark.sessionState.analyzer.executeAndCheck(plan, new QueryPlanningTracker)
    }
    checkError(
      exception = exception,
      condition = "WRONG_NUM_ARGS.WITHOUT_SUGGESTION",
      parameters = Map(
        "functionName" -> "`tooManyInputTypes`",
        "expectedNum" -> "2",
        "actualNum" -> "1",
        "docroot" -> "https://spark.apache.org/docs/latest"))
  }

  test("an external UDF in a higher-order function lambda is rejected") {
    val sensitiveValue = "sensitive-lambda-value"
    val spec = workerSpec("lambda", Map("UDF_SECRET" -> sensitiveValue))
    val lambdaVariable = NamedLambdaVariable("x", IntegerType, nullable = false)
    val function = udf("lambda", spec, Seq(lambdaVariable))
    val transform = ArrayTransform(
      CreateArray(Seq(input)),
      LambdaFunction(
        function,
        Seq(lambdaVariable)))
    val plan = Project(Seq(Alias(transform, "result")()), relation)

    val exception = intercept[AnalysisException] {
      spark.sessionState.analyzer.executeAndCheck(plan, new QueryPlanningTracker)
    }
    checkError(
      exception = exception,
      condition = "UNSUPPORTED_FEATURE.LAMBDA_FUNCTION_WITH_EXTERNAL_UDF",
      parameters = Map("funcName" -> ("\"" + function.sql + "\"")))
    assert(!exception.getMessage.contains(sensitiveValue))
  }

  test("an external UDF over an aggregate expression is evaluated after aggregate") {
    val spec = workerSpec("aggregate")
    val sum = Sum(input).toAggregateExpression()
    val plan = Aggregate(
      groupingExpressions = Seq.empty,
      aggregateExpressions = Seq(Alias(udf("aggregate", spec, Seq(sum)), "result")()),
      child = relation)

    val evaluation = singleEvalNode(extract(plan))
    assert(evaluation.child.isInstanceOf[Aggregate])
    assert(evaluation.udf.references.subsetOf(evaluation.child.outputSet))
    assert(!evaluation.udf.exists(_.isInstanceOf[Sum]))
  }

  test("a zero-argument non-deterministic UDF is evaluated after aggregate") {
    val spec = workerSpec("zero-argument")
    val plan = Aggregate(
      groupingExpressions = Seq.empty,
      aggregateExpressions = Seq(Alias(
        udf("zero-argument", spec, Seq.empty, deterministic = false), "result")()),
      child = relation)

    val extracted = extract(plan)
    val evaluation = singleEvalNode(extracted)
    assert(evaluation.child.isInstanceOf[Aggregate])
    assert(extracted.output == plan.output)
  }

  test("an external UDF used as a grouping key is evaluated before aggregate") {
    val spec = workerSpec("grouping")
    val plan = Aggregate(
      groupingExpressions = Seq(udf("grouping", spec, Seq(input))),
      aggregateExpressions = Seq(Alias(udf("grouping", spec, Seq(input)), "result")()),
      child = relation)

    val extracted = extract(plan)
    val aggregate = extracted.collectFirst { case node: Aggregate => node }.getOrElse {
      fail(s"Expected an Aggregate node, found:\n$extracted")
    }
    val groupingProjection = aggregate.child match {
      case project: Project => project
      case other => fail(s"Expected a Project below Aggregate, found:\n$other")
    }
    val evaluation = singleEvalNode(groupingProjection)
    assert(groupingProjection.projectList.exists {
      case alias: Alias =>
        aggregate.groupingExpressions.exists(_.semanticEquals(alias.toAttribute)) &&
          alias.child.semanticEquals(evaluation.resultAttr)
      case _ => false
    })
    assert(!aggregate.aggregateExpressions.exists(
      _.exists(_.isInstanceOf[ExternalUserDefinedFunction])))
  }

  test("a non-deterministic external UDF grouping key is evaluated before aggregate") {
    val spec = workerSpec("non-deterministic-grouping")
    val function = udf("non-deterministic-grouping", spec, Seq(input), deterministic = false)
    val plan = Aggregate(
      groupingExpressions = Seq(function),
      aggregateExpressions = Seq(Alias(function, "result")()),
      child = relation)

    val analyzed = withSQLConf(SQLConf.UNIFIED_UDF_EXECUTION_ENABLED.key -> "true") {
      spark.sessionState.analyzer.executeAndCheck(plan, new QueryPlanningTracker)
    }
    val analyzedAggregate = analyzed.collectFirst { case node: Aggregate => node }.getOrElse {
      fail(s"Expected an Aggregate node, found:\n$analyzed")
    }
    assert(analyzedAggregate.groupingExpressions.forall(_.deterministic))

    val extracted = extract(analyzed)
    val aggregate = extracted.collectFirst { case node: Aggregate => node }.getOrElse {
      fail(s"Expected an Aggregate node, found:\n$extracted")
    }
    val evaluation = singleEvalNode(aggregate.child)
    assert(!evaluation.udf.deterministic)
    assert(evaluation.child == relation)
    assert(aggregate.groupingExpressions.forall(_.deterministic))
    assert(!aggregate.expressions.exists(
      _.exists(_.isInstanceOf[ExternalUserDefinedFunction])))
  }

  test("an external UDF over a window expression is evaluated after window") {
    val spec = workerSpec("over-window")
    val windowSpec = WindowSpecDefinition(Seq.empty, Seq.empty, UnspecifiedFrame)
    val windowExpression = WindowExpression(new Lag(input), windowSpec)
    val plan = Window(
      windowExpressions = Seq(Alias(
        udf("over-window", spec, Seq(windowExpression, input)), "result")()),
      partitionSpec = Seq.empty,
      orderSpec = Seq.empty,
      child = relation)

    val extracted = extract(plan)
    val evaluation = singleEvalNode(extracted)
    val window = evaluation.child match {
      case node: Window => node
      case other => fail(s"Expected a Window node, found:\n$other")
    }
    val windowOutputExprIds =
      window.child.output.map(_.exprId) ++ window.windowExpressions.map(_.exprId)
    assert(windowOutputExprIds.distinct.size == windowOutputExprIds.size)
    assert(evaluation.udf.children.size == 2)
    assert(evaluation.udf.children.last.semanticEquals(input))
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
    val window = extracted.collectFirst { case node: Window => node }.getOrElse {
      fail(s"Expected a Window node, found:\n$extracted")
    }
    val evaluation = window.child match {
      case node: ExecuteExternalUDF => node
      case other => fail(s"Expected ExecuteExternalUDF below Window, found:\n$other")
    }
    assert(evaluation.child == relation)
  }

  test("an external UDF join condition referencing both sides is evaluated after join") {
    val spec = workerSpec("join")
    val rightInput = AttributeReference("right", IntegerType, nullable = false)()
    val right = LocalRelation(Seq(rightInput))
    val condition = udf("join", spec, Seq(input, rightInput), dataType = BooleanType)
    val plan = Join(relation, right, Inner, Some(condition), JoinHint.NONE)

    val extracted = extract(plan)
    val filter = extracted.collectFirst { case node: Filter => node }.getOrElse {
      fail(s"Expected a Filter node, found:\n$extracted")
    }
    val evaluation = filter.child match {
      case node: ExecuteExternalUDF => node
      case other => fail(s"Expected ExecuteExternalUDF below Filter, found:\n$other")
    }
    val join = evaluation.child match {
      case node: Join => node
      case other => fail(s"Expected Join below ExecuteExternalUDF, found:\n$other")
    }
    assert(join.condition.isEmpty)
    assert(filter.condition.semanticEquals(evaluation.resultAttr))
  }

  test("an external UDF is moved out of a mixed join condition") {
    val spec = workerSpec("mixed-join")
    val rightInput = AttributeReference("right", IntegerType, nullable = false)()
    val right = LocalRelation(Seq(rightInput))
    val equality = EqualTo(input, rightInput)
    val function = udf("mixed-join", spec, Seq(input, rightInput), dataType = BooleanType)
    val plan = Join(relation, right, Inner, Some(And(equality, function)), JoinHint.NONE)

    val extracted = extract(plan)
    val filter = extracted.collectFirst { case node: Filter => node }.getOrElse {
      fail(s"Expected a Filter node, found:\n$extracted")
    }
    val evaluation = filter.child match {
      case node: ExecuteExternalUDF => node
      case other => fail(s"Expected ExecuteExternalUDF below Filter, found:\n$other")
    }
    val join = evaluation.child match {
      case node: Join => node
      case other => fail(s"Expected Join below ExecuteExternalUDF, found:\n$other")
    }
    assert(join.condition.exists(_.semanticEquals(equality)))
    assert(filter.condition.semanticEquals(evaluation.resultAttr))
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
      parameters = Map("funcName" -> "\"multiple-children(input, right)\""))
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
      parameters = Map(
        "config" -> ("\"" + SQLConf.UNIFIED_UDF_EXECUTION_ENABLED.key + "\"")))
  }

  test("optimizer pushes a local limit through an external UDF node") {
    val spec = workerSpec("limit")
    val range = Range(0, 10, 1, 1)
    val function = udf("limit", spec, Seq(singleOutput(range)))
    val resultAttr = AttributeReference("externalUDF", IntegerType, nullable = false)()
    val plan = LocalLimit(Literal(1), ExecuteExternalUDF(function, resultAttr, range))

    val optimized = optimize(plan)
    val evaluation = singleEvalNode(optimized)
    assert(evaluation.child.isInstanceOf[LocalLimit])
  }

  test("strategy plans an external UDF execution node") {
    val spec = workerSpec("physical-planning")
    val range = Range(0, 10, 1, 1)
    val function = udf("physical-planning", spec, Seq(singleOutput(range)))
    val resultAttr = AttributeReference("externalUDF", IntegerType, nullable = false)()
    val logicalPlan = ExecuteExternalUDF(function, resultAttr, range)

    val execution = spark.sessionState.planner.plan(logicalPlan).toSeq match {
      case Seq(node: ExecuteExternalUDFExec) => node
      case other =>
        fail(s"Expected one ExecuteExternalUDFExec node, found:\n${other.mkString("\n")}")
    }
    assert(execution.udf == function)
    assert(execution.resultAttr == resultAttr)
    assert(execution.workerSpec == spec)
  }

  test("scalar external UDF execution reports unimplemented before worker startup") {
    val spec = workerSpec("unimplemented-scalar")
    val range = Range(0, 10, 1, 1)
    val function = udf("unimplemented-scalar", spec, Seq(singleOutput(range)))
    val resultAttr = AttributeReference("externalUDF", IntegerType, nullable = false)()
    val logicalPlan = ExecuteExternalUDF(function, resultAttr, range)
    val execution = spark.sessionState.planner.plan(logicalPlan).toSeq match {
      case Seq(node: ExecuteExternalUDFExec) => node
      case other =>
        fail(s"Expected one ExecuteExternalUDFExec node, found:\n${other.mkString("\n")}")
    }

    val exception = intercept[SparkUnsupportedOperationException] {
      execution.execute()
    }
    checkError(
      exception = exception,
      condition = "_LEGACY_ERROR_TEMP_2041",
      parameters = Map("methodName" -> "ExecuteExternalUDFExec.doExecute"))
  }

  test("map partitions external UDF execution reports unimplemented before worker startup") {
    val spec = workerSpec("unimplemented-map-partitions")
    val range = Range(0, 10, 1, 1)
    val outputType = StructType(Seq(StructField("result", IntegerType)))
    val function = udf(
      "unimplemented-map-partitions",
      spec,
      Seq.empty,
      dataType = outputType)
    val logicalPlan = MapPartitionsExternalUDF(
      function,
      isBarrier = false,
      profile = None,
      child = range)
    val execution = spark.sessionState.planner.plan(logicalPlan).toSeq match {
      case Seq(node: MapPartitionsExternalUDFExec) => node
      case other =>
        fail(s"Expected one MapPartitionsExternalUDFExec node, found:\n${other.mkString("\n")}")
    }

    val exception = intercept[SparkUnsupportedOperationException] {
      execution.execute()
    }
    checkError(
      exception = exception,
      condition = "_LEGACY_ERROR_TEMP_2041",
      parameters = Map("methodName" -> "MapPartitionsExternalUDFExec.doExecute"))
  }

  test("optimizer pushes child-only predicates through an external UDF node") {
    val spec = workerSpec("predicate")
    val range = Range(0, 10, 1, 1)
    val inputAttribute = singleOutput(range)
    val function = udf("predicate", spec, Seq(inputAttribute))
    val resultAttr = AttributeReference("externalUDF", IntegerType, nullable = false)()
    val childPredicate = GreaterThan(inputAttribute, Literal(0L))
    val resultPredicate = EqualTo(resultAttr, Literal(1))
    val plan = Filter(
      And(childPredicate, resultPredicate),
      ExecuteExternalUDF(function, resultAttr, range))

    val optimized = optimize(plan)
    val evaluation = singleEvalNode(optimized)
    val pushedPredicate = evaluation.child match {
      case Filter(condition, _) => condition
      case other => fail(s"Expected Filter below ExecuteExternalUDF, found:\n$other")
    }
    assert(pushedPredicate.semanticEquals(childPredicate))
  }

  test("optimizer checks cartesian products after external UDF extraction") {
    val spec = workerSpec("cartesian")
    val left = Range(0, 10, 1, 1)
    val right = Range(0, 10, 1, 1)
    val condition = udf(
      "cartesian",
      spec,
      Seq(singleOutput(left), singleOutput(right)),
      dataType = BooleanType)
    val plan = Join(left, right, Inner, Some(condition), JoinHint.NONE)

    val exception = withSQLConf(
        SQLConf.UNIFIED_UDF_EXECUTION_ENABLED.key -> "true",
        SQLConf.CROSS_JOINS_ENABLED.key -> "false") {
      intercept[AnalysisException] {
        spark.sessionState.optimizer.execute(plan)
      }
    }
    checkError(
      exception = exception,
      condition = "_LEGACY_ERROR_TEMP_1211",
      parameters = Map(
        "joinType" -> Inner.sql,
        "leftPlan" -> "Range (0, 10, step=1, splits=Some(1))",
        "rightPlan" -> "Range (0, 10, step=1, splits=Some(1))"))
  }
}
