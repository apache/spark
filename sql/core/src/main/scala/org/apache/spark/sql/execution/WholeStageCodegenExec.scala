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

package org.apache.spark.sql.execution

import java.io.Writer
import java.util.Locale

import scala.collection.mutable
import scala.util.control.NonFatal

import org.apache.spark.broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.expressions.codegen.Block._
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.aggregate.HashAggregateExec
import org.apache.spark.sql.execution.columnar.InMemoryTableScanExec
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, SortMergeJoinExec}
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.util.Utils

/**
 * An interface for those physical operators that support codegen.
 */
trait CodegenSupport extends SparkPlan {

  /** Prefix used in the current operator's variable names. */
  private def variablePrefix: String = this match {
    case _: HashAggregateExec => "agg"
    case _: BroadcastHashJoinExec => "bhj"
    case _: SortMergeJoinExec => "smj"
    case _: RDDScanExec => "rdd"
    case _: DataSourceScanExec => "scan"
    case _: InMemoryTableScanExec => "memoryScan"
    case _ => nodeName.toLowerCase(Locale.ROOT)
  }

  /**
   * Creates a metric using the specified name.
   *
   * @return name of the variable representing the metric
   */
  def metricTerm(ctx: CodegenContext, name: String): String = {
    ctx.addReferenceObj(name, longMetric(name))
  }

  /**
   * Whether this SparkPlan supports whole stage codegen or not.
   */
  def supportCodegen: Boolean = true

  /**
   * Which SparkPlan is calling produce() of this one. It's itself for the first SparkPlan.
   */
  protected var parent: CodegenSupport = null

  /**
   * Returns all the RDDs of InternalRow which generates the input rows.
   *
   * @note Right now we support up to two RDDs
   */
  def inputRDDs(): Seq[RDD[InternalRow]]

  /**
   * Returns Java source code to process the rows from input RDD.
   */
  final def produce(ctx: CodegenContext, parent: CodegenSupport): String = executeQuery {
    this.parent = parent
    ctx.freshNamePrefix = variablePrefix
    s"""
       |${ctx.registerComment(s"PRODUCE: ${this.simpleString(SQLConf.get.maxToStringFields)}")}
       |${doProduce(ctx)}
     """.stripMargin
  }

  /**
   * Generate the Java source code to process, should be overridden by subclass to support codegen.
   *
   * doProduce() usually generate the framework, for example, aggregation could generate this:
   *
   *   if (!initialized) {
   *     # create a hash map, then build the aggregation hash map
   *     # call child.produce()
   *     initialized = true;
   *   }
   *   while (hashmap.hasNext()) {
   *     row = hashmap.next();
   *     # build the aggregation results
   *     # create variables for results
   *     # call consume(), which will call parent.doConsume()
   *      if (shouldStop()) return;
   *   }
   */
  protected def doProduce(ctx: CodegenContext): String

  private def prepareRowVar(ctx: CodegenContext, row: String, colVars: Seq[ExprCode]): ExprCode = {
    if (row != null) {
      ExprCode.forNonNullValue(JavaCode.variable(row, classOf[UnsafeRow]))
    } else {
      if (colVars.nonEmpty) {
        val colExprs = output.zipWithIndex.map { case (attr, i) =>
          BoundReference(i, attr.dataType, attr.nullable)
        }
        val evaluateInputs = evaluateVariables(colVars)
        // generate the code to create a UnsafeRow
        ctx.INPUT_ROW = row
        ctx.currentVars = colVars
        val ev = GenerateUnsafeProjection.createCode(ctx, colExprs, false)
        val code = code"""
          |$evaluateInputs
          |${ev.code}
         """.stripMargin
        ExprCode(code, FalseLiteral, ev.value)
      } else {
        // There are no columns
        ExprCode.forNonNullValue(JavaCode.variable("unsafeRow", classOf[UnsafeRow]))
      }
    }
  }

  /**
   * Consume the generated columns or row from current SparkPlan, call its parent's `doConsume()`.
   *
   * Note that `outputVars` and `row` can't both be null.
   */
  final def consume(ctx: CodegenContext, outputVars: Seq[ExprCode], row: String = null): String = {
    val inputVarsCandidate =
      if (outputVars != null) {
        assert(outputVars.length == output.length)
        // outputVars will be used to generate the code for UnsafeRow, so we should copy them
        outputVars.map(_.copy())
      } else {
        assert(row != null, "outputVars and row cannot both be null.")
        ctx.currentVars = null
        ctx.INPUT_ROW = row
        output.zipWithIndex.map { case (attr, i) =>
          BoundReference(i, attr.dataType, attr.nullable).genCode(ctx)
        }
      }

    val inputVars = inputVarsCandidate match {
      case stream: Stream[ExprCode] => stream.force
      case other => other
    }

    val rowVar = prepareRowVar(ctx, row, outputVars)

    // Set up the `currentVars` in the codegen context, as we generate the code of `inputVars`
    // before calling `parent.doConsume`. We can't set up `INPUT_ROW`, because parent needs to
    // generate code of `rowVar` manually.
    ctx.currentVars = inputVars
    ctx.INPUT_ROW = null
    ctx.freshNamePrefix = parent.variablePrefix
    val evaluated = evaluateRequiredVariables(output, inputVars, parent.usedInputs)

    // Under certain conditions, we can put the logic to consume the rows of this operator into
    // another function. So we can prevent a generated function too long to be optimized by JIT.
    // The conditions:
    // 1. The config "spark.sql.codegen.splitConsumeFuncByOperator" is enabled.
    // 2. `inputVars` are all materialized. That is guaranteed to be true if the parent plan uses
    //    all variables in output (see `requireAllOutput`).
    // 3. The number of output variables must less than maximum number of parameters in Java method
    //    declaration.
    val confEnabled = SQLConf.get.wholeStageSplitConsumeFuncByOperator
    val requireAllOutput = output.forall(parent.usedInputs.contains(_))
    val paramLength = CodeGenerator.calculateParamLength(output) + (if (row != null) 1 else 0)
    val consumeFunc = if (confEnabled && requireAllOutput
        && CodeGenerator.isValidParamLength(paramLength)) {
      constructDoConsumeFunction(ctx, inputVars, row)
    } else {
      parent.doConsume(ctx, inputVars, rowVar)
    }
    s"""
       |${ctx.registerComment(s"CONSUME: ${parent.simpleString(SQLConf.get.maxToStringFields)}")}
       |$evaluated
       |$consumeFunc
     """.stripMargin
  }

  /**
   * To prevent concatenated function growing too long to be optimized by JIT. We can separate the
   * parent's `doConsume` codes of a `CodegenSupport` operator into a function to call.
   */
  private def constructDoConsumeFunction(
      ctx: CodegenContext,
      inputVars: Seq[ExprCode],
      row: String): String = {
    val (args, params, inputVarsInFunc) = constructConsumeParameters(ctx, output, inputVars, row)
    val rowVar = prepareRowVar(ctx, row, inputVarsInFunc)

    val doConsume = ctx.freshName("doConsume")
    ctx.currentVars = inputVarsInFunc
    ctx.INPUT_ROW = null

    val doConsumeFuncName = ctx.addNewFunction(doConsume,
      s"""
         | private void $doConsume(${params.mkString(", ")}) throws java.io.IOException {
         |   ${parent.doConsume(ctx, inputVarsInFunc, rowVar)}
         | }
       """.stripMargin)

    s"""
       | $doConsumeFuncName(${args.mkString(", ")});
     """.stripMargin
  }

  /**
   * Returns arguments for calling method and method definition parameters of the consume function.
   * And also returns the list of `ExprCode` for the parameters.
   */
  private def constructConsumeParameters(
      ctx: CodegenContext,
      attributes: Seq[Attribute],
      variables: Seq[ExprCode],
      row: String): (Seq[String], Seq[String], Seq[ExprCode]) = {
    val arguments = mutable.ArrayBuffer[String]()
    val parameters = mutable.ArrayBuffer[String]()
    val paramVars = mutable.ArrayBuffer[ExprCode]()

    if (row != null) {
      arguments += row
      parameters += s"InternalRow $row"
    }

    variables.zipWithIndex.foreach { case (ev, i) =>
      val paramName = ctx.freshName(s"expr_$i")
      val paramType = CodeGenerator.javaType(attributes(i).dataType)

      arguments += ev.value
      parameters += s"$paramType $paramName"
      val paramIsNull = if (!attributes(i).nullable) {
        // Use constant `false` without passing `isNull` for non-nullable variable.
        FalseLiteral
      } else {
        val isNull = ctx.freshName(s"exprIsNull_$i")
        arguments += ev.isNull
        parameters += s"boolean $isNull"
        JavaCode.isNullVariable(isNull)
      }

      paramVars += ExprCode(paramIsNull, JavaCode.variable(paramName, attributes(i).dataType))
    }
    (arguments, parameters, paramVars)
  }

  /**
   * Returns source code to evaluate all the variables, and clear the code of them, to prevent
   * them to be evaluated twice.
   */
  protected def evaluateVariables(variables: Seq[ExprCode]): String = {
    val evaluate = variables.filter(_.code.nonEmpty).map(_.code.toString).mkString("\n")
    variables.foreach(_.code = EmptyBlock)
    evaluate
  }

  /**
   * Returns source code to evaluate the variables for required attributes, and clear the code
   * of evaluated variables, to prevent them to be evaluated twice.
   */
  protected def evaluateRequiredVariables(
      attributes: Seq[Attribute],
      variables: Seq[ExprCode],
      required: AttributeSet): String = {
    val evaluateVars = new StringBuilder
    variables.zipWithIndex.foreach { case (ev, i) =>
      if (ev.code.nonEmpty && required.contains(attributes(i))) {
        evaluateVars.append(ev.code.toString + "\n")
        ev.code = EmptyBlock
      }
    }
    evaluateVars.toString()
  }

  /**
   * Returns source code to evaluate the variables for non-deterministic expressions, and clear the
   * code of evaluated variables, to prevent them to be evaluated twice.
   */
  protected def evaluateNondeterministicVariables(
      attributes: Seq[Attribute],
      variables: Seq[ExprCode],
      expressions: Seq[NamedExpression]): String = {
    val nondeterministicAttrs = expressions.filterNot(_.deterministic).map(_.toAttribute)
    evaluateRequiredVariables(attributes, variables, AttributeSet(nondeterministicAttrs))
  }

  /**
   * The subset of inputSet those should be evaluated before this plan.
   *
   * We will use this to insert some code to access those columns that are actually used by current
   * plan before calling doConsume().
   */
  def usedInputs: AttributeSet = references

  /**
   * Generate the Java source code to process the rows from child SparkPlan. This should only be
   * called from `consume`.
   *
   * This should be override by subclass to support codegen.
   *
   * Note: The operator should not assume the existence of an outer processing loop,
   *       which it can jump from with "continue;"!
   *
   * For example, filter could generate this:
   *   # code to evaluate the predicate expression, result is isNull1 and value2
   *   if (!isNull1 && value2) {
   *     # call consume(), which will call parent.doConsume()
   *   }
   *
   * Note: A plan can either consume the rows as UnsafeRow (row), or a list of variables (input).
   *       When consuming as a listing of variables, the code to produce the input is already
   *       generated and `CodegenContext.currentVars` is already set. When consuming as UnsafeRow,
   *       implementations need to put `row.code` in the generated code and set
   *       `CodegenContext.INPUT_ROW` manually. Some plans may need more tweaks as they have
   *       different inputs(join build side, aggregate buffer, etc.), or other special cases.
   */
  def doConsume(ctx: CodegenContext, input: Seq[ExprCode], row: ExprCode): String = {
    throw new UnsupportedOperationException
  }

  /**
   * Whether or not the result rows of this operator should be copied before putting into a buffer.
   *
   * If any operator inside WholeStageCodegen generate multiple rows from a single row (for
   * example, Join), this should be true.
   *
   * If an operator starts a new pipeline, this should be false.
   */
  def needCopyResult: Boolean = {
    if (children.isEmpty) {
      false
    } else if (children.length == 1) {
      children.head.asInstanceOf[CodegenSupport].needCopyResult
    } else {
      throw new UnsupportedOperationException
    }
  }

  /**
   * Whether or not the children of this operator should generate a stop check when consuming input
   * rows. This is used to suppress shouldStop() in a loop of WholeStageCodegen.
   *
   * This should be false if an operator starts a new pipeline, which means it consumes all rows
   * produced by children but doesn't output row to buffer by calling append(),  so the children
   * don't require shouldStop() in the loop of producing rows.
   */
  def needStopCheck: Boolean = parent.needStopCheck

  /**
   * Helper default should stop check code.
   */
  def shouldStopCheckCode: String = if (needStopCheck) {
    "if (shouldStop()) return;"
  } else {
    "// shouldStop check is eliminated"
  }

  /**
   * A sequence of checks which evaluate to true if the downstream Limit operators have not received
   * enough records and reached the limit. If current node is a data producing node, it can leverage
   * this information to stop producing data and complete the data flow earlier. Common data
   * producing nodes are leaf nodes like Range and Scan, and blocking nodes like Sort and Aggregate.
   * These checks should be put into the loop condition of the data producing loop.
   */
  def limitNotReachedChecks: Seq[String] = parent.limitNotReachedChecks

  /**
   * Check if the node is supposed to produce limit not reached checks.
   */
  protected def canCheckLimitNotReached: Boolean = children.isEmpty

  /**
   * A helper method to generate the data producing loop condition according to the
   * limit-not-reached checks.
   */
  final def limitNotReachedCond: String = {
    if (!canCheckLimitNotReached) {
      val errMsg = "Only leaf nodes and blocking nodes need to call 'limitNotReachedCond' " +
        "in its data producing loop."
      if (Utils.isTesting) {
        throw new IllegalStateException(errMsg)
      } else {
        logWarning(s"[BUG] $errMsg Please open a JIRA ticket to report it.")
      }
    }
    if (parent.limitNotReachedChecks.isEmpty) {
      ""
    } else {
      parent.limitNotReachedChecks.mkString("", " && ", " &&")
    }
  }
}

/**
 * A special kind of operators which support whole stage codegen. Blocking means these operators
 * will consume all the inputs first, before producing output. Typical blocking operators are
 * sort and aggregate.
 */
trait BlockingOperatorWithCodegen extends CodegenSupport {

  // Blocking operators usually have some kind of buffer to keep the data before producing them, so
  // then don't to copy its result even if its child does.
  override def needCopyResult: Boolean = false

  // Blocking operators always consume all the input first, so its upstream operators don't need a
  // stop check.
  override def needStopCheck: Boolean = false

  // Blocking operators need to consume all the inputs before producing any output. This means,
  // Limit operator after this blocking operator will never reach its limit during the execution of
  // this blocking operator's upstream operators. Here we override this method to return Nil, so
  // that upstream operators will not generate useless conditions (which are always evaluated to
  // false) for the Limit operators after this blocking operator.
  override def limitNotReachedChecks: Seq[String] = Nil

  // This is a blocking node so the node can produce these checks
  override protected def canCheckLimitNotReached: Boolean = true
}

/**
 * Leaf codegen node reading from a single RDD.
 */
trait InputRDDCodegen extends CodegenSupport {

  def inputRDD: RDD[InternalRow]

  // If the input can be InternalRows, an UnsafeProjection needs to be created.
  protected val createUnsafeProjection: Boolean

  override def inputRDDs(): Seq[RDD[InternalRow]] = {
    inputRDD :: Nil
  }

  override def doProduce(ctx: CodegenContext): String = {
    // Inline mutable state since an InputRDDCodegen is used once in a task for WholeStageCodegen
    val input = ctx.addMutableState("scala.collection.Iterator", "input", v => s"$v = inputs[0];",
      forceInline = true)
    val row = ctx.freshName("row")

    val outputVars = if (createUnsafeProjection) {
      // creating the vars will make the parent consume add an unsafe projection.
      ctx.INPUT_ROW = row
      ctx.currentVars = null
      output.zipWithIndex.map { case (a, i) =>
        BoundReference(i, a.dataType, a.nullable).genCode(ctx)
      }
    } else {
      null
    }

    val updateNumOutputRowsMetrics = if (metrics.contains("numOutputRows")) {
      val numOutputRows = metricTerm(ctx, "numOutputRows")
      s"$numOutputRows.add(1);"
    } else {
      ""
    }
    s"""
       | while ($limitNotReachedCond $input.hasNext()) {
       |   InternalRow $row = (InternalRow) $input.next();
       |   ${updateNumOutputRowsMetrics}
       |   ${consume(ctx, outputVars, if (createUnsafeProjection) null else row).trim}
       |   ${shouldStopCheckCode}
       | }
     """.stripMargin
  }
}

/**
 * InputAdapter is used to hide a SparkPlan from a subtree that supports codegen.
 *
 * This is the leaf node of a tree with WholeStageCodegen that is used to generate code
 * that consumes an RDD iterator of InternalRow.
 */
case class InputAdapter(child: SparkPlan) extends UnaryExecNode with InputRDDCodegen {

  override def output: Seq[Attribute] = child.output

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  override def doExecute(): RDD[InternalRow] = {
    child.execute()
  }

  override def doExecuteBroadcast[T](): broadcast.Broadcast[T] = {
    child.doExecuteBroadcast()
  }

  override def inputRDD: RDD[InternalRow] = child.execute()

  // This is a leaf node so the node can produce limit not reached checks.
  override protected def canCheckLimitNotReached: Boolean = true

  // InputAdapter does not need UnsafeProjection.
  protected val createUnsafeProjection: Boolean = false

  override def generateTreeString(
      depth: Int,
      lastChildren: Seq[Boolean],
      append: String => Unit,
      verbose: Boolean,
      prefix: String = "",
      addSuffix: Boolean = false,
      maxFields: Int): Unit = {
    child.generateTreeString(
      depth,
      lastChildren,
      append,
      verbose,
      prefix = "",
      addSuffix = false,
      maxFields)
  }

  override def needCopyResult: Boolean = false
}

object WholeStageCodegenExec {
  val PIPELINE_DURATION_METRIC = "duration"

  private def numOfNestedFields(dataType: DataType): Int = dataType match {
    case dt: StructType => dt.fields.map(f => numOfNestedFields(f.dataType)).sum
    case m: MapType => numOfNestedFields(m.keyType) + numOfNestedFields(m.valueType)
    case a: ArrayType => numOfNestedFields(a.elementType)
    case u: UserDefinedType[_] => numOfNestedFields(u.sqlType)
    case _ => 1
  }

  def isTooManyFields(conf: SQLConf, dataType: DataType): Boolean = {
    numOfNestedFields(dataType) > conf.wholeStageMaxNumFields
  }
}

object WholeStageCodegenId {
  // codegenStageId: ID for codegen stages within a query plan.
  // It does not affect equality, nor does it participate in destructuring pattern matching
  // of WholeStageCodegenExec.
  //
  // This ID is used to help differentiate between codegen stages. It is included as a part
  // of the explain output for physical plans, e.g.
  //
  // == Physical Plan ==
  // *(5) SortMergeJoin [x#3L], [y#9L], Inner
  // :- *(2) Sort [x#3L ASC NULLS FIRST], false, 0
  // :  +- Exchange hashpartitioning(x#3L, 200)
  // :     +- *(1) Project [(id#0L % 2) AS x#3L]
  // :        +- *(1) Filter isnotnull((id#0L % 2))
  // :           +- *(1) Range (0, 5, step=1, splits=8)
  // +- *(4) Sort [y#9L ASC NULLS FIRST], false, 0
  //    +- Exchange hashpartitioning(y#9L, 200)
  //       +- *(3) Project [(id#6L % 2) AS y#9L]
  //          +- *(3) Filter isnotnull((id#6L % 2))
  //             +- *(3) Range (0, 5, step=1, splits=8)
  //
  // where the ID makes it obvious that not all adjacent codegen'd plan operators are of the
  // same codegen stage.
  //
  // The codegen stage ID is also optionally included in the name of the generated classes as
  // a suffix, so that it's easier to associate a generated class back to the physical operator.
  // This is controlled by SQLConf: spark.sql.codegen.useIdInClassName
  //
  // The ID is also included in various log messages.
  //
  // Within a query, a codegen stage in a plan starts counting from 1, in "insertion order".
  // WholeStageCodegenExec operators are inserted into a plan in depth-first post-order.
  // See CollapseCodegenStages.insertWholeStageCodegen for the definition of insertion order.
  //
  // 0 is reserved as a special ID value to indicate a temporary WholeStageCodegenExec object
  // is created, e.g. for special fallback handling when an existing WholeStageCodegenExec
  // failed to generate/compile code.

  private val codegenStageCounter: ThreadLocal[Integer] = ThreadLocal.withInitial(() => 1)

  def resetPerQuery(): Unit = codegenStageCounter.set(1)

  def getNextStageId(): Int = {
    val counter = codegenStageCounter
    val id = counter.get()
    counter.set(id + 1)
    id
  }
}

/**
 * WholeStageCodegen compiles a subtree of plans that support codegen together into single Java
 * function.
 *
 * Here is the call graph of to generate Java source (plan A supports codegen, but plan B does not):
 *
 *   WholeStageCodegen       Plan A               FakeInput        Plan B
 * =========================================================================
 *
 * -> execute()
 *     |
 *  doExecute() --------->   inputRDDs() -------> inputRDDs() ------> execute()
 *     |
 *     +----------------->   produce()
 *                             |
 *                          doProduce()  -------> produce()
 *                                                   |
 *                                                doProduce()
 *                                                   |
 *                         doConsume() <--------- consume()
 *                             |
 *  doConsume()  <--------  consume()
 *
 * SparkPlan A should override `doProduce()` and `doConsume()`.
 *
 * `doCodeGen()` will create a `CodeGenContext`, which will hold a list of variables for input,
 * used to generated code for [[BoundReference]].
 */
case class WholeStageCodegenExec(child: SparkPlan)(val codegenStageId: Int)
    extends UnaryExecNode with CodegenSupport {

  override def output: Seq[Attribute] = child.output

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  override lazy val metrics = Map(
    "pipelineTime" -> SQLMetrics.createTimingMetric(sparkContext,
      WholeStageCodegenExec.PIPELINE_DURATION_METRIC))

  def generatedClassName(): String = if (conf.wholeStageUseIdInClassName) {
    s"GeneratedIteratorForCodegenStage$codegenStageId"
  } else {
    "GeneratedIterator"
  }

  /**
   * Generates code for this subtree.
   *
   * @return the tuple of the codegen context and the actual generated source.
   */
  def doCodeGen(): (CodegenContext, CodeAndComment) = {
    val ctx = new CodegenContext
    val code = child.asInstanceOf[CodegenSupport].produce(ctx, this)

    // main next function.
    ctx.addNewFunction("processNext",
      s"""
        protected void processNext() throws java.io.IOException {
          ${code.trim}
        }
       """, inlineToOuterClass = true)

    val className = generatedClassName()

    val source = s"""
      public Object generate(Object[] references) {
        return new $className(references);
      }

      ${ctx.registerComment(
        s"""Codegend pipeline for stage (id=$codegenStageId)
           |${this.treeString.trim}""".stripMargin,
         "wsc_codegenPipeline")}
      ${ctx.registerComment(s"codegenStageId=$codegenStageId", "wsc_codegenStageId", true)}
      final class $className extends ${classOf[BufferedRowIterator].getName} {

        private Object[] references;
        private scala.collection.Iterator[] inputs;
        ${ctx.declareMutableStates()}

        public $className(Object[] references) {
          this.references = references;
        }

        public void init(int index, scala.collection.Iterator[] inputs) {
          partitionIndex = index;
          this.inputs = inputs;
          ${ctx.initMutableStates()}
          ${ctx.initPartition()}
        }

        ${ctx.emitExtraCode()}

        ${ctx.declareAddedFunctions()}
      }
      """.trim

    // try to compile, helpful for debug
    val cleanedSource = CodeFormatter.stripOverlappingComments(
      new CodeAndComment(CodeFormatter.stripExtraNewLines(source), ctx.getPlaceHolderToComments()))

    logDebug(s"\n${CodeFormatter.format(cleanedSource)}")
    (ctx, cleanedSource)
  }

  override def doExecute(): RDD[InternalRow] = {
    val (ctx, cleanedSource) = doCodeGen()
    // try to compile and fallback if it failed
    val (_, maxCodeSize) = try {
      CodeGenerator.compile(cleanedSource)
    } catch {
      case NonFatal(_) if !Utils.isTesting && sqlContext.conf.codegenFallback =>
        // We should already saw the error message
        logWarning(s"Whole-stage codegen disabled for plan (id=$codegenStageId):\n $treeString")
        return child.execute()
    }

    // Check if compiled code has a too large function
    if (maxCodeSize > sqlContext.conf.hugeMethodLimit) {
      logInfo(s"Found too long generated codes and JIT optimization might not work: " +
        s"the bytecode size ($maxCodeSize) is above the limit " +
        s"${sqlContext.conf.hugeMethodLimit}, and the whole-stage codegen was disabled " +
        s"for this plan (id=$codegenStageId). To avoid this, you can raise the limit " +
        s"`${SQLConf.WHOLESTAGE_HUGE_METHOD_LIMIT.key}`:\n$treeString")
      child match {
        // The fallback solution of batch file source scan still uses WholeStageCodegenExec
        case f: FileSourceScanExec if f.supportsBatch => // do nothing
        case _ => return child.execute()
      }
    }

    val references = ctx.references.toArray

    val durationMs = longMetric("pipelineTime")

    val rdds = child.asInstanceOf[CodegenSupport].inputRDDs()
    assert(rdds.size <= 2, "Up to two input RDDs can be supported")
    if (rdds.length == 1) {
      rdds.head.mapPartitionsWithIndex { (index, iter) =>
        val (clazz, _) = CodeGenerator.compile(cleanedSource)
        val buffer = clazz.generate(references).asInstanceOf[BufferedRowIterator]
        buffer.init(index, Array(iter))
        new Iterator[InternalRow] {
          override def hasNext: Boolean = {
            val v = buffer.hasNext
            if (!v) durationMs += buffer.durationMs()
            v
          }
          override def next: InternalRow = buffer.next()
        }
      }
    } else {
      // Right now, we support up to two input RDDs.
      rdds.head.zipPartitions(rdds(1)) { (leftIter, rightIter) =>
        Iterator((leftIter, rightIter))
        // a small hack to obtain the correct partition index
      }.mapPartitionsWithIndex { (index, zippedIter) =>
        val (leftIter, rightIter) = zippedIter.next()
        val (clazz, _) = CodeGenerator.compile(cleanedSource)
        val buffer = clazz.generate(references).asInstanceOf[BufferedRowIterator]
        buffer.init(index, Array(leftIter, rightIter))
        new Iterator[InternalRow] {
          override def hasNext: Boolean = {
            val v = buffer.hasNext
            if (!v) durationMs += buffer.durationMs()
            v
          }
          override def next: InternalRow = buffer.next()
        }
      }
    }
  }

  override def inputRDDs(): Seq[RDD[InternalRow]] = {
    throw new UnsupportedOperationException
  }

  override def doProduce(ctx: CodegenContext): String = {
    throw new UnsupportedOperationException
  }

  override def doConsume(ctx: CodegenContext, input: Seq[ExprCode], row: ExprCode): String = {
    val doCopy = if (needCopyResult) {
      ".copy()"
    } else {
      ""
    }
    s"""
      |${row.code}
      |append(${row.value}$doCopy);
     """.stripMargin.trim
  }

  override def generateTreeString(
      depth: Int,
      lastChildren: Seq[Boolean],
      append: String => Unit,
      verbose: Boolean,
      prefix: String = "",
      addSuffix: Boolean = false,
      maxFields: Int): Unit = {
    child.generateTreeString(
      depth,
      lastChildren,
      append,
      verbose,
      s"*($codegenStageId) ",
      false,
      maxFields)
  }

  override def needStopCheck: Boolean = true

  override def limitNotReachedChecks: Seq[String] = Nil

  override protected def otherCopyArgs: Seq[AnyRef] = Seq(codegenStageId.asInstanceOf[Integer])
}


/**
 * Find the chained plans that support codegen, collapse them together as WholeStageCodegen.
 */
case class CollapseCodegenStages(conf: SQLConf) extends Rule[SparkPlan] {

  private def supportCodegen(e: Expression): Boolean = e match {
    case e: LeafExpression => true
    // CodegenFallback requires the input to be an InternalRow
    case e: CodegenFallback => false
    case _ => true
  }

  private def supportCodegen(plan: SparkPlan): Boolean = plan match {
    case plan: CodegenSupport if plan.supportCodegen =>
      val willFallback = plan.expressions.exists(_.find(e => !supportCodegen(e)).isDefined)
      // the generated code will be huge if there are too many columns
      val hasTooManyOutputFields =
        WholeStageCodegenExec.isTooManyFields(conf, plan.schema)
      val hasTooManyInputFields =
        plan.children.exists(p => WholeStageCodegenExec.isTooManyFields(conf, p.schema))
      !willFallback && !hasTooManyOutputFields && !hasTooManyInputFields
    case _ => false
  }

  /**
   * Inserts an InputAdapter on top of those that do not support codegen.
   */
  private def insertInputAdapter(plan: SparkPlan): SparkPlan = plan match {
    case p if !supportCodegen(p) =>
      // collapse them recursively
      InputAdapter(insertWholeStageCodegen(p))
    case j: SortMergeJoinExec =>
      // The children of SortMergeJoin should do codegen separately.
      j.withNewChildren(j.children.map(child => InputAdapter(insertWholeStageCodegen(child))))
    case p =>
      p.withNewChildren(p.children.map(insertInputAdapter))
  }

  /**
   * Inserts a WholeStageCodegen on top of those that support codegen.
   */
  private def insertWholeStageCodegen(plan: SparkPlan): SparkPlan = plan match {
    // For operators that will output domain object, do not insert WholeStageCodegen for it as
    // domain object can not be written into unsafe row.
    case plan if plan.output.length == 1 && plan.output.head.dataType.isInstanceOf[ObjectType] =>
      plan.withNewChildren(plan.children.map(insertWholeStageCodegen))
    case plan: CodegenSupport if supportCodegen(plan) =>
      WholeStageCodegenExec(insertInputAdapter(plan))(WholeStageCodegenId.getNextStageId())
    case other =>
      other.withNewChildren(other.children.map(insertWholeStageCodegen))
  }

  def apply(plan: SparkPlan): SparkPlan = {
    if (conf.wholeStageEnabled) {
      WholeStageCodegenId.resetPerQuery()
      insertWholeStageCodegen(plan)
    } else {
      plan
    }
  }
}
