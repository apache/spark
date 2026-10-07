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

import java.util.Locale
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}

import scala.collection.mutable
import scala.util.{Failure, Success, Try}
import scala.util.control.NonFatal

import com.google.common.cache.{Cache, CacheBuilder}

import org.apache.spark.{broadcast, SparkException, SparkUnsupportedOperationException}
import org.apache.spark.internal.LogKeys.{CODEGEN_STAGE_ID, CONFIG, ERROR, HUGE_METHOD_LIMIT, MAX_METHOD_CODE_SIZE, TREE_NODE}
import org.apache.spark.network.util.JavaUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.expressions.codegen.Block._
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.aggregate.{HashAggregateExec, SortAggregateExec}
import org.apache.spark.sql.execution.columnar.InMemoryTableScanExec
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, BroadcastNestedLoopJoinExec, ShuffledHashJoinExec, SortMergeJoinExec}
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.{KeyLock, Utils}

/**
 * An interface for those physical operators that support codegen.
 */
trait CodegenSupport extends SparkPlan {

  /** Prefix used in the current operator's variable names. */
  private def variablePrefix: String = this match {
    case _: HashAggregateExec => "hashAgg"
    case _: SortAggregateExec => "sortAgg"
    case _: BroadcastHashJoinExec => "bhj"
    case _: ShuffledHashJoinExec => "shj"
    case _: SortMergeJoinExec => "smj"
    case _: BroadcastNestedLoopJoinExec => "bnlj"
    case _: RDDScanExec => "rdd"
    case _: OneRowRelationExec => "orr"
    case _: DataSourceScanExec => "scan"
    case _: InMemoryTableScanExec => "memoryScan"
    case _: WholeStageCodegenExec => "wholestagecodegen"
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
       |${ctx.registerComment(s"PRODUCE: ${this.simpleString(conf.maxToStringFields)}")}
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
   *
   * If the emitted code depends on the partition index, read it from
   * `ctx.currentPartitionIndexVar` rather than hard-coding the field name
   * `partitionIndex`. `UnionExec` rebinds `currentPartitionIndexVar` per
   * fused child so that each child sees its own child-local index; a
   * hard-coded reference would silently observe the global `UnionRDD` index
   * under fusion.
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

    @scala.annotation.nowarn("cat=deprecation")
    val inputVars = inputVarsCandidate match {
      case stream: Stream[ExprCode] => stream.force
      case lazyList: LazyList[ExprCode] => lazyList.force
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
    val confEnabled = conf.wholeStageSplitConsumeFuncByOperator
    val requireAllOutput = output.forall(parent.usedInputs.contains(_))
    val paramLength = CodeGenerator.calculateParamLength(output) + (if (row != null) 1 else 0)
    val consumeFunc = if (confEnabled && requireAllOutput
        && CodeGenerator.isValidParamLength(paramLength)) {
      constructDoConsumeFunction(ctx, inputVars, row)
    } else {
      parent.doConsume(ctx, inputVars, rowVar)
    }
    s"""
       |${ctx.registerComment(s"CONSUME: ${parent.simpleString(conf.maxToStringFields)}")}
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
  protected def constructConsumeParameters(
      ctx: CodegenContext,
      attributes: Seq[Attribute],
      variables: Seq[ExprCode],
      row: String = null): (Seq[String], Seq[String], Seq[ExprCode]) = {
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
    (arguments.toSeq, parameters.toSeq, paramVars.toSeq)
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
    throw SparkUnsupportedOperationException()
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
      throw new SparkUnsupportedOperationException(
        errorClass = "_LEGACY_ERROR_TEMP_3163",
        messageParameters = Map("num" -> children.length.toString))
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
        throw SparkException.internalError(errMsg)
      } else {
        logWarning(log"[BUG] ${MDC(ERROR, errMsg)} Please open a JIRA ticket to report it.")
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
case class InputAdapter(child: SparkPlan)
    extends UnaryExecNode with InputRDDCodegen with SafeForKWayMerge {

  override def output: Seq[Attribute] = child.output

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  override def vectorTypes: Option[Seq[String]] = child.vectorTypes

  // This is not strictly needed because the codegen transformation happens after the columnar
  // transformation but just for consistency
  override def supportsColumnar: Boolean = child.supportsColumnar

  override def doExecute(): RDD[InternalRow] = {
    child.execute()
  }

  override def doExecuteBroadcast[T](): broadcast.Broadcast[T] = {
    child.doExecuteBroadcast()
  }

  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    child.executeColumnar()
  }

  // `InputAdapter` can only generate code to process the rows from its child. If the child produces
  // columnar batches, there must be a `ColumnarToRowExec` above `InputAdapter` to handle it by
  // overriding `inputRDDs` and calling `InputAdapter#executeColumnar` directly.
  override def inputRDD: RDD[InternalRow] = child.execute()

  // This is a leaf node so the node can produce limit not reached checks.
  override protected def canCheckLimitNotReached: Boolean = true

  // InputAdapter does not need UnsafeProjection.
  protected val createUnsafeProjection: Boolean = false

  override def generateTreeString(
      depth: Int,
      lastChildren: java.util.ArrayList[Boolean],
      append: String => Unit,
      verbose: Boolean,
      prefix: String = "",
      addSuffix: Boolean = false,
      maxFields: Int,
      printNodeId: Boolean,
      printOutputColumns: Boolean,
      indent: Int = 0): Unit = {
    child.generateTreeString(
      depth,
      lastChildren,
      append,
      verbose,
      prefix = "",
      addSuffix = false,
      maxFields,
      printNodeId,
      printOutputColumns,
      indent)
  }

  override def needCopyResult: Boolean = false

  override protected def withNewChildInternal(newChild: SparkPlan): InputAdapter =
    copy(child = newChild)
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

  // The whole-stage codegen generates Java code on the driver side and sends it to the Executors
  // for compilation and execution. The whole-stage codegen can bring significant performance
  // improvements with large dataset in distributed environments. However, in the test environment,
  // due to the small amount of data, the time to generate Java code takes up a major part of the
  // entire runtime. So we summarize the total code generation time and output it to the execution
  // log for easy analysis and view.
  private val _codeGenTime = new AtomicLong

  // Increase the total generation time of Java source code in nanoseconds.
  // Visible for testing
  def increaseCodeGenTime(time: Long): Unit = _codeGenTime.addAndGet(time)

  // Returns the total generation time of Java source code in nanoseconds.
  // Visible for testing
  def codeGenTime: Long = _codeGenTime.get

  // Reset generation time of Java source code.
  // Visible for testing
  def resetCodeGenTime(): Unit = _codeGenTime.set(0L)

  /**
   * A trial compile's outcome for `WholeStageCodegenExec.chooseCode`: the bytecode size of each
   * method, None where they are unknown, or the failure; and the reports and metric updates the
   * compile held back for the code kept.
   */
  private[execution] case class TrialResult(
      methodSizes: Try[Option[Map[String, Int]]],
      report: () => Unit)

  /**
   * Compiles `code` as a trial (`CodeCompiler.trial`). A hit in the compile cache produces no
   * method sizes, so the class is then dropped and compiled again. A hit happens where the class
   * is cached while the decision for its code is not: the decision was evicted, or the same body
   * was compiled earlier without being decided - with the split off, whose code is the recorded
   * code without its marks, or under another limit. Should another thread load the class again in
   * between, the second compile is a hit as well, and the sizes are unknown.
   */
  private[execution] def trialCompile(
      code: CodeAndComment,
      failureExpected: Boolean): TrialResult = {
    def once(): (Try[Map[String, Int]], CodeCompiler.TrialCompile) = {
      val (result, trial) = CodeCompiler.trial(failureExpected)(CodeGenerator.compile(code))
      (result.map(_ => trial.methodSizes), trial)
    }
    val (sizes, trial) = once() match {
      case (Success(m), _) if m.isEmpty =>
        CodeGenerator.invalidateCompiled(code)
        once()
      case result => result
    }
    TrialResult(sizes.map(m => Option.when(m.nonEmpty)(m)), () => trial.report())
  }

  /** The bytecode of the methods run per row that are past `limit`. */
  private def bytesOver(sizes: Map[String, Int], limit: Int): Int =
    sizes.iterator.collect {
      case (m, size) if size > limit && !CodeCompiler.runOnceMethods(m) => size
    }.sum

  /** What `chooseCode` decided for a stage's code; see [[splitDecisions]]. */
  private sealed trait SplitDecision
  /**
   * The code in one piece is kept: the JIT compiles it whole, no split lowers its bytes past the
   * limit, or no split compiles either; where it fails to compile itself, `doExecute` reports that.
   */
  private case object KeepWhole extends SplitDecision
  /** The expressions split, by their index in `CodegenContext.wholeStageSplitsRecorded`. */
  private case class SplitThese(indices: Option[Set[Int]]) extends SplitDecision
  /** Nothing decided, the method sizes being unknown: the code in one piece runs this time. */
  private case object Undecided extends SplitDecision

  /**
   * What `chooseCode` decided for each stage's code, by [[decisionKey]], so that a stage generated
   * again skips the trial compiles: the code kept in one piece or the expressions split. Sized as
   * the compile cache is, 0 being unbounded.
   *
   * A plain Guava cache, kept within sql/core: one handed to core's `NonFateSharingCache` fails
   * sql/core's Maven tests, whose classes refer to Guava as it is while the installed core jar has
   * it relocated (SPARK-44064, SPARK-50443). [[decided]] serializes the deciders itself.
   */
  private val splitDecisions: Cache[AnyRef, SplitDecision] = {
    val builder = CacheBuilder.newBuilder()
    val maxEntries = SQLConf.get.codegenCacheMaxEntries
    (if (maxEntries > 0) builder.maximumSize(maxEntries) else builder)
      .build[AnyRef, SplitDecision]()
  }

  /** Serializes the threads deciding one key of [[splitDecisions]]. */
  private val decisionLock = new KeyLock[AnyRef]

  /**
   * The key of a decision for the stage's code `code` under `limit`: what the compile cache keys a
   * class on besides its code - the class loader and the backend - with `methodSplitThreshold`,
   * which packs the split's blocks, and a digest of the source with the marks of the recorded
   * expressions (`CodegenContext.wholeStageSplitDigest`), whose marks say which expression each
   * index of a decision is; a digest rather than the source, which runs to megabytes for the
   * stages that split.
   */
  private def decisionKey(ctx: CodegenContext, code: CodeAndComment, limit: Int): AnyRef =
    (CodeGenerator.compileCacheScope(code), limit, SQLConf.get.methodSplitThreshold,
      ctx.wholeStageSplitDigest)

  /**
   * The decision remembered for `key`, or else `decide()`'s, which is remembered unless it is
   * `Undecided`. Threads that meet one key at once decide one at a time, so the later ones find
   * the first one's decision; one whose `decide()` throws remembers nothing, and the next decides
   * on its own.
   */
  private def decided(key: AnyRef, decide: () => SplitDecision): SplitDecision =
    decisionLock.withLock(key) {
      Option(splitDecisions.getIfPresent(key)).getOrElse {
        val decision = decide()
        if (decision != Undecided) {
          splitDecisions.put(key, decision)
        }
        decision
      }
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
    extends UnaryExecNode with CodegenSupport with SafeForKWayMerge {

  override def output: Seq[Attribute] = child.output

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  // This is not strictly needed because the codegen transformation happens after the columnar
  // transformation but just for consistency
  override def supportsColumnar: Boolean = child.supportsColumnar

  override lazy val metrics = Map(
    "pipelineTime" -> SQLMetrics.createTimingMetric(sparkContext,
      WholeStageCodegenExec.PIPELINE_DURATION_METRIC))

  override def nodeName: String = s"WholeStageCodegen (${codegenStageId})"

  def generatedClassName(): String = if (conf.wholeStageUseIdInClassName) {
    s"GeneratedIteratorForCodegenStage$codegenStageId"
  } else {
    "GeneratedIterator"
  }

  /**
   * Generates code for this subtree.
   *
   * Under `spark.sql.codegen.wholeStage.splitExpressions` the stage's code is first generated in
   * one piece, with the code of each expression a split could take marked, and only where there
   * is one is that code compiled to choose which expressions to split ([[chooseCode]]). A stage
   * the JIT compiles whole keeps that code, which a split's calls would only slow down; the trial
   * compile of the code kept is the one [[doExecute]] finds in the compile cache. That code
   * differs from the code with the conf off in one place: a slot of a compacted mutable state
   * array counts as the field it is when code moves into a method (`CodegenContext.collectInputs`).
   * An operator's method that took it as a parameter, and failed to compile, now compiles; a
   * `With` definition reading one, which stayed inline, gets its method.
   *
   * @return the tuple of the codegen context and the actual generated source.
   */
  def doCodeGen(): (CodegenContext, CodeAndComment) = {
    if (!conf.wholeStageSplitExpressions) {
      generate(WholeStageSplit.Off)
    } else if (conf.wholeStageSplitExpressionsMethodLimit == 0) {
      generate(WholeStageSplit.Split(None))
    } else {
      chooseCode(generate(WholeStageSplit.Record), only => generate(WholeStageSplit.Split(only)),
        WholeStageCodegenExec.trialCompile, CodeGenerator.invalidateCompiled,
        throwSplitFailure = Utils.isTesting)
    }
  }

  /**
   * Which code [[doCodeGen]] returns for the stage: `recorded`, its code in one piece with the
   * expressions a split could take marked (`WholeStageSplit.Record`), or the code `split` generates
   * with some of them split. The code in one piece is compiled by `compile` as a trial
   * ([[WholeStageCodegenExec.trialCompile]]), and the reports of a method past the JIT limit, with
   * the codegen metrics, are made for the code kept.
   *
   *  - Where it fails to compile, the split of every expression is the code that can run, and is
   *    compiled as a trial too; where that fails as well, or no block could be split, the code in
   *    one piece is left to [[doExecute]], which reports its failure, falls back or throws it, as
   *    without the split.
   *  - Otherwise the methods run for each row past the limit - the lower of
   *    `spark.sql.codegen.wholeStage.splitExpressions.methodLimit` (HotSpot's JIT limit by default)
   *    and `spark.sql.codegen.hugeMethodLimit` - are the ones a split could help, and the
   *    expressions marked in them are split: an expression in a method the JIT compiles whole
   *    would only be slowed down by a split's calls. Where none is marked in them, or no block of
   *    them could be split, the code in one piece is kept.
   *  - The split is kept if it lowers the bytes of the methods past the limit, and compiles;
   *    otherwise the code in one piece runs, as it did before the split existed.
   *
   * A split that fails to compile is thrown where `throwSplitFailure`, which is under testing, and
   * warned about otherwise. The decision is remembered by [[WholeStageCodegenExec.decisionKey]], so
   * that a stage generated again, as every execution of a query generates it, skips the trial
   * compiles, and threads that meet the stage at once wait for one decision; where a trial's sizes
   * are unknown, nothing is remembered. The variant not kept leaves the compile cache
   * (`invalidate`), so that it holds only code that runs and a later compile of that variant
   * reports what it finds.
   */
  private[execution] def chooseCode(
      recorded: (CodegenContext, CodeAndComment),
      split: Option[Set[WholeStageSplitKey]] => (CodegenContext, CodeAndComment),
      compile: (CodeAndComment, Boolean) => WholeStageCodegenExec.TrialResult,
      invalidate: CodeAndComment => Unit,
      throwSplitFailure: Boolean): (CodegenContext, CodeAndComment) = {
    import WholeStageCodegenExec._
    val (ctx, whole) = recorded
    if (ctx.wholeStageSplitsRecorded.isEmpty) {
      return recorded
    }
    val limit = math.min(conf.wholeStageSplitExpressionsMethodLimit, conf.hugeMethodLimit)
    def splitOf(indices: Option[Set[Int]]) =
      split(indices.map(_.map(ctx.wholeStageSplitsRecorded)))
    // The thread that decides keeps the code it chose; one that finds a decision generates its
    // code. The indices of a decision are among this recording's: its key holds the digest of the
    // marked source, which says which expression each index is.
    var chosen: Option[(CodegenContext, CodeAndComment)] = None
    def decide(): SplitDecision = {
      val (decision, code) = decideFromTrials(ctx, whole, recorded, limit, splitOf, compile,
        invalidate, throwSplitFailure)
      chosen = Some(code)
      decision
    }
    val decision = decided(decisionKey(ctx, whole, limit), () => decide())
    chosen.getOrElse(decision match {
      case SplitThese(indices) => splitOf(indices)
      case _ => recorded
    })
  }

  /** The decision of `chooseCode` from the trial compiles, with the code it keeps. */
  private def decideFromTrials(
      ctx: CodegenContext,
      whole: CodeAndComment,
      recorded: (CodegenContext, CodeAndComment),
      limit: Int,
      splitOf: Option[Set[Int]] => (CodegenContext, CodeAndComment),
      compile: (CodeAndComment, Boolean) => WholeStageCodegenExec.TrialResult,
      invalidate: CodeAndComment => Unit,
      throwSplitFailure: Boolean): (WholeStageCodegenExec.SplitDecision,
        (CodegenContext, CodeAndComment)) = {
    import WholeStageCodegenExec._
    // Whether the split generation `splitCode` turned any block into a method. Where it did not,
    // the split would change nothing but drop the helper functions of a discarded subexpression
    // pass, so the code in one piece is kept, as it is with no split compiled.
    def splitsSomething(splitCode: (CodegenContext, CodeAndComment)): Boolean =
      splitCode._1.wholeStageBlocksSplit > 0
    // A split that fails to compile is thrown under testing (`throwSplitFailure`), and warned
    // about otherwise. The warning carries the failure only where the split's compile expected
    // it (`reported` false); otherwise that compile has logged it as an error, with the source.
    def splitFailed(
        e: Throwable,
        reported: Boolean): (SplitDecision, (CodegenContext, CodeAndComment)) = {
      if (throwSplitFailure) throw e
      val message = log"Whole-stage codegen stage (id=${MDC(CODEGEN_STAGE_ID, codegenStageId)}) " +
        log"failed to compile with its expressions split, so its code is kept in one " +
        log"piece:\n ${MDC(TREE_NODE, treeString)}"
      if (reported) logWarning(message) else logWarning(message, e)
      (KeepWhole, recorded)
    }
    val wholeTrial = compile(whole, true)
    wholeTrial.methodSizes match {
      case Failure(_) =>
        val splitAll = splitOf(None)
        if (!splitsSomething(splitAll)) {
          (KeepWhole, recorded)
        } else {
          // Where the split fails as well, `doExecute` compiles the code in one piece again and
          // reports its failure as any compile's, so the split's compile expects its failure,
          // which the warning carries; under testing it is reported and thrown as any compile's.
          val splitTrial = compile(splitAll._2, !throwSplitFailure)
          splitTrial.methodSizes match {
            case Success(_) =>
              splitTrial.report()
              (SplitThese(None), splitAll)
            case Failure(e) =>
              splitFailed(e, reported = throwSplitFailure)
          }
        }
      case Success(None) =>
        wholeTrial.report()
        (Undecided, recorded)
      case Success(Some(sizes)) =>
        val over = sizes.filter { case (m, size) =>
          size > limit && !CodeCompiler.runOnceMethods(m)
        }
        val indices = if (over.isEmpty) {
          Nil
        } else {
          ctx.wholeStageSplitMarkersByFunction.collect {
            case (i, functions) if functions.exists(over.contains) => i
          }
        }
        if (indices.isEmpty) {
          wholeTrial.report()
          (KeepWhole, recorded)
        } else {
          val splitCode = splitOf(Some(indices.toSet))
          if (!splitsSomething(splitCode)) {
            wholeTrial.report()
            (KeepWhole, recorded)
          } else {
            val splitTrial = compile(splitCode._2, false)
            splitTrial.methodSizes match {
              case Success(Some(splitSizes)) if bytesOver(splitSizes, limit) < over.values.sum =>
                splitTrial.report()
                invalidate(whole)
                (SplitThese(Some(indices.toSet)), splitCode)
              case Success(Some(_)) =>
                wholeTrial.report()
                invalidate(splitCode._2)
                (KeepWhole, recorded)
              case Success(None) =>
                wholeTrial.report()
                (Undecided, recorded)
              case Failure(e) =>
                val keepWhole = splitFailed(e, reported = true)
                wholeTrial.report()
                keepWhole
            }
          }
        }
    }
  }

  /** The stage's code, its expression splits made as `mode` says; see [[doCodeGen]]. */
  private[execution] def generate(mode: WholeStageSplit): (CodegenContext, CodeAndComment) = {
    val startTime = System.nanoTime()
    val ctx = new CodegenContext
    ctx.wholeStageSplit = mode
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
        s"""Codegened pipeline for stage (id=$codegenStageId)
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

    // The marks go before the code is formatted, so that the code is what it is without them.
    val unmarked = if (ctx.wholeStageSplitsRecorded.isEmpty) {
      source
    } else {
      ctx.wholeStageSplitDigest = JavaUtils.sha256Hex(source)
      CodeGenerator.stripWholeStageSplitMarkers(source)
    }
    // try to compile, helpful for debug
    val cleanedSource = CodeFormatter.stripOverlappingComments(
      new CodeAndComment(CodeFormatter.stripExtraNewLines(unmarked),
        ctx.getPlaceHolderToComments()))

    val duration = System.nanoTime() - startTime
    WholeStageCodegenExec.increaseCodeGenTime(duration)

    logDebug(s"\n${CodeFormatter.format(cleanedSource)}")
    (ctx, cleanedSource)
  }

  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    // Code generation is not currently supported for columnar output, so just fall back to
    // the interpreted path
    child.executeColumnar()
  }

  override def doExecute(): RDD[InternalRow] = {
    val (ctx, cleanedSource) = doCodeGen()
    // try to compile and fallback if it failed
    val (_, compiledCodeStats) = try {
      CodeGenerator.compile(cleanedSource)
    } catch {
      case NonFatal(_) if !Utils.isTesting && conf.codegenFallback =>
        // We should already saw the error message
        logWarning(log"Whole-stage codegen disabled for plan " +
          log"(id=${MDC(CODEGEN_STAGE_ID, codegenStageId)}):\n " +
          log"${MDC(TREE_NODE, treeString)}")
        return child.execute()
    }

    // Check if compiled code has a too large function
    if (compiledCodeStats.maxMethodCodeSize > conf.hugeMethodLimit) {
      logInfo(log"Found too long generated codes and JIT optimization might not work: " +
        log"the bytecode size (${MDC(MAX_METHOD_CODE_SIZE, compiledCodeStats.maxMethodCodeSize)})" +
        log" is above the limit ${MDC(HUGE_METHOD_LIMIT, conf.hugeMethodLimit)}, " +
        log"and the whole-stage codegen was disabled for this plan " +
        log"(id=${MDC(CODEGEN_STAGE_ID, codegenStageId)}). To avoid this, you can raise the limit" +
        log" `${MDC(CONFIG, SQLConf.WHOLESTAGE_HUGE_METHOD_LIMIT.key)}`:\n" +
        log"${MDC(TREE_NODE, treeString)}")
      return child.execute()
    }

    val references = ctx.references.toArray

    val durationMs = longMetric("pipelineTime")

    // Even though rdds is an RDD[InternalRow] it may actually be an RDD[ColumnarBatch] with
    // type erasure hiding that. This allows for the input to a code gen stage to be columnar,
    // but the output must be rows.
    val rdds = child.asInstanceOf[CodegenSupport].inputRDDs()
    assert(rdds.size <= 2, "Up to two input RDDs can be supported")
    val cleanedSourceOpt = tryBroadcastCleanedSource(cleanedSource)
    val evaluatorFactory = new WholeStageCodegenEvaluatorFactory(
      cleanedSourceOpt, durationMs, references)
    if (rdds.length == 1) {
      if (conf.usePartitionEvaluator) {
        rdds.head.mapPartitionsWithEvaluator(evaluatorFactory)
      } else {
        rdds.head.mapPartitionsWithIndex { (index, iter) =>
          val evaluator = evaluatorFactory.createEvaluator()
          evaluator.eval(index, iter)
        }
      }
    } else {
      // Right now, we support up to two input RDDs.
      if (conf.usePartitionEvaluator) {
        rdds.head.zipPartitionsWithEvaluator(rdds(1), evaluatorFactory)
      } else {
        rdds.head.zipPartitions(rdds(1)) { (leftIter, rightIter) =>
          Iterator((leftIter, rightIter))
          // a small hack to obtain the correct partition index
        }.mapPartitionsWithIndex { (index, zippedIter) =>
          val (leftIter, rightIter) = zippedIter.next()
          val evaluator = evaluatorFactory.createEvaluator()
          evaluator.eval(index, leftIter, rightIter)
        }
      }
    }
  }

  override def inputRDDs(): Seq[RDD[InternalRow]] = {
    throw SparkUnsupportedOperationException()
  }

  override def doProduce(ctx: CodegenContext): String = {
    throw SparkUnsupportedOperationException()
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
      lastChildren: java.util.ArrayList[Boolean],
      append: String => Unit,
      verbose: Boolean,
      prefix: String = "",
      addSuffix: Boolean = false,
      maxFields: Int,
      printNodeId: Boolean,
      printOutputColumns: Boolean,
      indent: Int = 0): Unit = {
    child.generateTreeString(
      depth,
      lastChildren,
      append,
      verbose,
      if (printNodeId) "* " else s"*($codegenStageId) ",
      false,
      maxFields,
      printNodeId,
      printOutputColumns,
      indent)
  }

  override def needStopCheck: Boolean = true

  override def limitNotReachedChecks: Seq[String] = Nil

  override protected def otherCopyArgs: Seq[AnyRef] = Seq(codegenStageId.asInstanceOf[Integer])

  override protected def withNewChildInternal(newChild: SparkPlan): WholeStageCodegenExec =
    copy(child = newChild)(codegenStageId)

  // Use broadcast if the conf is enabled and the code + comment size exceeds the threshold,
  // otherwise, fallback to use cleanedSource directly. The method returns either a
  // broadcast variable or the original form of cleanedSource.
  private[spark] def tryBroadcastCleanedSource(code: CodeAndComment):
      Either[broadcast.Broadcast[CodeAndComment], CodeAndComment] = {
    def exceedThreshold(): Boolean = {
      code.body.length + code.comment.iterator.map(
        e => e._1.length + e._2.length).sum > conf.broadcastCleanedSourceThreshold
    }
    val enabled = conf.broadcastCleanedSourceThreshold >= 0
    if (enabled && exceedThreshold()) {
      scala.util.Left(sparkContext.broadcast(code))
    } else {
      scala.util.Right(code)
    }
  }
}


/**
 * Find the chained plans that support codegen, collapse them together as WholeStageCodegen.
 *
 * The `codegenStageCounter` generates ID for codegen stages within a query plan.
 * It does not affect equality, nor does it participate in destructuring pattern matching
 * of WholeStageCodegenExec.
 *
 * This ID is used to help differentiate between codegen stages. It is included as a part
 * of the explain output for physical plans, e.g.
 *
 * == Physical Plan ==
 * *(5) SortMergeJoin [x#3L], [y#9L], Inner
 * :- *(2) Sort [x#3L ASC NULLS FIRST], false, 0
 * :  +- Exchange hashpartitioning(x#3L, 200)
 * :     +- *(1) Project [(id#0L % 2) AS x#3L]
 * :        +- *(1) Filter isnotnull((id#0L % 2))
 * :           +- *(1) Range (0, 5, step=1, splits=8)
 * +- *(4) Sort [y#9L ASC NULLS FIRST], false, 0
 *    +- Exchange hashpartitioning(y#9L, 200)
 *       +- *(3) Project [(id#6L % 2) AS y#9L]
 *          +- *(3) Filter isnotnull((id#6L % 2))
 *             +- *(3) Range (0, 5, step=1, splits=8)
 *
 * where the ID makes it obvious that not all adjacent codegen'd plan operators are of the
 * same codegen stage.
 *
 * The codegen stage ID is also optionally included in the name of the generated classes as
 * a suffix, so that it's easier to associate a generated class back to the physical operator.
 * This is controlled by SQLConf: spark.sql.codegen.useIdInClassName
 *
 * The ID is also included in various log messages.
 *
 * Within a query, a codegen stage in a plan starts counting from 1, in "insertion order".
 * WholeStageCodegenExec operators are inserted into a plan in depth-first post-order.
 * See CollapseCodegenStages.insertWholeStageCodegen for the definition of insertion order.
 *
 * 0 is reserved as a special ID value to indicate a temporary WholeStageCodegenExec object
 * is created, e.g. for special fallback handling when an existing WholeStageCodegenExec
 * failed to generate/compile code.
 */
case class CollapseCodegenStages(
    codegenStageCounter: AtomicInteger = new AtomicInteger(0))
  extends Rule[SparkPlan] {

  private def supportCodegen(e: Expression): Boolean = e match {
    case e: LeafExpression => true
    // CodegenFallback requires the input to be an InternalRow
    case e: CodegenFallback => false
    case _ => true
  }

  private def supportCodegen(plan: SparkPlan): Boolean = plan match {
    case plan: CodegenSupport if plan.supportCodegen =>
      val willFallback = plan.expressions.exists(_.exists(e => !supportCodegen(e)))
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
  private def insertInputAdapter(plan: SparkPlan): SparkPlan = {
    plan match {
      case p if !supportCodegen(p) =>
        // collapse them recursively
        InputAdapter(insertWholeStageCodegen(p))
      case j: SortMergeJoinExec =>
        // The children of SortMergeJoin should do codegen separately.
        j.withNewChildren(j.children.map(
          child => InputAdapter(insertWholeStageCodegen(child))))
      case j: ShuffledHashJoinExec =>
        // The children of ShuffledHashJoin should do codegen separately.
        j.withNewChildren(j.children.map(
          child => InputAdapter(insertWholeStageCodegen(child))))
      case p => p.withNewChildren(p.children.map(insertInputAdapter))
    }
  }

  /**
   * Inserts a WholeStageCodegen on top of those that support codegen.
   */
  private def insertWholeStageCodegen(plan: SparkPlan): SparkPlan = {
    plan match {
      // For operators that will output domain object, do not insert WholeStageCodegen for it as
      // domain object can not be written into unsafe row.
      case plan if plan.output.length == 1 && plan.output.head.dataType.isInstanceOf[ObjectType] =>
        plan.withNewChildren(plan.children.map(insertWholeStageCodegen))
      case plan: LocalTableScanExec =>
        // Do not make LogicalTableScanExec the root of WholeStageCodegen
        // to support the fast driver-local collect/take paths.
        plan
      case plan: EmptyRelationExec =>
        // Do not make EmptyRelationExec the root of WholeStageCodegen
        // to support the fast driver-local collect/take paths.
        plan
      case plan: CommandResultExec =>
        // Do not make CommandResultExec the root of WholeStageCodegen
        // to support the fast driver-local collect/take paths.
        plan
      case plan: CodegenSupport if supportCodegen(plan) =>
        // The whole-stage-codegen framework is row-based. If a plan supports columnar execution,
        // it can't support whole-stage-codegen at the same time.
        assert(!plan.supportsColumnar)
        WholeStageCodegenExec(insertInputAdapter(plan))(codegenStageCounter.incrementAndGet())
      case other =>
        other.withNewChildren(other.children.map(insertWholeStageCodegen))
    }
  }

  def apply(plan: SparkPlan): SparkPlan = {
    if (conf.wholeStageEnabled && conf.codegenFactoryMode != CodegenObjectFactoryMode.NO_CODEGEN) {
      insertWholeStageCodegen(plan)
    } else {
      plan
    }
  }
}
