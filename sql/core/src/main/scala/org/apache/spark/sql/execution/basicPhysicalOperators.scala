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

import java.util.concurrent.{Future => JFuture}
import java.util.concurrent.TimeUnit._

import scala.collection.mutable
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.Duration

import org.apache.spark.{InterruptibleIterator, Partition, SparkContext, SparkException, TaskContext}
import org.apache.spark.internal.LogKeys
import org.apache.spark.rdd.{EmptyRDD, PartitionwiseSampledRDD, RDD, SQLPartitioningAwareUnionRDD, UnionPartition, UnionRDD}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.BindReferences.bindReferences
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.execution.joins.{ShuffledHashJoinExec, SortMergeJoinExec}
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.internal.{SQLConf, StaticSQLConf}
import org.apache.spark.sql.types.{LongType, StructType}
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.ThreadUtils
import org.apache.spark.util.random.{BernoulliCellSampler, PoissonSampler}

/** Physical plan for Project. */
case class ProjectExec(projectList: Seq[NamedExpression], child: SparkPlan)
  extends UnaryExecNode
    with CodegenSupport
    with SafeForKWayMerge
    with PartitioningPreservingUnaryExecNode
    with OrderPreservingUnaryExecNode {

  override def output: Seq[Attribute] = projectList.map(_.toAttribute)

  override def inputRDDs(): Seq[RDD[InternalRow]] = {
    child.asInstanceOf[CodegenSupport].inputRDDs()
  }

  protected override def doProduce(ctx: CodegenContext): String = {
    child.asInstanceOf[CodegenSupport].produce(ctx, this)
  }

  override def usedInputs: AttributeSet = {
    // only the attributes those are used at least twice should be evaluated before this plan,
    // otherwise we could defer the evaluation until output attribute is actually used.
    val usedExprIds = projectList.flatMap(_.collect {
      case a: Attribute => a.exprId
    })
    val usedMoreThanOnce = usedExprIds.groupBy(id => id).filter(_._2.size > 1).keySet
    references.filter(a => usedMoreThanOnce.contains(a.exprId))
  }

  override def doConsume(ctx: CodegenContext, input: Seq[ExprCode], row: ExprCode): String = {
    val exprs = bindReferences[Expression](projectList, child.output)
    val (subExprsCode, resultVars, localValInputs) = if (conf.subexpressionEliminationEnabled) {
      // subexpression elimination
      val subExprs = ctx.subexpressionEliminationForWholeStageCodegen(exprs)
      val genVars = ctx.withSubExprEliminationExprs(subExprs.states) {
        exprs.map(_.genCode(ctx))
      }
      (ctx.evaluateSubExprEliminationState(subExprs.states.values), genVars,
        subExprs.exprCodesNeedEvaluate)
    } else {
      ("", exprs.map(_.genCode(ctx)), Seq.empty)
    }

    // Evaluation of non-deterministic expressions can't be deferred.
    val nonDeterministicAttrs = projectList.filterNot(_.deterministic).map(_.toAttribute)
    s"""
       |// common sub-expressions
       |${evaluateVariables(localValInputs)}
       |$subExprsCode
       |${evaluateRequiredVariables(output, resultVars, AttributeSet(nonDeterministicAttrs))}
       |${consume(ctx, resultVars)}
     """.stripMargin
  }

  protected override def doExecute(): RDD[InternalRow] = {
    val evaluatorFactory = new ProjectEvaluatorFactory(projectList, child.output)
    if (conf.usePartitionEvaluator) {
      child.execute().mapPartitionsWithEvaluator(
        evaluatorFactory, preservesPartitionSizes = true
      )
    } else {
      child.execute().mapPartitionsWithIndexInternal(
        f = (index, iter) => {
          val evaluator = evaluatorFactory.createEvaluator()
          evaluator.eval(index, iter)
        }, preservesPartitionSizes = true
      )
    }
  }

  override protected def outputExpressions: Seq[NamedExpression] = projectList

  override protected def orderingExpressions: Seq[SortOrder] = child.outputOrdering

  override def verboseStringWithOperatorId(): String = {
    s"""
       |$formattedNodeName
       |${ExplainUtils.generateFieldString("Output", projectList)}
       |${ExplainUtils.generateFieldString("Input", child.output)}
       |""".stripMargin
  }

  override protected def withNewChildInternal(newChild: SparkPlan): ProjectExec =
    copy(child = newChild)
}

trait GeneratePredicateHelper extends PredicateHelper {
  self: CodegenSupport =>

  protected def generatePredicateCode(
      ctx: CodegenContext,
      condition: Expression,
      inputAttrs: Seq[Attribute],
      inputExprCode: Seq[ExprCode]): String = {
    val (notNullPreds, otherPreds) = splitConjunctivePredicates(condition).partition {
      case IsNotNull(a) => isNullIntolerant(a) && a.references.subsetOf(AttributeSet(inputAttrs))
      case _ => false
    }
    val nonNullAttrExprIds = notNullPreds.flatMap(_.references).distinct.map(_.exprId)
    val outputAttrs = outputWithNullability(inputAttrs, nonNullAttrExprIds)
    generatePredicateCode(
      ctx, inputAttrs, inputExprCode, outputAttrs, notNullPreds, otherPreds,
      nonNullAttrExprIds)
  }

  protected def generatePredicateCode(
      ctx: CodegenContext,
      inputAttrs: Seq[Attribute],
      inputExprCode: Seq[ExprCode],
      outputAttrs: Seq[Attribute],
      notNullPreds: Seq[Expression],
      otherPreds: Seq[Expression],
      nonNullAttrExprIds: Seq[ExprId]): String = {
    /**
     * Generates code for `c`, using `in` for input attributes and `attrs` for nullability.
     */
    def genPredicate(c: Expression, in: Seq[ExprCode], attrs: Seq[Attribute]): String = {
      val bound = BindReferences.bindReference(c, attrs)
      val evaluated = evaluateRequiredVariables(inputAttrs, in, c.references)

      // Generate the code for the predicate.
      val ev = ExpressionCanonicalizer.execute(bound).genCode(ctx)
      val nullCheck = if (bound.nullable) {
        s"${ev.isNull} || "
      } else {
        ""
      }

      s"""
         |$evaluated
         |${ev.code}
         |if (${nullCheck}!${ev.value}) continue;
       """.stripMargin
    }

    // To generate the predicates we will follow this algorithm.
    // For each predicate that is not IsNotNull, we will generate them one by one loading attributes
    // as necessary. For each of both attributes, if there is an IsNotNull predicate we will
    // generate that check *before* the predicate. After all of these predicates, we will generate
    // the remaining IsNotNull checks that were not part of other predicates.
    // This has the property of not doing redundant IsNotNull checks and taking better advantage of
    // short-circuiting, not loading attributes until they are needed.
    // This is very perf sensitive.
    // NOTE: `FilterExec.doConsume`'s CSE branch inlines the same interleaving so it can
    // emit CSE state precomputes between the IsNotNull checks and each otherPred body.
    // Any change to this algorithm must be mirrored there (and vice versa).
    // TODO: revisit this. We can consider reordering predicates as well.
    val generatedIsNotNullChecks = new Array[Boolean](notNullPreds.length)
    val extraIsNotNullAttrs = mutable.Set[Attribute]()
    val generated = otherPreds.map { c =>
      val nullChecks = c.references.map { r =>
        val idx = notNullPreds.indexWhere {
          case IsNotNull(n) => n.semanticEquals(r)
          case _ => false
        }
        if (idx != -1 && !generatedIsNotNullChecks(idx)) {
          generatedIsNotNullChecks(idx) = true
          // Use the child's output. The nullability is what the child produced.
          genPredicate(notNullPreds(idx), inputExprCode, inputAttrs)
        } else if (nonNullAttrExprIds.contains(r.exprId) && !extraIsNotNullAttrs.contains(r)) {
          extraIsNotNullAttrs += r
          genPredicate(IsNotNull(r), inputExprCode, inputAttrs)
        } else {
          ""
        }
      }.mkString("\n").trim

      // Here we use *this* operator's output with this output's nullability since we already
      // enforced them with the IsNotNull checks above.
      s"""
         |$nullChecks
         |${genPredicate(c, inputExprCode, outputAttrs)}
       """.stripMargin.trim
    }.mkString("\n")

    val nullChecks = notNullPreds.zipWithIndex.map { case (c, idx) =>
      if (!generatedIsNotNullChecks(idx)) {
        genPredicate(c, inputExprCode, inputAttrs)
      } else {
        ""
      }
    }.mkString("\n")

    s"""
       |$generated
       |$nullChecks
     """.stripMargin
  }
}

/** Physical plan for Filter. */
case class FilterExec(condition: Expression, child: SparkPlan)
  extends UnaryExecNode with CodegenSupport with GeneratePredicateHelper with SafeForKWayMerge {

  // Split out all the IsNotNulls from condition.
  private val (notNullPreds, otherPreds) = splitConjunctivePredicates(condition).partition {
    case IsNotNull(a) => isNullIntolerant(a) && a.references.subsetOf(child.outputSet)
    case _ => false
  }

  // The columns that will filtered out by `IsNotNull` could be considered as not nullable.
  private val notNullAttributes = notNullPreds.flatMap(_.references).distinct.map(_.exprId)

  // Mark this as empty. We'll evaluate the input during doConsume(). We don't want to evaluate
  // all the variables at the beginning to take advantage of short circuiting.
  override def usedInputs: AttributeSet = AttributeSet.empty

  override def output: Seq[Attribute] = outputWithNullability(child.output, notNullAttributes)

  override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"))

  override def inputRDDs(): Seq[RDD[InternalRow]] = {
    child.asInstanceOf[CodegenSupport].inputRDDs()
  }

  protected override def doProduce(ctx: CodegenContext): String = {
    child.asInstanceOf[CodegenSupport].produce(ctx, this)
  }

  override def doConsume(ctx: CodegenContext, input: Seq[ExprCode], row: ExprCode): String = {
    val numOutput = metricTerm(ctx, "numOutputRows")

    // Apply CSE to otherPreds, mirroring the non-CSE branch's ordering so we preserve
    // short-circuit semantics. The non-CSE branch lives in `generatePredicateCode`
    // above; any future change to that algorithm should be reflected here (and vice
    // versa) -- the two paths must stay in lockstep on notNullPreds/otherPreds
    // interleaving. Three invariants:
    //   (a) Between otherPreds: emit each common subexpression's precompute *just
    //       before* the first otherPred that references it, not at the top of the
    //       `do { }` block. A later otherPred still reuses the cached value -- it just
    //       doesn't pay the cost for rows an earlier otherPred rejected. See #55471.
    //   (b) Between notNullPreds and otherPreds: `notNullPreds` may include
    //       `IsNotNull(expensive_or_throwing)` inferred by `InferFiltersFromConstraints`,
    //       so emitting all notNullPreds up front would evaluate the inner expression
    //       on rows an earlier-ordered otherPred would have rejected -- same class of
    //       bug as (a) but across the notNullPreds / otherPreds boundary. Per-otherPred,
    //       for each reference `r`: if `notNullPreds` has a direct `IsNotNull(r)`, emit
    //       it; else if `r` is covered by `notNullAttributes` through some complex
    //       `IsNotNull(expr(r))` in `notNullPreds`, emit a synthetic `IsNotNull(r)` so
    //       the tightened-output binding below is safe without evaluating the
    //       throw-capable inner expression. Then emit the otherPred's CSE precompute
    //       and body. Remaining notNullPreds (including complex-child forms whose refs
    //       got synthetic coverage) emit after all otherPreds.
    //   (c) CSE state materialization: binding otherPreds (and the CSE analysis)
    //       against `output` (with `notNullAttributes` tightened to non-nullable)
    //       requires that the matching IsNotNull has already short-circuited -- else
    //       CSE's `value_X = isNull ? default : compute` materializes `null` into
    //       `value_X` for non-primitive types, which downstream accessors may NPE on
    //       without consulting `isNull_X`. The (b) interleaving gives us that ordering
    //       for free, since the IsNotNull check fires before the CSE precompute keyed
    //       off the same reference.
    val (prologueCode, predicateCode) =
      if (conf.subexpressionEliminationEnabled && otherPreds.nonEmpty) {
        // Pre-evaluate input variables before CSE analysis: CSE clears
        // ctx.currentVars[i].code as a side effect; without this pre-evaluation, Janino
        // fails when otherPreds reference the same input columns that CSE already
        // consumed.
        val otherPredInputAttrs = AttributeSet(otherPreds.flatMap(_.references))
        val inputVarsEvalCode = evaluateRequiredVariables(
          child.output, input, otherPredInputAttrs)

        val boundOtherPreds = otherPreds.map(BindReferences.bindReference(_, output))
        val subExprs = ctx.subexpressionEliminationForWholeStageCodegen(boundOtherPreds)

        // Group CSE states by the index of the first otherPred that references them.
        // `evaluateSubExprEliminationState` recursively emits each state's children
        // before the state itself and marks emitted code as `EmptyBlock`, so emitting
        // a later group whose states depend on an earlier group's states is a no-op
        // for the already-emitted dependencies.
        val statesByFirstUse: Map[Int, Seq[SubExprEliminationState]] =
          subExprs.states.toSeq.groupBy { case (exprEq, _) =>
            boundOtherPreds.indexWhere(_.exists(e => ExpressionEquals(e) == exprEq))
          }.map { case (idx, kvs) => idx -> kvs.map(_._2) }

        // Emit an IsNotNull check, binding against child.output so the inner expression
        // is evaluated with its original nullability (the tightened-nullability `output`
        // is only appropriate inside otherPreds, where the required checks have fired).
        // The `bound.nullable` gate below is defensive -- `IsNotNull` always produces a
        // non-nullable Boolean, so this branch is structurally unreachable today; it
        // mirrors the non-CSE helper's shape to keep future refactors safe.
        def genNotNull(pred: Expression): String = {
          val bound = BindReferences.bindReference(pred, child.output)
          val evaluated = evaluateRequiredVariables(child.output, input, pred.references)
          val ev = ExpressionCanonicalizer.execute(bound).genCode(ctx)
          val nullCheck = if (bound.nullable) s"${ev.isNull} || " else ""
          s"""
             |$evaluated
             |${ev.code}
             |if (${nullCheck}!${ev.value}) continue;
           """.stripMargin
        }

        val generatedIsNotNullChecks = new Array[Boolean](notNullPreds.length)
        val extraIsNotNullAttrs = mutable.Set[Attribute]()

        val predCode: String = {
          val parts = new StringBuilder
          ctx.withSubExprEliminationExprs(subExprs.states) {
            otherPreds.zip(boundOtherPreds).zipWithIndex.foreach {
              case ((orig, bound), idx) =>
                orig.references.foreach { r =>
                  val ni = notNullPreds.indexWhere {
                    case IsNotNull(c) => c.semanticEquals(r)
                    case _ => false
                  }
                  if (ni != -1 && !generatedIsNotNullChecks(ni)) {
                    generatedIsNotNullChecks(ni) = true
                    parts.append(genNotNull(notNullPreds(ni)))
                    parts.append('\n')
                  } else if (notNullAttributes.contains(r.exprId) &&
                      !extraIsNotNullAttrs.contains(r)) {
                    extraIsNotNullAttrs += r
                    parts.append(genNotNull(IsNotNull(r)))
                    parts.append('\n')
                  }
                }
                statesByFirstUse.get(idx).foreach { states =>
                  parts.append(ctx.evaluateSubExprEliminationState(states))
                  parts.append('\n')
                }
                val ev = ExpressionCanonicalizer.execute(bound).genCode(ctx)
                val nullCheck = if (bound.nullable) s"${ev.isNull} || " else ""
                parts.append(ev.code.toString)
                parts.append(s"\nif (${nullCheck}!${ev.value}) continue;\n")
            }
            Seq.empty
          }
          parts.toString
        }

        // Leftover notNullPreds: any IsNotNull that no otherPred's reference matched by
        // `semanticEquals`. These run after all otherPreds to match the non-CSE ordering.
        // Note: a complex `IsNotNull(expr(r))` can still land here even when the (b) path
        // already emitted a synthetic `IsNotNull(r)` ahead of some otherPred -- the
        // synthetic check only guards the tightened-output binding; the inner expression
        // itself may still need its null check (e.g. under non-ANSI `cast(s as int)` can
        // return null for unparseable strings, so we must filter the resulting row).
        val leftoverNotNull = notNullPreds.zipWithIndex.collect {
          case (p, i) if !generatedIsNotNullChecks(i) => genNotNull(p)
        }.mkString("\n")

        (inputVarsEvalCode, predCode + "\n" + leftoverNotNull)
      } else {
        ("", generatePredicateCode(
          ctx, child.output, input, output, notNullPreds, otherPreds, notNullAttributes))
      }

    // Reset the isNull to false for the not-null columns, then the followed operators could
    // generate better code (remove dead branches).
    val resultVars = input.zipWithIndex.map { case (ev, i) =>
      if (notNullAttributes.contains(child.output(i).exprId)) {
        ev.isNull = FalseLiteral
      }
      ev
    }

    // Note: wrap in "do { } while (false);", so the generated checks can jump out with "continue;"
    s"""
       |do {
       |  $prologueCode
       |  $predicateCode
       |  $numOutput.add(1);
       |  ${consume(ctx, resultVars)}
       |} while (false);
     """.stripMargin
  }

  protected override def doExecute(): RDD[InternalRow] = {
    val numOutputRows = longMetric("numOutputRows")
    val evaluatorFactory = new FilterEvaluatorFactory(condition, child.output, numOutputRows)
    if (conf.usePartitionEvaluator) {
      child.execute().mapPartitionsWithEvaluator(evaluatorFactory)
    } else {
      child.execute().mapPartitionsWithIndexInternal { (index, iter) =>
        val evaluator = evaluatorFactory.createEvaluator()
        evaluator.eval(index, iter)
      }
    }
  }

  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override def verboseStringWithOperatorId(): String = {
    s"""
       |$formattedNodeName
       |${ExplainUtils.generateFieldString("Input", child.output)}
       |Condition : ${condition}
       |""".stripMargin
  }

  override protected def withNewChildInternal(newChild: SparkPlan): FilterExec =
    copy(child = newChild)
}

/**
 * Physical plan for sampling the dataset.
 *
 * @param lowerBound Lower-bound of the sampling probability (usually 0.0)
 * @param upperBound Upper-bound of the sampling probability. The expected fraction sampled
 *                   will be ub - lb.
 * @param withReplacement Whether to sample with replacement.
 * @param seed the random seed
 * @param child the SparkPlan
 */
case class SampleExec(
    lowerBound: Double,
    upperBound: Double,
    withReplacement: Boolean,
    seed: Option[Long],
    child: SparkPlan) extends UnaryExecNode with CodegenSupport {

  val resolvedSeed: Long = seed.getOrElse((math.random() * 1000).toLong)

  override def output: Seq[Attribute] = child.output

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"))

  protected override def doExecute(): RDD[InternalRow] = {
    if (withReplacement) {
      // Disable gap sampling since the gap sampling method buffers two rows internally,
      // requiring us to copy the row, which is more expensive than the random number generator.
      new PartitionwiseSampledRDD[InternalRow, InternalRow](
        child.execute(),
        new PoissonSampler[InternalRow](upperBound - lowerBound, useGapSamplingIfPossible = false),
        preservesPartitioning = true,
        resolvedSeed)
    } else {
      child.execute().randomSampleWithRange(lowerBound, upperBound, resolvedSeed)
    }
  }

  // Mark this as empty. This plan doesn't need to evaluate any inputs and can defer the evaluation
  // to the parent operator.
  override def usedInputs: AttributeSet = AttributeSet.empty

  override def inputRDDs(): Seq[RDD[InternalRow]] = {
    child.asInstanceOf[CodegenSupport].inputRDDs()
  }

  protected override def doProduce(ctx: CodegenContext): String = {
    child.asInstanceOf[CodegenSupport].produce(ctx, this)
  }

  override def needCopyResult: Boolean = {
    child.asInstanceOf[CodegenSupport].needCopyResult || withReplacement
  }

  override def doConsume(ctx: CodegenContext, input: Seq[ExprCode], row: ExprCode): String = {
    val numOutput = metricTerm(ctx, "numOutputRows")

    if (withReplacement) {
      val samplerClass = classOf[PoissonSampler[UnsafeRow]].getName
      val initSampler = ctx.freshName("initSampler")

      // Inline mutable state since not many Sample operations in a task
      val sampler = ctx.addMutableState(s"$samplerClass<UnsafeRow>", "sampleReplace",
        v => {
          val initSamplerFuncName = ctx.addNewFunction(initSampler,
            s"""
              | private void $initSampler() {
              |   $v = new $samplerClass<UnsafeRow>($upperBound - $lowerBound, false);
              |   java.util.Random random = new java.util.Random(${resolvedSeed}L);
              |   long randomSeed = random.nextLong();
              |   int loopCount = 0;
              |   while (loopCount < ${ctx.currentPartitionIndexVar}) {
              |     randomSeed = random.nextLong();
              |     loopCount += 1;
              |   }
              |   $v.setSeed(randomSeed);
              | }
           """.stripMargin.trim)
          s"$initSamplerFuncName();"
        }, forceInline = true)

      val samplingCount = ctx.freshName("samplingCount")
      s"""
         | int $samplingCount = $sampler.sample();
         | while ($samplingCount-- > 0) {
         |   $numOutput.add(1);
         |   ${consume(ctx, input)}
         | }
       """.stripMargin.trim
    } else {
      val samplerClass = classOf[BernoulliCellSampler[UnsafeRow]].getName
      val sampler = ctx.addMutableState(s"$samplerClass<UnsafeRow>", "sampler",
        v => s"""
          | $v = new $samplerClass<UnsafeRow>($lowerBound, $upperBound, false);
          | $v.setSeed(${resolvedSeed}L + ${ctx.currentPartitionIndexVar});
         """.stripMargin.trim)

      s"""
         | if ($sampler.sample() != 0) {
         |   $numOutput.add(1);
         |   ${consume(ctx, input)}
         | }
       """.stripMargin.trim
    }
  }

  override protected def withNewChildInternal(newChild: SparkPlan): SampleExec =
    copy(child = newChild)
}


/**
 * Physical plan for range (generating a range of 64 bit numbers).
 */
case class RangeExec(range: org.apache.spark.sql.catalyst.plans.logical.Range)
  extends LeafExecNode with CodegenSupport {

  val start: Long = range.start
  val end: Long = range.end
  val step: Long = range.step
  val numSlices: Int = range.numSlices.getOrElse(session.leafNodeDefaultParallelism)
  val numElements: BigInt = range.numElements
  val isEmptyRange: Boolean = start == end || (start < end ^ 0 < step)

  override val output: Seq[Attribute] = range.output

  override def outputOrdering: Seq[SortOrder] = range.outputOrdering

  override def outputPartitioning: Partitioning = {
    if (numElements > 0) {
      if (numSlices == 1) {
        SinglePartition
      } else {
        RangePartitioning(outputOrdering, numSlices)
      }
    } else {
      UnknownPartitioning(0)
    }
  }

  override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"))

  override def doCanonicalize(): SparkPlan = {
    RangeExec(range.canonicalized.asInstanceOf[org.apache.spark.sql.catalyst.plans.logical.Range])
  }

  override def inputRDDs(): Seq[RDD[InternalRow]] = {
    val rdd = if (isEmptyRange) {
      new EmptyRDD[InternalRow](sparkContext)
    } else {
      sparkContext.parallelize(0 until numSlices, numSlices).map(i => InternalRow(i))
    }
    rdd :: Nil
  }

  protected override def doProduce(ctx: CodegenContext): String = {
    val numOutput = metricTerm(ctx, "numOutputRows")

    val initTerm = ctx.addMutableState(CodeGenerator.JAVA_BOOLEAN, "initRange")
    val nextIndex = ctx.addMutableState(CodeGenerator.JAVA_LONG, "nextIndex")

    val value = ctx.freshName("value")
    val ev = ExprCode.forNonNullValue(JavaCode.variable(value, LongType))
    val BigInt = classOf[java.math.BigInteger].getName

    // Inline mutable state since not many Range operations in a task
    val taskContext = ctx.addMutableState("TaskContext", "taskContext",
      v => s"$v = TaskContext.get();", forceInline = true)
    val inputMetrics = ctx.addMutableState("InputMetrics", "inputMetrics",
      v => s"$v = $taskContext.taskMetrics().inputMetrics();", forceInline = true)

    // In order to periodically update the metrics without inflicting performance penalty, this
    // operator produces elements in batches. After a batch is complete, the metrics are updated
    // and a new batch is started.
    // In the implementation below, the code in the inner loop is producing all the values
    // within a batch, while the code in the outer loop is setting batch parameters and updating
    // the metrics.

    // Once nextIndex == batchEnd, it's time to progress to the next batch.
    val batchEnd = ctx.addMutableState(CodeGenerator.JAVA_LONG, "batchEnd")

    // How many values should still be generated by this range operator.
    val numElementsTodo = ctx.addMutableState(CodeGenerator.JAVA_LONG, "numElementsTodo")

    // How many values should be generated in the next batch.
    val nextBatchTodo = ctx.freshName("nextBatchTodo")

    // The default size of a batch, which must be positive integer
    val batchSize = 1000

    val initRangeName = ctx.freshName("initRange")
    val initRangeFuncName = ctx.addNewFunction(initRangeName,
      s"""
        | private void $initRangeName(int idx) {
        |   $BigInt index = $BigInt.valueOf(idx);
        |   $BigInt numSlice = $BigInt.valueOf(${numSlices}L);
        |   $BigInt numElement = $BigInt.valueOf(${numElements.toLong}L);
        |   $BigInt step = $BigInt.valueOf(${step}L);
        |   $BigInt start = $BigInt.valueOf(${start}L);
        |   long partitionEnd;
        |
        |   $BigInt st = index.multiply(numElement).divide(numSlice).multiply(step).add(start);
        |   if (st.compareTo($BigInt.valueOf(Long.MAX_VALUE)) > 0) {
        |     $nextIndex = Long.MAX_VALUE;
        |   } else if (st.compareTo($BigInt.valueOf(Long.MIN_VALUE)) < 0) {
        |     $nextIndex = Long.MIN_VALUE;
        |   } else {
        |     $nextIndex = st.longValue();
        |   }
        |   $batchEnd = $nextIndex;
        |
        |   $BigInt end = index.add($BigInt.ONE).multiply(numElement).divide(numSlice)
        |     .multiply(step).add(start);
        |   if (end.compareTo($BigInt.valueOf(Long.MAX_VALUE)) > 0) {
        |     partitionEnd = Long.MAX_VALUE;
        |   } else if (end.compareTo($BigInt.valueOf(Long.MIN_VALUE)) < 0) {
        |     partitionEnd = Long.MIN_VALUE;
        |   } else {
        |     partitionEnd = end.longValue();
        |   }
        |
        |   $BigInt startToEnd = $BigInt.valueOf(partitionEnd).subtract(
        |     $BigInt.valueOf($nextIndex));
        |   $numElementsTodo  = startToEnd.divide(step).longValue();
        |   if ($numElementsTodo < 0) {
        |     $numElementsTodo = 0;
        |   } else if (startToEnd.remainder(step).compareTo($BigInt.valueOf(0L)) != 0) {
        |     $numElementsTodo++;
        |   }
        | }
       """.stripMargin)

    val localIdx = ctx.freshName("localIdx")
    val localEnd = ctx.freshName("localEnd")
    val stopCheck = if (parent.needStopCheck) {
      s"""
         |if (shouldStop()) {
         |  $nextIndex = $value + ${step}L;
         |  $numOutput.add($localIdx + 1);
         |  $inputMetrics.incRecordsRead($localIdx + 1);
         |  return;
         |}
       """.stripMargin
    } else {
      "// shouldStop check is eliminated"
    }
    val loopCondition = if (limitNotReachedChecks.isEmpty) {
      "true"
    } else {
      limitNotReachedChecks.mkString(" && ")
    }

    // An overview of the Range processing.
    //
    // For each partition, the Range task needs to produce records from partition start(inclusive)
    // to end(exclusive). For better performance, we separate the partition range into batches, and
    // use 2 loops to produce data. The outer while loop is used to iterate batches, and the inner
    // for loop is used to iterate records inside a batch.
    //
    // `nextIndex` tracks the index of the next record that is going to be consumed, initialized
    // with partition start. `batchEnd` tracks the end index of the current batch, initialized
    // with `nextIndex`. In the outer loop, we first check if `nextIndex == batchEnd`. If it's true,
    // it means the current batch is fully consumed, and we will update `batchEnd` to process the
    // next batch. If `batchEnd` reaches partition end, exit the outer loop. Finally we enter the
    // inner loop. Note that, when we enter inner loop, `nextIndex` must be different from
    // `batchEnd`, otherwise we already exit the outer loop.
    //
    // The inner loop iterates from 0 to `localEnd`, which is calculated by
    // `(batchEnd - nextIndex) / step`. Since `batchEnd` is increased by `nextBatchTodo * step` in
    // the outer loop, and initialized with `nextIndex`, so `batchEnd - nextIndex` is always
    // divisible by `step`. The `nextIndex` is increased by `step` during each iteration, and ends
    // up being equal to `batchEnd` when the inner loop finishes.
    //
    // The inner loop can be interrupted, if the query has produced at least one result row, so that
    // we don't buffer too many result rows and waste memory. It's ok to interrupt the inner loop,
    // because `nextIndex` will be updated before interrupting.

    s"""
      | // initialize Range
      | if (!$initTerm) {
      |   $initTerm = true;
      |   $initRangeFuncName(${ctx.currentPartitionIndexVar});
      | }
      |
      | while ($loopCondition) {
      |   if ($nextIndex == $batchEnd) {
      |     long $nextBatchTodo;
      |     if ($numElementsTodo > ${batchSize}L) {
      |       $nextBatchTodo = ${batchSize}L;
      |       $numElementsTodo -= ${batchSize}L;
      |     } else {
      |       $nextBatchTodo = $numElementsTodo;
      |       $numElementsTodo = 0;
      |       if ($nextBatchTodo == 0) break;
      |     }
      |     $batchEnd += $nextBatchTodo * ${step}L;
      |   }
      |
      |   int $localEnd = (int)(($batchEnd - $nextIndex) / ${step}L);
      |   for (int $localIdx = 0; $localIdx < $localEnd; $localIdx++) {
      |     long $value = ((long)$localIdx * ${step}L) + $nextIndex;
      |     ${consume(ctx, Seq(ev))}
      |     $stopCheck
      |   }
      |   $nextIndex = $batchEnd;
      |   $numOutput.add($localEnd);
      |   $inputMetrics.incRecordsRead($localEnd);
      |   $taskContext.killTaskIfInterrupted();
      | }
     """.stripMargin
  }

  protected override def doExecute(): RDD[InternalRow] = {
    val numOutputRows = longMetric("numOutputRows")
    if (isEmptyRange) {
      new EmptyRDD[InternalRow](sparkContext)
    } else {
      sparkContext
        .parallelize(0 until numSlices, numSlices)
        .mapPartitionsWithIndex { (i, _) =>
          val partitionStart = (i * numElements) / numSlices * step + start
          val partitionEnd = (((i + 1) * numElements) / numSlices) * step + start

          def getSafeMargin(bi: BigInt): Long =
            if (bi.isValidLong) {
              bi.toLong
            } else if (bi > 0) {
              Long.MaxValue
            } else {
              Long.MinValue
            }

          val safePartitionStart = getSafeMargin(partitionStart)
          val safePartitionEnd = getSafeMargin(partitionEnd)
          val rowSize = UnsafeRow.calculateBitSetWidthInBytes(1) + LongType.defaultSize
          val unsafeRow = UnsafeRow.createFromByteArray(rowSize, 1)
          val taskContext = TaskContext.get()

          val iter = new Iterator[InternalRow] {
            private[this] var number: Long = safePartitionStart
            private[this] var overflow: Boolean = false
            private[this] val inputMetrics = taskContext.taskMetrics().inputMetrics

            override def hasNext =
              if (!overflow) {
                if (step > 0) {
                  number < safePartitionEnd
                } else {
                  number > safePartitionEnd
                }
              } else false

            override def next() = {
              val ret = number
              number += step
              if (number < ret ^ step < 0) {
                // we have Long.MaxValue + Long.MaxValue < Long.MaxValue
                // and Long.MinValue + Long.MinValue > Long.MinValue, so iff the step causes a step
                // back, we are pretty sure that we have an overflow.
                overflow = true
              }

              numOutputRows += 1
              inputMetrics.incRecordsRead(1)
              unsafeRow.setLong(0, ret)
              unsafeRow
            }
          }
          new InterruptibleIterator(taskContext, iter)
        }
    }
  }

  override def simpleString(maxFields: Int): String = {
    s"Range ($start, $end, step=$step, splits=$numSlices)"
  }
}

/**
 * Physical plan for unioning two plans, without a distinct. This is UNION ALL in SQL.
 *
 * If we change how this is implemented physically, we'd need to update
 * [[org.apache.spark.sql.catalyst.plans.logical.Union.maxRowsPerPartition]].
 */
case class UnionExec(children: Seq[SparkPlan]) extends SparkPlan with CodegenSupport {
  // updating nullability to make all the children consistent
  override def output: Seq[Attribute] = {
    children.map(_.output).transpose.map { attrs =>
      val firstAttr = attrs.head
      val nullable = attrs.exists(_.nullable)
      val newDt = attrs.map(_.dataType).reduce(StructType.unionLikeMerge)
      if (firstAttr.dataType == newDt) {
        firstAttr.withNullability(nullable)
      } else {
        AttributeReference(firstAttr.name, newDt, nullable, firstAttr.metadata)(
          firstAttr.exprId, firstAttr.qualifier)
      }
    }
  }

  /**
   * Returns the output partitionings of the children, with the attributes converted to
   * the first child's attributes at the same position.
   */
  private def prepareOutputPartitioning(): Seq[Partitioning] = {
    // Create a map of attributes from the other children to the first child.
    val firstAttrs = children.head.output
    val attributesMap = children.tail.map(_.output).map { otherAttrs =>
      AttributeMap(otherAttrs.zip(firstAttrs))
    }

    val partitionings = children.map(_.outputPartitioning)
    val firstPartitioning = partitionings.head
    val otherPartitionings = partitionings.tail

    val convertedOtherPartitionings = otherPartitionings.zipWithIndex.map { case (p, idx) =>
      val attributeMap = attributesMap(idx)
      p match {
        case e: Expression =>
          e.transform {
            case a: Attribute if attributeMap.contains(a) =>
              attributeMap(a)
          }.asInstanceOf[Partitioning]
        case _ => p
      }
    }
    Seq(firstPartitioning) ++ convertedOtherPartitionings
  }

  private def comparePartitioning(left: Partitioning, right: Partitioning): Boolean = {
    (left, right) match {
      case (SinglePartition, SinglePartition) => true
      case (l: HashPartitioningLike, r: HashPartitioningLike) => l == r
      // Note: two `RangePartitioning`s with even same ordering and number of partitions
      // are not equal, because they might have different partition bounds.
      case _ => false
    }
  }

  override def outputPartitioning: Partitioning = {
    if (conf.getConf(SQLConf.UNION_OUTPUT_PARTITIONING)) {
      val partitionings = prepareOutputPartitioning()
      if (partitionings.forall(comparePartitioning(_, partitionings.head))) {
        val partitioner = partitionings.head

        // Take the output attributes of this union and map the partitioner to them.
        val attributeMap = children.head.output.zip(output).toMap
        partitioner match {
          case e: Expression =>
            e.transform {
              case a: Attribute if attributeMap.contains(a) => attributeMap(a)
            }.asInstanceOf[Partitioning]
          case _ => partitioner
        }
      } else {
        super.outputPartitioning
      }
    } else {
      super.outputPartitioning
    }
  }

  // `WidenSetOperationTypes` inserts a `Project(Cast)` above each child whose
  // dataType differs from the widened set type, so on the codegen path
  // `src.dataType == tgt.dataType` holds. The Alias only remaps each child
  // attribute onto the union's output exprId/name/metadata. Mismatched cases
  // are gated upstream by `allChildOutputDataTypesMatch`, so the assert is a
  // defensive guard.
  @transient private lazy val perChildProjections: IndexedSeq[Seq[NamedExpression]] =
    children.toIndexedSeq.map { child =>
      child.output.zip(output).map { case (src, tgt) =>
        assert(src.dataType == tgt.dataType,
          s"UnionExec child output dataType ${src.dataType} does not match " +
            s"union output dataType ${tgt.dataType}; supportCodegen should " +
            "have returned false via the 'type-mismatch' reason.")
        Alias(src, tgt.name)(
          exprId = tgt.exprId,
          qualifier = tgt.qualifier,
          explicitMetadata = Some(tgt.metadata))
      }
    }

  // True iff every child output dataType matches the corresponding union
  // output dataType, including all nested nullabilities.
  // `Union.allChildrenCompatible` ignores nested nullability, so children
  // differing only there bypass `WidenSetOperationTypes`; `UnionExec.output`
  // then merges those flags via `StructType.unionLikeMerge`, leaving src/tgt
  // mismatched.
  @transient private lazy val allChildOutputDataTypesMatch: Boolean =
    children.forall { c =>
      c.output.zip(output).forall { case (src, tgt) => src.dataType == tgt.dataType }
    }

  // Memoized: `supportCodegen` is called multiple times during planning.
  @transient private lazy val hasAnyPartitionIndexDependentDescendant: Boolean =
    children.exists(UnionExec.hasPartitionIndexDependentCodegen)

  // Memoized: consulted by `supportCodegen` (called multiple times by
  // `CollapseCodegenStages`) and by `metrics`. Conf and children are stable
  // for a given UnionExec instance; cross-plan staleness is impossible since
  // UnionExec is a case class and `withNewChildren` produces a fresh instance.
  @transient private lazy val supportCodegenFailureReason: Option[String] = {
    if (!conf.getConf(SQLConf.WHOLESTAGE_UNION_CODEGEN_ENABLED)) {
      Some("union-codegen-disabled")
    } else if (!outputPartitioning.isInstanceOf[UnknownPartitioning]) {
      Some("partitioning-aware")
    } else if (children.exists(_.exists(_.isInstanceOf[UnionExec]))) {
      Some("nested-union")
    } else if (children.exists(_.exists(UnionExec.isKnownMultiInputRDDCodegen))) {
      Some("multi-rdd-child")
    } else if (hasAnyPartitionIndexDependentDescendant) {
      Some("partition-index-dependent-child")
    } else if (children.size > conf.getConf(SQLConf.WHOLESTAGE_UNION_MAX_CHILDREN)) {
      Some("max-children-exceeded")
    } else if (supportsColumnar) {
      Some("columnar")
    } else if (!allChildOutputDataTypesMatch) {
      Some("type-mismatch")
    } else {
      None
    }
  }

  override def supportCodegen: Boolean = {
    val reason = supportCodegenFailureReason
    if (reason.isEmpty) true
    else {
      logDebug(log"UnionExec codegen skipped: " +
        log"reason=${MDC(LogKeys.REASON, reason.get)}, " +
        log"numChildren=${MDC(LogKeys.NUM_CHILDREN, children.size)}\n" +
        log"${MDC(LogKeys.TREE_NODE, treeString)}")
      false
    }
  }

  // Registered only when fusion will actually run, so plans that fall back
  // to `doExecute` (which never updates the metric) do not surface a
  // 0-valued row count in the SQL UI. `doConsume` is the sole incrementer.
  override lazy val metrics: Map[String, SQLMetric] =
    if (supportCodegenFailureReason.isEmpty) {
      Map("numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"))
    } else {
      Map.empty
    }

  // Builds a plain `UnionRDD` directly (not `SparkContext.union`) to preserve
  // a 1:1 partition-to-child mapping via `UnionPartition.parentRddIndex`.
  // The `require` below is a backstop: any multi-RDD `CodegenSupport`
  // operator missing from `isKnownMultiInputRDDCodegen` will trip here
  // instead of falling back gracefully.
  @transient private lazy val unionedInputRDD: RDD[InternalRow] = {
    val childRDDs: Seq[RDD[InternalRow]] = children.map { c =>
      val cs = c.asInstanceOf[CodegenSupport]
      val rdds = cs.inputRDDs()
      require(rdds.size == 1,
        s"UnionExec.inputRDDs: child ${c.nodeName} returned ${rdds.size} RDDs")
      rdds.head
    }
    new UnionRDD(sparkContext, childRDDs)
  }

  override def inputRDDs(): Seq[RDD[InternalRow]] = Seq(unionedInputRDD)

  // Driver-side cursor written by `doProduce` and read by `doConsume` during
  // single-threaded code emission; resets to -1 once emission completes.
  @transient private var currentEmittingChild: Int = -1

  override protected def doProduce(ctx: CodegenContext): String = {
    // For each partition of the unioned RDD, record its owning child and its
    // index within that child's RDD. Read both fields directly off the
    // `UnionPartition` so the lookup arrays do not assume `UnionRDD` lays
    // partitions out in child order.
    val (partitionToChild, partitionToLocalIdx) =
      unionedInputRDD.partitions.map {
        case up: UnionPartition[_] => (up.parentRddIndex, up.parentPartition.index)
        case other =>
          throw SparkException.internalError(
            s"UnionExec: Unexpected partition type ${other.getClass.getName}")
      }.unzip
    val p2cRef = ctx.addReferenceObj("partitionToChild", partitionToChild)
    val p2lRef = ctx.addReferenceObj("partitionToLocalIdx", partitionToLocalIdx)
    val childIndexVar = ctx.freshName("unionChildIdx")

    // Each child's produce output is wrapped in its own helper method. The
    // outer `switch` in `doProduce`'s return value dispatches to the helper.
    // Without this, the fused method's bytecode grows linearly with the
    // number of children and quickly exceeds HotSpot's per-method limit,
    // forcing the whole stage to run interpreted.
    //
    // `partitionIndex` is passed as a parameter (shadowing the superclass
    // field) rather than read from the enclosing scope. `addNewFunction` may
    // spill helpers into a nested class when the outer class fills up, and a
    // nested class cannot access the protected
    // `BufferedRowIterator.partitionIndex` field. Using the parameter name
    // `partitionIndex` keeps any child-emitted reference to that identifier
    // resolving locally.
    val savedPartIdxVar = ctx.currentPartitionIndexVar
    val cases = try {
      children.zipWithIndex.map { case (c, i) =>
        currentEmittingChild = i
        ctx.currentPartitionIndexVar = s"((int[]) $p2lRef)[partitionIndex]"
        val producedCode = c.asInstanceOf[CodegenSupport].produce(ctx, this)
        val helper = ctx.freshName("unionChildProcess")
        val qualifiedHelper = ctx.addNewFunction(helper,
          s"""
             |private void $helper(int partitionIndex) throws java.io.IOException {
             |  $producedCode
             |}
           """.stripMargin)
        s"""case $i: {
           |  $qualifiedHelper(partitionIndex);
           |  break;
           |}""".stripMargin
      }
    } finally {
      currentEmittingChild = -1
      ctx.currentPartitionIndexVar = savedPartIdxVar
    }

    s"""
       |int $childIndexVar = ((int[]) $p2cRef)[partitionIndex];
       |switch ($childIndexVar) {
       |  ${cases.mkString("\n")}
       |  default:
       |    throw new java.lang.IllegalStateException(
       |      "UnionExec: Unexpected childIndex=" + $childIndexVar);
       |}
     """.stripMargin
  }

  override def doConsume(
      ctx: CodegenContext, input: Seq[ExprCode], row: ExprCode): String = {
    require(currentEmittingChild >= 0,
      "UnionExec.doConsume invoked outside doProduce emission window")
    val i = currentEmittingChild
    // The wrapped child in each `perChildProjections(i)` element is always an
    // `Attribute`, which is deterministic by definition; no
    // `evaluateRequiredVariables` call is needed to force single-evaluation
    // of non-deterministic expressions.
    val bound = BindReferences.bindReferences(perChildProjections(i), children(i).output)

    // Route BoundReference reads through `currentVars` (the incoming row is
    // delivered as variables under WSCG, not via ctx.INPUT_ROW).
    ctx.currentVars = input
    ctx.INPUT_ROW = null
    val projectedExprCodes = bound.map(_.genCode(ctx))

    val numOutput = metricTerm(ctx, "numOutputRows")
    s"""
       |$numOutput.add(1L);
       |${consume(ctx, projectedExprCodes)}
     """.stripMargin
  }

  // True if any child requires result copying; the default throws for
  // multi-child operators and is unsuitable here.
  override def needCopyResult: Boolean =
    children.exists(_.asInstanceOf[CodegenSupport].needCopyResult)

  // `doConsume` handles projection and emission; the parent's `consume` driver
  // decides which output columns to materialize.
  override def usedInputs: AttributeSet = AttributeSet.empty

  protected override def doExecute(): RDD[InternalRow] = {
    if (outputPartitioning.isInstanceOf[UnknownPartitioning]) {
      sparkContext.union(children.map(_.execute()))
    } else {
      // This union has a known partitioning, i.e., its children have the same partitioning
      // in semantics so this union can choose not to change the partitioning by using a
      // custom partitioning aware union RDD.
      val nonEmptyRdds = children.map(_.execute()).filter(!_.partitions.isEmpty)
      new SQLPartitioningAwareUnionRDD(sparkContext, nonEmptyRdds, outputPartitioning.numPartitions)
    }
  }

  override def supportsColumnar: Boolean = children.forall(_.supportsColumnar)

  override def supportsRowBased: Boolean = children.forall(_.supportsRowBased)

  protected override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    sparkContext.union(children.map(_.executeColumnar()))
  }

  override protected def withNewChildrenInternal(newChildren: IndexedSeq[SparkPlan]): UnionExec =
    copy(children = newChildren)
}

object UnionExec {
  /**
   * Codegen operators that return more than one RDD from `inputRDDs()`.
   * `UnionExec`'s fusion assumes each direct child contributes one RDD.
   */
  def isKnownMultiInputRDDCodegen(p: SparkPlan): Boolean = p match {
    case _: SortMergeJoinExec => true
    case _: ShuffledHashJoinExec => true
    case _ => false
  }

  /**
   * True if any expression in the subtree is [[Nondeterministic]]. Such
   * expressions may embed the raw `partitionIndex` field via
   * `addPartitionInitializationStatement`, which would read the global
   * UnionRDD index instead of the child-local one under fusion.
   */
  def hasPartitionIndexDependentCodegen(p: SparkPlan): Boolean = p.exists {
    plan => plan.expressions.exists(_.exists(_.isInstanceOf[Nondeterministic]))
  }
}

/**
 * Physical plan for returning a new RDD that has exactly `numPartitions` partitions.
 * Similar to coalesce defined on an [[RDD]], this operation results in a narrow dependency, e.g.
 * if you go from 1000 partitions to 100 partitions, there will not be a shuffle, instead each of
 * the 100 new partitions will claim 10 of the current partitions.  If a larger number of partitions
 * is requested, it will stay at the current number of partitions.
 *
 * However, if you're doing a drastic coalesce, e.g. to numPartitions = 1,
 * this may result in your computation taking place on fewer nodes than
 * you like (e.g. one node in the case of numPartitions = 1). To avoid this,
 * you see ShuffleExchange. This will add a shuffle step, but means the
 * current upstream partitions will be executed in parallel (per whatever
 * the current partitioning is).
 */
case class CoalesceExec(numPartitions: Int, child: SparkPlan) extends UnaryExecNode {
  override def output: Seq[Attribute] = child.output

  override def outputPartitioning: Partitioning = {
    if (numPartitions == 1) SinglePartition
    else UnknownPartitioning(numPartitions)
  }

  protected override def doExecute(): RDD[InternalRow] = {
    val rdd = child.execute()
    if (numPartitions == 1 && rdd.getNumPartitions < 1) {
      // Make sure we don't output an RDD with 0 partitions, when claiming that we have a
      // `SinglePartition`.
      new CoalesceExec.EmptyRDDWithPartitions(sparkContext, numPartitions)
    } else {
      rdd.coalesce(numPartitions, shuffle = false)
    }
  }

  override protected def withNewChildInternal(newChild: SparkPlan): CoalesceExec =
    copy(child = newChild)
}

object CoalesceExec {
  /** A simple RDD with no data, but with the given number of partitions. */
  class EmptyRDDWithPartitions(
      @transient private val sc: SparkContext,
      numPartitions: Int) extends RDD[InternalRow](sc, Nil) {

    override def getPartitions: Array[Partition] =
      Array.tabulate(numPartitions)(i => EmptyPartition(i))

    override def compute(split: Partition, context: TaskContext): Iterator[InternalRow] = {
      Iterator.empty
    }
  }

  case class EmptyPartition(index: Int) extends Partition
}

/**
 * Parent class for different types of subquery plans
 */
abstract class BaseSubqueryExec extends SparkPlan {
  def name: String
  def child: SparkPlan

  override def output: Seq[Attribute] = child.output

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

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
    /**
     * In the new explain mode `EXPLAIN FORMATTED`, the subqueries are not shown in the
     * main plan and are printed separately along with correlation information with
     * its parent plan. The condition below makes sure that subquery plans are
     * excluded from the main plan.
     */
    if (!printNodeId) {
      super.generateTreeString(
        depth,
        lastChildren,
        append,
        verbose,
        "",
        false,
        maxFields,
        printNodeId,
        printOutputColumns,
        indent)
    }
  }
}

/**
 * Physical plan for a subquery.
 */
case class SubqueryExec(name: String, child: SparkPlan, maxNumRows: Option[Int] = None)
  extends BaseSubqueryExec with UnaryExecNode {

  override lazy val metrics = Map(
    "dataSize" -> SQLMetrics.createSizeMetric(sparkContext, "data size"),
    "collectTime" -> SQLMetrics.createTimingMetric(sparkContext, "time to collect"))

  @transient
  private lazy val relationFuture: JFuture[Array[InternalRow]] = {
    // relationFuture is used in "doExecute". Therefore we can get the execution id correctly here.
    val executionId = sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)
    SQLExecution.withThreadLocalCaptured[Array[InternalRow]](
      session,
      SubqueryExec.executionContext) {
      // This will run in another thread. Set the execution id so that we can connect these jobs
      // with the correct execution.
      SQLExecution.withExecutionId(session, executionId) {
        val beforeCollect = System.nanoTime()
        // Note that we use .executeCollect() because we don't want to convert data to Scala types
        val rows: Array[InternalRow] = if (maxNumRows.isDefined) {
          child.executeTake(maxNumRows.get)
        } else {
          child.executeCollect()
        }
        val beforeBuild = System.nanoTime()
        longMetric("collectTime") += NANOSECONDS.toMillis(beforeBuild - beforeCollect)
        val dataSize = rows.map(_.asInstanceOf[UnsafeRow].getSizeInBytes.toLong).sum
        longMetric("dataSize") += dataSize

        SQLMetrics.postDriverMetricUpdates(sparkContext, executionId, metrics.values.toSeq)
        rows
      }
    }
  }

  protected override def doCanonicalize(): SparkPlan = {
    SubqueryExec("Subquery", child.canonicalized, maxNumRows)
  }

  protected override def doPrepare(): Unit = {
    relationFuture
  }

  // `SubqueryExec` should only be used by calling `executeCollect`. It launches a new thread to
  // collect the result of `child`. We should not trigger codegen of `child` again in other threads,
  // as generating code is not thread-safe.
  override def executeCollect(): Array[InternalRow] = {
    ThreadUtils.awaitResult(relationFuture, Duration.Inf)
  }

  protected override def doExecute(): RDD[InternalRow] = {
    throw SparkException.internalError("SubqueryExec.doExecute should never be called")
  }

  override def executeTake(n: Int): Array[InternalRow] = {
    throw SparkException.internalError("SubqueryExec.executeTake should never be called")
  }

  override def executeTail(n: Int): Array[InternalRow] = {
    throw SparkException.internalError("SubqueryExec.executeTail should never be called")
  }

  override def stringArgs: Iterator[Any] = Iterator(name, child) ++ Iterator(s"[id=#$id]")

  override protected def withNewChildInternal(newChild: SparkPlan): SubqueryExec =
    copy(child = newChild)
}

object SubqueryExec {
  private[execution] val executionContext = ExecutionContext.fromExecutorService(
    ThreadUtils.newDaemonCachedThreadPool("subquery",
      SQLConf.get.getConf(StaticSQLConf.SUBQUERY_MAX_THREAD_THRESHOLD)))

  def createForScalarSubquery(name: String, child: SparkPlan): SubqueryExec = {
    // Scalar subquery needs only one row. We require 2 rows here to validate if the scalar query is
    // invalid(return more than one row). We don't need all the rows as it may OOM.
    SubqueryExec(name, child, maxNumRows = Some(2))
  }
}

/**
 * A wrapper for reused [[BaseSubqueryExec]].
 */
case class ReusedSubqueryExec(child: BaseSubqueryExec)
  extends BaseSubqueryExec with LeafExecNode {

  override def name: String = child.name

  override def output: Seq[Attribute] = child.output
  override def doCanonicalize(): SparkPlan = child.canonicalized
  override def outputOrdering: Seq[SortOrder] = child.outputOrdering
  override def outputPartitioning: Partitioning = child.outputPartitioning

  protected override def doPrepare(): Unit = child.prepare()

  protected override def doExecute(): RDD[InternalRow] = child.execute()

  override def executeCollect(): Array[InternalRow] = child.executeCollect()
}
