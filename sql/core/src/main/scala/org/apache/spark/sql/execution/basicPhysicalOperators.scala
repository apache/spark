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

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.Duration

import org.apache.spark.{InterruptibleIterator, SparkException, TaskContext}
import org.apache.spark.rdd.{EmptyRDD, PartitionwiseSampledRDD, RDD}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode, ExpressionCanonicalizer}
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.execution.ui.SparkListenerDriverAccumUpdates
import org.apache.spark.sql.types.LongType
import org.apache.spark.util.ThreadUtils
import org.apache.spark.util.random.{BernoulliCellSampler, PoissonSampler}

/** Physical plan for Project. */
case class ProjectExec(projectList: Seq[NamedExpression], child: SparkPlan)
  extends UnaryExecNode with CodegenSupport {

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
    val exprs = projectList.map(x =>
      ExpressionCanonicalizer.execute(BindReferences.bindReference(x, child.output)))
    ctx.currentVars = input
    val resultVars = exprs.map(_.genCode(ctx))
    // Evaluation of non-deterministic expressions can't be deferred.
    val nonDeterministicAttrs = projectList.filterNot(_.deterministic).map(_.toAttribute)
    s"""
       |${evaluateRequiredVariables(output, resultVars, AttributeSet(nonDeterministicAttrs))}
       |${consume(ctx, resultVars)}
     """.stripMargin
  }

  protected override def doExecute(): RDD[InternalRow] = {
    child.execute().mapPartitionsWithIndexInternal { (index, iter) =>
      val project = UnsafeProjection.create(projectList, child.output,
        subexpressionEliminationEnabled)
      project.initialize(index)
      iter.map(project)
    }
  }

  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  override def outputPartitioning: Partitioning = child.outputPartitioning
}


/** Physical plan for Filter. */
case class FilterExec(condition: Expression, child: SparkPlan)
  extends UnaryExecNode with CodegenSupport with PredicateHelper {

  // Split out all the IsNotNulls from condition.
  private val (notNullPreds, otherPreds) = splitConjunctivePredicates(condition).partition {
    case IsNotNull(a) => isNullIntolerant(a) && a.references.subsetOf(child.outputSet)
    case _ => false
  }

  // If one expression and its children are null intolerant, it is null intolerant.
  private def isNullIntolerant(expr: Expression): Boolean = expr match {
    case e: NullIntolerant => e.children.forall(isNullIntolerant)
    case _ => false
  }

  // The columns that will filtered out by `IsNotNull` could be considered as not nullable.
  private val notNullAttributes = notNullPreds.flatMap(_.references).distinct.map(_.exprId)

  // Mark this as empty. We'll evaluate the input during doConsume(). We don't want to evaluate
  // all the variables at the beginning to take advantage of short circuiting.
  override def usedInputs: AttributeSet = AttributeSet.empty

  override def output: Seq[Attribute] = {
    child.output.map { a =>
      if (a.nullable && notNullAttributes.contains(a.exprId)) {
        a.withNullability(false)
      } else {
        a
      }
    }
  }

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

    /**
     * Generates code for `c`, using `in` for input attributes and `attrs` for nullability.
     */
    def genPredicate(c: Expression, in: Seq[ExprCode], attrs: Seq[Attribute]): String = {
      val bound = BindReferences.bindReference(c, attrs)
      val evaluated = evaluateRequiredVariables(child.output, in, c.references)

      // Generate the code for the predicate.
      val ev = ExpressionCanonicalizer.execute(bound).genCode(ctx)
      val nullCheck = if (bound.nullable) {
        s"${ev.isNull} || "
      } else {
        s""
      }

      s"""
         |$evaluated
         |${ev.code}
         |if (${nullCheck}!${ev.value}) continue;
       """.stripMargin
    }

    ctx.currentVars = input

    // To generate the predicates we will follow this algorithm.
    // For each predicate that is not IsNotNull, we will generate them one by one loading attributes
    // as necessary. For each of both attributes, if there is an IsNotNull predicate we will
    // generate that check *before* the predicate. After all of these predicates, we will generate
    // the remaining IsNotNull checks that were not part of other predicates.
    // This has the property of not doing redundant IsNotNull checks and taking better advantage of
    // short-circuiting, not loading attributes until they are needed.
    // This is very perf sensitive.
    // TODO: revisit this. We can consider reordering predicates as well.
    val generatedIsNotNullChecks = new Array[Boolean](notNullPreds.length)
    val generated = otherPreds.map { c =>
      val nullChecks = c.references.map { r =>
        val idx = notNullPreds.indexWhere { n => n.asInstanceOf[IsNotNull].child.semanticEquals(r)}
        if (idx != -1 && !generatedIsNotNullChecks(idx)) {
          generatedIsNotNullChecks(idx) = true
          // Use the child's output. The nullability is what the child produced.
          genPredicate(notNullPreds(idx), input, child.output)
        } else {
          ""
        }
      }.mkString("\n").trim

      // Here we use *this* operator's output with this output's nullability since we already
      // enforced them with the IsNotNull checks above.
      s"""
         |$nullChecks
         |${genPredicate(c, input, output)}
       """.stripMargin.trim
    }.mkString("\n")

    val nullChecks = notNullPreds.zipWithIndex.map { case (c, idx) =>
      if (!generatedIsNotNullChecks(idx)) {
        genPredicate(c, input, child.output)
      } else {
        ""
      }
    }.mkString("\n")

    // Reset the isNull to false for the not-null columns, then the followed operators could
    // generate better code (remove dead branches).
    val resultVars = input.zipWithIndex.map { case (ev, i) =>
      if (notNullAttributes.contains(child.output(i).exprId)) {
        ev.isNull = "false"
      }
      ev
    }

    s"""
       |$generated
       |$nullChecks
       |$numOutput.add(1);
       |${consume(ctx, resultVars)}
     """.stripMargin
  }

  protected override def doExecute(): RDD[InternalRow] = {
    val numOutputRows = longMetric("numOutputRows")
    child.execute().mapPartitionsWithIndexInternal { (index, iter) =>
      val predicate = newPredicate(condition, child.output)
      predicate.initialize(0)
      iter.filter { row =>
        val r = predicate.eval(row)
        if (r) numOutputRows += 1
        r
      }
    }
  }

  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  override def outputPartitioning: Partitioning = child.outputPartitioning
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
    seed: Long,
    child: SparkPlan) extends UnaryExecNode with CodegenSupport {
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
        seed)
    } else {
      child.execute().randomSampleWithRange(lowerBound, upperBound, seed)
    }
  }

  override def inputRDDs(): Seq[RDD[InternalRow]] = {
    child.asInstanceOf[CodegenSupport].inputRDDs()
  }

  protected override def doProduce(ctx: CodegenContext): String = {
    child.asInstanceOf[CodegenSupport].produce(ctx, this)
  }

  override def doConsume(ctx: CodegenContext, input: Seq[ExprCode], row: ExprCode): String = {
    val numOutput = metricTerm(ctx, "numOutputRows")
    val sampler = ctx.freshName("sampler")

    if (withReplacement) {
      val samplerClass = classOf[PoissonSampler[UnsafeRow]].getName
      val initSampler = ctx.freshName("initSampler")
      ctx.copyResult = true
      ctx.addMutableState(s"$samplerClass<UnsafeRow>", sampler,
        s"$initSampler();")

      ctx.addNewFunction(initSampler,
        s"""
          | private void $initSampler() {
          |   $sampler = new $samplerClass<UnsafeRow>($upperBound - $lowerBound, false);
          |   java.util.Random random = new java.util.Random(${seed}L);
          |   long randomSeed = random.nextLong();
          |   int loopCount = 0;
          |   while (loopCount < partitionIndex) {
          |     randomSeed = random.nextLong();
          |     loopCount += 1;
          |   }
          |   $sampler.setSeed(randomSeed);
          | }
         """.stripMargin.trim)

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
      ctx.addMutableState(s"$samplerClass<UnsafeRow>", sampler,
        s"""
          | $sampler = new $samplerClass<UnsafeRow>($lowerBound, $upperBound, false);
          | $sampler.setSeed(${seed}L + partitionIndex);
         """.stripMargin.trim)

      s"""
         | if ($sampler.sample() == 0) continue;
         | $numOutput.add(1);
         | ${consume(ctx, input)}
       """.stripMargin.trim
    }
  }
}


/**
 * Physical plan for range (generating a range of 64 bit numbers).
 */
case class RangeExec(range: org.apache.spark.sql.catalyst.plans.logical.Range)
  extends LeafExecNode with CodegenSupport {

  val start: Long = range.start
  val end: Long = range.end
  val step: Long = range.step
  val numSlices: Int = range.numSlices.getOrElse(sparkContext.defaultParallelism)
  val numElements: BigInt = range.numElements

  override val output: Seq[Attribute] = range.output

  override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"))

  override lazy val canonicalized: SparkPlan = {
    RangeExec(range.canonicalized.asInstanceOf[org.apache.spark.sql.catalyst.plans.logical.Range])
  }

  override def inputRDDs(): Seq[RDD[InternalRow]] = {
    val rdd = if (start == end || (start < end ^ 0 < step)) {
      new EmptyRDD[InternalRow](sqlContext.sparkContext)
    } else {
      sqlContext.sparkContext.parallelize(0 until numSlices, numSlices).map(i => InternalRow(i))
    }
    rdd :: Nil
  }

  protected override def doProduce(ctx: CodegenContext): String = {
    val numOutput = metricTerm(ctx, "numOutputRows")

    val initTerm = ctx.freshName("initRange")
    ctx.addMutableState("boolean", initTerm, s"$initTerm = false;")
    val number = ctx.freshName("number")
    ctx.addMutableState("long", number, s"$number = 0L;")

    val value = ctx.freshName("value")
    val ev = ExprCode("", "false", value)
    val BigInt = classOf[java.math.BigInteger].getName

    val taskContext = ctx.freshName("taskContext")
    ctx.addMutableState("TaskContext", taskContext, s"$taskContext = TaskContext.get();")
    val inputMetrics = ctx.freshName("inputMetrics")
    ctx.addMutableState("InputMetrics", inputMetrics,
        s"$inputMetrics = $taskContext.taskMetrics().inputMetrics();")

    // In order to periodically update the metrics without inflicting performance penalty, this
    // operator produces elements in batches. After a batch is complete, the metrics are updated
    // and a new batch is started.
    // In the implementation below, the code in the inner loop is producing all the values
    // within a batch, while the code in the outer loop is setting batch parameters and updating
    // the metrics.

    // Once number == batchEnd, it's time to progress to the next batch.
    val batchEnd = ctx.freshName("batchEnd")
    ctx.addMutableState("long", batchEnd, s"$batchEnd = 0;")

    // How many values should still be generated by this range operator.
    val numElementsTodo = ctx.freshName("numElementsTodo")
    ctx.addMutableState("long", numElementsTodo, s"$numElementsTodo = 0L;")

    // How many values should be generated in the next batch.
    val nextBatchTodo = ctx.freshName("nextBatchTodo")

    // The default size of a batch, which must be positive integer
    val batchSize = 1000

    ctx.addNewFunction("initRange",
      s"""
        | private void initRange(int idx) {
        |   $BigInt index = $BigInt.valueOf(idx);
        |   $BigInt numSlice = $BigInt.valueOf(${numSlices}L);
        |   $BigInt numElement = $BigInt.valueOf(${numElements.toLong}L);
        |   $BigInt step = $BigInt.valueOf(${step}L);
        |   $BigInt start = $BigInt.valueOf(${start}L);
        |   long partitionEnd;
        |
        |   $BigInt st = index.multiply(numElement).divide(numSlice).multiply(step).add(start);
        |   if (st.compareTo($BigInt.valueOf(Long.MAX_VALUE)) > 0) {
        |     $number = Long.MAX_VALUE;
        |   } else if (st.compareTo($BigInt.valueOf(Long.MIN_VALUE)) < 0) {
        |     $number = Long.MIN_VALUE;
        |   } else {
        |     $number = st.longValue();
        |   }
        |   $batchEnd = $number;
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
        |     $BigInt.valueOf($number));
        |   $numElementsTodo  = startToEnd.divide(step).longValue();
        |   if ($numElementsTodo < 0) {
        |     $numElementsTodo = 0;
        |   } else if (startToEnd.remainder(step).compareTo($BigInt.valueOf(0L)) != 0) {
        |     $numElementsTodo++;
        |   }
        | }
       """.stripMargin)

    val input = ctx.freshName("input")
    // Right now, Range is only used when there is one upstream.
    ctx.addMutableState("scala.collection.Iterator", input, s"$input = inputs[0];")

    val localIdx = ctx.freshName("localIdx")
    val localEnd = ctx.freshName("localEnd")
    val range = ctx.freshName("range")
    val shouldStop = if (isShouldStopRequired) {
      s"if (shouldStop()) { $number = $value + ${step}L; return; }"
    } else {
      "// shouldStop check is eliminated"
    }
    s"""
      | // initialize Range
      | if (!$initTerm) {
      |   $initTerm = true;
      |   initRange(partitionIndex);
      | }
      |
      | while (true) {
      |   long $range = $batchEnd - $number;
      |   if ($range != 0L) {
      |     int $localEnd = (int)($range / ${step}L);
      |     for (int $localIdx = 0; $localIdx < $localEnd; $localIdx++) {
      |       long $value = ((long)$localIdx * ${step}L) + $number;
      |       ${consume(ctx, Seq(ev))}
      |       $shouldStop
      |     }
      |     $number = $batchEnd;
      |   }
      |
      |   $taskContext.killTaskIfInterrupted();
      |
      |   long $nextBatchTodo;
      |   if ($numElementsTodo > ${batchSize}L) {
      |     $nextBatchTodo = ${batchSize}L;
      |     $numElementsTodo -= ${batchSize}L;
      |   } else {
      |     $nextBatchTodo = $numElementsTodo;
      |     $numElementsTodo = 0;
      |     if ($nextBatchTodo == 0) break;
      |   }
      |   $numOutput.add($nextBatchTodo);
      |   $inputMetrics.incRecordsRead($nextBatchTodo);
      |
      |   $batchEnd += $nextBatchTodo * ${step}L;
      | }
     """.stripMargin
  }

  protected override def doExecute(): RDD[InternalRow] = {
    val numOutputRows = longMetric("numOutputRows")
    sqlContext
      .sparkContext
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

  override def simpleString: String = s"Range ($start, $end, step=$step, splits=$numSlices)"
}

/**
 * Physical plan for unioning two plans, without a distinct. This is UNION ALL in SQL.
 */
case class UnionExec(children: Seq[SparkPlan]) extends SparkPlan {
  override def output: Seq[Attribute] =
    children.map(_.output).transpose.map(attrs =>
      attrs.head.withNullability(attrs.exists(_.nullable)))

  protected override def doExecute(): RDD[InternalRow] =
    sparkContext.union(children.map(_.execute()))
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
    child.execute().coalesce(numPartitions, shuffle = false)
  }
}

/**
 * A plan node that does nothing but lie about the output of its child.  Used to spice a
 * (hopefully structurally equivalent) tree from a different optimization sequence into an already
 * resolved tree.
 */
case class OutputFakerExec(output: Seq[Attribute], child: SparkPlan) extends SparkPlan {
  def children: Seq[SparkPlan] = child :: Nil

  protected override def doExecute(): RDD[InternalRow] = child.execute()
}

/**
 * Physical plan for a subquery.
 */
case class SubqueryExec(name: String, child: SparkPlan) extends UnaryExecNode {

  override lazy val metrics = Map(
    "dataSize" -> SQLMetrics.createMetric(sparkContext, "data size (bytes)"),
    "collectTime" -> SQLMetrics.createMetric(sparkContext, "time to collect (ms)"))

  override def output: Seq[Attribute] = child.output

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  @transient
  private lazy val relationFuture: Future[Array[InternalRow]] = {
    // relationFuture is used in "doExecute". Therefore we can get the execution id correctly here.
    val executionId = sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)
    Future {
      // This will run in another thread. Set the execution id so that we can connect these jobs
      // with the correct execution.
      SQLExecution.withExecutionId(sparkContext, executionId) {
        val beforeCollect = System.nanoTime()
        // Note that we use .executeCollect() because we don't want to convert data to Scala types
        val rows: Array[InternalRow] = child.executeCollect()
        val beforeBuild = System.nanoTime()
        longMetric("collectTime") += (beforeBuild - beforeCollect) / 1000000
        val dataSize = rows.map(_.asInstanceOf[UnsafeRow].getSizeInBytes.toLong).sum
        longMetric("dataSize") += dataSize

        SQLMetrics.postDriverMetricUpdates(sparkContext, executionId, metrics.values.toSeq)
        rows
      }
    }(SubqueryExec.executionContext)
  }

  protected override def doPrepare(): Unit = {
    relationFuture
  }

  protected override def doExecute(): RDD[InternalRow] = {
    child.execute()
  }

  override def executeCollect(): Array[InternalRow] = {
    ThreadUtils.awaitResult(relationFuture, Duration.Inf)
  }
}

object SubqueryExec {
  private[execution] val executionContext = ExecutionContext.fromExecutorService(
    ThreadUtils.newDaemonCachedThreadPool("subquery", 16))
}
