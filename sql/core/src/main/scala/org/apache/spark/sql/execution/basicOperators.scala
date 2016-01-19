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

import org.apache.spark.{HashPartitioner, SparkEnv}
import org.apache.spark.rdd.{PartitionwiseSampledRDD, RDD, ShuffledRDD}
import org.apache.spark.shuffle.sort.SortShuffleManager
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode, ExpressionCanonicalizer}
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.types.LongType
import org.apache.spark.util.MutablePair
import org.apache.spark.util.random.PoissonSampler

case class Project(projectList: Seq[NamedExpression], child: SparkPlan)
  extends UnaryNode with CodegenSupport {

  override private[sql] lazy val metrics = Map(
    "numRows" -> SQLMetrics.createLongMetric(sparkContext, "number of rows"))

  override def output: Seq[Attribute] = projectList.map(_.toAttribute)

  protected override def doProduce(ctx: CodegenContext): (RDD[InternalRow], String) = {
    child.asInstanceOf[CodegenSupport].produce(ctx, this)
  }

  override def doConsume(ctx: CodegenContext, child: SparkPlan, input: Seq[ExprCode]): String = {
    val exprs = projectList.map(x =>
      ExpressionCanonicalizer.execute(BindReferences.bindReference(x, child.output)))
    ctx.currentVars = input
    val output = exprs.map(_.gen(ctx))
    s"""
       | ${output.map(_.code).mkString("\n")}
       |
       | ${consume(ctx, output)}
     """.stripMargin
  }

  protected override def doExecute(): RDD[InternalRow] = {
    val numRows = longMetric("numRows")
    child.execute().mapPartitionsInternal { iter =>
      val project = UnsafeProjection.create(projectList, child.output,
        subexpressionEliminationEnabled)
      iter.map { row =>
        numRows += 1
        project(row)
      }
    }
  }

  override def outputOrdering: Seq[SortOrder] = child.outputOrdering
}


case class Filter(condition: Expression, child: SparkPlan) extends UnaryNode with CodegenSupport {
  override def output: Seq[Attribute] = child.output

  private[sql] override lazy val metrics = Map(
    "numInputRows" -> SQLMetrics.createLongMetric(sparkContext, "number of input rows"),
    "numOutputRows" -> SQLMetrics.createLongMetric(sparkContext, "number of output rows"))

  protected override def doProduce(ctx: CodegenContext): (RDD[InternalRow], String) = {
    child.asInstanceOf[CodegenSupport].produce(ctx, this)
  }

  override def doConsume(ctx: CodegenContext, child: SparkPlan, input: Seq[ExprCode]): String = {
    val expr = ExpressionCanonicalizer.execute(
      BindReferences.bindReference(condition, child.output))
    ctx.currentVars = input
    val eval = expr.gen(ctx)
    s"""
       | ${eval.code}
       | if (!${eval.isNull} && ${eval.value}) {
       |   ${consume(ctx, ctx.currentVars)}
       | }
     """.stripMargin
  }

  protected override def doExecute(): RDD[InternalRow] = {
    val numInputRows = longMetric("numInputRows")
    val numOutputRows = longMetric("numOutputRows")
    child.execute().mapPartitionsInternal { iter =>
      val predicate = newPredicate(condition, child.output)
      iter.filter { row =>
        numInputRows += 1
        val r = predicate(row)
        if (r) numOutputRows += 1
        r
      }
    }
  }

  override def outputOrdering: Seq[SortOrder] = child.outputOrdering
}

/**
 * Sample the dataset.
 *
 * @param lowerBound Lower-bound of the sampling probability (usually 0.0)
 * @param upperBound Upper-bound of the sampling probability. The expected fraction sampled
 *                   will be ub - lb.
 * @param withReplacement Whether to sample with replacement.
 * @param seed the random seed
 * @param child the SparkPlan
 */
case class Sample(
    lowerBound: Double,
    upperBound: Double,
    withReplacement: Boolean,
    seed: Long,
    child: SparkPlan)
  extends UnaryNode
{
  override def output: Seq[Attribute] = child.output

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
}

case class Range(
    start: Long,
    step: Long,
    numSlices: Int,
    numElements: BigInt,
    output: Seq[Attribute])
  extends LeafNode with CodegenSupport {

  protected override def doProduce(ctx: CodegenContext): (RDD[InternalRow], String) = {
    val initTerm = ctx.freshName("range_initRange")
    ctx.addMutableState("boolean", initTerm, s"$initTerm = false;")
    val partitionEnd = ctx.freshName("range_partitionEnd")
    ctx.addMutableState("long", partitionEnd, s"$partitionEnd = 0L;")
    val number = ctx.freshName("range_number")
    ctx.addMutableState("long", number, s"$number = 0L;")
    val overflow = ctx.freshName("range_overflow")
    ctx.addMutableState("boolean", overflow, s"$overflow = false;")

    val value = ctx.freshName("range_value")
    val ev = ExprCode("", "false", value)
    val BigInt = classOf[java.math.BigInteger].getName
    val checkEnd = if (step > 0) {
      s"$number < $partitionEnd"
    } else {
      s"$number > $partitionEnd"
    }

    val rdd = sqlContext.sparkContext.parallelize(0 until numSlices, numSlices)
      .map(i => InternalRow(i))

    val code = s"""
      | // initialize Range
      | if (!$initTerm) {
      |   $initTerm = true;
      |   if (input.hasNext()) {
      |     $BigInt index = $BigInt.valueOf(((InternalRow) input.next()).getInt(0));
      |     $BigInt numSlice = $BigInt.valueOf(${numSlices}L);
      |     $BigInt numElement = $BigInt.valueOf(${numElements.toLong}L);
      |     $BigInt step = $BigInt.valueOf(${step}L);
      |     $BigInt start = $BigInt.valueOf(${start}L);
      |
      |     $BigInt st = index.multiply(numElement).divide(numSlice).multiply(step).add(start);
      |     if (st.compareTo($BigInt.valueOf(Long.MAX_VALUE)) > 0) {
      |       $number = Long.MAX_VALUE;
      |     } else if (st.compareTo($BigInt.valueOf(Long.MIN_VALUE)) < 0) {
      |       $number = Long.MIN_VALUE;
      |     } else {
      |       $number = st.longValue();
      |     }
      |
      |     $BigInt end = index.add($BigInt.ONE).multiply(numElement).divide(numSlice)
      |       .multiply(step).add(start);
      |     if (end.compareTo($BigInt.valueOf(Long.MAX_VALUE)) > 0) {
      |       $partitionEnd = Long.MAX_VALUE;
      |     } else if (end.compareTo($BigInt.valueOf(Long.MIN_VALUE)) < 0) {
      |       $partitionEnd = Long.MIN_VALUE;
      |     } else {
      |       $partitionEnd = end.longValue();
      |     }
      |   } else {
      |     return;
      |   }
      | }
      |
      | while (!$overflow && $checkEnd) {
      |  long $value = $number;
      |  $number += ${step}L;
      |  if ($number < $value ^ ${step}L < 0) {
      |    $overflow = true;
      |  }
      |  ${consume(ctx, Seq(ev))}
      | }
     """.stripMargin

    (rdd, code)
  }

  def doConsume(ctx: CodegenContext, child: SparkPlan, input: Seq[ExprCode]): String = {
    throw new UnsupportedOperationException
  }

  protected override def doExecute(): RDD[InternalRow] = {
    sqlContext
      .sparkContext
      .parallelize(0 until numSlices, numSlices)
      .mapPartitionsWithIndex((i, _) => {
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

        new Iterator[InternalRow] {
          private[this] var number: Long = safePartitionStart
          private[this] var overflow: Boolean = false

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

            unsafeRow.setLong(0, ret)
            unsafeRow
          }
        }
      })
  }
}

/**
 * Union two plans, without a distinct. This is UNION ALL in SQL.
 */
case class Union(children: Seq[SparkPlan]) extends SparkPlan {
  override def output: Seq[Attribute] = {
    children.tail.foldLeft(children.head.output) { case (currentOutput, child) =>
      currentOutput.zip(child.output).map { case (a1, a2) =>
        a1.withNullability(a1.nullable || a2.nullable)
      }
    }
  }
  protected override def doExecute(): RDD[InternalRow] =
    sparkContext.union(children.map(_.execute()))
}

/**
 * Take the first limit elements. Note that the implementation is different depending on whether
 * this is a terminal operator or not. If it is terminal and is invoked using executeCollect,
 * this operator uses something similar to Spark's take method on the Spark driver. If it is not
 * terminal or is invoked using execute, we first take the limit on each partition, and then
 * repartition all the data to a single partition to compute the global limit.
 */
case class Limit(limit: Int, child: SparkPlan)
  extends UnaryNode {
  // TODO: Implement a partition local limit, and use a strategy to generate the proper limit plan:
  // partition local limit -> exchange into one partition -> partition local limit again

  /** We must copy rows when sort based shuffle is on */
  private def sortBasedShuffleOn = SparkEnv.get.shuffleManager.isInstanceOf[SortShuffleManager]

  override def output: Seq[Attribute] = child.output
  override def outputPartitioning: Partitioning = SinglePartition

  override def executeCollect(): Array[InternalRow] = child.executeTake(limit)

  protected override def doExecute(): RDD[InternalRow] = {
    val rdd: RDD[_ <: Product2[Boolean, InternalRow]] = if (sortBasedShuffleOn) {
      child.execute().mapPartitionsInternal { iter =>
        iter.take(limit).map(row => (false, row.copy()))
      }
    } else {
      child.execute().mapPartitionsInternal { iter =>
        val mutablePair = new MutablePair[Boolean, InternalRow]()
        iter.take(limit).map(row => mutablePair.update(false, row))
      }
    }
    val part = new HashPartitioner(1)
    val shuffled = new ShuffledRDD[Boolean, InternalRow, InternalRow](rdd, part)
    shuffled.setSerializer(new SparkSqlSerializer(child.sqlContext.sparkContext.getConf))
    shuffled.mapPartitionsInternal(_.take(limit).map(_._2))
  }
}

/**
 * Take the first limit elements as defined by the sortOrder, and do projection if needed.
 * This is logically equivalent to having a [[Limit]] operator after a [[Sort]] operator,
 * or having a [[Project]] operator between them.
 * This could have been named TopK, but Spark's top operator does the opposite in ordering
 * so we name it TakeOrdered to avoid confusion.
 */
case class TakeOrderedAndProject(
    limit: Int,
    sortOrder: Seq[SortOrder],
    projectList: Option[Seq[NamedExpression]],
    child: SparkPlan) extends UnaryNode {

  override def output: Seq[Attribute] = {
    val projectOutput = projectList.map(_.map(_.toAttribute))
    projectOutput.getOrElse(child.output)
  }

  override def outputPartitioning: Partitioning = SinglePartition

  // We need to use an interpreted ordering here because generated orderings cannot be serialized
  // and this ordering needs to be created on the driver in order to be passed into Spark core code.
  private val ord: InterpretedOrdering = new InterpretedOrdering(sortOrder, child.output)

  private def collectData(): Array[InternalRow] = {
    val data = child.execute().map(_.copy()).takeOrdered(limit)(ord)
    if (projectList.isDefined) {
      val proj = UnsafeProjection.create(projectList.get, child.output)
      data.map(r => proj(r).copy())
    } else {
      data
    }
  }

  override def executeCollect(): Array[InternalRow] = {
    collectData()
  }

  // TODO: Terminal split should be implemented differently from non-terminal split.
  // TODO: Pick num splits based on |limit|.
  protected override def doExecute(): RDD[InternalRow] = sparkContext.makeRDD(collectData(), 1)

  override def outputOrdering: Seq[SortOrder] = sortOrder

  override def simpleString: String = {
    val orderByString = sortOrder.mkString("[", ",", "]")
    val outputString = output.mkString("[", ",", "]")

    s"TakeOrderedAndProject(limit=$limit, orderBy=$orderByString, output=$outputString)"
  }
}

/**
 * Return a new RDD that has exactly `numPartitions` partitions.
 * Similar to coalesce defined on an [[RDD]], this operation results in a narrow dependency, e.g.
 * if you go from 1000 partitions to 100 partitions, there will not be a shuffle, instead each of
 * the 100 new partitions will claim 10 of the current partitions.
 */
case class Coalesce(numPartitions: Int, child: SparkPlan) extends UnaryNode {
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
 * Returns a table with the elements from left that are not in right using
 * the built-in spark subtract function.
 */
case class Except(left: SparkPlan, right: SparkPlan) extends BinaryNode {
  override def output: Seq[Attribute] = left.output

  protected override def doExecute(): RDD[InternalRow] = {
    left.execute().map(_.copy()).subtract(right.execute().map(_.copy()))
  }
}

/**
 * Returns the rows in left that also appear in right using the built in spark
 * intersection function.
 */
case class Intersect(left: SparkPlan, right: SparkPlan) extends BinaryNode {
  override def output: Seq[Attribute] = children.head.output

  protected override def doExecute(): RDD[InternalRow] = {
    left.execute().map(_.copy()).intersection(right.execute().map(_.copy()))
  }
}

/**
 * A plan node that does nothing but lie about the output of its child.  Used to spice a
 * (hopefully structurally equivalent) tree from a different optimization sequence into an already
 * resolved tree.
 */
case class OutputFaker(output: Seq[Attribute], child: SparkPlan) extends SparkPlan {
  def children: Seq[SparkPlan] = child :: Nil

  protected override def doExecute(): RDD[InternalRow] = child.execute()
}
