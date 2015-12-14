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

import org.apache.spark.rdd.{PartitionwiseSampledRDD, RDD, ShuffledRDD}
import org.apache.spark.shuffle.sort.SortShuffleManager
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeRowJoiner
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.util.MutablePair
import org.apache.spark.util.random.PoissonSampler
import org.apache.spark.{HashPartitioner, SparkEnv}


case class Project(projectList: Seq[NamedExpression], child: SparkPlan) extends UnaryNode {

  override private[sql] lazy val metrics = Map(
    "numRows" -> SQLMetrics.createLongMetric(sparkContext, "number of rows"))

  override def outputsUnsafeRows: Boolean = true
  override def canProcessUnsafeRows: Boolean = true
  override def canProcessSafeRows: Boolean = true

  override def output: Seq[Attribute] = projectList.map(_.toAttribute)

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


case class Filter(condition: Expression, child: SparkPlan) extends UnaryNode {
  override def output: Seq[Attribute] = child.output

  private[sql] override lazy val metrics = Map(
    "numInputRows" -> SQLMetrics.createLongMetric(sparkContext, "number of input rows"),
    "numOutputRows" -> SQLMetrics.createLongMetric(sparkContext, "number of output rows"))

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

  override def outputsUnsafeRows: Boolean = child.outputsUnsafeRows

  override def canProcessUnsafeRows: Boolean = true

  override def canProcessSafeRows: Boolean = true
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

  override def outputsUnsafeRows: Boolean = child.outputsUnsafeRows
  override def canProcessUnsafeRows: Boolean = true
  override def canProcessSafeRows: Boolean = true

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
  override def outputsUnsafeRows: Boolean = children.forall(_.outputsUnsafeRows)
  override def canProcessUnsafeRows: Boolean = true
  override def canProcessSafeRows: Boolean = true
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

  // TODO: remove @transient after figure out how to clean closure at InsertIntoHiveTable.
  @transient private val projection = projectList.map(new InterpretedProjection(_, child.output))

  private def collectData(): Array[InternalRow] = {
    val data = child.execute().map(_.copy()).takeOrdered(limit)(ord)
    projection.map(data.map(_)).getOrElse(data)
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

  override def canProcessUnsafeRows: Boolean = true
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

/**
 * Applies the given function to each input row and encodes the result.
 */
case class MapPartitions[T, U](
    func: Iterator[T] => Iterator[U],
    tEncoder: ExpressionEncoder[T],
    uEncoder: ExpressionEncoder[U],
    output: Seq[Attribute],
    child: SparkPlan) extends UnaryNode {

  override protected def doExecute(): RDD[InternalRow] = {
    child.execute().mapPartitionsInternal { iter =>
      val tBoundEncoder = tEncoder.bind(child.output)
      func(iter.map(tBoundEncoder.fromRow)).map(uEncoder.toRow)
    }
  }
}

/**
 * Applies the given function to each input row, appending the encoded result at the end of the row.
 */
case class AppendColumns[T, U](
    func: T => U,
    tEncoder: ExpressionEncoder[T],
    uEncoder: ExpressionEncoder[U],
    newColumns: Seq[Attribute],
    child: SparkPlan) extends UnaryNode {

  // We are using an unsafe combiner.
  override def canProcessSafeRows: Boolean = false
  override def canProcessUnsafeRows: Boolean = true

  override def output: Seq[Attribute] = child.output ++ newColumns

  override protected def doExecute(): RDD[InternalRow] = {
    child.execute().mapPartitionsInternal { iter =>
      val tBoundEncoder = tEncoder.bind(child.output)
      val combiner = GenerateUnsafeRowJoiner.create(tEncoder.schema, uEncoder.schema)
      iter.map { row =>
        val newColumns = uEncoder.toRow(func(tBoundEncoder.fromRow(row)))
        combiner.join(row.asInstanceOf[UnsafeRow], newColumns.asInstanceOf[UnsafeRow]): InternalRow
      }
    }
  }
}

/**
 * Groups the input rows together and calls the function with each group and an iterator containing
 * all elements in the group.  The result of this function is encoded and flattened before
 * being output.
 */
case class MapGroups[K, T, U](
    func: (K, Iterator[T]) => TraversableOnce[U],
    kEncoder: ExpressionEncoder[K],
    tEncoder: ExpressionEncoder[T],
    uEncoder: ExpressionEncoder[U],
    groupingAttributes: Seq[Attribute],
    output: Seq[Attribute],
    child: SparkPlan) extends UnaryNode {

  override def requiredChildDistribution: Seq[Distribution] =
    ClusteredDistribution(groupingAttributes) :: Nil

  override def requiredChildOrdering: Seq[Seq[SortOrder]] =
    Seq(groupingAttributes.map(SortOrder(_, Ascending)))

  override protected def doExecute(): RDD[InternalRow] = {
    child.execute().mapPartitionsInternal { iter =>
      val grouped = GroupedIterator(iter, groupingAttributes, child.output)
      val groupKeyEncoder = kEncoder.bind(groupingAttributes)
      val groupDataEncoder = tEncoder.bind(child.output)

      grouped.flatMap { case (key, rowIter) =>
        val result = func(
          groupKeyEncoder.fromRow(key),
          rowIter.map(groupDataEncoder.fromRow))
        result.map(uEncoder.toRow)
      }
    }
  }
}

/**
 * Co-groups the data from left and right children, and calls the function with each group and 2
 * iterators containing all elements in the group from left and right side.
 * The result of this function is encoded and flattened before being output.
 */
case class CoGroup[Key, Left, Right, Result](
    func: (Key, Iterator[Left], Iterator[Right]) => TraversableOnce[Result],
    keyEnc: ExpressionEncoder[Key],
    leftEnc: ExpressionEncoder[Left],
    rightEnc: ExpressionEncoder[Right],
    resultEnc: ExpressionEncoder[Result],
    output: Seq[Attribute],
    leftGroup: Seq[Attribute],
    rightGroup: Seq[Attribute],
    left: SparkPlan,
    right: SparkPlan) extends BinaryNode {

  override def requiredChildDistribution: Seq[Distribution] =
    ClusteredDistribution(leftGroup) :: ClusteredDistribution(rightGroup) :: Nil

  override def requiredChildOrdering: Seq[Seq[SortOrder]] =
    leftGroup.map(SortOrder(_, Ascending)) :: rightGroup.map(SortOrder(_, Ascending)) :: Nil

  override protected def doExecute(): RDD[InternalRow] = {
    left.execute().zipPartitions(right.execute()) { (leftData, rightData) =>
      val leftGrouped = GroupedIterator(leftData, leftGroup, left.output)
      val rightGrouped = GroupedIterator(rightData, rightGroup, right.output)
      val boundKeyEnc = keyEnc.bind(leftGroup)
      val boundLeftEnc = leftEnc.bind(left.output)
      val boundRightEnc = rightEnc.bind(right.output)

      new CoGroupedIterator(leftGrouped, rightGrouped, leftGroup).flatMap {
        case (key, leftResult, rightResult) =>
          val result = func(
            boundKeyEnc.fromRow(key),
            leftResult.map(boundLeftEnc.fromRow),
            rightResult.map(boundRightEnc.fromRow))
          result.map(resultEnc.toRow)
      }
    }
  }
}
