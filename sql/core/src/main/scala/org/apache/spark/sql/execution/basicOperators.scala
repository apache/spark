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

import org.apache.spark.{SparkEnv, HashPartitioner, SparkConf}
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.{RDD, ShuffledRDD}
import org.apache.spark.shuffle.sort.SortShuffleManager
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.catalyst.errors._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.physical.{ClusteredDistribution, OrderedDistribution, SinglePartition, UnspecifiedDistribution}
import org.apache.spark.util.MutablePair
import org.apache.spark.util.collection.ExternalSorter

/**
 * :: DeveloperApi ::
 */
@DeveloperApi
case class Project(projectList: Seq[NamedExpression], child: SparkPlan) extends UnaryNode {
  override def output = projectList.map(_.toAttribute)

  @transient lazy val buildProjection = newMutableProjection(projectList, child.output)

  override def execute() = child.execute().mapPartitions { iter =>
    val resuableProjection = buildProjection()
    iter.map(resuableProjection)
  }
}

/**
 * :: DeveloperApi ::
 */
@DeveloperApi
case class Filter(condition: Expression, child: SparkPlan) extends UnaryNode {
  override def output = child.output

  @transient lazy val conditionEvaluator = newPredicate(condition, child.output)

  override def execute() = child.execute().mapPartitions { iter =>
    iter.filter(conditionEvaluator)
  }
}

/**
 * :: DeveloperApi ::
 */
@DeveloperApi
case class Sample(fraction: Double, withReplacement: Boolean, seed: Long, child: SparkPlan)
  extends UnaryNode
{
  override def output = child.output

  // TODO: How to pick seed?
  override def execute() = child.execute().map(_.copy()).sample(withReplacement, fraction, seed)
}

/**
 * :: DeveloperApi ::
 */
@DeveloperApi
case class Union(children: Seq[SparkPlan]) extends SparkPlan {
  // TODO: attributes output by union should be distinct for nullability purposes
  override def output = children.head.output
  override def execute() = sparkContext.union(children.map(_.execute()))
}

/**
 * :: DeveloperApi ::
 * Take the first limit elements. Note that the implementation is different depending on whether
 * this is a terminal operator or not. If it is terminal and is invoked using executeCollect,
 * this operator uses something similar to Spark's take method on the Spark driver. If it is not
 * terminal or is invoked using execute, we first take the limit on each partition, and then
 * repartition all the data to a single partition to compute the global limit.
 */
@DeveloperApi
case class Limit(limit: Int, child: SparkPlan)
  extends UnaryNode {
  // TODO: Implement a partition local limit, and use a strategy to generate the proper limit plan:
  // partition local limit -> exchange into one partition -> partition local limit again

  /** We must copy rows when sort based shuffle is on */
  private def sortBasedShuffleOn = SparkEnv.get.shuffleManager.isInstanceOf[SortShuffleManager]

  override def output = child.output
  override def outputPartitioning = SinglePartition

  override def executeCollect(): Array[Row] = child.executeTake(limit)

  override def execute() = {
    val rdd: RDD[_ <: Product2[Boolean, Row]] = if (sortBasedShuffleOn) {
      child.execute().mapPartitions { iter =>
        iter.take(limit).map(row => (false, row.copy()))
      }
    } else {
      child.execute().mapPartitions { iter =>
        val mutablePair = new MutablePair[Boolean, Row]()
        iter.take(limit).map(row => mutablePair.update(false, row))
      }
    }
    val part = new HashPartitioner(1)
    val shuffled = new ShuffledRDD[Boolean, Row, Row](rdd, part)
    shuffled.setSerializer(new SparkSqlSerializer(new SparkConf(false)))
    shuffled.mapPartitions(_.take(limit).map(_._2))
  }
}

/**
 * :: DeveloperApi ::
 * Take the first limit elements as defined by the sortOrder. This is logically equivalent to
 * having a [[Limit]] operator after a [[Sort]] operator. This could have been named TopK, but
 * Spark's top operator does the opposite in ordering so we name it TakeOrdered to avoid confusion.
 */
@DeveloperApi
case class TakeOrdered(limit: Int, sortOrder: Seq[SortOrder], child: SparkPlan) extends UnaryNode {

  override def output = child.output
  override def outputPartitioning = SinglePartition

  val ord = new RowOrdering(sortOrder, child.output)

  // TODO: Is this copying for no reason?
  override def executeCollect() = child.execute().map(_.copy()).takeOrdered(limit)(ord)
    .map(ScalaReflection.convertRowToScala(_, this.schema))

  // TODO: Terminal split should be implemented differently from non-terminal split.
  // TODO: Pick num splits based on |limit|.
  override def execute() = sparkContext.makeRDD(executeCollect(), 1)
}

/**
 * :: DeveloperApi ::
 * Performs a sort on-heap.
 * @param global when true performs a global sort of all partitions by shuffling the data first
 *               if necessary.
 */
@DeveloperApi
case class Sort(
    sortOrder: Seq[SortOrder],
    global: Boolean,
    child: SparkPlan)
  extends UnaryNode {
  override def requiredChildDistribution =
    if (global) OrderedDistribution(sortOrder) :: Nil else UnspecifiedDistribution :: Nil

  override def execute() = attachTree(this, "sort") {
    child.execute().mapPartitions( { iterator =>
      val ordering = newOrdering(sortOrder, child.output)
      iterator.map(_.copy()).toArray.sorted(ordering).iterator
    }, preservesPartitioning = true)
  }

  override def output = child.output
}

/**
 * :: DeveloperApi ::
 * Performs a sort, spilling to disk as needed.
 * @param global when true performs a global sort of all partitions by shuffling the data first
 *               if necessary.
 */
@DeveloperApi
case class ExternalSort(
    sortOrder: Seq[SortOrder],
    global: Boolean,
    child: SparkPlan)
  extends UnaryNode {
  override def requiredChildDistribution =
    if (global) OrderedDistribution(sortOrder) :: Nil else UnspecifiedDistribution :: Nil

  override def execute() = attachTree(this, "sort") {
    child.execute().mapPartitions( { iterator =>
      val ordering = newOrdering(sortOrder, child.output)
      val sorter = new ExternalSorter[Row, Null, Row](ordering = Some(ordering))
      sorter.insertAll(iterator.map(r => (r, null)))
      sorter.iterator.map(_._1)
    }, preservesPartitioning = true)
  }

  override def output = child.output
}

/**
 * :: DeveloperApi ::
 * Computes the set of distinct input rows using a HashSet.
 * @param partial when true the distinct operation is performed partially, per partition, without
 *                shuffling the data.
 * @param child the input query plan.
 */
@DeveloperApi
case class Distinct(partial: Boolean, child: SparkPlan) extends UnaryNode {
  override def output = child.output

  override def requiredChildDistribution =
    if (partial) UnspecifiedDistribution :: Nil else ClusteredDistribution(child.output) :: Nil

  override def execute() = {
    child.execute().mapPartitions { iter =>
      val hashSet = new scala.collection.mutable.HashSet[Row]()

      var currentRow: Row = null
      while (iter.hasNext) {
        currentRow = iter.next()
        if (!hashSet.contains(currentRow)) {
          hashSet.add(currentRow.copy())
        }
      }

      hashSet.iterator
    }
  }
}


/**
 * :: DeveloperApi ::
 * Returns a table with the elements from left that are not in right using
 * the built-in spark subtract function.
 */
@DeveloperApi
case class Except(left: SparkPlan, right: SparkPlan) extends BinaryNode {
  override def output = left.output

  override def execute() = {
    left.execute().map(_.copy()).subtract(right.execute().map(_.copy()))
  }
}

/**
 * :: DeveloperApi ::
 * Returns the rows in left that also appear in right using the built in spark
 * intersection function.
 */
@DeveloperApi
case class Intersect(left: SparkPlan, right: SparkPlan) extends BinaryNode {
  override def output = children.head.output

  override def execute() = {
    left.execute().map(_.copy()).intersection(right.execute().map(_.copy()))
  }
}

/**
 * :: DeveloperApi ::
 * A plan node that does nothing but lie about the output of its child.  Used to spice a
 * (hopefully structurally equivalent) tree from a different optimization sequence into an already
 * resolved tree.
 */
@DeveloperApi
case class OutputFaker(output: Seq[Attribute], child: SparkPlan) extends SparkPlan {
  def children = child :: Nil
  def execute() = child.execute()
}
