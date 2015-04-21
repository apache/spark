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

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, trees}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.physical._

import scala.collection.mutable.ArrayBuffer

object SparkPlan {
  protected[sql] val currentContext = new ThreadLocal[SQLContext]()
}

/**
 * :: DeveloperApi ::
 */
@DeveloperApi
abstract class SparkPlan extends QueryPlan[SparkPlan] with Logging with Serializable {
  self: Product =>

  /**
   * A handle to the SQL Context that was used to create this plan.   Since many operators need
   * access to the sqlContext for RDD operations or configuration this field is automatically
   * populated by the query planning infrastructure.
   */
  @transient
  protected[spark] final val sqlContext = SparkPlan.currentContext.get()

  protected def sparkContext = sqlContext.sparkContext

  // sqlContext will be null when we are being deserialized on the slaves.  In this instance
  // the value of codegenEnabled will be set by the desserializer after the constructor has run.
  val codegenEnabled: Boolean = if (sqlContext != null) {
    sqlContext.conf.codegenEnabled
  } else {
    false
  }

  /** Overridden make copy also propogates sqlContext to copied plan. */
  override def makeCopy(newArgs: Array[AnyRef]): this.type = {
    SparkPlan.currentContext.set(sqlContext)
    super.makeCopy(newArgs)
  }

  // TODO: Move to `DistributedPlan`
  /** Specifies how data is partitioned across different nodes in the cluster. */
  def outputPartitioning: Partitioning = UnknownPartitioning(0) // TODO: WRONG WIDTH!

  /** Specifies any partition requirements on the input data for this operator. */
  def requiredChildDistribution: Seq[Distribution] =
    Seq.fill(children.size)(UnspecifiedDistribution)

  /** Specifies how data is ordered in each partition. */
  def outputOrdering: Seq[SortOrder] = Nil

  /** Specifies sort order for each partition requirements on the input data for this operator. */
  def requiredChildOrdering: Seq[Seq[SortOrder]] = Seq.fill(children.size)(Nil)

  /**
   * Runs this query returning the result as an RDD.
   */
  def execute(): RDD[Row]

  /**
   * Runs this query returning the result as an array.
   */

  def executeCollect(): Array[Row] = {
    execute().mapPartitions { iter =>
      val converter = CatalystTypeConverters.createToScalaConverter(schema)
      iter.map(converter(_).asInstanceOf[Row])
    }.collect()
  }

  /**
   * Runs this query returning the first `n` rows as an array.
   *
   * This is modeled after RDD.take but never runs any job locally on the driver.
   */
  def executeTake(n: Int): Array[Row] = {
    if (n == 0) {
      return new Array[Row](0)
    }

    val childRDD = execute().map(_.copy())

    val buf = new ArrayBuffer[Row]
    val totalParts = childRDD.partitions.length
    var partsScanned = 0
    while (buf.size < n && partsScanned < totalParts) {
      // The number of partitions to try in this iteration. It is ok for this number to be
      // greater than totalParts because we actually cap it at totalParts in runJob.
      var numPartsToTry = 1
      if (partsScanned > 0) {
        // If we didn't find any rows after the first iteration, just try all partitions next.
        // Otherwise, interpolate the number of partitions we need to try, but overestimate it
        // by 50%.
        if (buf.size == 0) {
          numPartsToTry = totalParts - 1
        } else {
          numPartsToTry = (1.5 * n * partsScanned / buf.size).toInt
        }
      }
      numPartsToTry = math.max(0, numPartsToTry)  // guard against negative num of partitions

      val left = n - buf.size
      val p = partsScanned until math.min(partsScanned + numPartsToTry, totalParts)
      val sc = sqlContext.sparkContext
      val res =
        sc.runJob(childRDD, (it: Iterator[Row]) => it.take(left).toArray, p, allowLocal = false)

      res.foreach(buf ++= _.take(n - buf.size))
      partsScanned += numPartsToTry
    }

    val converter = CatalystTypeConverters.createToScalaConverter(schema)
    buf.toArray.map(converter(_).asInstanceOf[Row])
  }

  protected def newProjection(
      expressions: Seq[Expression], inputSchema: Seq[Attribute]): Projection = {
    log.debug(
      s"Creating Projection: $expressions, inputSchema: $inputSchema, codegen:$codegenEnabled")
    if (codegenEnabled) {
      GenerateProjection(expressions, inputSchema)
    } else {
      new InterpretedProjection(expressions, inputSchema)
    }
  }

  protected def newMutableProjection(
      expressions: Seq[Expression],
      inputSchema: Seq[Attribute]): () => MutableProjection = {
    log.debug(
      s"Creating MutableProj: $expressions, inputSchema: $inputSchema, codegen:$codegenEnabled")
    if(codegenEnabled) {
      GenerateMutableProjection(expressions, inputSchema)
    } else {
      () => new InterpretedMutableProjection(expressions, inputSchema)
    }
  }


  protected def newPredicate(
      expression: Expression, inputSchema: Seq[Attribute]): (Row) => Boolean = {
    if (codegenEnabled) {
      GeneratePredicate(expression, inputSchema)
    } else {
      InterpretedPredicate(expression, inputSchema)
    }
  }

  protected def newOrdering(order: Seq[SortOrder], inputSchema: Seq[Attribute]): Ordering[Row] = {
    if (codegenEnabled) {
      GenerateOrdering(order, inputSchema)
    } else {
      new RowOrdering(order, inputSchema)
    }
  }
}

private[sql] trait LeafNode extends SparkPlan with trees.LeafNode[SparkPlan] {
  self: Product =>
}

private[sql] trait UnaryNode extends SparkPlan with trees.UnaryNode[SparkPlan] {
  self: Product =>
  override def outputPartitioning: Partitioning = child.outputPartitioning
}

private[sql] trait BinaryNode extends SparkPlan with trees.BinaryNode[SparkPlan] {
  self: Product =>
}
