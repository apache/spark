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

package org.apache.spark.sql

import org.apache.spark.sql.catalyst.encoders.Encoder
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.QueryExecution

/**
 * A [[Dataset]] has been logically grouped by a user specified grouping key.  Users should not
 * construct a [[GroupedDataset]] directly, but should instead call `groupBy` on an existing
 * [[Dataset]].
 */
class GroupedDataset[K, T] private[sql](
    private val kEncoder: Encoder[K],
    private val tEncoder: Encoder[T],
    queryExecution: QueryExecution,
    private val dataAttributes: Seq[Attribute],
    private val groupingAttributes: Seq[Attribute]) extends Serializable {

  private implicit def kEnc = kEncoder
  private implicit def tEnc = tEncoder
  private def logicalPlan = queryExecution.analyzed
  private def sqlContext = queryExecution.sqlContext

  /**
   * Returns a [[Dataset]] that contains each unique key.
   */
  def keys: Dataset[K] = {
    new Dataset[K](
      sqlContext,
      Distinct(
        Project(groupingAttributes, logicalPlan)))
  }

  /**
   * Applies the given function to each group of data.  For each unique group, the function will
   * be passed the group key and an iterator that contains all of the elements in the group. The
   * function can return an iterator containing elements of an arbitrary type which will be returned
   * as a new [[Dataset]].
   *
   * Internally, the implementation will spill to disk if any given group is too large to fit into
   * memory.  However, users must take care to avoid materializing the whole iterator for a group
   * (for example, by calling `toList`) unless they are sure that this is possible given the memory
   * constraints of their cluster.
   */
  def mapGroups[U : Encoder](f: (K, Iterator[T]) => Iterator[U]): Dataset[U] = {
    new Dataset[U](
      sqlContext,
      MapGroups(f, groupingAttributes, logicalPlan))
  }

  // These methods are generated.
  // scalastyle:off

  /**
   * For each key k in `this` or other [[Dataset]]s, applies the given function on this key and
   * associated values, and return a resulting [[Dataset]] that contains the result of function.
   */
  def cogroup[W1, R : Encoder](other1: GroupedDataset[K, W1])(func: (K, Iterator[T], Iterator[W1]) => Iterator[R]): Dataset[R] = {
    implicit def wEnc1 = other1.tEnc
    val plan = CoGroup(func, groupingAttributes, other1.groupingAttributes, logicalPlan, other1.logicalPlan)
    new Dataset[R](sqlContext, plan)
  }

  /**
   * For each key k in `this` or other [[Dataset]]s, applies the given function on this key and
   * associated values, and return a resulting [[Dataset]] that contains the result of function.
   */
  def cogroup[W1, W2, R : Encoder](other1: GroupedDataset[K, W1], other2: GroupedDataset[K, W2])(func: (K, Iterator[T], Iterator[W1], Iterator[W2]) => Iterator[R]): Dataset[R] = {
    implicit def wEnc1 = other1.tEnc
    implicit def wEnc2 = other2.tEnc
    val plan = CoGroup(func, groupingAttributes, other1.groupingAttributes, other2.groupingAttributes, logicalPlan, other1.logicalPlan, other2.logicalPlan)
    new Dataset[R](sqlContext, plan)
  }

  /**
   * For each key k in `this` or other [[Dataset]]s, applies the given function on this key and
   * associated values, and return a resulting [[Dataset]] that contains the result of function.
   */
  def cogroup[W1, W2, W3, R : Encoder](other1: GroupedDataset[K, W1], other2: GroupedDataset[K, W2], other3: GroupedDataset[K, W3])(func: (K, Iterator[T], Iterator[W1], Iterator[W2], Iterator[W3]) => Iterator[R]): Dataset[R] = {
    implicit def wEnc1 = other1.tEnc
    implicit def wEnc2 = other2.tEnc
    implicit def wEnc3 = other3.tEnc
    val plan = CoGroup(func, groupingAttributes, other1.groupingAttributes, other2.groupingAttributes, other3.groupingAttributes, logicalPlan, other1.logicalPlan, other2.logicalPlan, other3.logicalPlan)
    new Dataset[R](sqlContext, plan)
  }

  // scalastyle:on
}

object GroupedDataset {

  // scalastyle:off
  /** Code generator for cogroup methods of various number of Datasets. */
  def main(args: Array[String]) {
    (1 until 4).map { n =>
      val types = (1 to n).map(t => s"W$t").mkString(", ") + ", R : Encoder"
      val datasets = (1 to n).map(t => s"other$t: GroupedDataset[K, W$t]").mkString(", ")
      val funcType = "(K, Iterator[T], " + (1 to n).map(t => s"Iterator[W$t]").mkString(", ") + ") => Iterator[R]"
      val implicits = (1 to n).map(t => s"implicit def wEnc$t = other$t.tEnc").mkString("\n")
      val groupings = "groupingAttributes, " + (1 to n).map(t => s"other$t.groupingAttributes").mkString(", ")
      val plans = "logicalPlan, " + (1 to n).map(t => s"other$t.logicalPlan").mkString(", ")

      println(
        s"""
           |/**
           | * For each key k in `this` or other [[Dataset]]s, applies the given function on this key and
           | * associated values, and return a resulting [[Dataset]] that contains the result of function.
           | */
           |def cogroup[$types]($datasets)(func: $funcType): Dataset[R] = {
           |  $implicits
           |  val plan = CoGroup(func, $groupings, $plans)
           |  new Dataset[R](sqlContext, plan)
           |}
         """.stripMargin)
    }
  }
  // scalastyle:on
}
