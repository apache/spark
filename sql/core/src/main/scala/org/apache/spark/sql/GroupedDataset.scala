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

import org.apache.spark.sql.aggregators.UserAggregator
import org.apache.spark.sql.catalyst.encoders.{Tuple3Encoder, JoinedEncoder, J, Encoder}
import org.apache.spark.sql.catalyst.expressions.{And, EqualTo, Attribute}
import org.apache.spark.sql.catalyst.plans.Inner
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

  def agg[B1 : Encoder, C1 : Encoder](agg: UserAggregator[T, B1, C1]): Dataset[J[K, C1]] = {
    val boundAggregator =
      BoundAggregator[T, B1, C1](
        agg, implicitly[Encoder[T]], implicitly[Encoder[B1]], implicitly[Encoder[C1]])
    val output =
      implicitly[Encoder[K]].schema.toAttributes ++ implicitly[Encoder[C1]].schema.toAttributes

    implicit val j = new JoinedEncoder(implicitly[Encoder[K]], implicitly[Encoder[C1]])
    new Dataset[J[K, C1]](
      sqlContext,
      ApplyAggregators(
        boundAggregator :: Nil,
        groupingAttributes,
        output,
        logicalPlan))
  }

  def agg[B1 : Encoder, C1 : Encoder, B2 : Encoder, C2 : Encoder](
      agg1: UserAggregator[T, B1, C1], agg2: UserAggregator[T, B2, C2]): Dataset[(K, C1, C2)] = {
    val boundAggregator1 =
      BoundAggregator[T, B1, C1](
        agg1, implicitly[Encoder[T]], implicitly[Encoder[B1]], implicitly[Encoder[C1]])
    val boundAggregator2 =
      BoundAggregator[T, B2, C2](
        agg2, implicitly[Encoder[T]], implicitly[Encoder[B2]], implicitly[Encoder[C2]])

    val output =
      implicitly[Encoder[K]].schema.toAttributes ++
        implicitly[Encoder[C1]].schema.toAttributes ++
        implicitly[Encoder[C2]].schema.toAttributes

    implicit val j = new Tuple3Encoder(
      implicitly[Encoder[K]], implicitly[Encoder[C1]], implicitly[Encoder[C2]])
    new Dataset[(K, C1, C2)](
      sqlContext,
      ApplyAggregators(
        boundAggregator1 :: boundAggregator2 :: Nil,
        groupingAttributes,
        output,
        logicalPlan))
  }

  def join[U](other: GroupedDataset[K, U]): Dataset[J[T, U]] = {
    assert(groupingAttributes.size == other.groupingAttributes.size)
    val condition = groupingAttributes.zip(other.groupingAttributes).map {
      case (l, r) => EqualTo(l, r)
    }.reduceOption(And)

    // TODO: This fails for self joins probably.
    implicit val joinedEncoder = new JoinedEncoder(tEncoder, other.tEncoder)
    new Dataset[J[T, U]](
      sqlContext,
      Project(dataAttributes ++ other.dataAttributes,
        Join(logicalPlan, other.logicalPlan, Inner, condition)))
  }

  def cogroup[U, V: Encoder](
      other: GroupedDataset[K, U])(
      f: (K, Iterator[T], Iterator[U]) => Iterator[V]): Dataset[V] = ???


  def countByKey: Dataset[(K, Long)] = ???
}