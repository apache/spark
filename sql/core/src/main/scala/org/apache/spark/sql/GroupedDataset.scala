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

  /**
   * Applies the given function to each cogrouped data.  For each unique group, the function will
   * be passed the grouping key and 2 iterators containing all elements in the group from
   * [[Dataset]] `this` and `other`.  The function can return an iterator containing elements of an
   * arbitrary type which will be returned as a new [[Dataset]].
   */
  def cogroup[U, R : Encoder](
      other: GroupedDataset[K, U])(
      f: (K, Iterator[T], Iterator[U]) => Iterator[R]): Dataset[R] = {
    implicit def uEnc: Encoder[U] = other.tEncoder
    new Dataset[R](
      sqlContext,
      CoGroup(
        f,
        this.groupingAttributes,
        other.groupingAttributes,
        this.logicalPlan,
        other.logicalPlan))
  }
}
