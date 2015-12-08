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

import scala.collection.JavaConverters._

import org.apache.spark.annotation.Experimental
import org.apache.spark.api.java.function._
import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, encoderFor, OuterScopes}
import org.apache.spark.sql.catalyst.expressions.{Alias, CreateStruct, Attribute}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.expressions.Aggregator

/**
 * :: Experimental ::
 * A [[Dataset]] has been logically grouped by a user specified grouping key.  Users should not
 * construct a [[GroupedDataset]] directly, but should instead call `groupBy` on an existing
 * [[Dataset]].
 *
 * COMPATIBILITY NOTE: Long term we plan to make [[GroupedDataset)]] extend `GroupedData`.  However,
 * making this change to the class hierarchy would break some function signatures. As such, this
 * class should be considered a preview of the final API.  Changes will be made to the interface
 * after Spark 1.6.
 *
 * @since 1.6.0
 */
@Experimental
class GroupedDataset[K, V] private[sql](
    kEncoder: Encoder[K],
    vEncoder: Encoder[V],
    val queryExecution: QueryExecution,
    private val dataAttributes: Seq[Attribute],
    private val groupingAttributes: Seq[Attribute]) extends Serializable {

  // Similar to [[Dataset]], we use unresolved encoders for later composition and resolved encoders
  // when constructing new logical plans that will operate on the output of the current
  // queryexecution.

  private implicit val unresolvedKEncoder = encoderFor(kEncoder)
  private implicit val unresolvedVEncoder = encoderFor(vEncoder)

  private val resolvedKEncoder =
    unresolvedKEncoder.resolve(groupingAttributes, OuterScopes.outerScopes)
  private val resolvedVEncoder =
    unresolvedVEncoder.resolve(dataAttributes, OuterScopes.outerScopes)

  private def logicalPlan = queryExecution.analyzed
  private def sqlContext = queryExecution.sqlContext

  private def groupedData =
    new GroupedData(
      new DataFrame(sqlContext, logicalPlan), groupingAttributes, GroupedData.GroupByType)

  /**
   * Returns a new [[GroupedDataset]] where the type of the key has been mapped to the specified
   * type. The mapping of key columns to the type follows the same rules as `as` on [[Dataset]].
   *
   * @since 1.6.0
   */
  def keyAs[L : Encoder]: GroupedDataset[L, V] =
    new GroupedDataset(
      encoderFor[L],
      unresolvedVEncoder,
      queryExecution,
      dataAttributes,
      groupingAttributes)

  /**
   * Returns a [[Dataset]] that contains each unique key.
   *
   * @since 1.6.0
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
   * This function does not support partial aggregation, and as a result requires shuffling all
   * the data in the [[Dataset]]. If an application intends to perform an aggregation over each
   * key, it is best to use the reduce function or an [[Aggregator]].
   *
   * Internally, the implementation will spill to disk if any given group is too large to fit into
   * memory.  However, users must take care to avoid materializing the whole iterator for a group
   * (for example, by calling `toList`) unless they are sure that this is possible given the memory
   * constraints of their cluster.
   *
   * @since 1.6.0
   */
  def flatMapGroups[U : Encoder](f: (K, Iterator[V]) => TraversableOnce[U]): Dataset[U] = {
    new Dataset[U](
      sqlContext,
      MapGroups(
        f,
        resolvedKEncoder,
        resolvedVEncoder,
        groupingAttributes,
        logicalPlan))
  }

  /**
   * Applies the given function to each group of data.  For each unique group, the function will
   * be passed the group key and an iterator that contains all of the elements in the group. The
   * function can return an iterator containing elements of an arbitrary type which will be returned
   * as a new [[Dataset]].
   *
   * This function does not support partial aggregation, and as a result requires shuffling all
   * the data in the [[Dataset]]. If an application intends to perform an aggregation over each
   * key, it is best to use the reduce function or an [[Aggregator]].
   *
   * Internally, the implementation will spill to disk if any given group is too large to fit into
   * memory.  However, users must take care to avoid materializing the whole iterator for a group
   * (for example, by calling `toList`) unless they are sure that this is possible given the memory
   * constraints of their cluster.
   *
   * @since 1.6.0
   */
  def flatMapGroups[U](f: FlatMapGroupsFunction[K, V, U], encoder: Encoder[U]): Dataset[U] = {
    flatMapGroups((key, data) => f.call(key, data.asJava).asScala)(encoder)
  }

  /**
   * Applies the given function to each group of data.  For each unique group, the function will
   * be passed the group key and an iterator that contains all of the elements in the group. The
   * function can return an element of arbitrary type which will be returned as a new [[Dataset]].
   *
   * This function does not support partial aggregation, and as a result requires shuffling all
   * the data in the [[Dataset]]. If an application intends to perform an aggregation over each
   * key, it is best to use the reduce function or an [[Aggregator]].
   *
   * Internally, the implementation will spill to disk if any given group is too large to fit into
   * memory.  However, users must take care to avoid materializing the whole iterator for a group
   * (for example, by calling `toList`) unless they are sure that this is possible given the memory
   * constraints of their cluster.
   *
   * @since 1.6.0
   */
  def mapGroups[U : Encoder](f: (K, Iterator[V]) => U): Dataset[U] = {
    val func = (key: K, it: Iterator[V]) => Iterator(f(key, it))
    flatMapGroups(func)
  }

  /**
   * Applies the given function to each group of data.  For each unique group, the function will
   * be passed the group key and an iterator that contains all of the elements in the group. The
   * function can return an element of arbitrary type which will be returned as a new [[Dataset]].
   *
   * This function does not support partial aggregation, and as a result requires shuffling all
   * the data in the [[Dataset]]. If an application intends to perform an aggregation over each
   * key, it is best to use the reduce function or an [[Aggregator]].
   *
   * Internally, the implementation will spill to disk if any given group is too large to fit into
   * memory.  However, users must take care to avoid materializing the whole iterator for a group
   * (for example, by calling `toList`) unless they are sure that this is possible given the memory
   * constraints of their cluster.
   *
   * @since 1.6.0
   */
  def mapGroups[U](f: MapGroupsFunction[K, V, U], encoder: Encoder[U]): Dataset[U] = {
    mapGroups((key, data) => f.call(key, data.asJava))(encoder)
  }

  /**
   * Reduces the elements of each group of data using the specified binary function.
   * The given function must be commutative and associative or the result may be non-deterministic.
   *
   * @since 1.6.0
   */
  def reduce(f: (V, V) => V): Dataset[(K, V)] = {
    val func = (key: K, it: Iterator[V]) => Iterator((key, it.reduce(f)))

    implicit val resultEncoder = ExpressionEncoder.tuple(unresolvedKEncoder, unresolvedVEncoder)
    flatMapGroups(func)
  }

  /**
   * Reduces the elements of each group of data using the specified binary function.
   * The given function must be commutative and associative or the result may be non-deterministic.
   *
   * @since 1.6.0
   */
  def reduce(f: ReduceFunction[V]): Dataset[(K, V)] = {
    reduce(f.call _)
  }

  // This is here to prevent us from adding overloads that would be ambiguous.
  @scala.annotation.varargs
  private def agg(exprs: Column*): DataFrame =
    groupedData.agg(withEncoder(exprs.head), exprs.tail.map(withEncoder): _*)

  private def withEncoder(c: Column): Column = c match {
    case tc: TypedColumn[_, _] =>
      tc.withInputType(resolvedVEncoder.bind(dataAttributes), dataAttributes)
    case _ => c
  }

  /**
   * Internal helper function for building typed aggregations that return tuples.  For simplicity
   * and code reuse, we do this without the help of the type system and then use helper functions
   * that cast appropriately for the user facing interface.
   * TODO: does not handle aggrecations that return nonflat results,
   */
  protected def aggUntyped(columns: TypedColumn[_, _]*): Dataset[_] = {
    val encoders = columns.map(_.encoder)
    val namedColumns =
      columns.map(
        _.withInputType(resolvedVEncoder, dataAttributes).named)
    val keyColumn = if (resolvedKEncoder.flat) {
      assert(groupingAttributes.length == 1)
      groupingAttributes.head
    } else {
      Alias(CreateStruct(groupingAttributes), "key")()
    }
    val aggregate = Aggregate(groupingAttributes, keyColumn +: namedColumns, logicalPlan)
    val execution = new QueryExecution(sqlContext, aggregate)

    new Dataset(
      sqlContext,
      execution,
      ExpressionEncoder.tuple(unresolvedKEncoder +: encoders))
  }

  /**
   * Computes the given aggregation, returning a [[Dataset]] of tuples for each unique key
   * and the result of computing this aggregation over all elements in the group.
   *
   * @since 1.6.0
   */
  def agg[U1](col1: TypedColumn[V, U1]): Dataset[(K, U1)] =
    aggUntyped(col1).asInstanceOf[Dataset[(K, U1)]]

  /**
   * Computes the given aggregations, returning a [[Dataset]] of tuples for each unique key
   * and the result of computing these aggregations over all elements in the group.
   *
   * @since 1.6.0
   */
  def agg[U1, U2](col1: TypedColumn[V, U1], col2: TypedColumn[V, U2]): Dataset[(K, U1, U2)] =
    aggUntyped(col1, col2).asInstanceOf[Dataset[(K, U1, U2)]]

  /**
   * Computes the given aggregations, returning a [[Dataset]] of tuples for each unique key
   * and the result of computing these aggregations over all elements in the group.
   *
   * @since 1.6.0
   */
  def agg[U1, U2, U3](
      col1: TypedColumn[V, U1],
      col2: TypedColumn[V, U2],
      col3: TypedColumn[V, U3]): Dataset[(K, U1, U2, U3)] =
    aggUntyped(col1, col2, col3).asInstanceOf[Dataset[(K, U1, U2, U3)]]

  /**
   * Computes the given aggregations, returning a [[Dataset]] of tuples for each unique key
   * and the result of computing these aggregations over all elements in the group.
   *
   * @since 1.6.0
   */
  def agg[U1, U2, U3, U4](
      col1: TypedColumn[V, U1],
      col2: TypedColumn[V, U2],
      col3: TypedColumn[V, U3],
      col4: TypedColumn[V, U4]): Dataset[(K, U1, U2, U3, U4)] =
    aggUntyped(col1, col2, col3, col4).asInstanceOf[Dataset[(K, U1, U2, U3, U4)]]

  /**
   * Returns a [[Dataset]] that contains a tuple with each key and the number of items present
   * for that key.
   *
   * @since 1.6.0
   */
  def count(): Dataset[(K, Long)] = agg(functions.count("*").as(ExpressionEncoder[Long]))

  /**
   * Applies the given function to each cogrouped data.  For each unique group, the function will
   * be passed the grouping key and 2 iterators containing all elements in the group from
   * [[Dataset]] `this` and `other`.  The function can return an iterator containing elements of an
   * arbitrary type which will be returned as a new [[Dataset]].
   *
   * @since 1.6.0
   */
  def cogroup[U, R : Encoder](
      other: GroupedDataset[K, U])(
      f: (K, Iterator[V], Iterator[U]) => TraversableOnce[R]): Dataset[R] = {
    new Dataset[R](
      sqlContext,
      CoGroup(
        f,
        this.resolvedKEncoder,
        this.resolvedVEncoder,
        other.resolvedVEncoder,
        this.groupingAttributes,
        other.groupingAttributes,
        this.logicalPlan,
        other.logicalPlan))
  }

  /**
   * Applies the given function to each cogrouped data.  For each unique group, the function will
   * be passed the grouping key and 2 iterators containing all elements in the group from
   * [[Dataset]] `this` and `other`.  The function can return an iterator containing elements of an
   * arbitrary type which will be returned as a new [[Dataset]].
   *
   * @since 1.6.0
   */
  def cogroup[U, R](
      other: GroupedDataset[K, U],
      f: CoGroupFunction[K, V, U, R],
      encoder: Encoder[R]): Dataset[R] = {
    cogroup(other)((key, left, right) => f.call(key, left.asJava, right.asJava).asScala)(encoder)
  }
}
