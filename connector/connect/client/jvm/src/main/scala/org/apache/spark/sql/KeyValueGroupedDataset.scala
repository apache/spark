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

import java.util.Arrays

import scala.collection.JavaConverters._
import scala.language.existentials

import org.apache.spark.api.java.function._
import org.apache.spark.connect.proto
import org.apache.spark.sql.catalyst.encoders.AgnosticEncoder
import org.apache.spark.sql.connect.client.UdfUtils
import org.apache.spark.sql.expressions.ScalarUserDefinedFunction
import org.apache.spark.sql.functions.col

/**
 * A [[Dataset]] has been logically grouped by a user specified grouping key. Users should not
 * construct a [[KeyValueGroupedDataset]] directly, but should instead call `groupByKey` on an
 * existing [[Dataset]].
 *
 * @since 3.5.0
 */
abstract class KeyValueGroupedDataset[K, V] private[sql] () extends Serializable {

  /**
   * Returns a new [[KeyValueGroupedDataset]] where the type of the key has been mapped to the
   * specified type. The mapping of key columns to the type follows the same rules as `as` on
   * [[Dataset]].
   *
   * @since 3.5.0
   */
  def keyAs[L: Encoder]: KeyValueGroupedDataset[L, V] = {
    throw new UnsupportedOperationException
  }

  /**
   * Returns a new [[KeyValueGroupedDataset]] where the given function `func` has been applied to
   * the data. The grouping key is unchanged by this.
   *
   * {{{
   *   // Create values grouped by key from a Dataset[(K, V)]
   *   ds.groupByKey(_._1).mapValues(_._2) // Scala
   * }}}
   *
   * @since 3.5.0
   */
  def mapValues[W: Encoder](valueFunc: V => W): KeyValueGroupedDataset[K, W] = {
    throw new UnsupportedOperationException
  }

  /**
   * Returns a new [[KeyValueGroupedDataset]] where the given function `func` has been applied to
   * the data. The grouping key is unchanged by this.
   *
   * {{{
   *   // Create Integer values grouped by String key from a Dataset<Tuple2<String, Integer>>
   *   Dataset<Tuple2<String, Integer>> ds = ...;
   *   KeyValueGroupedDataset<String, Integer> grouped =
   *     ds.groupByKey(t -> t._1, Encoders.STRING()).mapValues(t -> t._2, Encoders.INT());
   * }}}
   *
   * @since 3.5.0
   */
  def mapValues[W](func: MapFunction[V, W], encoder: Encoder[W]): KeyValueGroupedDataset[K, W] = {
    mapValues(UdfUtils.mapFunctionToScalaFunc(func))(encoder)
  }

  /**
   * Returns a [[Dataset]] that contains each unique key. This is equivalent to doing mapping over
   * the Dataset to extract the keys and then running a distinct operation on those.
   *
   * @since 3.5.0
   */
  def keys: Dataset[K] = {
    throw new UnsupportedOperationException
  }

  /**
   * (Scala-specific) Applies the given function to each group of data. For each unique group, the
   * function will be passed the group key and an iterator that contains all of the elements in
   * the group. The function can return an iterator containing elements of an arbitrary type which
   * will be returned as a new [[Dataset]].
   *
   * This function does not support partial aggregation, and as a result requires shuffling all
   * the data in the [[Dataset]]. If an application intends to perform an aggregation over each
   * key, it is best to use the reduce function or an
   * `org.apache.spark.sql.expressions#Aggregator`.
   *
   * Internally, the implementation will spill to disk if any given group is too large to fit into
   * memory. However, users must take care to avoid materializing the whole iterator for a group
   * (for example, by calling `toList`) unless they are sure that this is possible given the
   * memory constraints of their cluster.
   *
   * @since 3.5.0
   */
  def flatMapGroups[U: Encoder](f: (K, Iterator[V]) => TraversableOnce[U]): Dataset[U] = {
    flatMapSortedGroups()(f)
  }

  /**
   * (Java-specific) Applies the given function to each group of data. For each unique group, the
   * function will be passed the group key and an iterator that contains all of the elements in
   * the group. The function can return an iterator containing elements of an arbitrary type which
   * will be returned as a new [[Dataset]].
   *
   * This function does not support partial aggregation, and as a result requires shuffling all
   * the data in the [[Dataset]]. If an application intends to perform an aggregation over each
   * key, it is best to use the reduce function or an
   * `org.apache.spark.sql.expressions#Aggregator`.
   *
   * Internally, the implementation will spill to disk if any given group is too large to fit into
   * memory. However, users must take care to avoid materializing the whole iterator for a group
   * (for example, by calling `toList`) unless they are sure that this is possible given the
   * memory constraints of their cluster.
   *
   * @since 3.5.0
   */
  def flatMapGroups[U](f: FlatMapGroupsFunction[K, V, U], encoder: Encoder[U]): Dataset[U] = {
    flatMapGroups(UdfUtils.flatMapGroupsFuncToScalaFunc(f))(encoder)
  }

  /**
   * (Scala-specific) Applies the given function to each group of data. For each unique group, the
   * function will be passed the group key and a sorted iterator that contains all of the elements
   * in the group. The function can return an iterator containing elements of an arbitrary type
   * which will be returned as a new [[Dataset]].
   *
   * This function does not support partial aggregation, and as a result requires shuffling all
   * the data in the [[Dataset]]. If an application intends to perform an aggregation over each
   * key, it is best to use the reduce function or an
   * `org.apache.spark.sql.expressions#Aggregator`.
   *
   * Internally, the implementation will spill to disk if any given group is too large to fit into
   * memory. However, users must take care to avoid materializing the whole iterator for a group
   * (for example, by calling `toList`) unless they are sure that this is possible given the
   * memory constraints of their cluster.
   *
   * This is equivalent to [[KeyValueGroupedDataset#flatMapGroups]], except for the iterator to be
   * sorted according to the given sort expressions. That sorting does not add computational
   * complexity.
   *
   * @see
   *   [[org.apache.spark.sql.KeyValueGroupedDataset#flatMapGroups]]
   * @since 3.5.0
   */
  def flatMapSortedGroups[U: Encoder](sortExprs: Column*)(
      f: (K, Iterator[V]) => TraversableOnce[U]): Dataset[U] = {
    throw new UnsupportedOperationException
  }

  /**
   * (Java-specific) Applies the given function to each group of data. For each unique group, the
   * function will be passed the group key and a sorted iterator that contains all of the elements
   * in the group. The function can return an iterator containing elements of an arbitrary type
   * which will be returned as a new [[Dataset]].
   *
   * This function does not support partial aggregation, and as a result requires shuffling all
   * the data in the [[Dataset]]. If an application intends to perform an aggregation over each
   * key, it is best to use the reduce function or an
   * `org.apache.spark.sql.expressions#Aggregator`.
   *
   * Internally, the implementation will spill to disk if any given group is too large to fit into
   * memory. However, users must take care to avoid materializing the whole iterator for a group
   * (for example, by calling `toList`) unless they are sure that this is possible given the
   * memory constraints of their cluster.
   *
   * This is equivalent to [[KeyValueGroupedDataset#flatMapGroups]], except for the iterator to be
   * sorted according to the given sort expressions. That sorting does not add computational
   * complexity.
   *
   * @see
   *   [[org.apache.spark.sql.KeyValueGroupedDataset#flatMapGroups]]
   * @since 3.5.0
   */
  def flatMapSortedGroups[U](
      SortExprs: Array[Column],
      f: FlatMapGroupsFunction[K, V, U],
      encoder: Encoder[U]): Dataset[U] = {
    flatMapSortedGroups(SortExprs: _*)(UdfUtils.flatMapGroupsFuncToScalaFunc(f))(encoder)
  }

  /**
   * (Scala-specific) Applies the given function to each group of data. For each unique group, the
   * function will be passed the group key and an iterator that contains all of the elements in
   * the group. The function can return an element of arbitrary type which will be returned as a
   * new [[Dataset]].
   *
   * This function does not support partial aggregation, and as a result requires shuffling all
   * the data in the [[Dataset]]. If an application intends to perform an aggregation over each
   * key, it is best to use the reduce function or an
   * `org.apache.spark.sql.expressions#Aggregator`.
   *
   * Internally, the implementation will spill to disk if any given group is too large to fit into
   * memory. However, users must take care to avoid materializing the whole iterator for a group
   * (for example, by calling `toList`) unless they are sure that this is possible given the
   * memory constraints of their cluster.
   *
   * @since 3.5.0
   */
  def mapGroups[U: Encoder](f: (K, Iterator[V]) => U): Dataset[U] = {
    flatMapGroups(UdfUtils.mapGroupsFuncToFlatMapAdaptor(f))
  }

  /**
   * (Java-specific) Applies the given function to each group of data. For each unique group, the
   * function will be passed the group key and an iterator that contains all of the elements in
   * the group. The function can return an element of arbitrary type which will be returned as a
   * new [[Dataset]].
   *
   * This function does not support partial aggregation, and as a result requires shuffling all
   * the data in the [[Dataset]]. If an application intends to perform an aggregation over each
   * key, it is best to use the reduce function or an
   * `org.apache.spark.sql.expressions#Aggregator`.
   *
   * Internally, the implementation will spill to disk if any given group is too large to fit into
   * memory. However, users must take care to avoid materializing the whole iterator for a group
   * (for example, by calling `toList`) unless they are sure that this is possible given the
   * memory constraints of their cluster.
   *
   * @since 3.5.0
   */
  def mapGroups[U](f: MapGroupsFunction[K, V, U], encoder: Encoder[U]): Dataset[U] = {
    mapGroups(UdfUtils.mapGroupsFuncToScalaFunc(f))(encoder)
  }

  /**
   * (Scala-specific) Applies the given function to each cogrouped data. For each unique group,
   * the function will be passed the grouping key and 2 iterators containing all elements in the
   * group from [[Dataset]] `this` and `other`. The function can return an iterator containing
   * elements of an arbitrary type which will be returned as a new [[Dataset]].
   *
   * @since 3.5.0
   */
  def cogroup[U, R: Encoder](other: KeyValueGroupedDataset[K, U])(
      f: (K, Iterator[V], Iterator[U]) => TraversableOnce[R]): Dataset[R] = {
    cogroupSorted(other)()()(f)
  }

  /**
   * (Java-specific) Applies the given function to each cogrouped data. For each unique group, the
   * function will be passed the grouping key and 2 iterators containing all elements in the group
   * from [[Dataset]] `this` and `other`. The function can return an iterator containing elements
   * of an arbitrary type which will be returned as a new [[Dataset]].
   *
   * @since 3.5.0
   */
  def cogroup[U, R](
      other: KeyValueGroupedDataset[K, U],
      f: CoGroupFunction[K, V, U, R],
      encoder: Encoder[R]): Dataset[R] = {
    cogroup(other)(UdfUtils.coGroupFunctionToScalaFunc(f))(encoder)
  }

  /**
   * (Scala-specific) Applies the given function to each sorted cogrouped data. For each unique
   * group, the function will be passed the grouping key and 2 sorted iterators containing all
   * elements in the group from [[Dataset]] `this` and `other`. The function can return an
   * iterator containing elements of an arbitrary type which will be returned as a new
   * [[Dataset]].
   *
   * This is equivalent to [[KeyValueGroupedDataset#cogroup]], except for the iterators to be
   * sorted according to the given sort expressions. That sorting does not add computational
   * complexity.
   *
   * @see
   *   [[org.apache.spark.sql.KeyValueGroupedDataset#cogroup]]
   * @since 3.5.0
   */
  def cogroupSorted[U, R: Encoder](other: KeyValueGroupedDataset[K, U])(thisSortExprs: Column*)(
      otherSortExprs: Column*)(
      f: (K, Iterator[V], Iterator[U]) => TraversableOnce[R]): Dataset[R] = {
    throw new UnsupportedOperationException
  }

  /**
   * (Java-specific) Applies the given function to each sorted cogrouped data. For each unique
   * group, the function will be passed the grouping key and 2 sorted iterators containing all
   * elements in the group from [[Dataset]] `this` and `other`. The function can return an
   * iterator containing elements of an arbitrary type which will be returned as a new
   * [[Dataset]].
   *
   * This is equivalent to [[KeyValueGroupedDataset#cogroup]], except for the iterators to be
   * sorted according to the given sort expressions. That sorting does not add computational
   * complexity.
   *
   * @see
   *   [[org.apache.spark.sql.KeyValueGroupedDataset#cogroup]]
   * @since 3.5.0
   */
  def cogroupSorted[U, R](
      other: KeyValueGroupedDataset[K, U],
      thisSortExprs: Array[Column],
      otherSortExprs: Array[Column],
      f: CoGroupFunction[K, V, U, R],
      encoder: Encoder[R]): Dataset[R] = {
    cogroupSorted(other)(thisSortExprs: _*)(otherSortExprs: _*)(
      UdfUtils.coGroupFunctionToScalaFunc(f))(encoder)
  }
}

/**
 * This class is the implementation of class [[KeyValueGroupedDataset]]. This class memorizes the
 * initial types of the grouping function so that the original function will be sent to the server
 * to perform the grouping first. Then any type modifications on the keys and the values will be
 * applied sequentially to ensure the final type of the result remains the same as how
 * [[KeyValueGroupedDataset]] behaves on the server.
 */
private class KeyValueGroupedDatasetImpl[K, V, IK, IV](
    private val ds: Dataset[IV],
    private val sparkSession: SparkSession,
    private val plan: proto.Plan,
    private val ikEncoder: AgnosticEncoder[IK],
    private val kEncoder: AgnosticEncoder[K],
    private val groupingFunc: IV => IK,
    private val valueMapFunc: IV => V)
    extends KeyValueGroupedDataset[K, V] {

  private val ivEncoder = ds.encoder

  override def keyAs[L: Encoder]: KeyValueGroupedDataset[L, V] = {
    new KeyValueGroupedDatasetImpl[L, V, IK, IV](
      ds,
      sparkSession,
      plan,
      ikEncoder,
      encoderFor[L],
      groupingFunc,
      valueMapFunc)
  }

  override def mapValues[W: Encoder](valueFunc: V => W): KeyValueGroupedDataset[K, W] = {
    new KeyValueGroupedDatasetImpl[K, W, IK, IV](
      ds,
      sparkSession,
      plan,
      ikEncoder,
      kEncoder,
      groupingFunc,
      valueMapFunc.andThen(valueFunc))
  }

  override def keys: Dataset[K] = {
    ds.map(groupingFunc)(ikEncoder)
      .as(kEncoder)
      .dropDuplicates()
  }

  override def flatMapSortedGroups[U: Encoder](sortExprs: Column*)(
      f: (K, Iterator[V]) => TraversableOnce[U]): Dataset[U] = {
    // Apply mapValues changes to the udf
    val nf =
      if (valueMapFunc == UdfUtils.identical()) f else UdfUtils.mapValuesAdaptor(f, valueMapFunc)
    val outputEncoder = encoderFor[U]
    sparkSession.newDataset[U](outputEncoder) { builder =>
      builder.getGroupMapBuilder
        .setInput(plan.getRoot)
        .addAllSortingExpressions(sortExprs.map(e => e.expr).asJava)
        .addAllGroupingExpressions(getGroupingExpressions)
        .setFunc(getUdf(nf, outputEncoder)(ivEncoder))
    }
  }

  override def cogroupSorted[U, R: Encoder](other: KeyValueGroupedDataset[K, U])(
      thisSortExprs: Column*)(otherSortExprs: Column*)(
      f: (K, Iterator[V], Iterator[U]) => TraversableOnce[R]): Dataset[R] = {
    assert(other.isInstanceOf[KeyValueGroupedDatasetImpl[K, U, _, _]])
    val otherImpl = other.asInstanceOf[KeyValueGroupedDatasetImpl[K, U, _, _]]
    // Apply mapValues changes to the udf
    val nf = UdfUtils.mapValuesAdaptor(f, valueMapFunc, otherImpl.valueMapFunc)
    val outputEncoder = encoderFor[R]
    sparkSession.newDataset[R](outputEncoder) { builder =>
      builder.getCoGroupMapBuilder
        .setInput(plan.getRoot)
        .addAllInputGroupingExpressions(getGroupingExpressions)
        .addAllInputSortingExpressions(thisSortExprs.map(e => e.expr).asJava)
        .setOther(otherImpl.plan.getRoot)
        .addAllOtherGroupingExpressions(otherImpl.getGroupingExpressions)
        .addAllOtherSortingExpressions(otherSortExprs.map(e => e.expr).asJava)
        .setFunc(getUdf(nf, outputEncoder)(ivEncoder, otherImpl.ivEncoder))
    }
  }

  private def getGroupingExpressions = {
    val gf = ScalarUserDefinedFunction(
      function = groupingFunc,
      inputEncoders = ivEncoder :: Nil, // Using the original value and key encoders
      outputEncoder = ikEncoder)
    Arrays.asList(gf.apply(col("*")).expr)
  }

  private def getUdf[U: Encoder](nf: AnyRef, outputEncoder: AgnosticEncoder[U])(
      inEncoders: AgnosticEncoder[_]*): proto.CommonInlineUserDefinedFunction = {
    val inputEncoders = kEncoder +: inEncoders // Apply keyAs changes by setting kEncoder
    val udf = ScalarUserDefinedFunction(
      function = nf,
      inputEncoders = inputEncoders,
      outputEncoder = outputEncoder)
    udf.apply(inputEncoders.map(_ => col("*")): _*).expr.getCommonInlineUserDefinedFunction
  }
}
