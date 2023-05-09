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
import org.apache.spark.sql.connect.common.UdfUtils
import org.apache.spark.sql.expressions.ScalarUserDefinedFunction
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, OutputMode}

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

  /**
   * (Scala-specific) Applies the given function to each group of data, while maintaining a
   * user-defined per-group state. The result Dataset will represent the objects returned by the
   * function. For a static batch Dataset, the function will be invoked once per group. For a
   * streaming Dataset, the function will be invoked for each group repeatedly in every trigger,
   * and updates to each group's state will be saved across invocations. See
   * [[org.apache.spark.sql.streaming.GroupState]] for more details.
   *
   * @tparam S
   *   The type of the user-defined state. Must be encodable to Spark SQL types.
   * @tparam U
   *   The type of the output objects. Must be encodable to Spark SQL types.
   * @param func
   *   Function to be called on every group.
   *
   * See [[Encoder]] for more details on what types are encodable to Spark SQL.
   * @since 2.2.0
   */
  def mapGroupsWithState[S: Encoder, U: Encoder](
      func: (K, Iterator[V], GroupState[S]) => U): Dataset[U] = {
    mapGroupsWithState(GroupStateTimeout.NoTimeout)(func)
  }

  /**
   * (Scala-specific) Applies the given function to each group of data, while maintaining a
   * user-defined per-group state. The result Dataset will represent the objects returned by the
   * function. For a static batch Dataset, the function will be invoked once per group. For a
   * streaming Dataset, the function will be invoked for each group repeatedly in every trigger,
   * and updates to each group's state will be saved across invocations. See
   * [[org.apache.spark.sql.streaming.GroupState]] for more details.
   *
   * @tparam S
   *   The type of the user-defined state. Must be encodable to Spark SQL types.
   * @tparam U
   *   The type of the output objects. Must be encodable to Spark SQL types.
   * @param func
   *   Function to be called on every group.
   * @param timeoutConf
   *   Timeout configuration for groups that do not receive data for a while.
   *
   * See [[Encoder]] for more details on what types are encodable to Spark SQL.
   * @since 2.2.0
   */
  def mapGroupsWithState[S: Encoder, U: Encoder](timeoutConf: GroupStateTimeout)(
      func: (K, Iterator[V], GroupState[S]) => U): Dataset[U] = {
    throw new UnsupportedOperationException
  }

  /**
   * (Scala-specific) Applies the given function to each group of data, while maintaining a
   * user-defined per-group state. The result Dataset will represent the objects returned by the
   * function. For a static batch Dataset, the function will be invoked once per group. For a
   * streaming Dataset, the function will be invoked for each group repeatedly in every trigger,
   * and updates to each group's state will be saved across invocations. See
   * [[org.apache.spark.sql.streaming.GroupState]] for more details.
   *
   * @tparam S
   *   The type of the user-defined state. Must be encodable to Spark SQL types.
   * @tparam U
   *   The type of the output objects. Must be encodable to Spark SQL types.
   * @param func
   *   Function to be called on every group.
   * @param timeoutConf
   *   Timeout Conf, see GroupStateTimeout for more details
   * @param initialState
   *   The user provided state that will be initialized when the first batch of data is processed
   *   in the streaming query. The user defined function will be called on the state data even if
   *   there are no other values in the group. To convert a Dataset ds of type Dataset[(K, S)] to
   *   a KeyValueGroupedDataset[K, S] do {{{ds.groupByKey(x => x._1).mapValues(_._2)}}}
   *
   * See [[Encoder]] for more details on what types are encodable to Spark SQL.
   * @since 3.2.0
   */
  def mapGroupsWithState[S: Encoder, U: Encoder](
      timeoutConf: GroupStateTimeout,
      initialState: KeyValueGroupedDataset[K, S])(
      func: (K, Iterator[V], GroupState[S]) => U): Dataset[U] = {
    throw new UnsupportedOperationException
  }

  /**
   * (Scala-specific) Applies the given function to each group of data, while maintaining a
   * user-defined per-group state. The result Dataset will represent the objects returned by the
   * function. For a static batch Dataset, the function will be invoked once per group. For a
   * streaming Dataset, the function will be invoked for each group repeatedly in every trigger,
   * and updates to each group's state will be saved across invocations. See `GroupState` for more
   * details.
   *
   * @tparam S
   *   The type of the user-defined state. Must be encodable to Spark SQL types.
   * @tparam U
   *   The type of the output objects. Must be encodable to Spark SQL types.
   * @param func
   *   Function to be called on every group.
   * @param outputMode
   *   The output mode of the function.
   * @param timeoutConf
   *   Timeout configuration for groups that do not receive data for a while.
   *
   * See [[Encoder]] for more details on what types are encodable to Spark SQL.
   * @since 2.2.0
   */
  def flatMapGroupsWithState[S: Encoder, U: Encoder](
      outputMode: OutputMode,
      timeoutConf: GroupStateTimeout)(
      func: (K, Iterator[V], GroupState[S]) => Iterator[U]): Dataset[U] = {
    throw new UnsupportedOperationException
  }

  /**
   * (Scala-specific) Applies the given function to each group of data, while maintaining a
   * user-defined per-group state. The result Dataset will represent the objects returned by the
   * function. For a static batch Dataset, the function will be invoked once per group. For a
   * streaming Dataset, the function will be invoked for each group repeatedly in every trigger,
   * and updates to each group's state will be saved across invocations. See `GroupState` for more
   * details.
   *
   * @tparam S
   *   The type of the user-defined state. Must be encodable to Spark SQL types.
   * @tparam U
   *   The type of the output objects. Must be encodable to Spark SQL types.
   * @param func
   *   Function to be called on every group.
   * @param outputMode
   *   The output mode of the function.
   * @param timeoutConf
   *   Timeout configuration for groups that do not receive data for a while.
   * @param initialState
   *   The user provided state that will be initialized when the first batch of data is processed
   *   in the streaming query. The user defined function will be called on the state data even if
   *   there are no other values in the group. To covert a Dataset `ds` of type of type
   *   `Dataset[(K, S)]` to a `KeyValueGroupedDataset[K, S]`, use
   *   {{{ds.groupByKey(x => x._1).mapValues(_._2)}}} See [[Encoder]] for more details on what
   *   types are encodable to Spark SQL.
   * @since 3.2.0
   */
  def flatMapGroupsWithState[S: Encoder, U: Encoder](
      outputMode: OutputMode,
      timeoutConf: GroupStateTimeout,
      initialState: KeyValueGroupedDataset[K, S])(
      func: (K, Iterator[V], GroupState[S]) => Iterator[U]): Dataset[U] = {
    throw new UnsupportedOperationException
  }

  def getGroupingExpressions(): java.util.List[proto.Expression] = {
    throw new UnsupportedOperationException
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
      .dropDuplicates()
      .as(kEncoder)
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

  override def mapGroupsWithState[S: Encoder, U: Encoder](
      timeoutConf: GroupStateTimeout,
      initialState: KeyValueGroupedDataset[K, S])(
      func: (K, Iterator[V], GroupState[S]) => U): Dataset[U] = {
    val nf = if (valueMapFunc == UdfUtils.identical()) {
      func
    } else {
      UdfUtils.mapValuesAdaptor(func, valueMapFunc)
    }
    val outputEncoder = encoderFor[U]
    sparkSession.newDataset[U](outputEncoder) { builder =>
      builder.getFlatMapGroupsWithStateBuilder
        .setInput(plan.getRoot)
        .addAllGroupingExpressions(getGroupingExpressions)
        .setFunc(getUdf(nf, outputEncoder)(ivEncoder))
        .setIsMapGroupsWithState(true)
        .setOutputMode(OutputMode.Update.toString)
        .setTimeoutConf(timeoutConf.toString)
        .addAllInitialGroupingExpressions(initialState.getGroupingExpressions)
    }
  }

  override def mapGroupsWithState[S: Encoder, U: Encoder](timeoutConf: GroupStateTimeout)(
      func: (K, Iterator[V], GroupState[S]) => U): Dataset[U] = {
    val nf = if (valueMapFunc == UdfUtils.identical()) {
      func
    } else {
      UdfUtils.mapValuesAdaptor(func, valueMapFunc)
    }
    val outputEncoder = encoderFor[U]
    sparkSession.newDataset[U](outputEncoder) { builder =>
      builder.getFlatMapGroupsWithStateBuilder
        .setInput(plan.getRoot)
        .addAllGroupingExpressions(getGroupingExpressions)
        .setFunc(getUdf(nf, outputEncoder)(ivEncoder))
        .setIsMapGroupsWithState(true)
        .setOutputMode(OutputMode.Update.toString)
        .setTimeoutConf(timeoutConf.toString)
    }
  }

  override def flatMapGroupsWithState[S: Encoder, U: Encoder](
      outputMode: OutputMode,
      timeoutConf: GroupStateTimeout,
      initialState: KeyValueGroupedDataset[K, S])(
      func: (K, Iterator[V], GroupState[S]) => Iterator[U]): Dataset[U] = {
    if (outputMode != OutputMode.Append && outputMode != OutputMode.Update) {
      throw new IllegalArgumentException("The output mode of function should be append or update")
    }
    val nf = if (valueMapFunc == UdfUtils.identical()) {
      func
    } else {
      UdfUtils.mapValuesAdaptor(func, valueMapFunc)
    }
    val outputEncoder = encoderFor[U]
    sparkSession.newDataset[U](outputEncoder) { builder =>
      builder.getFlatMapGroupsWithStateBuilder
        .setInput(plan.getRoot)
        .addAllGroupingExpressions(getGroupingExpressions)
        .setFunc(getUdf(nf, outputEncoder)(ivEncoder))
        .setIsMapGroupsWithState(false)
        .setOutputMode(outputMode.toString)
        .setTimeoutConf(timeoutConf.toString)
        .addAllInitialGroupingExpressions(initialState.getGroupingExpressions)
    }
  }

  override def flatMapGroupsWithState[S: Encoder, U: Encoder](
      outputMode: OutputMode,
      timeoutConf: GroupStateTimeout)(
      func: (K, Iterator[V], GroupState[S]) => Iterator[U]): Dataset[U] = {
    if (outputMode != OutputMode.Append && outputMode != OutputMode.Update) {
      throw new IllegalArgumentException("The output mode of function should be append or update")
    }
    val nf = if (valueMapFunc == UdfUtils.identical()) {
      func
    } else {
      UdfUtils.mapValuesAdaptor(func, valueMapFunc)
    }
    val outputEncoder = encoderFor[U]
    sparkSession.newDataset[U](outputEncoder) { builder =>
      builder.getFlatMapGroupsWithStateBuilder
        .setInput(plan.getRoot)
        .addAllGroupingExpressions(getGroupingExpressions)
        .setFunc(getUdf(nf, outputEncoder)(ivEncoder))
        .setIsMapGroupsWithState(false)
        .setOutputMode(outputMode.toString)
        .setTimeoutConf(timeoutConf.toString)
    }
  }

  override def getGroupingExpressions(): java.util.List[proto.Expression] = {
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
