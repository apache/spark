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
import org.apache.spark.connect.proto.Plan
import org.apache.spark.sql.catalyst.encoders.AgnosticEncoder
import org.apache.spark.sql.connect.common.UdfUtils
import org.apache.spark.sql.catalyst.encoders.AgnosticEncoders.ProductEncoder
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
   * (Scala-specific) Reduces the elements of each group of data using the specified binary
   * function. The given function must be commutative and associative or the result may be
   * non-deterministic.
   *
   * @since 3.5.0
   */
  def reduceGroups(f: (V, V) => V): Dataset[(K, V)] = {
    throw new UnsupportedOperationException
  }

  /**
   * (Java-specific) Reduces the elements of each group of data using the specified binary
   * function. The given function must be commutative and associative or the result may be
   * non-deterministic.
   *
   * @since 3.5.0
   */
  def reduceGroups(f: ReduceFunction[V]): Dataset[(K, V)] = {
    reduceGroups(UdfUtils.mapReduceFuncToScalaFunc(f))
  }

  /**
   * Internal helper function for building typed aggregations that return tuples. For simplicity
   * and code reuse, we do this without the help of the type system and then use helper functions
   * that cast appropriately for the user facing interface.
   */
  protected def aggUntyped(columns: TypedColumn[_, _]*): Dataset[_] = {
    throw new UnsupportedOperationException
  }

  /**
   * Computes the given aggregation, returning a [[Dataset]] of tuples for each unique key and the
   * result of computing this aggregation over all elements in the group.
   *
   * @since 3.5.0
   */
  def agg[U1](col1: TypedColumn[V, U1]): Dataset[(K, U1)] =
    aggUntyped(col1).asInstanceOf[Dataset[(K, U1)]]

  /**
   * Computes the given aggregations, returning a [[Dataset]] of tuples for each unique key and
   * the result of computing these aggregations over all elements in the group.
   *
   * @since 3.5.0
   */
  def agg[U1, U2](col1: TypedColumn[V, U1], col2: TypedColumn[V, U2]): Dataset[(K, U1, U2)] =
    aggUntyped(col1, col2).asInstanceOf[Dataset[(K, U1, U2)]]

  /**
   * Computes the given aggregations, returning a [[Dataset]] of tuples for each unique key and
   * the result of computing these aggregations over all elements in the group.
   *
   * @since 3.5.0
   */
  def agg[U1, U2, U3](
      col1: TypedColumn[V, U1],
      col2: TypedColumn[V, U2],
      col3: TypedColumn[V, U3]): Dataset[(K, U1, U2, U3)] =
    aggUntyped(col1, col2, col3).asInstanceOf[Dataset[(K, U1, U2, U3)]]

  /**
   * Computes the given aggregations, returning a [[Dataset]] of tuples for each unique key and
   * the result of computing these aggregations over all elements in the group.
   *
   * @since 3.5.0
   */
  def agg[U1, U2, U3, U4](
      col1: TypedColumn[V, U1],
      col2: TypedColumn[V, U2],
      col3: TypedColumn[V, U3],
      col4: TypedColumn[V, U4]): Dataset[(K, U1, U2, U3, U4)] =
    aggUntyped(col1, col2, col3, col4).asInstanceOf[Dataset[(K, U1, U2, U3, U4)]]

  /**
   * Computes the given aggregations, returning a [[Dataset]] of tuples for each unique key and
   * the result of computing these aggregations over all elements in the group.
   *
   * @since 3.5.0
   */
  def agg[U1, U2, U3, U4, U5](
      col1: TypedColumn[V, U1],
      col2: TypedColumn[V, U2],
      col3: TypedColumn[V, U3],
      col4: TypedColumn[V, U4],
      col5: TypedColumn[V, U5]): Dataset[(K, U1, U2, U3, U4, U5)] =
    aggUntyped(col1, col2, col3, col4, col5).asInstanceOf[Dataset[(K, U1, U2, U3, U4, U5)]]

  /**
   * Computes the given aggregations, returning a [[Dataset]] of tuples for each unique key and
   * the result of computing these aggregations over all elements in the group.
   *
   * @since 3.5.0
   */
  def agg[U1, U2, U3, U4, U5, U6](
      col1: TypedColumn[V, U1],
      col2: TypedColumn[V, U2],
      col3: TypedColumn[V, U3],
      col4: TypedColumn[V, U4],
      col5: TypedColumn[V, U5],
      col6: TypedColumn[V, U6]): Dataset[(K, U1, U2, U3, U4, U5, U6)] =
    aggUntyped(col1, col2, col3, col4, col5, col6)
      .asInstanceOf[Dataset[(K, U1, U2, U3, U4, U5, U6)]]

  /**
   * Computes the given aggregations, returning a [[Dataset]] of tuples for each unique key and
   * the result of computing these aggregations over all elements in the group.
   *
   * @since 3.5.0
   */
  def agg[U1, U2, U3, U4, U5, U6, U7](
      col1: TypedColumn[V, U1],
      col2: TypedColumn[V, U2],
      col3: TypedColumn[V, U3],
      col4: TypedColumn[V, U4],
      col5: TypedColumn[V, U5],
      col6: TypedColumn[V, U6],
      col7: TypedColumn[V, U7]): Dataset[(K, U1, U2, U3, U4, U5, U6, U7)] =
    aggUntyped(col1, col2, col3, col4, col5, col6, col7)
      .asInstanceOf[Dataset[(K, U1, U2, U3, U4, U5, U6, U7)]]

  /**
   * Computes the given aggregations, returning a [[Dataset]] of tuples for each unique key and
   * the result of computing these aggregations over all elements in the group.
   *
   * @since 3.5.0
   */
  def agg[U1, U2, U3, U4, U5, U6, U7, U8](
      col1: TypedColumn[V, U1],
      col2: TypedColumn[V, U2],
      col3: TypedColumn[V, U3],
      col4: TypedColumn[V, U4],
      col5: TypedColumn[V, U5],
      col6: TypedColumn[V, U6],
      col7: TypedColumn[V, U7],
      col8: TypedColumn[V, U8]): Dataset[(K, U1, U2, U3, U4, U5, U6, U7, U8)] =
    aggUntyped(col1, col2, col3, col4, col5, col6, col7, col8)
      .asInstanceOf[Dataset[(K, U1, U2, U3, U4, U5, U6, U7, U8)]]

  /**
   * Returns a [[Dataset]] that contains a tuple with each key and the number of items present for
   * that key.
   *
   * @since 3.5.0
   */
  def count(): Dataset[(K, Long)] = agg(functions.count("*"))

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
    private val sparkSession: SparkSession,
    private val plan: proto.Plan,
    private val ikEncoder: AgnosticEncoder[IK],
    private val kEncoder: AgnosticEncoder[K],
    private val ivEncoder: AgnosticEncoder[IV],
    private val vEncoder: AgnosticEncoder[V],
    private val groupingExprs: java.util.List[proto.Expression],
    private val valueMapFunc: IV => V,
    private val keysFunc: () => Dataset[IK])
    extends KeyValueGroupedDataset[K, V] {

  override def keyAs[L: Encoder]: KeyValueGroupedDataset[L, V] = {
    new KeyValueGroupedDatasetImpl[L, V, IK, IV](
      sparkSession,
      plan,
      ikEncoder,
      encoderFor[L],
      ivEncoder,
      vEncoder,
      groupingExprs,
      valueMapFunc,
      keysFunc)
  }

  override def mapValues[W: Encoder](valueFunc: V => W): KeyValueGroupedDataset[K, W] = {
    new KeyValueGroupedDatasetImpl[K, W, IK, IV](
      sparkSession,
      plan,
      ikEncoder,
      kEncoder,
      ivEncoder,
      encoderFor[W],
      groupingExprs,
      valueMapFunc.andThen(valueFunc),
      keysFunc)
  }

  override def keys: Dataset[K] = {
    keysFunc()
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
        .addAllGroupingExpressions(groupingExprs)
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
        .addAllInputGroupingExpressions(groupingExprs)
        .addAllInputSortingExpressions(thisSortExprs.map(e => e.expr).asJava)
        .setOther(otherImpl.plan.getRoot)
        .addAllOtherGroupingExpressions(otherImpl.groupingExprs)
        .addAllOtherSortingExpressions(otherSortExprs.map(e => e.expr).asJava)
        .setFunc(getUdf(nf, outputEncoder)(ivEncoder, otherImpl.ivEncoder))
    }
  }

  override protected def aggUntyped(columns: TypedColumn[_, _]*): Dataset[_] = {
    // TODO: For each column, apply the valueMap func first
    val rEnc = ProductEncoder.tuple(kEncoder +: columns.map(_.encoder)) // apply keyAs change
    sparkSession.newDataset(rEnc) { builder =>
      builder.getAggregateBuilder
        .setInput(plan.getRoot)
        .setGroupType(proto.Aggregate.GroupType.GROUP_TYPE_GROUPBYKEY)
        .addAllGroupingExpressions(groupingExprs)
        .addAllAggregateExpressions(columns.map(_.expr).asJava)
    }
  }

  override def reduceGroups(f: (V, V) => V): Dataset[(K, V)] = {
    val inputEncoders = Seq(vEncoder, vEncoder)
    val udf = ScalarUserDefinedFunction(
      function = f,
      inputEncoders = inputEncoders,
      outputEncoder = vEncoder)
    val input = udf.apply(inputEncoders.map(_ => col("*")): _*)
    val expr = Column.fn("reduce", input).expr
    val aggregator: TypedColumn[V, V] = new TypedColumn[V, V](expr, vEncoder)
    agg(aggregator)
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

private object KeyValueGroupedDatasetImpl {
  def apply[K, V](
      ds: Dataset[V],
      sparkSession: SparkSession,
      plan: Plan,
      kEncoder: AgnosticEncoder[K],
      groupingFunc: V => K): KeyValueGroupedDatasetImpl[K, V, K, V] = {
    val gf = ScalarUserDefinedFunction(
      function = groupingFunc,
      inputEncoders = ds.encoder :: Nil, // Using the original value and key encoders
      outputEncoder = kEncoder)
    new KeyValueGroupedDatasetImpl(
      sparkSession, plan, kEncoder, kEncoder, ds.encoder, ds.encoder,
      Arrays.asList(gf.apply(col("*")).expr), UdfUtils.identical(),
      () => ds.map(groupingFunc)(kEncoder))
  }

  def apply[K, V](
      df: DataFrame,
      sparkSession: SparkSession,
      plan: Plan,
      kEncoder: AgnosticEncoder[K],
      vEncoder: AgnosticEncoder[V],
      groupingExprs: Seq[Column]): KeyValueGroupedDatasetImpl[K, V, K, V] = {
    // Use a dummy udf to pass the K V encoders
    val dummyGroupingFunc = ScalarUserDefinedFunction(
      function = UdfUtils.as[V, K](),
      inputEncoders = vEncoder :: Nil,
      outputEncoder = kEncoder).apply(col("*"))

    new KeyValueGroupedDatasetImpl(
      sparkSession, plan, kEncoder, kEncoder, vEncoder, vEncoder,
      (Seq(dummyGroupingFunc) ++ groupingExprs).map(_.expr).asJava, UdfUtils.identical(),
      () => df.select(groupingExprs: _*).as(kEncoder))
  }
}
