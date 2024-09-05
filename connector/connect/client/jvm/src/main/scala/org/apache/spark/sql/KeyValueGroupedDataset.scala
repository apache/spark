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

import scala.annotation.unused
import scala.jdk.CollectionConverters._

import org.apache.spark.api.java.function._
import org.apache.spark.connect.proto
import org.apache.spark.sql.catalyst.encoders.AgnosticEncoder
import org.apache.spark.sql.catalyst.encoders.AgnosticEncoders.ProductEncoder
import org.apache.spark.sql.connect.common.UdfUtils
import org.apache.spark.sql.expressions.SparkUserDefinedFunction
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.internal.ColumnNodeToProtoConverter.toExpr
import org.apache.spark.sql.internal.UDFAdaptors
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, OutputMode, StatefulProcessor, StatefulProcessorWithInitialState, TimeMode}

/**
 * A [[Dataset]] has been logically grouped by a user specified grouping key. Users should not
 * construct a [[KeyValueGroupedDataset]] directly, but should instead call `groupByKey` on an
 * existing [[Dataset]].
 *
 * @since 3.5.0
 */
class KeyValueGroupedDataset[K, V] private[sql] ()
    extends api.KeyValueGroupedDataset[K, V, Dataset] {
  type KVDS[KY, VL] = KeyValueGroupedDataset[KY, VL]

  private def unsupported(): Nothing = throw new UnsupportedOperationException()

  /**
   * Returns a new [[KeyValueGroupedDataset]] where the type of the key has been mapped to the
   * specified type. The mapping of key columns to the type follows the same rules as `as` on
   * [[Dataset]].
   *
   * @since 3.5.0
   */
  def keyAs[L: Encoder]: KeyValueGroupedDataset[L, V] = unsupported()

  /** @inheritdoc */
  def mapValues[W: Encoder](valueFunc: V => W): KeyValueGroupedDataset[K, W] =
    unsupported()

  /** @inheritdoc */
  def keys: Dataset[K] = unsupported()

  /** @inheritdoc */
  def flatMapSortedGroups[U: Encoder](sortExprs: Column*)(
      f: (K, Iterator[V]) => IterableOnce[U]): Dataset[U] =
    unsupported()

  /** @inheritdoc */
  def reduceGroups(f: (V, V) => V): Dataset[(K, V)] = unsupported()

  /** @inheritdoc */
  protected def aggUntyped(columns: TypedColumn[_, _]*): Dataset[_] = unsupported()

  /** @inheritdoc */
  def cogroupSorted[U, R: Encoder](other: KeyValueGroupedDataset[K, U])(thisSortExprs: Column*)(
      otherSortExprs: Column*)(f: (K, Iterator[V], Iterator[U]) => IterableOnce[R]): Dataset[R] =
    unsupported()

  protected[sql] def flatMapGroupsWithStateHelper[S: Encoder, U: Encoder](
      outputMode: Option[OutputMode],
      timeoutConf: GroupStateTimeout,
      initialState: Option[KeyValueGroupedDataset[K, S]],
      isMapGroupWithState: Boolean)(
      func: (K, Iterator[V], GroupState[S]) => Iterator[U]): Dataset[U] = unsupported()

  /** @inheritdoc */
  def mapGroupsWithState[S: Encoder, U: Encoder](
      func: (K, Iterator[V], GroupState[S]) => U): Dataset[U] = {
    mapGroupsWithState(GroupStateTimeout.NoTimeout)(func)
  }

  /** @inheritdoc */
  def mapGroupsWithState[S: Encoder, U: Encoder](timeoutConf: GroupStateTimeout)(
      func: (K, Iterator[V], GroupState[S]) => U): Dataset[U] = {
    flatMapGroupsWithStateHelper(None, timeoutConf, None, isMapGroupWithState = true)(
      UDFAdaptors.mapGroupsWithStateToFlatMapWithState(func))
  }

  /** @inheritdoc */
  def mapGroupsWithState[S: Encoder, U: Encoder](
      timeoutConf: GroupStateTimeout,
      initialState: KeyValueGroupedDataset[K, S])(
      func: (K, Iterator[V], GroupState[S]) => U): Dataset[U] = {
    flatMapGroupsWithStateHelper(
      None,
      timeoutConf,
      Some(initialState),
      isMapGroupWithState = true)(UDFAdaptors.mapGroupsWithStateToFlatMapWithState(func))
  }

  /** @inheritdoc */
  def flatMapGroupsWithState[S: Encoder, U: Encoder](
      outputMode: OutputMode,
      timeoutConf: GroupStateTimeout)(
      func: (K, Iterator[V], GroupState[S]) => Iterator[U]): Dataset[U] = {
    flatMapGroupsWithStateHelper(
      Some(outputMode),
      timeoutConf,
      None,
      isMapGroupWithState = false)(func)
  }

  /** @inheritdoc */
  def flatMapGroupsWithState[S: Encoder, U: Encoder](
      outputMode: OutputMode,
      timeoutConf: GroupStateTimeout,
      initialState: KeyValueGroupedDataset[K, S])(
      func: (K, Iterator[V], GroupState[S]) => Iterator[U]): Dataset[U] = {
    flatMapGroupsWithStateHelper(
      Some(outputMode),
      timeoutConf,
      Some(initialState),
      isMapGroupWithState = false)(func)
  }

  /** @inheritdoc */
  private[sql] def transformWithState[U: Encoder](
      statefulProcessor: StatefulProcessor[K, V, U],
      timeMode: TimeMode,
      outputMode: OutputMode): Dataset[U] =
    unsupported()

  /** @inheritdoc */
  private[sql] def transformWithState[U: Encoder, S: Encoder](
      statefulProcessor: StatefulProcessorWithInitialState[K, V, U, S],
      timeMode: TimeMode,
      outputMode: OutputMode,
      initialState: KeyValueGroupedDataset[K, S]): Dataset[U] =
    unsupported()

  /** @inheritdoc */
  override private[sql] def transformWithState[U: Encoder](
      statefulProcessor: StatefulProcessor[K, V, U],
      eventTimeColumnName: String,
      outputMode: OutputMode): Dataset[U] = unsupported()

  /** @inheritdoc */
  override private[sql] def transformWithState[U: Encoder, S: Encoder](
      statefulProcessor: StatefulProcessorWithInitialState[K, V, U, S],
      eventTimeColumnName: String,
      outputMode: OutputMode,
      initialState: KeyValueGroupedDataset[K, S]): Dataset[U] = unsupported()

  // Overrides...
  /** @inheritdoc */
  override def mapValues[W](
      func: MapFunction[V, W],
      encoder: Encoder[W]): KeyValueGroupedDataset[K, W] = super.mapValues(func, encoder)

  /** @inheritdoc */
  override def flatMapGroups[U: Encoder](f: (K, Iterator[V]) => IterableOnce[U]): Dataset[U] =
    super.flatMapGroups(f)

  /** @inheritdoc */
  override def flatMapGroups[U](
      f: FlatMapGroupsFunction[K, V, U],
      encoder: Encoder[U]): Dataset[U] = super.flatMapGroups(f, encoder)

  /** @inheritdoc */
  override def flatMapSortedGroups[U](
      SortExprs: Array[Column],
      f: FlatMapGroupsFunction[K, V, U],
      encoder: Encoder[U]): Dataset[U] = super.flatMapSortedGroups(SortExprs, f, encoder)

  /** @inheritdoc */
  override def mapGroups[U: Encoder](f: (K, Iterator[V]) => U): Dataset[U] = super.mapGroups(f)

  /** @inheritdoc */
  override def mapGroups[U](f: MapGroupsFunction[K, V, U], encoder: Encoder[U]): Dataset[U] =
    super.mapGroups(f, encoder)

  /** @inheritdoc */
  override def mapGroupsWithState[S, U](
      func: MapGroupsWithStateFunction[K, V, S, U],
      stateEncoder: Encoder[S],
      outputEncoder: Encoder[U]): Dataset[U] =
    super.mapGroupsWithState(func, stateEncoder, outputEncoder)

  /** @inheritdoc */
  override def mapGroupsWithState[S, U](
      func: MapGroupsWithStateFunction[K, V, S, U],
      stateEncoder: Encoder[S],
      outputEncoder: Encoder[U],
      timeoutConf: GroupStateTimeout): Dataset[U] =
    super.mapGroupsWithState(func, stateEncoder, outputEncoder, timeoutConf)

  /** @inheritdoc */
  override def mapGroupsWithState[S, U](
      func: MapGroupsWithStateFunction[K, V, S, U],
      stateEncoder: Encoder[S],
      outputEncoder: Encoder[U],
      timeoutConf: GroupStateTimeout,
      initialState: KeyValueGroupedDataset[K, S]): Dataset[U] =
    super.mapGroupsWithState(func, stateEncoder, outputEncoder, timeoutConf, initialState)

  /** @inheritdoc */
  override def flatMapGroupsWithState[S, U](
      func: FlatMapGroupsWithStateFunction[K, V, S, U],
      outputMode: OutputMode,
      stateEncoder: Encoder[S],
      outputEncoder: Encoder[U],
      timeoutConf: GroupStateTimeout): Dataset[U] =
    super.flatMapGroupsWithState(func, outputMode, stateEncoder, outputEncoder, timeoutConf)

  /** @inheritdoc */
  override def flatMapGroupsWithState[S, U](
      func: FlatMapGroupsWithStateFunction[K, V, S, U],
      outputMode: OutputMode,
      stateEncoder: Encoder[S],
      outputEncoder: Encoder[U],
      timeoutConf: GroupStateTimeout,
      initialState: KeyValueGroupedDataset[K, S]): Dataset[U] = super.flatMapGroupsWithState(
    func,
    outputMode,
    stateEncoder,
    outputEncoder,
    timeoutConf,
    initialState)

  /** @inheritdoc */
  override private[sql] def transformWithState[U: Encoder](
      statefulProcessor: StatefulProcessor[K, V, U],
      timeMode: TimeMode,
      outputMode: OutputMode,
      outputEncoder: Encoder[U]) =
    super.transformWithState(statefulProcessor, timeMode, outputMode, outputEncoder)

  /** @inheritdoc */
  override private[sql] def transformWithState[U: Encoder](
      statefulProcessor: StatefulProcessor[K, V, U],
      eventTimeColumnName: String,
      outputMode: OutputMode,
      outputEncoder: Encoder[U]) =
    super.transformWithState(statefulProcessor, eventTimeColumnName, outputMode, outputEncoder)

  /** @inheritdoc */
  override private[sql] def transformWithState[U: Encoder, S: Encoder](
      statefulProcessor: StatefulProcessorWithInitialState[K, V, U, S],
      timeMode: TimeMode,
      outputMode: OutputMode,
      initialState: KeyValueGroupedDataset[K, S],
      outputEncoder: Encoder[U],
      initialStateEncoder: Encoder[S]) = super.transformWithState(
    statefulProcessor,
    timeMode,
    outputMode,
    initialState,
    outputEncoder,
    initialStateEncoder)

  /** @inheritdoc */
  override private[sql] def transformWithState[U: Encoder, S: Encoder](
      statefulProcessor: StatefulProcessorWithInitialState[K, V, U, S],
      outputMode: OutputMode,
      initialState: KeyValueGroupedDataset[K, S],
      eventTimeColumnName: String,
      outputEncoder: Encoder[U],
      initialStateEncoder: Encoder[S]) = super.transformWithState(
    statefulProcessor,
    outputMode,
    initialState,
    eventTimeColumnName,
    outputEncoder,
    initialStateEncoder)

  /** @inheritdoc */
  override def reduceGroups(f: ReduceFunction[V]): Dataset[(K, V)] = super.reduceGroups(f)

  /** @inheritdoc */
  override def agg[U1](col1: TypedColumn[V, U1]): Dataset[(K, U1)] = super.agg(col1)

  /** @inheritdoc */
  override def agg[U1, U2](
      col1: TypedColumn[V, U1],
      col2: TypedColumn[V, U2]): Dataset[(K, U1, U2)] = super.agg(col1, col2)

  /** @inheritdoc */
  override def agg[U1, U2, U3](
      col1: TypedColumn[V, U1],
      col2: TypedColumn[V, U2],
      col3: TypedColumn[V, U3]): Dataset[(K, U1, U2, U3)] = super.agg(col1, col2, col3)

  /** @inheritdoc */
  override def agg[U1, U2, U3, U4](
      col1: TypedColumn[V, U1],
      col2: TypedColumn[V, U2],
      col3: TypedColumn[V, U3],
      col4: TypedColumn[V, U4]): Dataset[(K, U1, U2, U3, U4)] = super.agg(col1, col2, col3, col4)

  /** @inheritdoc */
  override def agg[U1, U2, U3, U4, U5](
      col1: TypedColumn[V, U1],
      col2: TypedColumn[V, U2],
      col3: TypedColumn[V, U3],
      col4: TypedColumn[V, U4],
      col5: TypedColumn[V, U5]): Dataset[(K, U1, U2, U3, U4, U5)] =
    super.agg(col1, col2, col3, col4, col5)

  /** @inheritdoc */
  override def agg[U1, U2, U3, U4, U5, U6](
      col1: TypedColumn[V, U1],
      col2: TypedColumn[V, U2],
      col3: TypedColumn[V, U3],
      col4: TypedColumn[V, U4],
      col5: TypedColumn[V, U5],
      col6: TypedColumn[V, U6]): Dataset[(K, U1, U2, U3, U4, U5, U6)] =
    super.agg(col1, col2, col3, col4, col5, col6)

  /** @inheritdoc */
  override def agg[U1, U2, U3, U4, U5, U6, U7](
      col1: TypedColumn[V, U1],
      col2: TypedColumn[V, U2],
      col3: TypedColumn[V, U3],
      col4: TypedColumn[V, U4],
      col5: TypedColumn[V, U5],
      col6: TypedColumn[V, U6],
      col7: TypedColumn[V, U7]): Dataset[(K, U1, U2, U3, U4, U5, U6, U7)] =
    super.agg(col1, col2, col3, col4, col5, col6, col7)

  /** @inheritdoc */
  override def agg[U1, U2, U3, U4, U5, U6, U7, U8](
      col1: TypedColumn[V, U1],
      col2: TypedColumn[V, U2],
      col3: TypedColumn[V, U3],
      col4: TypedColumn[V, U4],
      col5: TypedColumn[V, U5],
      col6: TypedColumn[V, U6],
      col7: TypedColumn[V, U7],
      col8: TypedColumn[V, U8]): Dataset[(K, U1, U2, U3, U4, U5, U6, U7, U8)] =
    super.agg(col1, col2, col3, col4, col5, col6, col7, col8)

  /** @inheritdoc */
  override def count(): Dataset[(K, Long)] = super.count()

  /** @inheritdoc */
  override def cogroup[U, R: Encoder](other: KeyValueGroupedDataset[K, U])(
      f: (K, Iterator[V], Iterator[U]) => IterableOnce[R]): Dataset[R] =
    super.cogroup(other)(f)

  /** @inheritdoc */
  override def cogroup[U, R](
      other: KeyValueGroupedDataset[K, U],
      f: CoGroupFunction[K, V, U, R],
      encoder: Encoder[R]): Dataset[R] = super.cogroup(other, f, encoder)

  /** @inheritdoc */
  override def cogroupSorted[U, R](
      other: KeyValueGroupedDataset[K, U],
      thisSortExprs: Array[Column],
      otherSortExprs: Array[Column],
      f: CoGroupFunction[K, V, U, R],
      encoder: Encoder[R]): Dataset[R] =
    super.cogroupSorted(other, thisSortExprs, otherSortExprs, f, encoder)
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
    private val kEncoder: AgnosticEncoder[K],
    private val ivEncoder: AgnosticEncoder[IV],
    private val vEncoder: AgnosticEncoder[V],
    private val groupingExprs: java.util.List[proto.Expression],
    private val valueMapFunc: Option[IV => V],
    private val keysFunc: () => Dataset[IK])
    extends KeyValueGroupedDataset[K, V] {
  import sparkSession.RichColumn

  override def keyAs[L: Encoder]: KeyValueGroupedDataset[L, V] = {
    new KeyValueGroupedDatasetImpl[L, V, IK, IV](
      sparkSession,
      plan,
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
      kEncoder,
      ivEncoder,
      encoderFor[W],
      groupingExprs,
      valueMapFunc
        .map(_.andThen(valueFunc))
        .orElse(Option(valueFunc.asInstanceOf[IV => W])),
      keysFunc)
  }

  override def keys: Dataset[K] = {
    keysFunc()
      .dropDuplicates()
      .as(kEncoder)
  }

  override def flatMapSortedGroups[U: Encoder](sortExprs: Column*)(
      f: (K, Iterator[V]) => IterableOnce[U]): Dataset[U] = {
    // Apply mapValues changes to the udf
    val nf = UDFAdaptors.flatMapGroupsWithMappedValues(f, valueMapFunc)
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
      f: (K, Iterator[V], Iterator[U]) => IterableOnce[R]): Dataset[R] = {
    val otherImpl = other.asInstanceOf[KeyValueGroupedDatasetImpl[K, U, _, Any]]
    // Apply mapValues changes to the udf
    val nf = UDFAdaptors.coGroupWithMappedValues(f, valueMapFunc, otherImpl.valueMapFunc)
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
    // TODO(SPARK-43415): For each column, apply the valueMap func first...
    val rEnc = ProductEncoder.tuple(kEncoder +: columns.map(c => encoderFor(c.encoder)))
    sparkSession.newDataset(rEnc) { builder =>
      builder.getAggregateBuilder
        .setInput(plan.getRoot)
        .setGroupType(proto.Aggregate.GroupType.GROUP_TYPE_GROUPBY)
        .addAllGroupingExpressions(groupingExprs)
        .addAllAggregateExpressions(columns.map(_.typedExpr(vEncoder)).asJava)
    }
  }

  override def reduceGroups(f: (V, V) => V): Dataset[(K, V)] = {
    val inputEncoders = Seq(vEncoder, vEncoder)
    val udf = SparkUserDefinedFunction(
      function = f,
      inputEncoders = inputEncoders,
      outputEncoder = vEncoder)
    val input = udf.apply(inputEncoders.map(_ => col("*")): _*)
    val expr = Column.fn("reduce", input)
    val aggregator: TypedColumn[V, V] = new TypedColumn[V, V](expr.node, vEncoder)
    agg(aggregator)
  }

  override protected[sql] def flatMapGroupsWithStateHelper[S: Encoder, U: Encoder](
      outputMode: Option[OutputMode],
      timeoutConf: GroupStateTimeout,
      initialState: Option[KeyValueGroupedDataset[K, S]],
      isMapGroupWithState: Boolean)(
      func: (K, Iterator[V], GroupState[S]) => Iterator[U]): Dataset[U] = {
    if (outputMode.isDefined && outputMode.get != OutputMode.Append &&
      outputMode.get != OutputMode.Update) {
      throw new IllegalArgumentException("The output mode of function should be append or update")
    }

    val initialStateImpl = if (initialState.isDefined) {
      assert(initialState.get.isInstanceOf[KeyValueGroupedDatasetImpl[K, S, _, _]])
      initialState.get.asInstanceOf[KeyValueGroupedDatasetImpl[K, S, _, _]]
    } else {
      null
    }

    val outputEncoder = encoderFor[U]
    val nf = UDFAdaptors.flatMapGroupsWithStateWithMappedValues(func, valueMapFunc)

    sparkSession.newDataset[U](outputEncoder) { builder =>
      val groupMapBuilder = builder.getGroupMapBuilder
      groupMapBuilder
        .setInput(plan.getRoot)
        .addAllGroupingExpressions(groupingExprs)
        .setFunc(getUdf(nf, outputEncoder)(ivEncoder))
        .setIsMapGroupsWithState(isMapGroupWithState)
        .setOutputMode(if (outputMode.isEmpty) OutputMode.Update.toString
        else outputMode.get.toString)
        .setTimeoutConf(timeoutConf.toString)

      if (initialStateImpl != null) {
        groupMapBuilder
          .addAllInitialGroupingExpressions(initialStateImpl.groupingExprs)
          .setInitialInput(initialStateImpl.plan.getRoot)
      }
    }
  }

  private def getUdf[U: Encoder](nf: AnyRef, outputEncoder: AgnosticEncoder[U])(
      inEncoders: AgnosticEncoder[_]*): proto.CommonInlineUserDefinedFunction = {
    val inputEncoders = kEncoder +: inEncoders // Apply keyAs changes by setting kEncoder
    val udf = SparkUserDefinedFunction(
      function = nf,
      inputEncoders = inputEncoders,
      outputEncoder = outputEncoder)
    udf.apply(inputEncoders.map(_ => col("*")): _*).expr.getCommonInlineUserDefinedFunction
  }

  /**
   * We cannot deserialize a connect [[KeyValueGroupedDataset]] because of a class clash on the
   * server side. We null out the instance for now.
   */
  @unused("this is used by java serialization")
  private def writeReplace(): Any = null
}

private object KeyValueGroupedDatasetImpl {
  def apply[K, V](
      ds: Dataset[V],
      kEncoder: AgnosticEncoder[K],
      groupingFunc: V => K): KeyValueGroupedDatasetImpl[K, V, K, V] = {
    val gf = SparkUserDefinedFunction(
      function = groupingFunc,
      inputEncoders = ds.agnosticEncoder :: Nil, // Using the original value and key encoders
      outputEncoder = kEncoder)
    val session = ds.sparkSession
    new KeyValueGroupedDatasetImpl(
      session,
      ds.plan,
      kEncoder,
      ds.agnosticEncoder,
      ds.agnosticEncoder,
      Arrays.asList(toExpr(gf.apply(col("*")))),
      None,
      () => ds.map(groupingFunc)(kEncoder))
  }

  def apply[K, V](
      df: DataFrame,
      kEncoder: AgnosticEncoder[K],
      vEncoder: AgnosticEncoder[V],
      groupingExprs: Seq[Column]): KeyValueGroupedDatasetImpl[K, V, K, V] = {
    // Use a dummy udf to pass the K V encoders
    val dummyGroupingFunc = SparkUserDefinedFunction(
      function = UdfUtils.noOp[V, K](),
      inputEncoders = vEncoder :: Nil,
      outputEncoder = kEncoder).apply(col("*"))
    val session = df.sparkSession
    new KeyValueGroupedDatasetImpl(
      session,
      df.plan,
      kEncoder,
      vEncoder,
      vEncoder,
      (Seq(dummyGroupingFunc) ++ groupingExprs).map(toExpr).asJava,
      None,
      () => df.select(groupingExprs: _*).as(kEncoder))
  }
}
