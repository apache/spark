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

package org.apache.spark.sql.connect

import java.util.{List => JList}

import scala.annotation.unused
import scala.jdk.CollectionConverters._

import org.apache.spark.api.java.function._
import org.apache.spark.connect.proto
import org.apache.spark.sql
import org.apache.spark.sql.{Column, Encoder, TypedColumn}
import org.apache.spark.sql.catalyst.encoders.AgnosticEncoder
import org.apache.spark.sql.catalyst.encoders.AgnosticEncoders.{agnosticEncoderFor, ProductEncoder}
import org.apache.spark.sql.connect.ColumnNodeToProtoConverter.{toExpr, toTypedExpr}
import org.apache.spark.sql.connect.ColumnUtils._
import org.apache.spark.sql.connect.ConnectConversions._
import org.apache.spark.sql.connect.KeyValueGroupedDatasetImpl.Grouping
import org.apache.spark.sql.connect.common.{DataTypeProtoConverter, UdfUtils}
import org.apache.spark.sql.expressions.{ReduceAggregator, SparkUserDefinedFunction}
import org.apache.spark.sql.functions.{col, lit, struct}
import org.apache.spark.sql.internal.UDFAdaptors
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, OutputMode, StatefulProcessor, StatefulProcessorWithInitialState, TimeMode}

/**
 * A [[Dataset]] has been logically grouped by a user specified grouping key. Users should not
 * construct a [[KeyValueGroupedDataset]] directly, but should instead call `groupByKey` on an
 * existing [[Dataset]].
 *
 * @since 3.5.0
 */
class KeyValueGroupedDataset[K, V] private[sql] () extends sql.KeyValueGroupedDataset[K, V] {

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
  def cogroupSorted[U, R: Encoder](other: sql.KeyValueGroupedDataset[K, U])(
      thisSortExprs: Column*)(otherSortExprs: Column*)(
      f: (K, Iterator[V], Iterator[U]) => IterableOnce[R]): Dataset[R] =
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
      initialState: sql.KeyValueGroupedDataset[K, S])(
      func: (K, Iterator[V], GroupState[S]) => U): Dataset[U] = {
    flatMapGroupsWithStateHelper(
      None,
      timeoutConf,
      Some(castToImpl(initialState)),
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
      initialState: sql.KeyValueGroupedDataset[K, S])(
      func: (K, Iterator[V], GroupState[S]) => Iterator[U]): Dataset[U] = {
    flatMapGroupsWithStateHelper(
      Some(outputMode),
      timeoutConf,
      Some(castToImpl(initialState)),
      isMapGroupWithState = false)(func)
  }

  /** @inheritdoc */
  def transformWithState[U: Encoder](
      statefulProcessor: StatefulProcessor[K, V, U],
      timeMode: TimeMode,
      outputMode: OutputMode): Dataset[U] =
    transformWithStateHelper(statefulProcessor, timeMode, outputMode)

  /** @inheritdoc */
  def transformWithState[U: Encoder, S: Encoder](
      statefulProcessor: StatefulProcessorWithInitialState[K, V, U, S],
      timeMode: TimeMode,
      outputMode: OutputMode,
      initialState: sql.KeyValueGroupedDataset[K, S]): Dataset[U] =
    transformWithStateHelper(statefulProcessor, timeMode, outputMode, Some(initialState))

  /** @inheritdoc */
  override def transformWithState[U: Encoder](
      statefulProcessor: StatefulProcessor[K, V, U],
      eventTimeColumnName: String,
      outputMode: OutputMode): Dataset[U] =
    transformWithStateHelper(
      statefulProcessor,
      TimeMode.EventTime(),
      outputMode,
      eventTimeColumnName = eventTimeColumnName)

  /** @inheritdoc */
  override def transformWithState[U: Encoder, S: Encoder](
      statefulProcessor: StatefulProcessorWithInitialState[K, V, U, S],
      eventTimeColumnName: String,
      outputMode: OutputMode,
      initialState: sql.KeyValueGroupedDataset[K, S]): Dataset[U] =
    transformWithStateHelper(
      statefulProcessor,
      TimeMode.EventTime(),
      outputMode,
      Some(initialState),
      eventTimeColumnName)

  // This is an interface, and it should not be used. The real implementation is in the
  // inherited class.
  protected[sql] def transformWithStateHelper[U: Encoder, S: Encoder](
      statefulProcessor: StatefulProcessor[K, V, U],
      timeMode: TimeMode,
      outputMode: OutputMode,
      initialState: Option[sql.KeyValueGroupedDataset[K, S]] = None,
      eventTimeColumnName: String = ""): Dataset[U] = unsupported()

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
      initialState: sql.KeyValueGroupedDataset[K, S]): Dataset[U] =
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
      initialState: sql.KeyValueGroupedDataset[K, S]): Dataset[U] = super.flatMapGroupsWithState(
    func,
    outputMode,
    stateEncoder,
    outputEncoder,
    timeoutConf,
    initialState)

  /** @inheritdoc */
  override def transformWithState[U: Encoder](
      statefulProcessor: StatefulProcessor[K, V, U],
      timeMode: TimeMode,
      outputMode: OutputMode,
      outputEncoder: Encoder[U]): Dataset[U] =
    super.transformWithState(statefulProcessor, timeMode, outputMode, outputEncoder)

  /** @inheritdoc */
  override def transformWithState[U: Encoder](
      statefulProcessor: StatefulProcessor[K, V, U],
      eventTimeColumnName: String,
      outputMode: OutputMode,
      outputEncoder: Encoder[U]): Dataset[U] =
    super.transformWithState(statefulProcessor, eventTimeColumnName, outputMode, outputEncoder)

  /** @inheritdoc */
  override def transformWithState[U: Encoder, S: Encoder](
      statefulProcessor: StatefulProcessorWithInitialState[K, V, U, S],
      timeMode: TimeMode,
      outputMode: OutputMode,
      initialState: sql.KeyValueGroupedDataset[K, S],
      outputEncoder: Encoder[U],
      initialStateEncoder: Encoder[S]): Dataset[U] = super.transformWithState(
    statefulProcessor,
    timeMode,
    outputMode,
    initialState,
    outputEncoder,
    initialStateEncoder)

  /** @inheritdoc */
  override def transformWithState[U: Encoder, S: Encoder](
      statefulProcessor: StatefulProcessorWithInitialState[K, V, U, S],
      outputMode: OutputMode,
      initialState: sql.KeyValueGroupedDataset[K, S],
      eventTimeColumnName: String,
      outputEncoder: Encoder[U],
      initialStateEncoder: Encoder[S]): Dataset[U] = super.transformWithState(
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
  override def cogroup[U, R: Encoder](other: sql.KeyValueGroupedDataset[K, U])(
      f: (K, Iterator[V], Iterator[U]) => IterableOnce[R]): Dataset[R] =
    super.cogroup(other)(f)

  /** @inheritdoc */
  override def cogroup[U, R](
      other: sql.KeyValueGroupedDataset[K, U],
      f: CoGroupFunction[K, V, U, R],
      encoder: Encoder[R]): Dataset[R] = super.cogroup(other, f, encoder)

  /** @inheritdoc */
  override def cogroupSorted[U, R](
      other: sql.KeyValueGroupedDataset[K, U],
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
private class KeyValueGroupedDatasetImpl[K, V, IV](
    private val sparkSession: SparkSession,
    private val plan: proto.Plan,
    private val grouping: Grouping[K, IV],
    private val ivEncoder: AgnosticEncoder[IV],
    private val vEncoder: AgnosticEncoder[V],
    private val valueMapFunc: Option[IV => V])
    extends KeyValueGroupedDataset[K, V] {

  private lazy val ds: Dataset[IV] = sparkSession.newDataset(ivEncoder, plan)

  override def keyAs[L: Encoder]: KeyValueGroupedDataset[L, V] = {
    new KeyValueGroupedDatasetImpl[L, V, IV](
      sparkSession,
      plan,
      grouping.as[L],
      ivEncoder,
      vEncoder,
      valueMapFunc)
  }

  override def mapValues[W: Encoder](valueFunc: V => W): KeyValueGroupedDataset[K, W] = {
    new KeyValueGroupedDatasetImpl[K, W, IV](
      sparkSession,
      plan,
      grouping,
      ivEncoder,
      agnosticEncoderFor[W],
      valueMapFunc
        .map(_.andThen(valueFunc))
        .orElse(Option(valueFunc.asInstanceOf[IV => W])))
  }

  override def keys: Dataset[K] = grouping.keys(ds).dropDuplicates()

  override def flatMapSortedGroups[U: Encoder](sortExprs: Column*)(
      f: (K, Iterator[V]) => IterableOnce[U]): Dataset[U] = {
    // Apply mapValues changes to the udf
    val nf = UDFAdaptors.flatMapGroupsWithMappedValues(f, valueMapFunc)
    val outputEncoder = agnosticEncoderFor[U]
    sparkSession.newDataset[U](outputEncoder) { builder =>
      builder.getGroupMapBuilder
        .setInput(plan.getRoot)
        .addAllSortingExpressions(sortExprs.map(toExpr).asJava)
        .addAllGroupingExpressions(grouping.exprs)
        .setFunc(getUdf(nf, outputEncoder, ivEncoder))
    }
  }

  override def cogroupSorted[U, R: Encoder](other: sql.KeyValueGroupedDataset[K, U])(
      thisSortExprs: Column*)(otherSortExprs: Column*)(
      f: (K, Iterator[V], Iterator[U]) => IterableOnce[R]): Dataset[R] = {
    val otherImpl = other.asInstanceOf[KeyValueGroupedDatasetImpl[K, U, Any]]
    // Apply mapValues changes to the udf
    val nf = UDFAdaptors.coGroupWithMappedValues(f, valueMapFunc, otherImpl.valueMapFunc)
    val outputEncoder = agnosticEncoderFor[R]
    sparkSession.newDataset[R](outputEncoder) { builder =>
      builder.getCoGroupMapBuilder
        .setInput(plan.getRoot)
        .addAllInputGroupingExpressions(grouping.exprs)
        .addAllInputSortingExpressions(thisSortExprs.map(toExpr).asJava)
        .setOther(otherImpl.plan.getRoot)
        .addAllOtherGroupingExpressions(otherImpl.grouping.exprs)
        .addAllOtherSortingExpressions(otherSortExprs.map(toExpr).asJava)
        .setFunc(getUdf(nf, outputEncoder, ivEncoder, otherImpl.ivEncoder))
    }
  }

  override protected def aggUntyped(columns: TypedColumn[_, _]*): Dataset[_] = {
    // The proto Aggregate message does not support passing in a value map function, so we need to
    // do transformation on the client side. We check if a value map function is defined and only
    // if so we do some additional transformations.
    val (plan, key) = if (valueMapFunc.isDefined) {
      prepareAggWithMapValues()
    } else {
      prepareAggWithoutMapValues()
    }
    val rEnc =
      ProductEncoder.tuple(grouping.encoder +: columns.map(c => agnosticEncoderFor(c.encoder)))
    sparkSession.newDataset(rEnc) { builder =>
      builder.getAggregateBuilder
        .setInput(plan.getRoot)
        .setGroupType(proto.Aggregate.GroupType.GROUP_TYPE_GROUPBY)
        .addGroupingExpressions(toExpr(key.asRegularColumn("key")))
        .addAllAggregateExpressions(columns.map(c => toTypedExpr(c, vEncoder)).asJava)
    }
  }

  private def prepareAggWithoutMapValues(): (proto.Plan, Column) = {
    // Append the grouping key to the end of the Dataset. We mark the key column as a metadata
    // column to avoid name collisions with the input Dataset. This step is needed because Spark
    // currently does not support star expansion on Aggregate Grouping Expressions.
    val dsWithKey = sparkSession
      .newDataset(ivEncoder, plan)
      .appendColumn(grouping.aggregateColumn(ds).asMetadataColumn("key"))
    (dsWithKey.plan, dsWithKey.metadataColumn("key"))
  }

  private def prepareAggWithMapValues(): (proto.Plan, Column) = {
    // Create a Dataset in the form of (value, key) where value is the transformed value. We mark
    // the key column as a metadata column to avoid naming conflicts if the mapValueFunction
    // returns a column with the same name.
    val valueMapUdf = SparkUserDefinedFunction(valueMapFunc.get, ivEncoder :: Nil, vEncoder)
      .withName("mapValues")
    val valueKeyDs =
      ds.select(valueMapUdf(ds).as("value"), grouping.aggregateColumn(ds).asMetadataColumn("key"))
    val keyRef = valueKeyDs.metadataColumn("key")

    // Expand the value column if the mapValue function returns a struct. This is needed to make
    // sure the value encoder can bind to the dataset without any modifications to the typed
    // columns. The reason for this is that tuple and value encoders use ordinal based binding
    // instead name base binding. The key is appended at the end.
    val valuesKeyDs = if (valueMapUdf.canFlattenResult) {
      valueKeyDs.select(valueMapUdf.flattenResult("value"), keyRef)
    } else {
      valueKeyDs
    }
    (valuesKeyDs.plan, keyRef)
  }

  override def reduceGroups(f: (V, V) => V): Dataset[(K, V)] = {
    val r = ReduceAggregator(f)(vEncoder)
    agg(r.toColumn)
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
      initialState.get.asInstanceOf[KeyValueGroupedDatasetImpl[K, S, _]]
    } else {
      null
    }

    val outputEncoder = agnosticEncoderFor[U]
    val stateEncoder = agnosticEncoderFor[S]
    val nf = UDFAdaptors.flatMapGroupsWithStateWithMappedValues(func, valueMapFunc)

    sparkSession.newDataset[U](outputEncoder) { builder =>
      val groupMapBuilder = builder.getGroupMapBuilder
      groupMapBuilder
        .setInput(plan.getRoot)
        .addAllGroupingExpressions(grouping.exprs)
        .setFunc(getUdf(nf, outputEncoder, stateEncoder, ivEncoder))
        .setIsMapGroupsWithState(isMapGroupWithState)
        .setOutputMode(if (outputMode.isEmpty) OutputMode.Update.toString
        else outputMode.get.toString)
        .setTimeoutConf(timeoutConf.toString)
        .setStateSchema(DataTypeProtoConverter.toConnectProtoType(stateEncoder.schema))

      if (initialStateImpl != null) {
        groupMapBuilder
          .addAllInitialGroupingExpressions(initialStateImpl.grouping.exprs)
          .setInitialInput(initialStateImpl.plan.getRoot)
      }
    }
  }

  override protected[sql] def transformWithStateHelper[U: Encoder, S: Encoder](
      statefulProcessor: StatefulProcessor[K, V, U],
      timeMode: TimeMode,
      outputMode: OutputMode,
      initialState: Option[sql.KeyValueGroupedDataset[K, S]] = None,
      eventTimeColumnName: String = ""): Dataset[U] = {
    val outputEncoder = agnosticEncoderFor[U]
    val stateEncoder = agnosticEncoderFor[S]
    val inputEncoders: Seq[AgnosticEncoder[_]] = Seq(kEncoder, stateEncoder, ivEncoder)

    // SparkUserDefinedFunction is creating a udfPacket where the input function are
    // being java serialized into bytes; we pass in `statefulProcessor` as function so it can be
    // serialized into bytes and deserialized back on connect server
    val sparkUserDefinedFunc =
      SparkUserDefinedFunction(statefulProcessor, inputEncoders, outputEncoder)
    val funcProto = UdfToProtoUtils.toProto(sparkUserDefinedFunc)

    val initialStateImpl = if (initialState.isDefined) {
      initialState.get.asInstanceOf[KeyValueGroupedDatasetImpl[K, S, _, _]]
    } else {
      null
    }

    sparkSession.newDataset[U](outputEncoder) { builder =>
      val twsBuilder = builder.getGroupMapBuilder
      val twsInfoBuilder = proto.TransformWithStateInfo.newBuilder()
      if (!eventTimeColumnName.isEmpty) {
        twsInfoBuilder.setEventTimeColumnName(eventTimeColumnName)
      }
      twsBuilder
        .setInput(plan.getRoot)
        .addAllGroupingExpressions(groupingExprs)
        .setFunc(funcProto)
        .setOutputMode(outputMode.toString)
        .setTransformWithStateInfo(
          twsInfoBuilder
            // we pass time mode as string here and deterministically restored on server
            .setTimeMode(timeMode.toString)
            .build())
      if (initialStateImpl != null) {
        twsBuilder
          .addAllInitialGroupingExpressions(initialStateImpl.groupingExprs)
          .setInitialInput(initialStateImpl.plan.getRoot)
      }
    }
  }

  private def getUdf[U: Encoder](
      nf: AnyRef,
      outputEncoder: AgnosticEncoder[U],
      inEncoders: AgnosticEncoder[_]*): proto.CommonInlineUserDefinedFunction = {
    val inputEncoders = grouping.encoder +: inEncoders // Apply keyAs changes by setting kEncoder
    val udf = SparkUserDefinedFunction(
      function = nf,
      inputEncoders = inputEncoders,
      outputEncoder = outputEncoder)
    toExpr(udf.applyToAll()).getCommonInlineUserDefinedFunction
  }

  /**
   * We cannot deserialize a connect [[KeyValueGroupedDataset]] because of a class clash on the
   * server side. We null out the instance for now.
   */
  @unused("this is used by java serialization")
  private def writeReplace(): Any = null
}

private object KeyValueGroupedDatasetImpl {
  private[connect] val firstCol = Column.internalFn("get_column_by_ordinal", lit(0))

  private def apply[K, V](
      ds: Dataset[_],
      grouping: Grouping[K, V],
      vEncoder: AgnosticEncoder[V]): KeyValueGroupedDatasetImpl[K, V, V] = {
    new KeyValueGroupedDatasetImpl(ds.sparkSession, ds.plan, grouping, vEncoder, vEncoder, None)
  }

  def apply[K, V](
      ds: Dataset[V],
      kEncoder: AgnosticEncoder[K],
      groupingFunc: V => K): KeyValueGroupedDatasetImpl[K, V, V] = {
    val vEncoder = ds.agnosticEncoder
    val udf = SparkUserDefinedFunction(groupingFunc, vEncoder :: Nil, kEncoder)
      .withName("groupingFun")
    val grouping = KeyFunctionGrouping(groupingFunc, udf, kEncoder, kEncoder)
    apply(ds, grouping, vEncoder)
  }

  def apply[K, V](
      df: DataFrame,
      kEncoder: AgnosticEncoder[K],
      vEncoder: AgnosticEncoder[V],
      groupingExprs: Seq[Column]): KeyValueGroupedDatasetImpl[K, V, V] = {
    // We use a dummy UDF to pass the key encoder to the SparkConnectPlanner. This is not really
    // needed because the only time we need this encoder is when we already pass a function that
    // contains the proper encoder.
    val udf = SparkUserDefinedFunction(UdfUtils.identical(), vEncoder :: Nil, kEncoder)
    val grouping = RelationalGrouping[K, V](groupingExprs, udf(all), kEncoder)
    apply(df, grouping, vEncoder)
  }

  abstract class Grouping[K, V] {
    val encoder: AgnosticEncoder[K]
    protected def keyColumns: Seq[Column]
    lazy val exprs: JList[proto.Expression] = keyColumns.map(toExpr).asJava
    def aggregateColumn(ds: Dataset[V]): Column
    def keys(ds: Dataset[V]): Dataset[K]
    def as[L: Encoder]: Grouping[L, V]
  }

  case class KeyFunctionGrouping[IK, K, V](
      f: V => IK,
      udf: SparkUserDefinedFunction,
      initialEncoder: AgnosticEncoder[IK],
      encoder: AgnosticEncoder[K])
      extends Grouping[K, V] {
    override lazy val keyColumns: Seq[Column] = Seq(udf(col("*")))
    override def aggregateColumn(ds: Dataset[V]): Column = udf(ds)
    override def keys(ds: Dataset[V]): Dataset[K] = ds.map(f)(initialEncoder).as(encoder)
    override def as[L: Encoder]: KeyFunctionGrouping[IK, L, V] =
      copy(encoder = agnosticEncoderFor[L])
  }

  case class RelationalGrouping[K, V](
      columns: Seq[Column],
      dummy: Column,
      encoder: AgnosticEncoder[K])
      extends Grouping[K, V] {
    override def keyColumns: Seq[Column] = dummy +: columns
    override def aggregateColumn(ds: Dataset[V]): Column = columns match {
      case Seq(col) => col
      case _ => struct(columns: _*)
    }
    override def keys(ds: Dataset[V]): Dataset[K] = ds.select(columns: _*).as(encoder)
    override def as[L: Encoder]: RelationalGrouping[L, V] = copy(encoder = agnosticEncoderFor[L])
  }
}
