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

package org.apache.spark.sql.classic

import org.apache.spark.api.java.function._
import org.apache.spark.sql
import org.apache.spark.sql.{Column, Encoder, TypedColumn}
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.encoders.AgnosticEncoders.{agnosticEncoderFor, ProductEncoder}
import org.apache.spark.sql.catalyst.encoders.encoderFor
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.classic.ClassicConversions._
import org.apache.spark.sql.classic.TypedAggUtils.{aggKeyColumn, withInputType}
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.expressions.ReduceAggregator
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, OutputMode, StatefulProcessor, StatefulProcessorWithInitialState, TimeMode}

/**
 * A [[Dataset]] has been logically grouped by a user specified grouping key.  Users should not
 * construct a [[KeyValueGroupedDataset]] directly, but should instead call `groupByKey` on
 * an existing [[Dataset]].
 *
 * @since 2.0.0
 */
class KeyValueGroupedDataset[K, V] private[sql](
    kEncoder: Encoder[K],
    vEncoder: Encoder[V],
    @transient private[sql] val queryExecution: QueryExecution,
    private val dataAttributes: Seq[Attribute],
    private val groupingAttributes: Seq[Attribute])
  extends sql.KeyValueGroupedDataset[K, V] {

  private implicit def kEncoderImpl: Encoder[K] = kEncoder
  private implicit def vEncoderImpl: Encoder[V] = vEncoder

  private def logicalPlan = queryExecution.analyzed
  private def sparkSession = queryExecution.sparkSession
  import queryExecution.sparkSession.toRichColumn

  /** @inheritdoc */
  def keyAs[L : Encoder]: KeyValueGroupedDataset[L, V] =
    new KeyValueGroupedDataset(
      implicitly[Encoder[L]],
      vEncoder,
      queryExecution,
      dataAttributes,
      groupingAttributes)

  /** @inheritdoc */
  def mapValues[W : Encoder](func: V => W): KeyValueGroupedDataset[K, W] = {
    val withNewData = AppendColumns(func, dataAttributes, logicalPlan)
    val projected = Project(withNewData.newColumns ++ groupingAttributes, withNewData)
    val executed = sparkSession.sessionState.executePlan(projected)

    new KeyValueGroupedDataset(
      kEncoder,
      implicitly[Encoder[W]],
      executed,
      withNewData.newColumns,
      groupingAttributes)
  }

  /** @inheritdoc */
  def keys: Dataset[K] = {
    Dataset[K](
      sparkSession,
      Distinct(
        Project(groupingAttributes, logicalPlan)))
  }

  /** @inheritdoc */
  def flatMapSortedGroups[U : Encoder](
      sortExprs: Column*)(
      f: (K, Iterator[V]) => IterableOnce[U]): Dataset[U] = {
    Dataset[U](
      sparkSession,
      MapGroups(
        f,
        groupingAttributes,
        dataAttributes,
        MapGroups.sortOrder(sortExprs.map(_.expr)),
        logicalPlan
      )
    )
  }

  /** @inheritdoc */
  def mapGroupsWithState[S: Encoder, U: Encoder](
      func: (K, Iterator[V], GroupState[S]) => U): Dataset[U] = {
    val flatMapFunc = (key: K, it: Iterator[V], s: GroupState[S]) => Iterator(func(key, it, s))
    Dataset[U](
      sparkSession,
      FlatMapGroupsWithState[K, V, S, U](
        flatMapFunc.asInstanceOf[(Any, Iterator[Any], LogicalGroupState[Any]) => Iterator[Any]],
        groupingAttributes,
        dataAttributes,
        OutputMode.Update,
        isMapGroupsWithState = true,
        GroupStateTimeout.NoTimeout,
        child = logicalPlan))
  }

  /** @inheritdoc */
  def mapGroupsWithState[S: Encoder, U: Encoder](
      timeoutConf: GroupStateTimeout)(
      func: (K, Iterator[V], GroupState[S]) => U): Dataset[U] = {
    val flatMapFunc = (key: K, it: Iterator[V], s: GroupState[S]) => Iterator(func(key, it, s))
    Dataset[U](
      sparkSession,
      FlatMapGroupsWithState[K, V, S, U](
        flatMapFunc.asInstanceOf[(Any, Iterator[Any], LogicalGroupState[Any]) => Iterator[Any]],
        groupingAttributes,
        dataAttributes,
        OutputMode.Update,
        isMapGroupsWithState = true,
        timeoutConf,
        child = logicalPlan))
  }

  /** @inheritdoc */
  def mapGroupsWithState[S: Encoder, U: Encoder](
      timeoutConf: GroupStateTimeout,
      initialState: sql.KeyValueGroupedDataset[K, S])(
      func: (K, Iterator[V], GroupState[S]) => U): Dataset[U] = {
    val flatMapFunc = (key: K, it: Iterator[V], s: GroupState[S]) => Iterator(func(key, it, s))
    val initialStateImpl = castToImpl(initialState)

    Dataset[U](
      sparkSession,
      FlatMapGroupsWithState[K, V, S, U](
        flatMapFunc.asInstanceOf[(Any, Iterator[Any], LogicalGroupState[Any]) => Iterator[Any]],
        groupingAttributes,
        dataAttributes,
        OutputMode.Update,
        isMapGroupsWithState = true,
        timeoutConf,
        child = logicalPlan,
        initialStateImpl.groupingAttributes,
        initialStateImpl.dataAttributes,
        initialStateImpl.queryExecution.analyzed
      ))
  }

  /** @inheritdoc */
  def flatMapGroupsWithState[S: Encoder, U: Encoder](
      outputMode: OutputMode,
      timeoutConf: GroupStateTimeout)(
      func: (K, Iterator[V], GroupState[S]) => Iterator[U]): Dataset[U] = {
    if (outputMode != OutputMode.Append && outputMode != OutputMode.Update) {
      throw new IllegalArgumentException("The output mode of function should be append or update")
    }
    Dataset[U](
      sparkSession,
      FlatMapGroupsWithState[K, V, S, U](
        func.asInstanceOf[(Any, Iterator[Any], LogicalGroupState[Any]) => Iterator[Any]],
        groupingAttributes,
        dataAttributes,
        outputMode,
        isMapGroupsWithState = false,
        timeoutConf,
        child = logicalPlan))
  }

  /** @inheritdoc */
  def flatMapGroupsWithState[S: Encoder, U: Encoder](
      outputMode: OutputMode,
      timeoutConf: GroupStateTimeout,
      initialState: sql.KeyValueGroupedDataset[K, S])(
      func: (K, Iterator[V], GroupState[S]) => Iterator[U]): Dataset[U] = {
    if (outputMode != OutputMode.Append && outputMode != OutputMode.Update) {
      throw new IllegalArgumentException("The output mode of function should be append or update")
    }
    val initialStateImpl = castToImpl(initialState)
    Dataset[U](
      sparkSession,
      FlatMapGroupsWithState[K, V, S, U](
        func.asInstanceOf[(Any, Iterator[Any], LogicalGroupState[Any]) => Iterator[Any]],
        groupingAttributes,
        dataAttributes,
        outputMode,
        isMapGroupsWithState = false,
        timeoutConf,
        child = logicalPlan,
        initialStateImpl.groupingAttributes,
        initialStateImpl.dataAttributes,
        initialStateImpl.queryExecution.analyzed
      ))
  }

  /** @inheritdoc */
  def transformWithState[U: Encoder](
      statefulProcessor: StatefulProcessor[K, V, U],
      timeMode: TimeMode,
      outputMode: OutputMode): Dataset[U] = {
    Dataset[U](
      sparkSession,
      TransformWithState[K, V, U](
        groupingAttributes,
        dataAttributes,
        statefulProcessor,
        timeMode,
        outputMode,
        child = logicalPlan
      )
    )
  }

  /** @inheritdoc */
  def transformWithState[U: Encoder](
      statefulProcessor: StatefulProcessor[K, V, U],
      eventTimeColumnName: String,
      outputMode: OutputMode): Dataset[U] = {
    val transformWithState = TransformWithState[K, V, U](
      groupingAttributes,
      dataAttributes,
      statefulProcessor,
      TimeMode.EventTime(),
      outputMode,
      child = logicalPlan
    )
    updateEventTimeColumnAfterTransformWithState(transformWithState, eventTimeColumnName)
  }

  /** @inheritdoc */
  def transformWithState[U: Encoder, S: Encoder](
      statefulProcessor: StatefulProcessorWithInitialState[K, V, U, S],
      timeMode: TimeMode,
      outputMode: OutputMode,
      initialState: sql.KeyValueGroupedDataset[K, S]): Dataset[U] = {
    val initialStateImpl = castToImpl(initialState)
    Dataset[U](
      sparkSession,
      TransformWithState[K, V, U, S](
        groupingAttributes,
        dataAttributes,
        statefulProcessor,
        timeMode,
        outputMode,
        child = logicalPlan,
        initialStateImpl.groupingAttributes,
        initialStateImpl.dataAttributes,
        initialStateImpl.queryExecution.analyzed
      )
    )
  }

  /** @inheritdoc */
  def transformWithState[U: Encoder, S: Encoder](
      statefulProcessor: StatefulProcessorWithInitialState[K, V, U, S],
      eventTimeColumnName: String,
      outputMode: OutputMode,
      initialState: sql.KeyValueGroupedDataset[K, S]): Dataset[U] = {
    val initialStateImpl = castToImpl(initialState)
    val transformWithState = TransformWithState[K, V, U, S](
      groupingAttributes,
      dataAttributes,
      statefulProcessor,
      TimeMode.EventTime(),
      outputMode,
      child = logicalPlan,
      initialStateImpl.groupingAttributes,
      initialStateImpl.dataAttributes,
      initialStateImpl.queryExecution.analyzed
    )

    updateEventTimeColumnAfterTransformWithState(transformWithState, eventTimeColumnName)
  }

  /**
   * Creates a new dataset with updated eventTimeColumn after the transformWithState
   * logical node.
   */
  private def updateEventTimeColumnAfterTransformWithState[U: Encoder](
      transformWithState: LogicalPlan,
      eventTimeColumnName: String): Dataset[U] = {
    val transformWithStateDataset = Dataset[U](
      sparkSession,
      transformWithState
    )

    Dataset[U](sparkSession,
      UpdateEventTimeWatermarkColumn(
        UnresolvedAttribute(eventTimeColumnName),
        None,
        transformWithStateDataset.logicalPlan))
  }

  /** @inheritdoc */
  def reduceGroups(f: (V, V) => V): Dataset[(K, V)] = {
    val aggregator: TypedColumn[V, V] = new ReduceAggregator[V](f)(vEncoder).toColumn
    agg(aggregator)
  }

  /** @inheritdoc */
  protected def aggUntyped(columns: TypedColumn[_, _]*): Dataset[_] = {
    val keyAgEncoder = agnosticEncoderFor(kEncoder)
    val valueExprEncoder = encoderFor(vEncoder)
    val encoders = columns.map(c => agnosticEncoderFor(c.encoder))
    val namedColumns = columns.map { c =>
      withInputType(c.named, valueExprEncoder, dataAttributes)
    }
    val keyColumn = aggKeyColumn(keyAgEncoder, groupingAttributes)
    val aggregate = Aggregate(groupingAttributes, keyColumn +: namedColumns, logicalPlan)
    new Dataset(sparkSession, aggregate, ProductEncoder.tuple(keyAgEncoder +: encoders))
  }

  /** @inheritdoc */
  def cogroupSorted[U, R : Encoder](
      other: sql.KeyValueGroupedDataset[K, U])(
      thisSortExprs: Column*)(
      otherSortExprs: Column*)(
      f: (K, Iterator[V], Iterator[U]) => IterableOnce[R]): Dataset[R] = {
    val otherImpl = castToImpl(other)
    implicit val uEncoder: Encoder[U] = otherImpl.vEncoderImpl
    Dataset[R](
      sparkSession,
      CoGroup(
        f,
        Seq(this.groupingAttributes, otherImpl.groupingAttributes),
        Seq(this.dataAttributes, otherImpl.dataAttributes),
        Seq(
          MapGroups.sortOrder(thisSortExprs.map(_.expr)),
          MapGroups.sortOrder(otherSortExprs.map(_.expr))),
        Seq(this.logicalPlan, otherImpl.logicalPlan)))
  }

  /** @inheritdoc */
  def cogroupSorted[U1, U2, R: Encoder](
      other1: sql.KeyValueGroupedDataset[K, U1],
      other2: sql.KeyValueGroupedDataset[K, U2])(
      thisSortExprs: Column*)(
      other1SortExprs: Column*)(
      other2SortExprs: Column*)(
      f: (K, Iterator[V], Iterator[U1], Iterator[U2]) => IterableOnce[R]): Dataset[R] = {
    val other1Impl = castToImpl(other1)
    val other2Impl = castToImpl(other2)
    implicit val u1Encoder: Encoder[U1] = other1Impl.vEncoderImpl
    implicit val u2Encoder: Encoder[U2] = other2Impl.vEncoderImpl
    Dataset[R](
      sparkSession,
      CoGroup(
        f,
        Seq(this.groupingAttributes, other1Impl.groupingAttributes, other2Impl.groupingAttributes),
        Seq(this.dataAttributes, other1Impl.dataAttributes, other2Impl.dataAttributes),
        Seq(
          MapGroups.sortOrder(thisSortExprs.map(_.expr)),
          MapGroups.sortOrder(other1SortExprs.map(_.expr)),
          MapGroups.sortOrder(other2SortExprs.map(_.expr))),
        Seq(this.logicalPlan, other1Impl.logicalPlan, other2Impl.logicalPlan)))
  }

  /** @inheritdoc */
  def cogroupSorted[U1, U2, U3, R: Encoder](
      other1: sql.KeyValueGroupedDataset[K, U1],
      other2: sql.KeyValueGroupedDataset[K, U2],
      other3: sql.KeyValueGroupedDataset[K, U3])(
      thisSortExprs: Column*)(
      other1SortExprs: Column*)(
      other2SortExprs: Column*)(
      other3SortExprs: Column*)(
      f: (
        K,
        Iterator[V],
        Iterator[U1],
        Iterator[U2],
        Iterator[U3]
      ) => IterableOnce[R]): Dataset[R] = {
    val other1Impl = castToImpl(other1)
    val other2Impl = castToImpl(other2)
    val other3Impl = castToImpl(other3)
    implicit val u1Encoder: Encoder[U1] = other1Impl.vEncoderImpl
    implicit val u2Encoder: Encoder[U2] = other2Impl.vEncoderImpl
    implicit val u3Encoder: Encoder[U3] = other3Impl.vEncoderImpl
    Dataset[R](
      sparkSession,
      CoGroup(
        f,
        Seq(
          this.groupingAttributes,
          other1Impl.groupingAttributes,
          other2Impl.groupingAttributes,
          other3Impl.groupingAttributes),
        Seq(
          this.dataAttributes,
          other1Impl.dataAttributes,
          other2Impl.dataAttributes,
          other3Impl.dataAttributes),
        Seq(
          MapGroups.sortOrder(thisSortExprs.map(_.expr)),
          MapGroups.sortOrder(other1SortExprs.map(_.expr)),
          MapGroups.sortOrder(other2SortExprs.map(_.expr)),
          MapGroups.sortOrder(other3SortExprs.map(_.expr))),
        Seq(
          this.logicalPlan,
          other1Impl.logicalPlan,
          other2Impl.logicalPlan,
          other3Impl.logicalPlan)))
  }

  /** @inheritdoc */
  def cogroupSorted[U1, U2, U3, U4, R: Encoder](
      other1: sql.KeyValueGroupedDataset[K, U1],
      other2: sql.KeyValueGroupedDataset[K, U2],
      other3: sql.KeyValueGroupedDataset[K, U3],
      other4: sql.KeyValueGroupedDataset[K, U4])(
      thisSortExprs: Column*)(
      other1SortExprs: Column*)(
      other2SortExprs: Column*)(
      other3SortExprs: Column*)(
      other4SortExprs: Column*)(
      f: (
        K,
        Iterator[V],
        Iterator[U1],
        Iterator[U2],
        Iterator[U3],
        Iterator[U4]
      ) => IterableOnce[R]): Dataset[R] = {
    val other1Impl = castToImpl(other1)
    val other2Impl = castToImpl(other2)
    val other3Impl = castToImpl(other3)
    val other4Impl = castToImpl(other4)
    implicit val u1Encoder: Encoder[U1] = other1Impl.vEncoderImpl
    implicit val u2Encoder: Encoder[U2] = other2Impl.vEncoderImpl
    implicit val u3Encoder: Encoder[U3] = other3Impl.vEncoderImpl
    implicit val u4Encoder: Encoder[U4] = other4Impl.vEncoderImpl
    Dataset[R](
      sparkSession,
      CoGroup(
        f,
        Seq(
          this.groupingAttributes,
          other1Impl.groupingAttributes,
          other2Impl.groupingAttributes,
          other3Impl.groupingAttributes,
          other4Impl.groupingAttributes),
        Seq(
          this.dataAttributes,
          other1Impl.dataAttributes,
          other2Impl.dataAttributes,
          other3Impl.dataAttributes,
          other4Impl.dataAttributes),
        Seq(
          MapGroups.sortOrder(thisSortExprs.map(_.expr)),
          MapGroups.sortOrder(other1SortExprs.map(_.expr)),
          MapGroups.sortOrder(other2SortExprs.map(_.expr)),
          MapGroups.sortOrder(other3SortExprs.map(_.expr)),
          MapGroups.sortOrder(other4SortExprs.map(_.expr))),
        Seq(
          this.logicalPlan,
          other1Impl.logicalPlan,
          other2Impl.logicalPlan,
          other3Impl.logicalPlan,
          other4Impl.logicalPlan)))
  }

  // scalastyle:off argcount
  /** @inheritdoc */
  def cogroupSorted[U1, U2, U3, U4, U5, R: Encoder](
      other1: sql.KeyValueGroupedDataset[K, U1],
      other2: sql.KeyValueGroupedDataset[K, U2],
      other3: sql.KeyValueGroupedDataset[K, U3],
      other4: sql.KeyValueGroupedDataset[K, U4],
      other5: sql.KeyValueGroupedDataset[K, U5])(
      thisSortExprs: Column*)(
      other1SortExprs: Column*)(
      other2SortExprs: Column*)(
      other3SortExprs: Column*)(
      other4SortExprs: Column*)(
      other5SortExprs: Column*)(
      f: (
        K,
        Iterator[V],
        Iterator[U1],
        Iterator[U2],
        Iterator[U3],
        Iterator[U4],
        Iterator[U5]
      ) => IterableOnce[R]): Dataset[R] = {
    val other1Impl = castToImpl(other1)
    val other2Impl = castToImpl(other2)
    val other3Impl = castToImpl(other3)
    val other4Impl = castToImpl(other4)
    val other5Impl = castToImpl(other5)
    implicit val u1Encoder: Encoder[U1] = other1Impl.vEncoderImpl
    implicit val u2Encoder: Encoder[U2] = other2Impl.vEncoderImpl
    implicit val u3Encoder: Encoder[U3] = other3Impl.vEncoderImpl
    implicit val u4Encoder: Encoder[U4] = other4Impl.vEncoderImpl
    implicit val u5Encoder: Encoder[U5] = other5Impl.vEncoderImpl
    Dataset[R](
      sparkSession,
      CoGroup(
        f,
        Seq(
          this.groupingAttributes,
          other1Impl.groupingAttributes,
          other2Impl.groupingAttributes,
          other3Impl.groupingAttributes,
          other4Impl.groupingAttributes,
          other5Impl.groupingAttributes),
        Seq(
          this.dataAttributes,
          other1Impl.dataAttributes,
          other2Impl.dataAttributes,
          other3Impl.dataAttributes,
          other4Impl.dataAttributes,
          other5Impl.dataAttributes),
        Seq(
          MapGroups.sortOrder(thisSortExprs.map(_.expr)),
          MapGroups.sortOrder(other1SortExprs.map(_.expr)),
          MapGroups.sortOrder(other2SortExprs.map(_.expr)),
          MapGroups.sortOrder(other3SortExprs.map(_.expr)),
          MapGroups.sortOrder(other4SortExprs.map(_.expr)),
          MapGroups.sortOrder(other5SortExprs.map(_.expr))),
        Seq(
          this.logicalPlan,
          other1Impl.logicalPlan,
          other2Impl.logicalPlan,
          other3Impl.logicalPlan,
          other4Impl.logicalPlan,
          other5Impl.logicalPlan)))
  }

  /** @inheritdoc */
  def cogroupSorted[U1, U2, U3, U4, U5, U6, R: Encoder](
      other1: sql.KeyValueGroupedDataset[K, U1],
      other2: sql.KeyValueGroupedDataset[K, U2],
      other3: sql.KeyValueGroupedDataset[K, U3],
      other4: sql.KeyValueGroupedDataset[K, U4],
      other5: sql.KeyValueGroupedDataset[K, U5],
      other6: sql.KeyValueGroupedDataset[K, U6])(
      thisSortExprs: Column*)(
      other1SortExprs: Column*)(
      other2SortExprs: Column*)(
      other3SortExprs: Column*)(
      other4SortExprs: Column*)(
      other5SortExprs: Column*)(
      other6SortExprs: Column*)(
      f: (
        K,
        Iterator[V],
        Iterator[U1],
        Iterator[U2],
        Iterator[U3],
        Iterator[U4],
        Iterator[U5],
        Iterator[U6]
      ) => IterableOnce[R]): Dataset[R] = {
    val other1Impl = castToImpl(other1)
    val other2Impl = castToImpl(other2)
    val other3Impl = castToImpl(other3)
    val other4Impl = castToImpl(other4)
    val other5Impl = castToImpl(other5)
    val other6Impl = castToImpl(other6)
    implicit val u1Encoder: Encoder[U1] = other1Impl.vEncoderImpl
    implicit val u2Encoder: Encoder[U2] = other2Impl.vEncoderImpl
    implicit val u3Encoder: Encoder[U3] = other3Impl.vEncoderImpl
    implicit val u4Encoder: Encoder[U4] = other4Impl.vEncoderImpl
    implicit val u5Encoder: Encoder[U5] = other5Impl.vEncoderImpl
    implicit val u6Encoder: Encoder[U6] = other6Impl.vEncoderImpl
    Dataset[R](
      sparkSession,
      CoGroup(
        f,
        Seq(
          this.groupingAttributes,
          other1Impl.groupingAttributes,
          other2Impl.groupingAttributes,
          other3Impl.groupingAttributes,
          other4Impl.groupingAttributes,
          other5Impl.groupingAttributes,
          other6Impl.groupingAttributes),
        Seq(
          this.dataAttributes,
          other1Impl.dataAttributes,
          other2Impl.dataAttributes,
          other3Impl.dataAttributes,
          other4Impl.dataAttributes,
          other5Impl.dataAttributes,
          other6Impl.dataAttributes),
        Seq(
          MapGroups.sortOrder(thisSortExprs.map(_.expr)),
          MapGroups.sortOrder(other1SortExprs.map(_.expr)),
          MapGroups.sortOrder(other2SortExprs.map(_.expr)),
          MapGroups.sortOrder(other3SortExprs.map(_.expr)),
          MapGroups.sortOrder(other4SortExprs.map(_.expr)),
          MapGroups.sortOrder(other5SortExprs.map(_.expr)),
          MapGroups.sortOrder(other6SortExprs.map(_.expr))),
        Seq(
          this.logicalPlan,
          other1Impl.logicalPlan,
          other2Impl.logicalPlan,
          other3Impl.logicalPlan,
          other4Impl.logicalPlan,
          other5Impl.logicalPlan,
          other6Impl.logicalPlan)))
  }
  // scalastyle:on argcount

  override def toString: String = {
    val builder = new StringBuilder
    val kFields = kEncoder.schema.map { f =>
      s"${f.name}: ${f.dataType.simpleString(2)}"
    }
    val vFields = vEncoder.schema.map { f =>
      s"${f.name}: ${f.dataType.simpleString(2)}"
    }
    builder.append("KeyValueGroupedDataset: [key: [")
    builder.append(kFields.take(2).mkString(", "))
    if (kFields.length > 2) {
      builder.append(" ... " + (kFields.length - 2) + " more field(s)")
    }
    builder.append("], value: [")
    builder.append(vFields.take(2).mkString(", "))
    if (vFields.length > 2) {
      builder.append(" ... " + (vFields.length - 2) + " more field(s)")
    }
    builder.append("]]").toString()
  }

  ////////////////////////////////////////////////////////////////////////////
  // Return type overrides to make sure we return the implementation instead
  // of the interface.
  ////////////////////////////////////////////////////////////////////////////
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
      initialState: sql.KeyValueGroupedDataset[K, S]): Dataset[U] =
    super.flatMapGroupsWithState(
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
      initialState: sql.KeyValueGroupedDataset[K, S],
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
      initialState: sql.KeyValueGroupedDataset[K, S],
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
  override def cogroup[U, R: Encoder](
      other: sql.KeyValueGroupedDataset[K, U])(
      f: (K, Iterator[V], Iterator[U]) => IterableOnce[R]): Dataset[R] =
    super.cogroup(other)(f)

  /** @inheritdoc */
  override def cogroup[U, R](
      other: sql.KeyValueGroupedDataset[K, U],
      f: CoGroupFunction[K, V, U, R],
      encoder: Encoder[R]): Dataset[R] =
    super.cogroup(other, f, encoder)

  /** @inheritdoc */
  override def cogroup[U1, U2, R: Encoder](
      other1: sql.KeyValueGroupedDataset[K, U1],
      other2: sql.KeyValueGroupedDataset[K, U2])(
      f: (K, Iterator[V], Iterator[U1], Iterator[U2]) => IterableOnce[R]): Dataset[R] =
    super.cogroup(other1, other2)(f)

  /** @inheritdoc */
  override def cogroup[U1, U2, R](
      other1: sql.KeyValueGroupedDataset[K, U1],
      other2: sql.KeyValueGroupedDataset[K, U2],
      f: CoGroupFunction3[K, V, U1, U2, R],
      encoder: Encoder[R]): Dataset[R] =
    super.cogroup(other1, other2, f, encoder)

  /** @inheritdoc */
  override def cogroup[U1, U2, U3, R: Encoder](
      other1: sql.KeyValueGroupedDataset[K, U1],
      other2: sql.KeyValueGroupedDataset[K, U2],
      other3: sql.KeyValueGroupedDataset[K, U3])(
      f: (K, Iterator[V], Iterator[U1], Iterator[U2], Iterator[U3]) => IterableOnce[R])
      : Dataset[R] =
    super.cogroup(other1, other2, other3)(f)

  /** @inheritdoc */
  override def cogroup[U1, U2, U3, R](
      other1: sql.KeyValueGroupedDataset[K, U1],
      other2: sql.KeyValueGroupedDataset[K, U2],
      other3: sql.KeyValueGroupedDataset[K, U3],
      f: CoGroupFunction4[K, V, U1, U2, U3, R],
      encoder: Encoder[R]): Dataset[R] =
    super.cogroup(other1, other2, other3, f, encoder)

  /** @inheritdoc */
  override def cogroup[U1, U2, U3, U4, R: Encoder](
      other1: sql.KeyValueGroupedDataset[K, U1],
      other2: sql.KeyValueGroupedDataset[K, U2],
      other3: sql.KeyValueGroupedDataset[K, U3],
      other4: sql.KeyValueGroupedDataset[K, U4])(
      f: (
        K, Iterator[V], Iterator[U1], Iterator[U2], Iterator[U3], Iterator[U4]) => IterableOnce[R])
      : Dataset[R] =
    super.cogroup(other1, other2, other3, other4)(f)

  /** @inheritdoc */
  override def cogroup[U1, U2, U3, U4, R](
      other1: sql.KeyValueGroupedDataset[K, U1],
      other2: sql.KeyValueGroupedDataset[K, U2],
      other3: sql.KeyValueGroupedDataset[K, U3],
      other4: sql.KeyValueGroupedDataset[K, U4],
      f: CoGroupFunction5[K, V, U1, U2, U3, U4, R],
      encoder: Encoder[R]): Dataset[R] =
    super.cogroup(other1, other2, other3, other4, f, encoder)

  /** @inheritdoc */
  override def cogroup[U1, U2, U3, U4, U5, R: Encoder](
      other1: sql.KeyValueGroupedDataset[K, U1],
      other2: sql.KeyValueGroupedDataset[K, U2],
      other3: sql.KeyValueGroupedDataset[K, U3],
      other4: sql.KeyValueGroupedDataset[K, U4],
      other5: sql.KeyValueGroupedDataset[K, U5])(
      f: (
        K,
        Iterator[V],
        Iterator[U1],
        Iterator[U2],
        Iterator[U3],
        Iterator[U4],
        Iterator[U5]
      ) => IterableOnce[R])
      : Dataset[R] =
    super.cogroup(other1, other2, other3, other4, other5)(f)

  /** @inheritdoc */
  override def cogroup[U1, U2, U3, U4, U5, R](
      other1: sql.KeyValueGroupedDataset[K, U1],
      other2: sql.KeyValueGroupedDataset[K, U2],
      other3: sql.KeyValueGroupedDataset[K, U3],
      other4: sql.KeyValueGroupedDataset[K, U4],
      other5: sql.KeyValueGroupedDataset[K, U5],
      f: CoGroupFunction6[K, V, U1, U2, U3, U4, U5, R],
      encoder: Encoder[R]): Dataset[R] =
    super.cogroup(other1, other2, other3, other4, other5, f, encoder)

  /** @inheritdoc */
  override def cogroup[U1, U2, U3, U4, U5, U6, R: Encoder](
      other1: sql.KeyValueGroupedDataset[K, U1],
      other2: sql.KeyValueGroupedDataset[K, U2],
      other3: sql.KeyValueGroupedDataset[K, U3],
      other4: sql.KeyValueGroupedDataset[K, U4],
      other5: sql.KeyValueGroupedDataset[K, U5],
      other6: sql.KeyValueGroupedDataset[K, U6])(
      f: (
        K,
        Iterator[V],
        Iterator[U1],
        Iterator[U2],
        Iterator[U3],
        Iterator[U4],
        Iterator[U5],
        Iterator[U6]
      ) => IterableOnce[R])
      : Dataset[R] =
    super.cogroup(other1, other2, other3, other4, other5, other6)(f)

  /** @inheritdoc */
  override def cogroup[U1, U2, U3, U4, U5, U6, R](
      other1: sql.KeyValueGroupedDataset[K, U1],
      other2: sql.KeyValueGroupedDataset[K, U2],
      other3: sql.KeyValueGroupedDataset[K, U3],
      other4: sql.KeyValueGroupedDataset[K, U4],
      other5: sql.KeyValueGroupedDataset[K, U5],
      other6: sql.KeyValueGroupedDataset[K, U6],
      f: CoGroupFunction7[K, V, U1, U2, U3, U4, U5, U6, R],
      encoder: Encoder[R]): Dataset[R] =
    super.cogroup(other1, other2, other3, other4, other5, other6, f, encoder)

  /** @inheritdoc */
  override def cogroupSorted[U, R](
      other: sql.KeyValueGroupedDataset[K, U],
      thisSortExprs: Array[Column],
      otherSortExprs: Array[Column],
      f: CoGroupFunction[K, V, U, R],
      encoder: Encoder[R]): Dataset[R] =
    super.cogroupSorted(other, thisSortExprs, otherSortExprs, f, encoder)

  /** @inheritdoc */
  override def cogroupSorted[U1, U2, R](
      other1: sql.KeyValueGroupedDataset[K, U1],
      other2: sql.KeyValueGroupedDataset[K, U2],
      thisSortExprs: Array[Column],
      other1SortExprs: Array[Column],
      other2SortExprs: Array[Column],
      f: CoGroupFunction3[K, V, U1, U2, R],
      encoder: Encoder[R]): Dataset[R] =
    super.cogroupSorted(other1, other2, thisSortExprs, other1SortExprs, other2SortExprs, f, encoder)

  /** @inheritdoc */
  override def cogroupSorted[U1, U2, U3, R](
      other1: sql.KeyValueGroupedDataset[K, U1],
      other2: sql.KeyValueGroupedDataset[K, U2],
      other3: sql.KeyValueGroupedDataset[K, U3],
      thisSortExprs: Array[Column],
      other1SortExprs: Array[Column],
      other2SortExprs: Array[Column],
      other3SortExprs: Array[Column],
      f: CoGroupFunction4[K, V, U1, U2, U3, R],
      encoder: Encoder[R]): Dataset[R] =
    super.cogroupSorted(
      other1,
      other2,
      other3,
      thisSortExprs,
      other1SortExprs,
      other2SortExprs,
      other3SortExprs,
      f,
      encoder)

  // scalastyle:off argcount
  /** @inheritdoc */
  override def cogroupSorted[U1, U2, U3, U4, R](
      other1: sql.KeyValueGroupedDataset[K, U1],
      other2: sql.KeyValueGroupedDataset[K, U2],
      other3: sql.KeyValueGroupedDataset[K, U3],
      other4: sql.KeyValueGroupedDataset[K, U4],
      thisSortExprs: Array[Column],
      other1SortExprs: Array[Column],
      other2SortExprs: Array[Column],
      other3SortExprs: Array[Column],
      other4SortExprs: Array[Column],
      f: CoGroupFunction5[K, V, U1, U2, U3, U4, R],
      encoder: Encoder[R]): Dataset[R] =
    super.cogroupSorted(
      other1,
      other2,
      other3,
      other4,
      thisSortExprs,
      other1SortExprs,
      other2SortExprs,
      other3SortExprs,
      other4SortExprs,
      f,
      encoder)

  /** @inheritdoc */
  override def cogroupSorted[U1, U2, U3, U4, U5, R](
      other1: sql.KeyValueGroupedDataset[K, U1],
      other2: sql.KeyValueGroupedDataset[K, U2],
      other3: sql.KeyValueGroupedDataset[K, U3],
      other4: sql.KeyValueGroupedDataset[K, U4],
      other5: sql.KeyValueGroupedDataset[K, U5],
      thisSortExprs: Array[Column],
      other1SortExprs: Array[Column],
      other2SortExprs: Array[Column],
      other3SortExprs: Array[Column],
      other4SortExprs: Array[Column],
      other5SortExprs: Array[Column],
      f: CoGroupFunction6[K, V, U1, U2, U3, U4, U5, R],
      encoder: Encoder[R]): Dataset[R] =
    super.cogroupSorted(
      other1,
      other2,
      other3,
      other4,
      other5,
      thisSortExprs,
      other1SortExprs,
      other2SortExprs,
      other3SortExprs,
      other4SortExprs,
      other5SortExprs,
      f,
      encoder)

  /** @inheritdoc */
  override def cogroupSorted[U1, U2, U3, U4, U5, U6, R](
      other1: sql.KeyValueGroupedDataset[K, U1],
      other2: sql.KeyValueGroupedDataset[K, U2],
      other3: sql.KeyValueGroupedDataset[K, U3],
      other4: sql.KeyValueGroupedDataset[K, U4],
      other5: sql.KeyValueGroupedDataset[K, U5],
      other6: sql.KeyValueGroupedDataset[K, U6],
      thisSortExprs: Array[Column],
      other1SortExprs: Array[Column],
      other2SortExprs: Array[Column],
      other3SortExprs: Array[Column],
      other4SortExprs: Array[Column],
      other5SortExprs: Array[Column],
      other6SortExprs: Array[Column],
      f: CoGroupFunction7[K, V, U1, U2, U3, U4, U5, U6, R],
      encoder: Encoder[R]): Dataset[R] =
    super.cogroupSorted(
      other1,
      other2,
      other3,
      other4,
      other5,
      other6,
      thisSortExprs,
      other1SortExprs,
      other2SortExprs,
      other3SortExprs,
      other4SortExprs,
      other5SortExprs,
      other6SortExprs,
      f,
      encoder)
  // scalastyle:on argcount
}
